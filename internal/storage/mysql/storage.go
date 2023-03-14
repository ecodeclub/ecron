package mysql

import (
	"context"
	"errors"
	"log"
	"math/rand"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/gotomicro/ecron/internal/errs"
	"github.com/gotomicro/ecron/internal/storage"
	"github.com/gotomicro/ecron/internal/task"
	"github.com/gotomicro/ekit/bean/option"
	"github.com/gotomicro/eorm"
)

type Storage struct {
	db                *eorm.DB
	preemptInterval   time.Duration         // 发起任务抢占时间间隔
	refreshInterval   time.Duration         // 发起任务抢占续约时间间隔
	preemptionTimeout time.Duration         // 抢占任务超时时间
	refreshRetry      storage.RetryStrategy // 续约重试策略
	payLoad           int64                 // 当前storage节点的负载
	storageId         int64                 // 唯一标识一个storage
	events            chan storage.Event
}

func NewMysqlStorage(dsn string, opt ...option.Option[Storage]) (*Storage, error) {
	//db, err := eorm.Open("mysql", dsn, eorm.DBWithMiddlewares(querylog.NewBuilder().Build()))
	db, err := eorm.Open("mysql", dsn)
	if err != nil {
		return nil, errs.NewCreateStorageError(err)
	}

	s := &Storage{
		events:            make(chan storage.Event),
		db:                db,
		preemptInterval:   2 * time.Second,
		refreshInterval:   3 * time.Second,
		preemptionTimeout: time.Minute,
		refreshRetry: &storage.RefreshIntervalRetry{
			Interval: time.Second,
			Max:      3,
		},
	}
	option.Apply[Storage](s, opt...)

	// db创建一条该storage记录
	sId, err := eorm.NewInserter[StorageInfo](db).Columns("Payload").Values(&StorageInfo{Payload: 0}).
		Exec(context.TODO()).LastInsertId()
	if err != nil {
		return nil, err
	}

	s.storageId = sId
	return s, nil
}

func WithPreemptInterval(t time.Duration) option.Option[Storage] {
	return func(s *Storage) {
		s.preemptInterval = t
	}
}

func WithRefreshRetry(r storage.RetryStrategy) option.Option[Storage] {
	return func(s *Storage) {
		s.refreshRetry = r
	}
}

func WithRefreshInterval(t time.Duration) option.Option[Storage] {
	return func(s *Storage) {
		s.refreshInterval = t
	}
}

func WithPreemptTimeout(t time.Duration) option.Option[Storage] {
	return func(s *Storage) {
		s.preemptionTimeout = t
	}
}

func (s *Storage) Get(ctx context.Context, taskId int64) (*task.Task, error) {
	ts, err := eorm.NewSelector[TaskInfo](s.db).From(eorm.TableOf(&TaskInfo{}, "t1")).
		Where(eorm.C("Id").EQ(taskId)).
		Get(ctx)
	if err == eorm.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &task.Task{
		Config: task.Config{
			Name:       ts.Name,
			Cron:       ts.Cron,
			Type:       task.Type(ts.Type),
			Parameters: ts.Config,
		},
		TaskId: ts.Id,
	}, nil
}

// Add 创建task，设置调度状态是created
func (s *Storage) Add(ctx context.Context, t *task.Task) (int64, error) {
	id, err := eorm.NewInserter[TaskInfo](s.db).Values(&TaskInfo{
		Name:            t.Name,
		Cron:            t.Cron,
		SchedulerStatus: storage.EventTypeCreated,
		Type:            string(t.Type),
		Config:          t.Parameters,
		CreateTime:      time.Now().UnixMilli(),
		UpdateTime:      time.Now().UnixMilli(),
	}).Exec(ctx).LastInsertId()
	if err != nil {
		return -1, errs.NewAddTaskError(err)
	}
	return id, nil
}

// AddExecution 创建一条执行记录
func (s *Storage) AddExecution(ctx context.Context, taskId int64) (int64, error) {
	id, err := eorm.NewInserter[TaskExecution](s.db).Values(&TaskExecution{
		ExecuteStatus: task.EventTypeInit,
		TaskId:        taskId,
		CreateTime:    time.Now().UnixMilli(),
		UpdateTime:    time.Now().UnixMilli(),
	}).Exec(ctx).LastInsertId()
	if err != nil {
		return -1, errs.NewAddTaskError(err)
	}
	return id, nil
}

func (s *Storage) CompareAndUpdateTaskStatus(ctx context.Context, taskId int64, old, new string) error {
	cond := eorm.C("Id").EQ(taskId).And(eorm.C("SchedulerStatus").EQ(old))
	ra, err := eorm.NewUpdater[TaskInfo](s.db).Update(&TaskInfo{
		SchedulerStatus: new,
		UpdateTime:      time.Now().UnixMilli(),
	}).Set(eorm.Columns("SchedulerStatus", "UpdateTime")).Where(cond).Exec(ctx).RowsAffected()
	if err != nil {
		return errs.NewCompareAndUpdateDbError(err)
	}
	if ra == 0 {
		return errs.NewCompareAndUpdateAffectZeroError()
	}
	return nil
}

func (s *Storage) CompareAndUpdateTaskExecutionStatus(ctx context.Context, taskId int64, old, new string) error {
	cond := eorm.C("TaskId").EQ(taskId).And(eorm.C("ExecuteStatus").EQ(old))
	ra, err := eorm.NewUpdater[TaskExecution](s.db).Update(&TaskExecution{
		ExecuteStatus: new,
		UpdateTime:    time.Now().UnixMilli(),
	}).Set(eorm.Columns("ExecuteStatus", "UpdateTime")).Where(cond).Exec(ctx).RowsAffected()
	if err != nil {
		return errs.NewCompareAndUpdateDbError(err)
	}
	if ra == 0 {
		return errs.NewCompareAndUpdateAffectZeroError()
	}
	return nil
}

func (s *Storage) Update(ctx context.Context, t *task.Task) error {
	var setCols []eorm.Assignable
	if t.Name != "" {
		setCols = append(setCols, eorm.C("Name"))
	}
	if t.Cron != "" {
		setCols = append(setCols, eorm.C("Cron"))
	}
	if t.Type != "" {
		setCols = append(setCols, eorm.C("Type"))
	}
	if t.Parameters != "" {
		setCols = append(setCols, eorm.C("Config"))
	}
	if len(setCols) == 0 {
		return errors.New("ecron: Update操作执行时没有变化的字段")
	}
	setCols = append(setCols, eorm.C("UpdateTime"))
	updateTask := &TaskInfo{
		Name:       t.Name,
		Cron:       t.Cron,
		Type:       string(t.Type),
		Config:     t.Parameters,
		UpdateTime: time.Now().UnixMilli(),
	}
	return eorm.NewUpdater[TaskInfo](s.db).
		Update(updateTask).Set(setCols...).Where(eorm.C("Id").EQ(t.TaskId)).Exec(ctx).Err()
}

func (s *Storage) Delete(ctx context.Context, taskId int64) error {
	// TODO 处于某些状态的task不能被删除
	return eorm.NewDeleter[TaskInfo](s.db).From(&TaskInfo{}).Where(eorm.C("Id").EQ(taskId)).Exec(ctx).Err()
}

// RunPreempt 每隔固定时间去db中抢占任务
func (s *Storage) RunPreempt(ctx context.Context) {
	// 抢占任务间隔
	tickerP := time.NewTicker(s.preemptInterval)
	for {
		select {
		case <-tickerP.C:
			log.Println("storage begin preempt task")
			s.preempted(ctx)
		}
	}
}

// 抢占两种类型的任务：
// 1. 长时间没有续约的已经抢占的任务
// 2. 处于创建状态的任务
// 3. 占有者主动放弃(续约时会检查是否需要放弃)，且候选者是当前storage
func (s *Storage) preempted(ctx context.Context) {
	tCtx, cancel := context.WithTimeout(ctx, s.preemptionTimeout)
	defer func() {
		cancel()
	}()

	maxRefreshInterval := s.refreshRetry.GetMaxRetry() * s.refreshInterval.Milliseconds()

	// 1. 长时间没有续约的已经抢占的任务
	cond1 := eorm.C("SchedulerStatus").EQ(storage.EventTypePreempted).
		And(eorm.C("UpdateTime").LTEQ(time.Now().UnixMilli() - maxRefreshInterval))
	// 2. 处于创建状态
	cond2 := eorm.C("SchedulerStatus").EQ(storage.EventTypeCreated)
	// 3. 占有者主动放弃(续约时会检查是否需要放弃)，且候选者是当前storage
	cond3 := eorm.C("SchedulerStatus").EQ(storage.EventTypeDiscarded).And(eorm.C("CandidateId").EQ(s.storageId))

	tasks, err := eorm.NewSelector[TaskInfo](s.db).From(eorm.TableOf(&TaskInfo{}, "t1")).
		Where(cond1.Or(cond2).Or(cond3)).
		GetMulti(tCtx)
	if err != nil {
		log.Println("ecron: 获取待抢占任务失败", err)
		return
	}

	for _, item := range tasks {
		preemptedEvent := storage.Event{
			Type: storage.EventTypePreempted,
			Task: &task.Task{
				Config: task.Config{Name: item.Name, Cron: item.Cron, Type: task.Type(item.Type), Parameters: item.Config},
				TaskId: item.Id,
			},
		}

		err = s.CompareAndUpdateTaskStatus(tCtx, item.Id, item.SchedulerStatus, storage.EventTypePreempted)
		if err != nil {
			log.Println("ecron: 抢占CAS操作错误，", err)
			continue
		}

		s.payLoad += 1
		// 这里需要更新已经抢占该任务的占有storage负载
		err = s.updatePayload(ctx, s.payLoad, true)
		if err != nil {
			log.Println("ecron: 更新占有者负载时出现错误: ", err)
			continue
		}

		select {
		case <-tCtx.Done():
			log.Printf("ecron: 抢占任务ID: %d 超时退出", item.Id)
		case s.events <- preemptedEvent:
		}
	}
}

// 定时检查task的候选者是否需要更新当前storage
func (s *Storage) lookup(ctx context.Context) {
	var updateTaskIds []any
	tasks, err := eorm.NewSelector[TaskInfo](s.db).From(eorm.TableOf(&TaskInfo{}, "t1")).
		Where(eorm.C("SchedulerStatus").EQ(storage.EventTypePreempted).
				And(eorm.C("OccupierId").NEQ(s.storageId))).
		Limit(20). // 这里限制下更新的任务数，防止任务数过多导致的性能问题
		GetMulti(ctx)
	if err != nil {
		log.Printf("ecron: 更新候选者负载时，获取待更新任务时出错: %s", err)
	}

	storageIds := make([]any, 0, len(tasks))
	for _, t := range tasks {
		storageIds = append(storageIds, t.Id)
	}
	payloads, err := s.getPayload(ctx, storageIds)
	if err != nil {
		log.Println("ecron: ")
		return
	}

	for _, t := range tasks {
		occupierPayload, ok := payloads[t.OccupierId]
		if !ok {
			log.Println("ecron: 找不到storage id：", t.OccupierId)
			continue
		}
		candidatePayload, ok := payloads[t.CandidateId]
		if !ok {
			log.Println("ecron: 找不到storage id：", t.OccupierId)
			continue
		}
		// 获取要更新候选者为当前storage的task
		// 1. 如果task无候选者：
		//	- 比较task的占有者的负载是不是比当前storage的负载大，是的话就加入当前storage id到task的候选者中
		// 2. 如果task有候选者：
		// - 和候选者比较当前storage的负载，谁小就更新成谁。保险起见也一起比较下占有者负载，候选者不管怎样都得小于占有者，否则候选者置空
		if t.CandidateId == 0 && occupierPayload > s.payLoad {
			updateTaskIds = append(updateTaskIds, t.Id)
		}
		if t.CandidateId != 0 && candidatePayload > s.payLoad && occupierPayload > candidatePayload {
			updateTaskIds = append(updateTaskIds, t.Id)
		}
	}
	// 写入候选者为当前storage到task
	err = s.updateCandidate(ctx, updateTaskIds)
	if err != nil {
		log.Println("ecron: 更新候选者负载出错: ", err)
	}
}

func (s *Storage) getPayload(ctx context.Context, storageIds []any) (map[int64]int64, error) {
	payloads := make(map[int64]int64, 16)
	info, err := eorm.NewSelector[StorageInfo](s.db).Select(eorm.C("Id"), eorm.C("Payload")).
		Where(eorm.C("Id").In(storageIds...)).GetMulti(ctx)

	if err != nil {
		return nil, err
	}

	for _, item := range info {
		if _, ok := payloads[item.Id]; !ok {
			payloads[item.Id] = item.Payload
		}
	}
	return payloads, nil
}

func (s *Storage) updateCandidate(ctx context.Context, taskIds []any) error {
	ra, err := eorm.NewUpdater[TaskInfo](s.db).Update(&TaskInfo{CandidateId: s.storageId}).
		Set(eorm.C("CandidateId")).Where(eorm.C("Id").In(taskIds...)).Exec(ctx).RowsAffected()
	if err != nil {
		return err
	}
	log.Printf("ecron: 有%d个任务更新候选者为：%d", ra, s.storageId)
	return nil
}

func (s *Storage) updatePayload(ctx context.Context, payload int64, setCandidateNull bool) error {
	err := eorm.NewUpdater[StorageInfo](s.db).Update(&StorageInfo{Payload: payload}).Set(eorm.C("PayLoad")).
		Where(eorm.C("StorageId").EQ(s.storageId)).Exec(ctx).Err()
	if err != nil {
		return err
	}
	if setCandidateNull {
		err := eorm.NewUpdater[TaskInfo](s.db).Update(&TaskInfo{CandidateId: 0}).Set(eorm.C("CandidateId")).Exec(ctx).Err()
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Storage) Events(ctx context.Context, taskEvents <-chan task.Event) (<-chan storage.Event, error) {
	go func() {
		for {
			select {
			case <-ctx.Done():
			case event := <-taskEvents:
				switch event.Type {
				case task.EventTypeRunning:
					log.Println("storage 收到 task执行中信号")
				case task.EventTypeSuccess:
					err := s.CompareAndUpdateTaskStatus(ctx, event.TaskId, storage.EventTypePreempted,
						storage.EventTypeEnd)
					if err != nil {
						log.Println("storage: ", err)
					}
					log.Println("storage 收到 task执行成功信号")
				case task.EventTypeFailed:
					err := s.CompareAndUpdateTaskStatus(ctx, event.TaskId, storage.EventTypePreempted,
						storage.EventTypeEnd)
					if err != nil {
						log.Println("storage: ", err)
					}
					log.Println("storage 收到 task执行失败的信号")
				}
			}
		}
	}()
	return s.events, nil
}

// Refresh 获取所有已经抢占的任务，并更新时间和epoch
// 目前epoch仅作为续约持续时间的评估
func (s *Storage) refresh(ctx context.Context, taskId, epoch, candidateId int64) {
	var (
		timer    *time.Timer
		sc       []eorm.Assignable
		newEpoch int64 = epoch
	)
	sc = append(sc, eorm.Assign("Epoch", eorm.C("Epoch").Add(1)), eorm.C("UpdateTime"))
	for {
		newEpoch++
		log.Printf("taskId: %d storage begin refresh preempted task", taskId)
		// 根据候选者负载决定是否需要发起该任务，如果决定放弃就修改该任务状态为discard
		if s.isNeedDiscard(ctx, candidateId) {
			sc = append(sc, eorm.C("SchedulerStatus"))
		}
		rowsAffect, err := eorm.NewUpdater[TaskInfo](s.db).
			Update(&TaskInfo{
				Epoch:           newEpoch,
				SchedulerStatus: storage.EventTypeDiscarded,
				UpdateTime:      time.Now().UnixMilli(),
			}).Set(sc...).Where(eorm.C("Id").EQ(taskId).
			And(eorm.C("SchedulerStatus").EQ(storage.EventTypePreempted)).
			And(eorm.C("Epoch").EQ(epoch))).Exec(ctx).RowsAffected()

		// 1. 续约成功，退出该任务的续约，等待下一次到时间续约
		// 2. 获取的任务抢占状态改变，即已放弃了任务，终止这次续约，等待下一次到时间续约
		if err == nil {
			log.Printf("taskId: %d refresh success, 第%d次", taskId, epoch)
			return
		}
		if rowsAffect == 0 {
			log.Printf("taskId: %d refresh stop, 第%d次", taskId, epoch)
			return
		}

		// 这里意味续约失败，进行重试
		interval, ok := s.refreshRetry.Next()
		if !ok {
			log.Printf("taskId: %d refresh preempted fail, %s", taskId, err)
			return
		}

		if timer == nil {
			timer = time.NewTimer(interval)
		} else {
			timer.Reset(interval)
		}
		select {
		case <-timer.C:
			log.Printf("ecron: 续约开始第%d次重试", s.refreshRetry.GetCntRetry())
		case <-ctx.Done():
			log.Printf("ecron: 续约终止，%s", ctx.Err())
			return
		}
	}
}

func (s *Storage) AutoRefresh(ctx context.Context) {
	// 续约任务间隔
	timer := time.NewTicker(s.refreshInterval)
	for {
		select {
		case <-timer.C:
			tasks, _ := eorm.NewSelector[TaskInfo](s.db).
				From(eorm.TableOf(&TaskInfo{}, "t1")).
				Where(eorm.C("SchedulerStatus").EQ(storage.EventTypePreempted)).
				GetMulti(ctx)
			for _, t := range tasks {
				go s.refresh(ctx, t.Id, t.Epoch, t.CandidateId)
			}
		}
	}
}

func (s *Storage) isNeedDiscard(ctx context.Context, candidateId int64) bool {
	sInfo, err := eorm.NewSelector[StorageInfo](s.db).Select(eorm.C("Payload")).Where(eorm.C("Id").EQ(candidateId)).Get(ctx)
	if err != nil {
		return true
	}
	rand.Seed(time.Now().UnixNano())
	tolerance := rand.Int63n(6)
	// 避免抢占抖动，随机一个范围[s.payLoad-3, s.payLoad+3]的范围
	return sInfo.Payload < s.payLoad+tolerance-3
}

// Stop storage的关闭, 这里终止所有正在执行的任务
func (s *Storage) Stop(ctx context.Context) error {
	return eorm.NewUpdater[StorageInfo](s.db).Update(&StorageInfo{
		Status: storage.Stop,
	}).Set(eorm.Columns("SchedulerStatus", "UpdateTime")).Where(eorm.C("Id").EQ(s.storageId)).Exec(ctx).Err()
}
