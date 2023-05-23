package mysql

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/ecodeclub/ecron/internal/errs"
	"github.com/ecodeclub/ecron/internal/storage"
	"github.com/ecodeclub/ecron/internal/task"
	_ "github.com/go-sql-driver/mysql"
	"github.com/ecodeclub/ekit/bean/option"
	"github.com/ecodeclub/eorm"
)

type Storage struct {
	db                *eorm.DB
	preemptInterval   time.Duration         // 发起任务抢占时间间隔
	refreshInterval   time.Duration         // 发起任务抢占续约时间间隔
	preemptionTimeout time.Duration         // 抢占任务超时时间
	refreshRetry      storage.RetryStrategy // 续约重试策略
	occupierPayload   int64                 // 当前storage节点的占有者负载
	candidatePayload  int64                 // 当前storage节点的候选者负载
	storageId         int64                 // 唯一标识一个storage
	events            chan storage.Event
	stop              chan struct{}
	once              *sync.Once
	n                 int64 // 一次最多更新的候选者个数
}

func NewMysqlStorage(dsn string, opt ...option.Option[Storage]) (*Storage, error) {
	// db, err := eorm.Open("mysql", dsn, eorm.DBWithMiddlewares(querylog.NewBuilder().Build()))
	db, err := eorm.Open("mysql", dsn)
	if err != nil {
		return nil, errs.NewCreateStorageError(err)
	}
	return newMysqlStorage(db, opt...)
}

func newMysqlStorage(db *eorm.DB, opt ...option.Option[Storage]) (*Storage, error) {

	s := &Storage{
		events:            make(chan storage.Event),
		db:                db,
		preemptInterval:   2 * time.Second,
		refreshInterval:   5 * time.Second,
		preemptionTimeout: time.Minute,
		refreshRetry: &storage.RefreshIntervalRetry{
			Interval: time.Second,
			Max:      3,
		},
		stop: make(chan struct{}),
		once: &sync.Once{},
		n:    3,
	}
	option.Apply[Storage](s, opt...)

	// db创建一条该storage记录
	sId, err := eorm.NewInserter[StorageInfo](db).Columns("OccupierPayload", "CandidatePayload").Values(&StorageInfo{OccupierPayload: 0, CandidatePayload: 0}).
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
		CreateTime:      time.Now().Unix(),
		UpdateTime:      time.Now().Unix(),
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
		CreateTime:    time.Now().Unix(),
		UpdateTime:    time.Now().Unix(),
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
		OccupierId:      s.storageId,
		UpdateTime:      time.Now().Unix(),
	}).Set(eorm.Columns("SchedulerStatus", "UpdateTime", "OccupierId")).Where(cond).Exec(ctx).RowsAffected()
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
		UpdateTime:    time.Now().Unix(),
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
		UpdateTime: time.Now().Unix(),
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
			log.Printf("ecron: storage[%d] 开始进行任务抢占", s.storageId)
			go s.preempted(ctx)
		case <-s.stop:
			log.Printf("ecron: storage[%d] 停止任务抢占", s.storageId)
			return
		default:
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

	maxRefreshInterval := s.refreshRetry.GetMaxRetry() * int64(s.refreshInterval.Seconds())

	// 1. 长时间没有续约的已经抢占的任务
	cond1 := eorm.C("SchedulerStatus").EQ(storage.EventTypePreempted).
		And(eorm.C("UpdateTime").LTEQ(time.Now().Unix() - maxRefreshInterval))
	// 2. 处于创建状态
	cond2 := eorm.C("SchedulerStatus").EQ(storage.EventTypeCreated)
	// 3. 占有者主动放弃(续约时会检查是否需要放弃)，且候选者是当前storage
	cond3 := eorm.C("SchedulerStatus").EQ(storage.EventTypeDiscarded).And(eorm.C("CandidateId").EQ(s.storageId))

	tasks, err := eorm.NewSelector[TaskInfo](s.db).From(eorm.TableOf(&TaskInfo{}, "t1")).
		Where(cond1.Or(cond2).Or(cond3)).
		GetMulti(tCtx)
	if err != nil {
		log.Printf("ecron：[%d]本次抢占任务数：%s", s.storageId, err)
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

		// 更新前确保 task原来的占有者没有变化
		err = s.CompareAndUpdateTaskStatus(tCtx, item.Id, item.SchedulerStatus, storage.EventTypePreempted)
		if err != nil {
			log.Printf("ecron：[%d]抢占CAS操作错误：%s", s.storageId, err)
			continue
		}

		// 如果要删除候选者，需要更新db中候选者负载，并同步负载到候选者内存...
		// 考虑不同storage间的负载同步问题，主要是候选者负载同步问题，不在通过内存保存负载
		if item.CandidateId != 0 {
			err = s.delCandidate(ctx, item.CandidateId, item.Id)
			if err != nil {
				log.Printf("ecron: [%d]删除task[%d]候选者storage[%d]时出现错误: %v",
					s.storageId, item.Id, item.CandidateId, err)
				continue
			}
		}

		s.events <- preemptedEvent
	}
}

func (s *Storage) AutoLookup(ctx context.Context) {
	timer := time.NewTicker(10 * time.Second)
	for {
		select {
		case <-timer.C:
			log.Printf("ecron: storge[%d]开始候选者标记", s.storageId)
			s.lookup(ctx)
		case <-s.stop:
			log.Printf("ecron: storage[%d]关闭，停止所有task的负载均衡", s.storageId)
			return
		}
	}
}

// 定时检查task的候选者是否需要更新当前storage
func (s *Storage) lookup(ctx context.Context) {
	var updateTaskIds []any
	tasks, err := eorm.NewSelector[TaskInfo](s.db).From(eorm.TableOf(&TaskInfo{}, "t1")).
		Where(eorm.C("SchedulerStatus").EQ(storage.EventTypePreempted).
			And(eorm.C("OccupierId").NEQ(s.storageId))).GetMulti(ctx)
	if err != nil {
		log.Printf("ecron: [%d]更新候选者负载时，获取待更新任务时出错: %s", s.storageId, err)
	}

	var curTaskIndex int64 = 0
	for _, t := range tasks {
		curPayload := s.getPayload(ctx, s.storageId)

		occupierPayload := s.getPayload(ctx, t.OccupierId)
		// 获取要更新候选者为当前storage的task
		// 1. 如果task无候选者：
		//  -  比较task的占有者的负载(候选+占有)是不是比当前storage的负载大，
		//  -  同时还要保证大的数量是2*n，是的话就加入当前storage id到task的候选者中，并记录已经加入的个数，超过n就不在添加候选者
		// 2. 如果task有候选者()：
		//  - 比较方式同上，只不过比较的是候选者负载
		if t.CandidateId == 0 {
			if occupierPayload > curPayload+2*s.n && curTaskIndex < s.n {
				updateTaskIds = append(updateTaskIds, t.Id)
				curTaskIndex += 1
				err = s.updateCandidateCAS(ctx, t.Id, t.OccupierId, t.CandidateId)
			}
		} else {
			candidatePayload := s.getPayload(ctx, t.CandidateId)
			if candidatePayload > curPayload+2*s.n && curTaskIndex < s.n {
				updateTaskIds = append(updateTaskIds, t.Id)
				curTaskIndex += 1
				err = s.updateCandidateCAS(ctx, t.Id, t.OccupierId, t.CandidateId)
			}
		}
		if err != nil {
			log.Printf("ecron: task[%d]更新从旧候选者[%d]更新候选者为[%d]出错: [%s]", t.Id,
				t.CandidateId, s.storageId, err)
		}
	}
}

// 计算指定storage节点的负载
// 包括作为候选者的负载、作为占有者的负载
func (s *Storage) getPayload(ctx context.Context, storageId int64) int64 {
	// 1. 作为候选者的负载
	cond1 := eorm.C("CandidateId").EQ(storageId)
	// 2. 作为占有者的负载, 注意这里需要去掉的该storage作为占有者的任务中已经有候选者的任务数
	cond2 := eorm.C("OccupierId").EQ(storageId).And(eorm.C("CandidateId").EQ(0))
	info, err := eorm.NewSelector[any](s.db).Select(eorm.Count("Id")).From(eorm.TableOf(&TaskInfo{}, "t1")).
		Where(cond1.Or(cond2)).Get(ctx)
	if err != nil {
		return -1
	}
	return (*info).(int64)
}

func (s *Storage) updateCandidateCAS(ctx context.Context, taskId int64, occupierId int64, candidateId int64) error {
	casConds := eorm.C("CandidateId").EQ(candidateId).And(eorm.C("OccupierId").EQ(occupierId))
	err := eorm.NewUpdater[TaskInfo](s.db).Update(&TaskInfo{CandidateId: s.storageId}).
		Set(eorm.C("CandidateId")).Where(eorm.C("Id").EQ(taskId).And(casConds)).Exec(ctx).Err()
	if err != nil {
		return err
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
		timer *time.Timer
		sc    []eorm.Assignable
	)
	sc = append(sc, eorm.Assign("Epoch", eorm.C("Epoch").Add(1)), eorm.C("UpdateTime"))
	for {
		log.Printf("storage[%d] 开始续约task[%d]", s.storageId, taskId)
		// 根据候选者负载决定是否需要放弃该任务，如果决定放弃就修改该任务状态为discard
		needDiscard := s.isNeedDiscard(ctx, candidateId)
		if needDiscard {
			sc = append(sc, eorm.C("SchedulerStatus"))
		}
		rowsAffect, err := eorm.NewUpdater[TaskInfo](s.db).
			Update(&TaskInfo{
				SchedulerStatus: storage.EventTypeDiscarded,
				UpdateTime:      time.Now().Unix(),
			}).Set(sc...).Where(eorm.C("Id").EQ(taskId).
			And(eorm.C("SchedulerStatus").EQ(storage.EventTypePreempted)).
			And(eorm.C("OccupierId").EQ(s.storageId)).
			And(eorm.C("Epoch").EQ(epoch))).Exec(ctx).RowsAffected()

		// 1. 续约成功，退出该任务的续约，等待下一次到时间续约
		// 2. 获取的任务抢占状态改变，即已放弃了任务，终止这次续约，等待下一次到时间续约
		if err == nil {
			log.Printf("storage[%d]上task%d refresh success, 第%d次", s.storageId, taskId, epoch)
			return
		}
		if rowsAffect == 0 {
			log.Printf("storage[%d]上task%d refresh stop, 第%d次", s.storageId, taskId, epoch)
			return
		}

		// 这里意味续约失败，进行重试
		interval, ok := s.refreshRetry.Next()
		if !ok {
			log.Printf("storage[%d]上task%d refresh preempted fail, %s", s.storageId, taskId, err)
			return
		}

		if timer == nil {
			timer = time.NewTimer(interval)
		} else {
			timer.Reset(interval)
		}
		select {
		case <-timer.C:
			log.Printf("ecron: storage[%d]续约开始第%d次重试", s.storageId, s.refreshRetry.GetCntRetry())
		case <-ctx.Done():
			log.Printf("ecron: storage[%d]续约终止，%s", s.storageId, ctx.Err())
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
				Where(eorm.C("SchedulerStatus").EQ(storage.EventTypePreempted).
					And(eorm.C("OccupierId").EQ(s.storageId))).
				GetMulti(ctx)
			for _, t := range tasks {
				go s.refresh(ctx, t.Id, t.Epoch, t.CandidateId)
			}
		case <-s.stop:
			log.Printf("ecron: storage[%d]关闭，停止所有task的自动续约", s.storageId)
			return
		}
	}
}

// 续约的时候决定当前storage是否要放弃一个任务，需要将候选者负载、占有者负载一起比较
func (s *Storage) isNeedDiscard(ctx context.Context, candidateId int64) bool {
	return candidateId != 0
}

// 获取storage作为占有者的负载
func (s *Storage) getOccupierPayload(ctx context.Context, storageId int64) int64 {
	info, err := eorm.NewSelector[any](s.db).Select(eorm.Count("Id")).From(eorm.TableOf(&TaskInfo{}, "t1")).
		Where(eorm.C("OccupierId").EQ(storageId)).Get(ctx)
	if err != nil {
		return -1
	}
	return (*info).(int64)
}

// 获取storage作为候选者的负载
func (s *Storage) getCandidatePayload(ctx context.Context, storageId int64) int64 {
	info, err := eorm.NewSelector[any](s.db).Select(eorm.Count("Id")).From(eorm.TableOf(&TaskInfo{}, "t1")).
		Where(eorm.C("CandidateId").EQ(storageId)).Get(ctx)
	if err != nil {
		return -1
	}
	return (*info).(int64)
}

func (s *Storage) delCandidate(ctx context.Context, storageId, taskId int64) error {
	return eorm.NewUpdater[TaskInfo](s.db).Update(&TaskInfo{
		CandidateId: 0,
		UpdateTime:  time.Now().Unix(),
	}).Set(eorm.Columns("CandidateId", "UpdateTime")).
		Where(eorm.C("Id").EQ(taskId).And(eorm.C("CandidateId").EQ(storageId))).Exec(ctx).Err()
}

// Stop storage的关闭, 这里终止所有正在执行的任务
func (s *Storage) Stop(ctx context.Context) error {
	s.once.Do(func() {
		close(s.stop)
	})
	return eorm.NewUpdater[StorageInfo](s.db).Update(&StorageInfo{
		Status: storage.Stop,
	}).Set(eorm.Columns("Status")).Where(eorm.C("Id").EQ(s.storageId)).Exec(ctx).Err()
}
