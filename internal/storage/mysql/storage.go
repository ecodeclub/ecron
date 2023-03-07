package mysql

import (
	"context"
	"log"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
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
	payLoad           int64                 // 当前storage节点的载荷
	uuid              uint32                // 唯一标识一个storage
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
		uuid: uuid.New().ID(),
	}
	option.Apply[Storage](s, opt...)

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
		BaseColumns: BaseColumns{
			CreateTime: time.Now().UnixMilli(),
			UpdateTime: time.Now().UnixMilli(),
		},
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
		BaseColumns: BaseColumns{
			CreateTime: time.Now().UnixMilli(),
			UpdateTime: time.Now().UnixMilli(),
		},
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
		BaseColumns:     BaseColumns{UpdateTime: time.Now().UnixMilli()},
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
		BaseColumns:   BaseColumns{UpdateTime: time.Now().UnixMilli()},
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
	return eorm.NewUpdater[TaskInfo](s.db).
		Update(&TaskInfo{
			Name:        t.Name,
			Cron:        t.Cron,
			Type:        string(t.Type),
			Config:      t.Parameters,
			BaseColumns: BaseColumns{UpdateTime: time.Now().UnixMilli()},
		}).Where(eorm.C("Id").EQ(t.TaskId)).Exec(ctx).Err()
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
	cond3 := eorm.C("SchedulerStatus").EQ(storage.EventTypeDiscarded).And(eorm.C("CandidateId").EQ(s.uuid))
	tasks, err := eorm.NewSelector[TaskInfo](s.db).From(eorm.TableOf(&TaskInfo{}, "t1")).
		Where(cond1.Or(cond2).Or(cond3)).
		GetMulti(tCtx)
	if err != nil {
		log.Println("获取待抢占任务失败", err)
		return
	}

	for _, item := range tasks {
		preemptedEvent := storage.Event{
			Type: storage.EventTypePreempted,
			Task: &task.Task{
				Config: task.Config{
					Name:       item.Name,
					Cron:       item.Cron,
					Type:       task.Type(item.Type),
					Parameters: item.Config,
				},
				TaskId: item.Id,
			},
		}

		err = s.CompareAndUpdateTaskStatus(tCtx, item.Id, item.SchedulerStatus, storage.EventTypePreempted)
		if err != nil {
			log.Println(err)
			continue
		}
		s.payLoad += 1
		// 这里需要更新已经抢占该任务的占有storage载荷
		// TODO 合并更新状态函数，减少修改次数
		err = s.UpdateOccupierPayload(ctx, item.Id, s.payLoad, true)
		if err != nil {
			log.Println("ecron: update occupier payload error: ", err)
			continue
		}

		s.events <- preemptedEvent
	}
}

// 定时更新已被其它storage抢占任务的候选者节点载荷
func (s *Storage) lookup(ctx context.Context) {
	var updateTaskIds []any
	tasks, _ := eorm.NewSelector[TaskInfo](s.db).From(eorm.TableOf(&TaskInfo{}, "t1")).
		Where(eorm.C("SchedulerStatus").EQ(storage.EventTypePreempted).And(eorm.C("OccupierId").NEQ(s.uuid))).
		GetMulti(ctx)
	for _, t := range tasks {
		if t.CandidatePayload > s.payLoad {
			updateTaskIds = append(updateTaskIds, t.Id)
		}
	}
	err := eorm.NewUpdater[TaskInfo](s.db).Update(&TaskInfo{
		CandidatePayload: s.payLoad,
		CandidateId:      s.uuid,
	}).Set(eorm.C("CandidatePayload"), eorm.C("CandidateId")).
		Where(eorm.C("Id").In(updateTaskIds...)).Exec(ctx).Err()
	if err != nil {
		log.Println("ecron: update candidate payload fail, ", err)
	}
}

func (s *Storage) UpdateOccupierPayload(ctx context.Context, taskId, payload int64, setCandidateNull bool) error {
	sc := []eorm.Assignable{eorm.C("OccupierPayload"), eorm.C("OccupierId")}
	if setCandidateNull {
		sc = append(sc, eorm.C("CandidatePayload"))
	}
	return eorm.NewUpdater[TaskInfo](s.db).Update(&TaskInfo{
		OccupierPayload:  payload,
		OccupierId:       s.uuid,
		CandidatePayload: 0,
	}).Set(sc...).Where(eorm.C("Id").EQ(taskId)).Exec(ctx).Err()
}

func (s *Storage) UpdateCandidatesPayload(ctx context.Context, taskId, payload int64) error {
	return eorm.NewUpdater[TaskInfo](s.db).Update(&TaskInfo{
		CandidatePayload: payload,
		CandidateId:      s.uuid,
	}).Set(eorm.C("CandidatePayload"), eorm.C("CandidateId")).Where(eorm.C("Id").EQ(taskId)).Exec(ctx).Err()
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
func (s *Storage) refresh(ctx context.Context, taskId, epoch, candidatePayload int64) {
	var (
		timer *time.Timer
		sc    []eorm.Assignable
	)
	sc = append(sc, eorm.Assign("Epoch", eorm.C("Epoch").Add(1)), eorm.C("UpdateTime"))
	for {
		epoch++
		log.Printf("taskId: %d storage begin refresh preempted task", taskId)
		// 此时修改该任务状态为放弃
		if s.payLoad > candidatePayload {
			sc = append(sc, eorm.C("SchedulerStatus"))
		}
		rowsAffect, err := eorm.NewUpdater[TaskInfo](s.db).
			Update(&TaskInfo{
				Epoch:           epoch,
				SchedulerStatus: storage.EventTypeDiscarded,
				BaseColumns:     BaseColumns{UpdateTime: time.Now().UnixMilli()},
			}).Set(sc...).Where(eorm.C("Id").EQ(taskId).
			And(eorm.C("SchedulerStatus").EQ(storage.EventTypePreempted))).Exec(ctx).RowsAffected()

		// 1. 续约成功
		if err == nil {
			log.Printf("taskId: %d refresh success, 第%d次", taskId, epoch)
			return
		}
		// 2. 获取的任务抢占状态改变，终止续约
		if rowsAffect == 0 {
			log.Printf("taskId: %d refresh stop, 第%d次", taskId, epoch)
			return
		}
		interval, ok := s.refreshRetry.Next()
		if !ok {
			log.Printf("taskId: %d refresh preempted fail, %s", taskId, err)
			return
		}

		// 若续约失败，重试
		if timer == nil {
			timer = time.NewTimer(interval)
		} else {
			timer.Reset(interval)
		}
		select {
		case <-timer.C:
		case <-ctx.Done():
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
				go s.refresh(ctx, t.Id, t.Epoch, t.CandidatePayload)
			}
		}
	}
}
