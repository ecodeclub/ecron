package mysql

import (
	"context"
	"log"
	"time"

	"github.com/ecodeclub/ecron/internal/errs"
	"github.com/ecodeclub/ecron/internal/storage"
	"github.com/ecodeclub/ecron/internal/task"
	"github.com/ecodeclub/eorm"
	_ "github.com/go-sql-driver/mysql"
	"github.com/gotomicro/ekit/bean/option"
)

type Storage struct {
	db                *eorm.DB
	preemptInterval   time.Duration         // 发起任务抢占时间间隔
	refreshInterval   time.Duration         // 发起任务抢占续约时间间隔
	preemptionTimeout time.Duration         // 抢占任务超时时间
	refreshRetry      storage.RetryStrategy // 续约重试策略
	events            chan storage.Event
}

func NewMysqlStorage(dsn string, opt ...option.Option[Storage]) (*Storage, error) {
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
	ts, err := eorm.NewSelector[TaskInfo](s.db).
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
		case <-ctx.Done():
			return
		}
	}
}

// 抢占两种类型的任务：
// 1. 长时间没有续约的已经抢占的任务
// 2. 处于创建状态的任务
func (s *Storage) preempted(ctx context.Context) {
	tCtx, cancel := context.WithTimeout(ctx, s.preemptionTimeout)
	defer func() {
		cancel()
	}()
	maxRefreshInterval := s.refreshRetry.GetMaxRetry() * s.refreshInterval.Milliseconds()
	tasks, err := eorm.NewSelector[TaskInfo](s.db).
		Where(eorm.C("SchedulerStatus").EQ(storage.EventTypeCreated).Or(
			eorm.C("SchedulerStatus").EQ(storage.EventTypePreempted).
				And(eorm.C("UpdateTime").LTEQ(time.Now().UnixMilli() - maxRefreshInterval)))).
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
		// 若是已抢占且未续约的task，直接抢占
		if item.SchedulerStatus == storage.EventTypePreempted {
			s.events <- preemptedEvent
			continue
		}
		// 如果get到的task是创建状态，则写入已抢占状态
		err = s.CompareAndUpdateTaskStatus(tCtx, item.Id, storage.EventTypeCreated, storage.EventTypePreempted)
		if err != nil {
			log.Println(err)
			continue
		}
		s.events <- preemptedEvent
	}
}

func (s *Storage) Events(ctx context.Context, taskEvents <-chan task.Event) (<-chan storage.Event, error) {
	go func() {
		for {
			select {
			case <-ctx.Done():
			case event := <-taskEvents:
				switch event.Type {
				case task.EventTypeRunning:
					//err := s.CompareAndUpdateTaskStatus(ctx, event.TaskId, storage.EventTypePreempted,
					//	storage.EventTypeRunnable)
					//if err != nil {
					//	log.Println(err)
					//}
					log.Println("storage 收到 task执行中信号")
				case task.EventTypeSuccess:
					err := s.CompareAndUpdateTaskStatus(ctx, event.TaskId, storage.EventTypePreempted,
						storage.EventTypeEnd)
					if err != nil {
						log.Println(err)
					}
					log.Println("storage 收到 task执行成功信号")
				case task.EventTypeFailed:
					err := s.CompareAndUpdateTaskStatus(ctx, event.TaskId, storage.EventTypePreempted,
						storage.EventTypeEnd)
					if err != nil {
						log.Println(err)
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
func (s *Storage) refresh(ctx context.Context, taskId, epoch int64) {
	// 续约任务间隔
	tickerR := time.NewTicker(s.refreshInterval)
	end := make(chan struct{})
LOOP:
	for {
		select {
		case <-end:
			log.Printf("taskId: %d refresh preemted end", taskId)
			return
		case <-tickerR.C:
			var timer *time.Timer
			for {
				epoch++
				log.Printf("taskId: %d storage begin refresh preempted task", taskId)
				rowsAffect, err := eorm.NewUpdater[TaskInfo](s.db).
					Update(&TaskInfo{
						Epoch:       epoch,
						BaseColumns: BaseColumns{UpdateTime: time.Now().UnixMilli()},
					}).Set(eorm.Assign("Epoch", eorm.C("Epoch").Add(1)), eorm.C("UpdateTime")).
					Where(eorm.C("Id").EQ(taskId).And(eorm.C("SchedulerStatus").
						EQ(storage.EventTypePreempted))).Exec(ctx).RowsAffected()
				// 续约成功
				if err == nil {
					log.Printf("taskId: %d refresh preempted success, 第%d次", taskId, epoch)
					break LOOP
				}
				// 获取的任务抢占状态改变，终止续约
				if rowsAffect == 0 {
					close(end)
				}
				interval, ok := s.refreshRetry.Next()
				if !ok {
					log.Printf("taskId: %d refresh preempted fail, %s", taskId, err)
					return
				}
				// 续约失败，重试
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
	}
}

func (s *Storage) AutoRefresh(ctx context.Context) {
	// 获取当前出去抢占状态的任务，需要在后续任务变成非抢占状态时结束续约
	tasks, _ := eorm.NewSelector[TaskInfo](s.db).
		Select().Where(eorm.C("SchedulerStatus").EQ(storage.EventTypePreempted)).
		GetMulti(ctx)

	for _, t := range tasks {
		go s.refresh(ctx, t.Id, t.Epoch)
	}

}
