package mysql

import (
	"context"
	"database/sql"
	"log"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/gotomicro/ecron/internal/storage"
	"github.com/gotomicro/ecron/internal/task"
	"github.com/gotomicro/eorm"
)

type Storage struct {
	db                *eorm.DB
	intervalP         time.Duration // 发起任务抢占时间间隔
	intervalR         time.Duration // 发起任务抢占续约时间间隔
	preemptionTimeout time.Duration // 抢占任务超时时间
	events            chan storage.Event
}

func NewMysqlStorage(dsn string, intervalP, intervalR, timeout time.Duration) *Storage {
	db, err := eorm.Open("mysql", dsn)
	if err != nil {
		panic(err)
	}
	err = db.Wait()
	if err != nil {
		panic(err)
	}

	s := &Storage{
		events:            make(chan storage.Event),
		db:                db,
		intervalP:         intervalP,
		intervalR:         intervalR,
		preemptionTimeout: timeout,
	}
	return s
}

func (s *Storage) Get(ctx context.Context) ([]*task.Task, error) {
	// 抢占两种类型的任务：
	// 1. 长时间没有续约的已经抢占的任务
	// 2. 处于创建状态的任务
	tasks, err := eorm.NewSelector[TaskInfo](s.db).
		Select().From(&TaskInfo{}).
		Where(eorm.C("SchedulerStatus").EQ(storage.EventTypeCreated).
			And(eorm.C("UpdateTime").
				LTEQ(time.Now().Add(-s.intervalR).Format("2006-01-02 15:04:05")))).
		GetMulti(ctx)
	if err != nil {
		return nil, err
	}

	ts := make([]*task.Task, 0, len(tasks))
	for _, item := range tasks {
		ts = append(ts, &task.Task{
			Config: task.Config{
				Name:       item.Name,
				Cron:       item.Cron,
				Type:       task.Type(item.Type),
				Parameters: item.Config,
			},
			TaskId: item.Id,
		})
	}
	return ts, nil
}

// Add 创建task，设置调度状态是created
func (s *Storage) Add(ctx context.Context, t *task.Task) (int64, error) {
	return s.addTask(ctx, t.Name, t.Cron, string(t.Type), t.Parameters)
}

func (s *Storage) addTask(ctx context.Context, name, cron, typ, config string) (int64, error) {
	id, err := eorm.NewInserter[TaskInfo](s.db).Values(&TaskInfo{
		Name:            name,
		Cron:            cron,
		SchedulerStatus: storage.EventTypeCreated,
		Type:            typ,
		Config:          config,
		BaseColumns: BaseColumns{
			CreateTime: &NullTime{Time: time.Now(), Valid: true},
			UpdateTime: &NullTime{Time: time.Now(), Valid: true},
		},
	}).Exec(ctx).LastInsertId()
	if err != nil {
		return -1, err
	}
	return id, nil
}

// AddExecution 创建一条执行记录
func (s *Storage) AddExecution(ctx context.Context, taskId int64) (int64, error) {
	id, err := eorm.NewInserter[TaskExecution](s.db).Values(&TaskExecution{
		ExecuteStatus: storage.EventTypeCreated,
		TaskId:        taskId,
		BaseColumns: BaseColumns{
			CreateTime: &NullTime{Time: time.Now(), Valid: true},
			UpdateTime: &NullTime{Time: time.Now(), Valid: true},
		},
	}).Exec(ctx).LastInsertId()
	if err != nil {
		return -1, err
	}
	return id, nil
}

func (s *Storage) Update(ctx context.Context, taskId int64, taskStatus, executeStatus *storage.Status) error {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return err
	}
	if taskStatus != nil {
		cond := eorm.C("Id").EQ(taskId)
		if taskStatus.ExpectStatus != "" {
			cond = cond.And(eorm.C("SchedulerStatus").EQ(taskStatus.ExpectStatus))
		}
		if er := eorm.NewUpdater[TaskInfo](tx).Update(&TaskInfo{
			SchedulerStatus: taskStatus.UseStatus,
			BaseColumns:     BaseColumns{UpdateTime: &NullTime{time.Now(), true}},
		}).
			Set(eorm.Columns("SchedulerStatus", "UpdateTime")).Where(cond).Exec(ctx).Err(); er != nil {
			return tx.Rollback()
		}
	}
	if executeStatus != nil {
		cond := eorm.C("TaskId").EQ(taskId)
		if executeStatus.ExpectStatus != "" {
			cond = cond.And(eorm.C("ExecuteStatus").EQ(executeStatus.ExpectStatus))
		}
		if er := eorm.NewUpdater[TaskExecution](tx).Update(&TaskExecution{
			ExecuteStatus: executeStatus.UseStatus,
			BaseColumns:   BaseColumns{UpdateTime: &NullTime{time.Now(), true}},
		}).
			Set(eorm.Columns("ExecuteStatus", "UpdateTime")).Where(cond).Exec(ctx).Err(); er != nil {
			return tx.Rollback()
		}
	}
	return tx.Commit()
}

func (s *Storage) Delete(ctx context.Context, taskId int64) error {
	// TODO 处于某些状态的task不能被删除
	return eorm.NewDeleter[TaskInfo](s.db).From(&TaskInfo{}).Where(eorm.C("Id").EQ(taskId)).Exec(ctx).Err()
}

// RunPreempt 每隔固定时间去db中抢占任务
func (s *Storage) RunPreempt(ctx context.Context) {
	// 抢占任务间隔
	tickerP := time.NewTicker(s.intervalP)
	for {
		select {
		case <-tickerP.C:
			log.Println("storage begin preempt task")
			s.preempted(ctx)
		default:
		}
	}
}

func (s *Storage) preempted(ctx context.Context) {
	tCtx, cancel := context.WithTimeout(ctx, s.preemptionTimeout)
	defer func() {
		cancel()
	}()

	// get 之后，进行update 更新
	tasks, err := s.Get(tCtx)
	if err != nil {
		log.Println("获取待抢占任务失败", err)
		return
	}

	for _, item := range tasks {
		// 如果get到的task是创建状态，则写入已抢占状态
		err = s.Update(ctx, item.TaskId, &storage.Status{
			ExpectStatus: storage.EventTypeCreated,
			UseStatus:    storage.EventTypePreempted,
		}, nil)
		if err != nil {
			log.Println(err)
			continue
		}

		// 写入storage抢占事件，供调度去执行
		s.events <- storage.Event{
			Type: storage.EventTypePreempted,
			Task: item,
		}
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
					err := s.Update(ctx, event.TaskId, &storage.Status{
						ExpectStatus: storage.EventTypePreempted,
						UseStatus:    storage.EventTypeRunnable,
					}, nil)
					if err != nil {
						log.Println(err)
					}
					log.Println("storage 收到 task执行中信号")
				case task.EventTypeSuccess:
					err := s.Update(ctx, event.TaskId, &storage.Status{
						ExpectStatus: storage.EventTypePreempted,
						UseStatus:    storage.EventTypeEnd,
					}, nil)
					if err != nil {
						log.Println(err)
					}
					log.Println("storage 收到 task执行成功信号")
				case task.EventTypeFailed:
					err := s.Update(ctx, event.TaskId, &storage.Status{
						ExpectStatus: storage.EventTypePreempted,
						UseStatus:    storage.EventTypeEnd,
					}, nil)
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
	tickerR := time.NewTicker(s.intervalR)
	end := make(chan struct{})
	for {
		select {
		case <-end:
			log.Printf("taskId: %d refresh preemted end", taskId)
			return
		case <-tickerR.C:
			epoch++
			log.Printf("taskId: %d storage begin refresh preempted task", taskId)
			rowsAffect, err := eorm.NewUpdater[TaskInfo](s.db).
				Update(&TaskInfo{
					Epoch:       epoch,
					BaseColumns: BaseColumns{UpdateTime: &NullTime{Time: time.Now(), Valid: true}},
				}).
				Set(eorm.Columns("Epoch"), eorm.C("UpdateTime")).
				Where(eorm.C("Id").EQ(taskId).And(eorm.C("Epoch").EQ(epoch - 1)).
					And(eorm.C("SchedulerStatus").EQ(storage.EventTypePreempted))).Exec(ctx).RowsAffected()
			if err != nil {
				log.Printf("taskId: %d refresh preempted fail, %s", taskId, err)
				return
			}
			// 获取的任务抢占状态改变，则终止续约
			if rowsAffect == 0 {
				close(end)
			}
		}
	}
}

func (s *Storage) AutoRefresh(ctx context.Context) {
	// 获取当前出去抢占状态的任务，需要在后续任务变成非抢占状态时结束续约
	tasks, _ := eorm.NewSelector[TaskInfo](s.db).
		Select().From(&TaskInfo{}).
		Where(eorm.C("SchedulerStatus").EQ(storage.EventTypePreempted)).
		GetMulti(ctx)

	for _, t := range tasks {
		go s.refresh(ctx, t.Id, t.Epoch)
	}

}
