package mysql

import (
	"context"
	"errors"
	"log"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/gotomicro/ecron/internal/storage"
	"github.com/gotomicro/ecron/internal/task"
	"github.com/gotomicro/eorm"
)

type Storage struct {
	db                *eorm.DB
	interval          time.Duration // 发起任务抢占时间间隔
	preemptionTimeout time.Duration // 抢占任务超时时间
	events            chan storage.Event
}

func NewMysqlStorage(ctx context.Context, dsn string, interval, timeout time.Duration) *Storage {
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
		interval:          interval,
		preemptionTimeout: timeout,
	}

	go s.RunPreempt(ctx)

	return s
}

// Get 防止因多个节点请求db抢占任务引起的冲突，引入epoch，抢占之后会+1
// 抢占续约也会用到这个字段
// 同时记录下每一个task的epoch在task结构体中
func (s *Storage) Get(ctx context.Context, status string) (*task.Task, error) {
	// 先获取符合条件的task，初始的epoch都是0，每次获取到之后就+1
	// 然后修改task的epoch，防止多个storage抢占冲突
	taskInfo, err := eorm.NewSelector[TaskInfo](s.db).
		Select().From(&TaskInfo{}).
		Where(eorm.C("SchedulerStatus").EQ(status), eorm.C("Epoch").EQ(0)).
		Get(ctx)
	if err != nil {
		return nil, err
	}
	// 获取成功之后会将调度状态由created 改成 not-preempt
	err = eorm.NewUpdater[TaskInfo](s.db).
		Update(&TaskInfo{Epoch: taskInfo.Epoch + 1, SchedulerStatus: storage.EventTypeNotPreempted}).
		Set(eorm.Columns("Epoch", "SchedulerStatus")).
		Where(eorm.C("Id").EQ(taskInfo.Id)).
		Exec(ctx).Err()
	if err != nil {
		return nil, err
	}

	t := &task.Task{
		Config: task.Config{
			Name:       taskInfo.Name,
			Cron:       taskInfo.Cron,
			Type:       task.Type(taskInfo.Type),
			Parameters: taskInfo.Config,
		},
		TaskId: taskInfo.Id,
		Epoch:  taskInfo.Epoch + 1,
	}
	return t, nil
}

// Add 创建task，设置调度状态是created
func (s *Storage) Add(ctx context.Context, t *task.Task, dao ...any) (int64, error) {
	var (
		id  int64
		err error
	)
	for _, d := range dao {
		switch d.(type) {
		case TaskInfo:
			if id, err = eorm.NewInserter[TaskInfo](s.db).Values(&TaskInfo{
				Name:            t.Name,
				Cron:            t.Cron,
				SchedulerStatus: storage.EventTypeCreated,
				Type:            string(t.Type),
				Config:          t.Parameters,
				Epoch:           0,
			}).Exec(ctx).LastInsertId(); err != nil {
				return -1, err
			}
		case TaskExecution:
			if id, err = eorm.NewInserter[TaskExecution](s.db).Values(&TaskExecution{
				ExecuteStatus: storage.EventTypeCreated,
				TaskId:        t.TaskId,
			}).Exec(ctx).LastInsertId(); err != nil {
				return -1, err
			}
		default:
			return -1, errors.New("不支持的类型")
		}
	}
	return id, nil
}

func (s *Storage) Update(ctx context.Context, t *task.Task, dao ...any) error {
	for _, d := range dao {
		switch ins := d.(type) {
		case TaskInfo:
			if err := eorm.NewUpdater[TaskInfo](s.db).
				Update(&ins).
				Set(eorm.Columns("SchedulerStatus", "Epoch")).
				Where(eorm.C("Id").EQ(t.TaskId)).
				Exec(ctx).Err(); err != nil {
				return err
			}
		case TaskExecution:
			if err := eorm.NewUpdater[TaskExecution](s.db).
				Update(&ins).
				Set(eorm.Columns("ExecuteStatus")).
				Where(eorm.C("Id").EQ(ins.Id)).
				Exec(ctx).Err(); err != nil {
				return err
			}
		default:
			return errors.New("不支持的类型")
		}
	}
	return nil
}

func (s *Storage) Delete(ctx context.Context, taskId int64) error {
	// TODO 处于某些状态的task不能被删除
	return eorm.NewDeleter[TaskInfo](s.db).From(&TaskInfo{}).Where(eorm.C("Id").EQ(taskId)).Exec(ctx).Err()
}

// RunPreempt 每隔固定时间去db中抢占任务
func (s *Storage) RunPreempt(ctx context.Context) {
	ticker := time.NewTicker(s.interval)
	for {
		select {
		case <-ticker.C:
			log.Println("storage begin preempt task")
			s.preempted(ctx)
		default:
		}
	}
}

// TODO 抢占之后节点挂掉的情况，考虑通过租约的方式实现，定期刷新task epoch
func (s *Storage) preempted(ctx context.Context) {
	tCtx, cancel := context.WithTimeout(ctx, s.preemptionTimeout)
	defer func() {
		cancel()
	}()

	// 抢占已添加任务, 每次执行只抢一个
	// get成功就表示我已经能抢占一个任务，此时epoch已经更新成+1了
	tsk, err := s.Get(tCtx, storage.EventTypeCreated)
	if err != nil {
		log.Println("获取待抢占任务失败", err)
		return
	}

	// 将get到的task写入已抢占状态
	err = s.Update(ctx, tsk, TaskInfo{SchedulerStatus: storage.EventTypePreempted, Epoch: tsk.Epoch})
	if err != nil {
		log.Println(err)
		return
	}

	// 写入storage抢占事件，供调度去执行
	s.events <- storage.Event{
		Type: storage.EventTypePreempted,
		Task: tsk,
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
					err := s.Update(ctx, &event.Task, TaskInfo{SchedulerStatus: storage.EventTypeRunnable, Epoch: event.Epoch})
					if err != nil {
						log.Println(err)
					}
					log.Println("storage 收到 task执行中信号")
				case task.EventTypeSuccess:
					err := s.Update(ctx, &event.Task, TaskInfo{SchedulerStatus: storage.EventTypeEnd, Epoch: event.Epoch})
					if err != nil {
						log.Println(err)
					}
					log.Println("storage 收到 task执行成功信号")
				case task.EventTypeFailed:
					err := s.Update(ctx, &event.Task, TaskInfo{SchedulerStatus: storage.EventTypeEnd, Epoch: event.Epoch})
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
