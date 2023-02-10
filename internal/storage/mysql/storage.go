package mysql

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/gotomicro/ecron/internal/storage"
	"github.com/gotomicro/ecron/internal/task"
	"github.com/gotomicro/eorm"
)

type Storage struct {
	events chan storage.Event
}

func NewStorage() *Storage {
	s := &Storage{
		events: make(chan storage.Event),
	}
	return s
}

func (s *Storage) Get(ctx context.Context, name string) *task.Task {
	res, err := eorm.NewSelector[Task](db).Select().From(&Task{}).
		Where(eorm.C("Name").EQ(name)).Get(ctx)
	if err != nil {
		return nil
	}
	return &task.Task{
		Config: task.Config{
			Name:       res.Name,
			Cron:       res.Cron,
			Type:       task.Type(res.Type),
			Parameters: res.Config,
		},
	}
}

func (s *Storage) GetMulti(ctx context.Context, status string) []*task.Task {
	res, err := eorm.NewSelector[Task](db).Select().From(&Task{}).
		Where(eorm.C("SchedulerStatus").EQ(status)).GetMulti(ctx)
	if err != nil {
		return nil
	}
	ts := make([]*task.Task, 0, len(res))
	for _, t := range res {
		ts = append(ts, &task.Task{
			Config: task.Config{
				Name:       t.Name,
				Cron:       t.Cron,
				Type:       task.Type(t.Type),
				Parameters: t.Config,
			},
		})
	}
	return ts
}

func (s *Storage) Add(ctx context.Context, t *task.Task) error {
	return eorm.NewInserter[Task](db).Values(&Task{
		Name:            t.Name,
		Cron:            t.Cron,
		ExecuteStatus:   task.EventTypeInit,
		SchedulerStatus: storage.EventCreated,
		Type:            string(t.Type),
		Config:          t.Parameters,
	}).Exec(ctx).Err()
}

func (s *Storage) Update(ctx context.Context, t *task.Task, status map[string]string) error {
	taskInfo := &Task{
		SchedulerStatus: status["SchedulerStatus"],
		ExecuteStatus:   status["ExecuteStatus"],
	}

	cols := make([]string, 0, 2)
	if taskInfo.ExecuteStatus != "" {
		cols = append(cols, "ExecuteStatus")
	}
	if taskInfo.SchedulerStatus != "" {
		cols = append(cols, "SchedulerStatus")
	}

	return eorm.NewUpdater[Task](db).Update(taskInfo).Set(eorm.Columns(cols...)).Where(eorm.C("Name").EQ(t.Name)).
		Exec(ctx).Err()
}

func (s *Storage) Delete(ctx context.Context, name string) error {
	return eorm.NewDeleter[Task](db).From(&Task{}).Where(eorm.C("Name").EQ(name)).Exec(ctx).Err()
}

// RunPreempt 每隔固定时间去db中抢占任务
func (s *Storage) RunPreempt(ctx context.Context) {
	ticker := time.NewTicker(2 * time.Second)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			log.Println("storage begin preempt task")
			if err := s.preempted(ctx); err != nil {
				log.Println(err)
			}
		}
	}
}

func (s *Storage) preempted(ctx context.Context) error {
	// 多个节点的部署下，如何实现cas操作，是否需要加分布式读锁？
	// 获取待抢占任务
	ts := s.GetMulti(ctx, storage.EventCreated)
	if len(ts) == 0 {
		return errors.New("not find tasks")
	}
	// 写入已抢占状态
	for _, t := range ts {
		err := s.Update(ctx, t, map[string]string{"SchedulerStatus": storage.EventTypePreempted})
		if err != nil {
			log.Println(err)
			continue
		}

		// 写入storage事件
		s.events <- storage.Event{
			Type: storage.EventTypePreempted,
			Task: &task.Task{
				Config: task.Config{
					Name:       t.Name,
					Cron:       t.Cron,
					Type:       task.Type(t.Type),
					Parameters: t.Parameters,
				},
			},
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
					err := s.Update(ctx, &event.Task, map[string]string{"SchedulerStatus": storage.EventTypeRunnable})
					if err != nil {
						log.Println(err)
					}
					log.Println("storage 收到 task执行中信号")
				case task.EventTypeSuccess:
					err := s.Update(ctx, &event.Task, map[string]string{"SchedulerStatus": storage.EventTypeEnd})
					if err != nil {
						log.Println(err)
					}
					log.Println("storage 收到 task执行成功信号")
				case task.EventTypeFailed:
					err := s.Update(ctx, &event.Task, map[string]string{"SchedulerStatus": storage.EventTypeEnd})
					if err != nil {
						log.Println(err)
					}
				}
			}
		}
	}()

	return s.events, nil
}
