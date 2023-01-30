package scheduler

import (
	"context"
	"github.com/gorhill/cronexpr"
	"github.com/gotomicro/ecron/internal/executor"
	"github.com/gotomicro/ecron/internal/storage"
	"github.com/gotomicro/ecron/internal/task"
	"github.com/gotomicro/ekit/queue"
	"sync"
	"time"
)

type Scheduler struct {
	s          storage.Storage
	tasks      map[string]scheduledTask
	executors  map[string]executor.Executor
	mux        sync.Mutex
	readyTasks queue.DelayQueue[execution]
	taskEvents chan task.Event
}

func (s *Scheduler) Start(ctx context.Context) error {
	events, err := s.s.Events(ctx, s.taskEvents)
	if err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event := <-events:
			switch event.Type {
			case storage.EventTypePreempted:
				t := newRunningTask(event.Task, s.executors[event.Task.Type])
				s.mux.Lock()
				s.tasks[t.task.Name] = t
				s.mux.Unlock()
			case storage.EventTypeDeleted:
				s.mux.Lock()
				tn, ok := s.tasks[event.Task.Name]
				delete(s.executors, event.Task.Name)
				s.mux.Unlock()
				if ok {
					tn.stop()
				}
			}
		}
	}
}

func (s *Scheduler) executeLoop(ctx context.Context) error {
	for {
		t, err := s.readyTasks.Dequeue(ctx)
		if err != nil {
			return err
		}
		err = t.run()
	}
}

type scheduledTask struct {
	task     *task.Task
	executor executor.Executor
	expr     *cronexpr.Expression
	stopped  bool
}

func newRunningTask(t *task.Task, exe executor.Executor) scheduledTask {
	expr := cronexpr.MustParse(t.Cron)
	return scheduledTask{
		task:     t,
		executor: exe,
		expr:     expr,
	}
}

func (r *scheduledTask) next() time.Time {
	return r.expr.Next(time.Now())
}

func (r *scheduledTask) stop() {
	r.stopped = true
}

func (r *scheduledTask) run() error {
	// 如果这个任务已经被停止/取消了，什么也不做
	if r.stopped {
		return nil
	}
	return r.executor.Execute(r.task)
}

type execution struct {
	*scheduledTask
	time time.Time
}

func (e execution) Delay() time.Duration {
	return e.time.Sub(time.Now())
}
