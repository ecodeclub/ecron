package scheduler

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/gorhill/cronexpr"
	"github.com/gotomicro/ecron/internal/executor"
	"github.com/gotomicro/ecron/internal/storage"
	"github.com/gotomicro/ecron/internal/task"
	"github.com/gotomicro/ekit/queue"
)

type Scheduler struct {
	s          storage.Storager
	tasks      map[string]scheduledTask
	executors  map[string]executor.Executor
	mux        sync.Mutex
	readyTasks *queue.DelayQueue[execution]
	taskEvents chan task.Event
}

func NewScheduler(s storage.Storager) *Scheduler {
	sc := &Scheduler{
		s:          s,
		tasks:      make(map[string]scheduledTask),
		executors:  make(map[string]executor.Executor),
		mux:        sync.Mutex{},
		readyTasks: queue.NewDelayQueue[execution](10),
		taskEvents: make(chan task.Event),
	}

	sc.executors = map[string]executor.Executor{
		task.TypeHTTP: executor.NewHttpExec(),
	}

	return sc
}

func (s *Scheduler) Start(ctx context.Context) error {
	go func() {
		e := s.executeLoop(ctx)
		if e != nil {
			log.Println(e)
		}
	}()

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
				t := newRunningTask(event.Task, s.executors[string(event.Task.Type)])
				s.mux.Lock()
				s.tasks[t.task.Name] = t
				s.mux.Unlock()
				_ = s.readyTasks.Enqueue(ctx, execution{
					scheduledTask: &t,
					// 这里表示延迟多久执行, 通过cron expr包解析获取
					time: t.next(),
				})
				log.Println("preempted success, enqueued done")
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

// 这里需要启动一个goroutine后台执行
func (s *Scheduler) executeLoop(ctx context.Context) error {
	for {
		t, err := s.readyTasks.Dequeue(ctx)
		log.Println("executeLoop: ", t)
		if err != nil {
			return err
		}
		err = t.run()
		if err != nil {
			select {
			case s.taskEvents <- task.Event{Task: *t.task, Type: task.EventTypeFailed}:
				if er := s.s.Update(ctx, t.task, map[string]string{"ExecuteStatus": task.EventTypeFailed}); er != nil {
					log.Println(er)
				}
			}
		} else {
			select {
			case s.taskEvents <- task.Event{Task: *t.task, Type: task.EventTypeSuccess}:
				if er := s.s.Update(ctx, t.task, map[string]string{"ExecuteStatus": task.EventTypeSuccess}); er != nil {
					log.Println("111===", er)
				}
			}
		}
	}
}

type scheduledTask struct {
	task       *task.Task
	executor   executor.Executor
	expr       *cronexpr.Expression
	stopped    bool
	taskEvents chan task.Event
}

func newRunningTask(t *task.Task, exe executor.Executor) scheduledTask {
	expr := cronexpr.MustParse(t.Cron)

	return scheduledTask{
		task:       t,
		executor:   exe,
		expr:       expr,
		taskEvents: make(chan task.Event),
	}
}

func (r *scheduledTask) next() time.Time {
	return r.expr.Next(time.Now())
}

func (r *scheduledTask) stop() {
	r.stopped = true
}

func (r *scheduledTask) run() error {
	var err error
	// 如果这个任务已经被停止/取消了，什么也不做
	if r.stopped {
		return nil
	}
	// 这里executor返回一个task.Event,表示任务的执行状态
	taskEvent := r.executor.Execute(r.task)
Loop:
	for {
		select {
		case e := <-taskEvent:
			switch e.Type {
			case task.EventTypeRunning:
				log.Println("+++++get task event: running")
			case task.EventTypeSuccess:
				log.Println("+++++get task event: success")
				break Loop
			case task.EventTypeFailed:
				err = errors.New("task execute failed")
				break Loop
			}
		}
	}
	return err
}

type execution struct {
	*scheduledTask
	time time.Time
}

func (e execution) Delay() time.Duration {
	return e.time.Sub(time.Now())
}
