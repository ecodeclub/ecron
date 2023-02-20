package scheduler

import (
	"context"
	"github.com/gorhill/cronexpr"
	"github.com/gotomicro/ecron/internal/executor"
	"github.com/gotomicro/ecron/internal/storage"
	"github.com/gotomicro/ecron/internal/task"
	"github.com/gotomicro/ekit/queue"
	"log"
	"sync"
	"time"
)

// scheduledTask与Scheduler间交互的事件
type scheduledEvent struct {
	task *scheduledTask
	t    task.EventType
}

type Scheduler struct {
	s               storage.Storage
	tasks           map[string]scheduledTask
	executors       map[string]executor.Executor
	mux             sync.Mutex
	readyTasks      queue.DelayQueue[execution]
	taskEvents      chan task.Event     //send to storage
	scheduledEvents chan scheduledEvent //recv from executor
}

func (s *Scheduler) Start(ctx context.Context) error {
	events, err := s.s.Events(ctx, s.taskEvents)
	if err != nil {
		return err
	}
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		if err := s.executeLoop(cctx); err != nil {
			log.Printf(`退出executeLoop: %v`, err)
		}
	}()
	go func() {
		if err := s.listenOutLoop(cctx); err != nil {
			log.Printf(`退出listenOutLoop: %v`, err)
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event := <-events:
			switch event.Type {
			case storage.EventTypePreempted:
				t := newRunningTask(event.Task, s.executors[event.Task.Type], s.scheduledEvents)
				s.mux.Lock()
				s.tasks[t.task.Name] = t
				s.mux.Unlock()
				// 首次抢占任务 将任务添加进待执行队列 之后依赖单次任务执行完毕后添加后续任务
				if err = s.readyTasks.Enqueue(ctx, execution{
					scheduledTask: &t,
					time:          t.next(),
				}); err != nil {
					return err
				}
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

func (s *Scheduler) listenOutLoop(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case e := <-s.scheduledEvents:
			s.taskEvents <- task.Event{
				TaskName: e.task.task.Name,
				Type:     e.t,
			}
			if e.t == task.EventTypeRunning {
				continue
			}
			// 完成本次任务后再将下次任务入队
			if err := s.readyTasks.Enqueue(ctx, execution{
				scheduledTask: e.task,
				time:          e.task.next(),
			}); err != nil {
				return err
			}
		}
	}
}

func (s *Scheduler) executeLoop(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		t, err := s.readyTasks.Dequeue(ctx)
		if err != nil {
			log.Printf(`scheduler 入队失败: %v`, err)
			continue
		}
		go t.run()
	}
}

type scheduledTask struct {
	task     *task.Task
	executor executor.Executor
	expr     *cronexpr.Expression
	stopped  bool

	events chan<- scheduledEvent
}

func newRunningTask(t *task.Task, exe executor.Executor, executorEvents chan<- scheduledEvent) scheduledTask {
	expr := cronexpr.MustParse(t.Cron)
	return scheduledTask{
		task:     t,
		executor: exe,
		expr:     expr,
		events:   executorEvents,
	}
}

func (r *scheduledTask) next() time.Time {
	return r.expr.Next(time.Now())
}

func (r *scheduledTask) stop() {
	r.stopped = true
}

func (r *scheduledTask) isStop() bool {
	return r.stopped
}

func (r *scheduledTask) run() {
	// 如果这个任务已经被停止/取消了，什么也不做
	if r.stopped {
		return
	}
	errCount := 0
Loop:
	events, err := r.executor.Execute(r.task)
	if err != nil {
		r.events <- scheduledEvent{task: r, t: task.EventTypeFailed}
		return
	}
	for e := range events {
		se := scheduledEvent{task: r}
		switch e.Type {
		case executor.EventTypeWaiting:
			se.t = task.EventTypeRunning
			r.events <- se
			continue
		case executor.EventTypeFailed:
			// 根据自定义配置 控制单次任务失败后的重试
			if r.task.Retry.Need && errCount < r.task.Retry.Count {
				se.t = executor.EventTypeWaiting
				r.events <- se
				errCount++
				goto Loop
			}
			se.t = task.EventTypeFailed
		case executor.EventTypeSuccess:
			se.t = task.EventTypeSuccess
		}
		r.events <- se
		return
	}
}

type execution struct {
	*scheduledTask
	time time.Time
}

func (e execution) Delay() time.Duration {
	return e.time.Sub(time.Now())
}
