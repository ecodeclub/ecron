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

func (sc *Scheduler) Start(ctx context.Context) error {
	go func() {
		// 这里进行已经写入延迟队列中的事件执行，并且写入时间执的结果
		e := sc.executeLoop(ctx)
		if e != nil {
			log.Println(e)
		}
	}()

	events, err := sc.s.Events(ctx, sc.taskEvents)
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
				t := sc.newRunningTask(ctx, event.Task, sc.executors[string(event.Task.Type)])
				sc.mux.Lock()
				sc.tasks[t.task.Name] = t
				sc.mux.Unlock()
				_ = sc.readyTasks.Enqueue(ctx, execution{
					scheduledTask: &t,
					// 这里表示延迟多久执行, 通过cron expr包解析获取
					time: t.next(),
				})
				log.Println("preempted success, enqueued done, expect dequeue time: ", t.next())
			case storage.EventTypeDeleted:
				sc.mux.Lock()
				tn, ok := sc.tasks[event.Task.Name]
				delete(sc.executors, event.Task.Name)
				sc.mux.Unlock()
				if ok {
					tn.stop()
				}
			}
		}
	}
}

func (sc *Scheduler) executeLoop(ctx context.Context) error {
	for {
		t, err := sc.readyTasks.Dequeue(ctx)
		log.Println("executeLoop: want execute in: ", t.time)
		if err != nil {
			return err
		}
		err = t.run()
		if err != nil {
			if er := sc.s.Update(ctx, t.task.TaskId, nil, &storage.Status{
				UseStatus: task.EventTypeFailed}); er != nil {
				log.Println(er)
			}
			sc.taskEvents <- task.Event{Task: *t.task, Type: task.EventTypeFailed}
		} else {
			if er := sc.s.Update(ctx, t.task.TaskId, nil, &storage.Status{
				UseStatus: task.EventTypeSuccess}); er != nil {
				log.Println(er)
			}
			sc.taskEvents <- task.Event{Task: *t.task, Type: task.EventTypeSuccess}
		}
	}
}

func (sc *Scheduler) newRunningTask(ctx context.Context, t *task.Task, exe executor.Executor) scheduledTask {
	var (
		st    scheduledTask
		exeId int64
		err   error
	)
	// 根据任务配置，在db创建一个执行记录
	if exeId, err = sc.s.AddExecution(ctx, t.TaskId); err != nil {
		log.Println(err)
		return st
	}
	st = scheduledTask{
		task:       t,
		executor:   exe,
		executeId:  exeId,
		expr:       cronexpr.MustParse(t.Cron),
		taskEvents: make(chan task.Event),
	}
	return st
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
