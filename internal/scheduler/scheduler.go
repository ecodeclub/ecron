package scheduler

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/ecodeclub/ecron/internal/executor"
	"github.com/ecodeclub/ecron/internal/storage"
	"github.com/ecodeclub/ecron/internal/task"
	"github.com/ecodeclub/ekit/queue"
	"github.com/gorhill/cronexpr"
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
		// 这里进行已经写入延迟队列中的事件执行，并且写入时间执行的结果
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
					// TODO 当前demo只是跑的一次性任务，需要适配定时任务
					time: t.next(),
				})
				log.Println("preempted success, enqueued done")
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
		log.Println("executeLoop: want execute task in: ", t.time)
		if err != nil {
			return err
		}
		go t.run()
		for {
			select {
			case te := <-t.taskEvents:
				switch te.Type {
				case task.EventTypeRunning:
					if er := sc.s.CompareAndUpdateTaskExecutionStatus(ctx, t.task.TaskId,
						task.EventTypeInit, task.EventTypeRunning); err != nil {
						log.Println("sche running: ", er)
					}
					log.Println("scheduler 收到task running信号")
				case task.EventTypeSuccess:
					if er := sc.s.CompareAndUpdateTaskExecutionStatus(ctx, t.task.TaskId,
						task.EventTypeRunning, task.EventTypeSuccess); err != nil {
						log.Println("sche succ: ", er)
					}
					log.Println("scheduler 收到task run success信号:", t.task.TaskId)
				case task.EventTypeFailed:
					if er := sc.s.CompareAndUpdateTaskExecutionStatus(ctx, t.task.TaskId,
						task.EventTypeRunning, task.EventTypeFailed); err != nil {
						log.Println("sche fail: ", er)
					}
					log.Println("scheduler 收到task run fail信号")
				}
				sc.taskEvents <- te
			}
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

func (r *scheduledTask) run() {
	// 如果这个任务已经被停止/取消了，什么也不做
	if r.stopped {
		return
	}
	select {
	case r.taskEvents <- task.Event{Task: *r.task, Type: task.EventTypeRunning}:
		log.Printf("task id: %d, is running", r.task.TaskId)
	}
	// 这里executor返回一个task.Event,表示任务的执行状态
	taskEvent := r.executor.Execute(r.task)
	select {
	case e := <-taskEvent:
		r.taskEvents <- e
		switch e.Type {
		case task.EventTypeFailed, task.EventTypeSuccess:
			return
		}
	}
}
