package scheduler

import (
	"context"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/ecodeclub/ecron/internal/executor"
	"github.com/ecodeclub/ecron/internal/storage"
	"github.com/ecodeclub/ecron/internal/task"
	"github.com/gorhill/cronexpr"
	"github.com/gotomicro/ekit/queue"
)

func NewScheduler(s storage.Storager) *Scheduler {
	sc := &Scheduler{
		s:             s,
		tasks:         make(map[string]scheduledTask),
		executors:     make(map[string]executor.Executor),
		mux:           sync.Mutex{},
		readyTasks:    queue.NewDelayQueue[execution](10),
		taskEvents:    make(chan task.Event),
		executeEvents: make(chan executeEvent),
	}

	sc.executors = map[string]executor.Executor{
		task.TypeHTTP: executor.NewHttpExec(http.DefaultClient),
	}

	return sc
}

func (sc *Scheduler) Start(ctx context.Context) error {
	go func() {
		// 这里进行已经写入延迟队列中的事件执行
		if e := sc.executeLoop(ctx); e != nil {
			log.Println(e)
		}
	}()
	go func() {
		// 这里监听执行的结果
		if e := sc.executeEventLoop(ctx); e != nil {
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

// 负责从队列中获取当前可以执行的任务
func (sc *Scheduler) executeLoop(ctx context.Context) error {
	for {
		t, err := sc.readyTasks.Dequeue(ctx)
		log.Println("executeLoop: want execute task in: ", t.time)
		if err != nil {
			return err
		}
		go t.run(sc.executeEvents)
	}
}

// 负责统一监听任务执行的后续
// 目前实现的为轮询模式
// 同时负责新任务的执行和旧任务的轮训查询
// 后续父子任务的思路 想基于scheduledTask的用链表串联后续任务
func (sc *Scheduler) executeEventLoop(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			log.Println("scheduler 收到ctx cancel信号 退出执行返回事件的监听")
		case te := <-sc.executeEvents:
			needEnqueue := false
			event := task.Event{
				Task: *te.task.task,
			}
			now := time.Now()
			next := now.Add(time.Second)
			switch te.event.Type {
			case executor.ExecuteWaiting:
				if err := sc.s.CompareAndUpdateTaskExecutionStatus(ctx, te.task.task.TaskId,
					task.EventTypePreempted, task.EventTypeRunnable); err != nil {
					log.Println(err)
				}
				event.Type = task.EventTypeRunnable
				if te.event.Delay > time.Second {
					next = now.Add(te.event.Delay)
				}
				log.Println("scheduler 收到task runnable信号")
			case executor.ExecuteReady:
				if err := sc.s.CompareAndUpdateTaskExecutionStatus(ctx, te.task.task.TaskId,
					task.EventTypeRunnable, task.EventTypeInit); err != nil {
					log.Println(err)
				}
				event.Type = task.EventTypeInit
				if te.event.Delay > time.Second {
					next = now.Add(te.event.Delay)
				}
				log.Println("scheduler 收到task init信号")
			case executor.ExecuteRunning:
				if err := sc.s.CompareAndUpdateTaskExecutionStatus(ctx, te.task.task.TaskId,
					task.EventTypeInit, task.EventTypeRunning); err != nil {
					log.Println(err)
				}
				event.Type = task.EventTypeRunning
				if te.event.Delay > time.Second {
					next = now.Add(te.event.Delay)
				}
				log.Println("scheduler 收到task running信号")
			case executor.ExecuteSuccess:
				//反馈进度至Storage
				if err := sc.s.CompareAndUpdateTaskExecutionStatus(ctx, te.task.task.TaskId,
					task.EventTypeRunning, task.EventTypeSuccess); err != nil {
					log.Println(err)
				}
				needEnqueue = true
				event.Type = task.EventTypeSuccess
				next = te.task.next()
				log.Println("scheduler 收到task run success信号")
			case executor.ExecuteFailed:
				if err := sc.s.CompareAndUpdateTaskExecutionStatus(ctx, te.task.task.TaskId,
					task.EventTypeRunning, task.EventTypeFailed); err != nil {
					log.Println(err)
				}
				needEnqueue = true
				event.Type = task.EventTypeFailed
				next = te.task.next()
				log.Println("scheduler 收到task run fail信号")
			}
			if needEnqueue {
				//安排下一次执行
				_ = sc.readyTasks.Enqueue(ctx, execution{
					scheduledTask: &te.task,
					time:          next,
				})
			}
			sc.taskEvents <- event
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
		ctx:       ctx,
		task:      t,
		executor:  exe,
		executeId: exeId,
		expr:      cronexpr.MustParse(t.Cron),
	}
	return st
}

func (r *scheduledTask) next() time.Time {
	return r.expr.Next(time.Now())
}

func (r *scheduledTask) stop() {
	r.stopped = true
}

func (r *scheduledTask) run(ec chan<- executeEvent) {
	// 如果这个任务已经被停止/取消了，什么也不做
	if r.stopped {
		return
	}
	// 如果进行后续监控的任务是由专门的goroutine负责
	// 且此处只是等待executor的返回、没有其他任务，并且是快速调用快速返回
	// 则应该不需要利用chan通信
	event := r.executor.Execute(r.ctx, r.task)
	select {
	case ec <- executeEvent{
		task:  *r,
		event: event,
	}:
		log.Printf(`task(%v) 已执行`, r.task.TaskId)
	case <-r.ctx.Done(): // 利用ctx避免泄露 控制结束
		log.Printf(`task(%v) 收到ctx结束信号`, r.task.TaskId)
	}
}
