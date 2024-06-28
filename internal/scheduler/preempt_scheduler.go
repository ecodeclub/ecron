package scheduler

import (
	"context"
	"github.com/ecodeclub/ecron/internal/executor"
	"github.com/ecodeclub/ecron/internal/storage"
	"github.com/ecodeclub/ecron/internal/task"
	"golang.org/x/sync/semaphore"
	"log"
	"time"
)

type PreemptScheduler struct {
	dao             storage.TaskDAO
	history         storage.HistoryDAO
	executors       map[string]executor.Executor
	refreshInterval time.Duration
	limiter         *semaphore.Weighted
}

func NewPreemptScheduler(dao storage.TaskDAO, history storage.HistoryDAO, refreshInterval time.Duration, limiter *semaphore.Weighted) *PreemptScheduler {
	return &PreemptScheduler{
		dao:             dao,
		history:         history,
		refreshInterval: refreshInterval,
		limiter:         limiter,
		executors:       make(map[string]executor.Executor),
	}
}

func (p *PreemptScheduler) RegisterExecutor(execs ...executor.Executor) {
	for _, exec := range execs {
		p.executors[exec.Name()] = exec
	}
}

func (p *PreemptScheduler) Schedule(ctx context.Context) error {
	for {
		if ctx.Err() != nil {
			log.Println("退出调度")
			return ctx.Err()
		}
		err := p.limiter.Acquire(ctx, 1)
		if err != nil {
			time.Sleep(time.Second)
		}
		ctx2, cancel := context.WithTimeout(ctx, time.Second*3)
		t, err := p.dao.Get(ctx2)
		cancel()
		if err != nil {
			continue
		}
		exec, ok := p.executors[t.Executor]
		if !ok {
			log.Println("找不任务的执行器", t.ID, t.Executor)
		}
		go func() {
			// 执行任务
			ticker := time.NewTicker(p.refreshInterval)
			p.before(t, ticker)
			err := exec.Run(ctx, t)
			ticker.Stop()
			if err != nil {
				p.after(t, task.TaskHistoryStatusFail)
				log.Println("任务执行出错", err, t.ID)
			} else {
				p.after(t, task.TaskHistoryStatusSuccess)
			}
			p.limiter.Release(1)
		}()
		// 更新下一次的执行时间
		err = p.setNextTime(t)
		if err != nil {
			log.Println("更新下一次执行时间出错", err, t.ID)
		}
	}
}

func (p *PreemptScheduler) before(t task.Task, ticker *time.Ticker) {
	// 执行前，更新一下任务执行历史，
	// 并且开启续约
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	p.history.Add(ctx, t, task.TaskHistoryStatusStart)
	cancel()
	go func() {
		for range ticker.C {
			// 在这里面续约
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
			p.dao.UpdateUtime(ctx, t.ID)
			cancel()
		}
	}()
}

func (p *PreemptScheduler) after(t task.Task, status int) {
	// 执行完后，更新一下任务执行历史，
	// 以及释放任务
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	p.history.Add(ctx, t, status)
	err := p.dao.Release(ctx, t)
	if err != nil {
		log.Println("释放任务失败", err, t.ID)
	}
}

func (p *PreemptScheduler) setNextTime(t task.Task) error {
	next := t.NextTime()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	if next.IsZero() {
		err := p.dao.Stop(ctx, t.ID)
		if err != nil {
			log.Println("停止任务调度失败", t.ID, err)
		}
		return err
	}
	return p.dao.UpdateNextTime(ctx, t.ID, next)
}
