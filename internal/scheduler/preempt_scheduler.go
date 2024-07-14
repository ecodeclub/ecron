package scheduler

import (
	"context"
	"github.com/ecodeclub/ecron/internal/executor"
	"github.com/ecodeclub/ecron/internal/storage"
	"github.com/ecodeclub/ecron/internal/task"
	"golang.org/x/sync/semaphore"
	"log/slog"
	"time"
)

type PreemptScheduler struct {
	dao             storage.TaskDAO
	history         storage.HistoryDAO
	executors       map[string]executor.Executor
	refreshInterval time.Duration
	limiter         *semaphore.Weighted
	logger          *slog.Logger
}

func NewPreemptScheduler(dao storage.TaskDAO, history storage.HistoryDAO,
	refreshInterval time.Duration, limiter *semaphore.Weighted, logger *slog.Logger) *PreemptScheduler {
	return &PreemptScheduler{
		dao:             dao,
		history:         history,
		refreshInterval: refreshInterval,
		limiter:         limiter,
		executors:       make(map[string]executor.Executor),
		logger:          logger,
	}
}

func (p *PreemptScheduler) RegisterExecutor(execs ...executor.Executor) {
	for _, exec := range execs {
		p.executors[exec.Name()] = exec
	}
}

func (p *PreemptScheduler) Schedule(ctx context.Context) error {
	for {
		err := p.limiter.Acquire(ctx, 1)
		if err != nil {
			return err
		}

		ctx2, cancel := context.WithTimeout(ctx, time.Second*3)
		t, err := p.dao.Preempt(ctx2)
		cancel()
		if err != nil {
			continue
		}
		exec, ok := p.executors[t.Executor]
		if !ok {
			p.logger.Error("找不到任务的执行器",
				slog.Int64("TaskID", t.ID),
				slog.String("Executor", t.Executor))
			continue
		}

		go p.doTask(ctx, t, exec)
		// 更新下一次的执行时间
		err = p.setNextTime(t)
		if err != nil {
			p.logger.Error("更新下一次执行时间出错",
				slog.Int64("TaskID", t.ID),
				slog.Any("error", err))
		}
	}
}

func (p *PreemptScheduler) doTask(ctx context.Context, t task.Task, exec executor.Executor) {
	p.recordExecHistory(t.ID, task.ExecStatusStarted)
	ctx2, cancel2 := context.WithCancel(ctx)
	defer cancel2()
	ticker := time.NewTicker(p.refreshInterval)
	go func() {
		err := p.refreshTask(ctx2, ticker, t.ID)
		if err != nil {
			// 续约失败时，通知用户停止执行任务
			cancel2()
		}
	}()
	err := exec.Run(ctx2, t)
	ticker.Stop()
	if err != nil {
		p.recordExecHistory(t.ID, task.ExecStatusFailed)
		p.logger.Error("任务执行失败",
			slog.Int64("TaskID", t.ID),
			slog.Any("error", err))
	} else {
		p.recordExecHistory(t.ID, task.ExecStatusSuccess)
	}

	p.releaseTask(t)
	p.limiter.Release(1)
}

func (p *PreemptScheduler) recordExecHistory(id int64, status task.ExecStatus) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	_ = p.history.Add(ctx, id, status)
	cancel()
}

func (p *PreemptScheduler) refreshTask(ctx context.Context, ticker *time.Ticker, id int64) error {
	for {
		select {
		case <-ticker.C:
			ctx2, cancel := context.WithTimeout(context.Background(), time.Second*3)
			err := p.dao.UpdateUtime(ctx2, id)
			cancel()
			if err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (p *PreemptScheduler) releaseTask(t task.Task) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	err := p.dao.Release(ctx, t)
	if err != nil {
		p.logger.Error("释放任务失败",
			slog.Int64("TaskID", t.ID),
			slog.Any("error", err))
	}
}

func (p *PreemptScheduler) setNextTime(t task.Task) error {
	next, err := t.NextTime()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	if next.IsZero() {
		err := p.dao.Stop(ctx, t.ID)
		if err != nil {
			p.logger.Error("停止任务调度失败",
				slog.Int64("TaskID", t.ID),
				slog.Any("error", err))
		}
		return err
	}
	return p.dao.UpdateNextTime(ctx, t.ID, next)
}
