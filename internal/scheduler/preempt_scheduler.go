package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/ecodeclub/ecron/internal/errs"
	"github.com/ecodeclub/ecron/internal/executor"
	"github.com/ecodeclub/ecron/internal/storage"
	"github.com/ecodeclub/ecron/internal/task"
	"golang.org/x/sync/semaphore"
	"log/slog"
	"time"
)

type PreemptScheduler struct {
	dao             storage.TaskDAO
	executionDAO    storage.ExecutionDAO
	executors       map[string]executor.Executor
	refreshInterval time.Duration
	limiter         *semaphore.Weighted
	logger          *slog.Logger
}

func NewPreemptScheduler(dao storage.TaskDAO, executionDAO storage.ExecutionDAO,
	refreshInterval time.Duration, limiter *semaphore.Weighted, logger *slog.Logger) *PreemptScheduler {
	return &PreemptScheduler{
		dao:             dao,
		executionDAO:    executionDAO,
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
	}
}

func (p *PreemptScheduler) doTask(ctx context.Context, t task.Task, exec executor.Executor) {
	timeout, err := p.getTaskTimeout(t)
	if err != nil || timeout == 0 {
		// 尽力去调度一个任务，设置一个默认的执行时间
		timeout = time.Second * 3
	}

	eid := p.markStatus(t.ID, task.ExecStatusStarted)
	ctx2, cancel2 := context.WithTimeout(ctx, timeout)
	defer cancel2()
	ticker := time.NewTicker(p.refreshInterval)
	go func() {
		err := p.refreshTask(ctx2, ticker, t.ID)
		if err != nil {
			// 续约失败时，通知用户停止执行任务
			cancel2()
		}
	}()

	err = exec.Run(ctx2, t, eid)
	ticker.Stop()
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		p.markStatus(t.ID, task.ExecStatusDeadlineExceeded)
	case errors.Is(err, context.Canceled):
		p.markStatus(t.ID, task.ExecStatusCancelled)
	case err == nil:
		p.markStatus(t.ID, task.ExecStatusSuccess)
	default:
		p.markStatus(t.ID, task.ExecStatusFailed)
	}

	p.setNextTime(t)
	p.releaseTask(t)
	p.limiter.Release(1)
}

func (p *PreemptScheduler) getTaskTimeout(t task.Task) (time.Duration, error) {
	switch t.Type {
	case task.TypeHttp:
		var cfg executor.HttpCfg
		err := json.Unmarshal([]byte(t.Cfg), &cfg)
		if err != nil {
			p.logger.Error("任务配置信息错误",
				slog.Int64("ID", t.ID), slog.String("Cfg", t.Cfg))
			return 0, errs.ErrWrongTaskCfg
		}
		return cfg.TaskTimeout, nil
	case task.TypeLocal:
		var cfg executor.LocalCfg
		err := json.Unmarshal([]byte(t.Cfg), &cfg)
		if err != nil {
			p.logger.Error("任务配置信息错误",
				slog.Int64("ID", t.ID), slog.String("Cfg", t.Cfg))
			return 0, errs.ErrWrongTaskCfg
		}
		return cfg.TaskTimeout, nil
	default:
		return 0, errs.ErrUnknownTask
	}
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

func (p *PreemptScheduler) setNextTime(t task.Task) {
	next, err := t.NextTime()
	if err != nil {
		p.logger.Error("计算任务下一次执行时间失败",
			slog.Int64("TaskID", t.ID),
			slog.Any("error", err))
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
	}
	err = p.dao.UpdateNextTime(ctx, t.ID, next)
	if err != nil {
		p.logger.Error("更新下一次执行时间出错",
			slog.Int64("TaskID", t.ID),
			slog.Any("error", err))
	}
}

func (p *PreemptScheduler) markStatus(tid int64, status task.ExecStatus) int64 {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	var eid int64
	eid, err := p.executionDAO.InsertExecStatus(ctx, tid, status)
	if err != nil {
		p.logger.Error("记录任务执行失败",
			slog.Int64("TaskID", tid),
			slog.Any("error", err))
	}
	return eid
}
