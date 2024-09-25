package scheduler

import (
	"context"
	"errors"
	"github.com/ecodeclub/ecron/internal/executor"
	"github.com/ecodeclub/ecron/internal/preempt"
	"github.com/ecodeclub/ecron/internal/storage"
	"github.com/ecodeclub/ecron/internal/task"
	"golang.org/x/sync/semaphore"
	"log/slog"
	"time"
)

type PreemptScheduler struct {
	executionDAO      storage.ExecutionDAO
	taskCfgRepository storage.TaskCfgRepository
	executors         map[string]executor.Executor
	refreshInterval   time.Duration
	limiter           *semaphore.Weighted
	logger            *slog.Logger
	pe                preempt.Preempter
}

func NewPreemptScheduler(executionDAO storage.ExecutionDAO,
	refreshInterval time.Duration, limiter *semaphore.Weighted, logger *slog.Logger,
	preempter preempt.Preempter, taskCfgRepository storage.TaskCfgRepository) *PreemptScheduler {
	return &PreemptScheduler{
		executionDAO:      executionDAO,
		refreshInterval:   refreshInterval,
		limiter:           limiter,
		executors:         make(map[string]executor.Executor),
		logger:            logger,
		pe:                preempter,
		taskCfgRepository: taskCfgRepository,
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
			return ctx.Err()
		}

		err := p.limiter.Acquire(ctx, 1)
		if err != nil {
			return err
		}

		timeout, cancel := context.WithTimeout(ctx, time.Second*3)
		leaser, err := p.pe.Preempt(timeout)
		cancel()
		if err != nil {
			p.logger.Error("抢占任务失败,可能没有任务了",
				slog.Any("error", err))
			p.limiter.Release(1)
			time.Sleep(time.Second * 3)
			continue
		}

		t := leaser.GetTask()
		exec, ok := p.executors[t.Executor]
		if !ok {
			p.logger.Error("找不到任务的执行器",
				slog.Int64("TaskID", t.ID),
				slog.String("Executor", t.Executor))
			p.limiter.Release(1)
			err1 := leaser.Release(ctx)
			if err1 != nil {
				p.logger.Error("于抢占后释放任务失败",
					slog.Int64("TaskID", t.ID),
					slog.Any("err1", err1))
			}
			continue
		}

		go p.doTaskWithAutoRefresh(ctx, leaser, exec)
	}
}

func (p *PreemptScheduler) doTaskWithAutoRefresh(ctx context.Context, l preempt.TaskLeaser, exec executor.Executor) {

	cancelCtx, cancelCause := context.WithCancelCause(ctx)
	t := l.GetTask()
	defer func() {
		ctx1, cancel := context.WithTimeout(ctx, time.Second*3)
		defer cancel()

		err := l.Release(ctx1)
		if err != nil {
			p.logger.Error("停止任务异常", slog.Int64("task_id", t.ID), slog.Any("error", err))
		}
	}()

	go func() {
		ch, err := l.AutoRefresh(cancelCtx)
		if err != nil {
			cancelCause(err)
			return
		}

		for {
			s, ok := <-ch
			if ok && s.Err() != nil {
				p.logger.Error(s.Err().Error(), slog.Int64("TaskID", t.ID))
				cancelCause(s.Err())
				return
			}
		}
	}()

	p.doTask(ctx, t, exec)
}

func (p *PreemptScheduler) doTask(ctx context.Context, t task.Task, exec executor.Executor) {
	defer p.limiter.Release(1)
	//defer p.releaseTask(t)

	// 任务执行超时配置
	timeout := exec.TaskTimeout(t)

	eid, err := p.updateProgressStatus(t.ID, 0, task.ExecStatusRunning)
	if err != nil {
		// 这里我直接返回，如果只是网络抖动，那么任务释放后，不修改下一次执行时间，该任务可以立刻再次被抢占执行。
		// 如果是数据库异常，则无法记录任务执行情况，那么放弃这一次执行。
		return
	}
	// 控制任务执行时长
	execCtx, execCancel := context.WithDeadline(ctx, time.Now().Add(timeout))
	defer execCancel()

	status, _ := exec.Run(execCtx, t, eid)
	defer p.setNextTime(t)

	err = p.reportExecuteResult(status, t.ID)
	if err != nil {
		p.logger.Error("上报执行结果失败", slog.Int64("task_id", t.ID),
			slog.String("exec_status", status.String()), slog.Any("error", err))
	}

	if status == task.ExecStatusRunning {
		p.explore(execCtx, exec, eid, t)
	}
}

func (p *PreemptScheduler) reportExecuteResult(status task.ExecStatus, id int64) error {
	var err error
	switch status {
	case task.ExecStatusSuccess:
		_, err = p.updateProgressStatus(id, 100, task.ExecStatusSuccess)
	case task.ExecStatusDeadlineExceeded:
		_, err = p.updateProgressStatus(id, 0, task.ExecStatusDeadlineExceeded)
	case task.ExecStatusCancelled:
		_, err = p.updateProgressStatus(id, 0, task.ExecStatusCancelled)
	case task.ExecStatusRunning:
		_, err = p.updateProgressStatus(id, 0, task.ExecStatusRunning)
	default:
		_, _ = p.updateProgressStatus(id, 0, task.ExecStatusFailed)
	}
	return err
}

func (p *PreemptScheduler) explore(ctx context.Context, exec executor.Executor, eid int64, t task.Task) {
	ch := exec.Explore(ctx, eid, t)
	if ch == nil {
		return
	}
	// 保存每一次探查时的进度，确保执行ctx.Done()分支时进度不会更新为零值
	progress := 0
	for {
		select {
		case <-ctx.Done():
			var status string
			// 主动取消或者超时
			err := ctx.Err()
			if errors.Is(err, context.DeadlineExceeded) {
				_, err = p.updateProgressStatus(t.ID, progress, task.ExecStatusDeadlineExceeded)
				status = task.ExecStatusDeadlineExceeded.String()
			} else {
				_, err = p.updateProgressStatus(t.ID, progress, task.ExecStatusCancelled)
				status = task.ExecStatusCancelled.String()
			}

			if err != nil {
				p.logger.Error("更新最终执行结果失败", slog.Int64("task_id", t.ID),
					slog.String("exec_status", status), slog.Int("progress", progress),
					slog.Any("error", err))
			}
			return
		case res, ok := <-ch:
			if !ok {
				return
			}

			progress = res.Progress
			status := p.from(res.Status)
			_, err := p.updateProgressStatus(t.ID, progress, status)
			if err != nil {
				p.logger.Error("上报探查结果失败", slog.Int64("task_id", t.ID),
					slog.String("exec_status", status.String()), slog.Int("progress", progress),
					slog.Any("error", err))
			}

			if status != task.ExecStatusRunning {
				return
			}
		}
	}
}

func (p *PreemptScheduler) from(status executor.Status) task.ExecStatus {
	switch status {
	case executor.StatusSuccess:
		return task.ExecStatusSuccess
	case executor.StatusFailed:
		return task.ExecStatusFailed
	default:
		return task.ExecStatusRunning
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
		err := p.taskCfgRepository.Stop(ctx, t.ID)
		if err != nil {
			p.logger.Error("停止任务调度失败",
				slog.Int64("TaskID", t.ID),
				slog.Any("error", err))
		}
	}
	err = p.taskCfgRepository.UpdateNextTime(ctx, t.ID, next)
	if err != nil {
		p.logger.Error("更新下一次执行时间出错",
			slog.Int64("TaskID", t.ID),
			slog.Any("error", err))
	}
}

func (p *PreemptScheduler) updateProgressStatus(tid int64, progress int, status task.ExecStatus) (int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	eid, err := p.executionDAO.Upsert(ctx, tid, status, uint8(progress))
	return eid, err
}
