package executor

import (
	"context"
	"github.com/ecodeclub/ecron/internal/errs"
	"github.com/ecodeclub/ecron/internal/task"
	"log/slog"
	"time"
)

type LocalExecutor struct {
	logger *slog.Logger
	fn     map[string]func(ctx context.Context, t task.Task) error
}

func NewLocalExecutor(logger *slog.Logger) *LocalExecutor {
	return &LocalExecutor{
		logger: logger,
		fn:     make(map[string]func(ctx context.Context, t task.Task) error),
	}
}

func (l *LocalExecutor) RegisterFunc(name string, fn func(ctx context.Context, t task.Task) error) {
	l.fn[name] = fn
}

func (l *LocalExecutor) Name() string {
	return "LOCAL"
}

func (l *LocalExecutor) Run(ctx context.Context, t task.Task, eid int64) error {
	fn, ok := l.fn[t.Name]
	if !ok {
		l.logger.Error("未知执行方法的任务",
			slog.Int64("ID", t.ID),
			slog.String("Name", t.Name))
		return errs.ErrUnknownTask
	}
	return fn(ctx, t)
}

type LocalCfg struct {
	TaskTimeout time.Duration `json:"taskTimeout"`
}
