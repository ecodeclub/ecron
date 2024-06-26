package executor

import (
	"context"
	"errors"
	"github.com/ecodeclub/ecron/internal/task"
)

var ErrUnknownJob = errors.New("未知的任务，注册器里找不到该方法")

type LocalExecutor struct {
	fn map[string]func(ctx context.Context, t task.Task) error
}

func NewLocalExecutor() *LocalExecutor {
	return &LocalExecutor{fn: make(map[string]func(ctx context.Context, t task.Task) error)}
}

func (l *LocalExecutor) RegisterFunc(name string, fn func(ctx context.Context, t task.Task) error) {
	l.fn[name] = fn
}

func (l *LocalExecutor) Name() string {
	return "local"
}

func (l *LocalExecutor) Run(ctx context.Context, t task.Task) error {
	fn, ok := l.fn[t.Name]
	if !ok {
		return ErrUnknownJob
	}
	return fn(ctx, t)
}
