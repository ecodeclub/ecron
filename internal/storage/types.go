package storage

import (
	"context"
	"github.com/ecodeclub/ecron/internal/task"
	"time"
)

//go:generate mockgen -source=./types.go -package=daomocks -destination=./mocks/dao.mock.go

type TaskDAO interface {
	// Preempt 获取一个任务
	Preempt(ctx context.Context) (task.Task, error)
	// Add 添加任务
	Add(ctx context.Context, t task.Task) error
	// Release 释放任务
	Release(ctx context.Context, t task.Task) error
	// Stop 停止任务
	Stop(ctx context.Context, id int64) error
	UpdateNextTime(ctx context.Context, id int64, next time.Time) error
	UpdateUtime(ctx context.Context, id int64) error
}

// ExecutionDAO 任务执行情况
type ExecutionDAO interface {
	InsertExecStatus(ctx context.Context, id int64, status task.ExecStatus) error
}
