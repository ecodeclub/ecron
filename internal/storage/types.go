package storage

import (
	"context"
	"github.com/ecodeclub/ecron/internal/task"
	"time"
)

type TaskDAO interface {
	// Get 获取一个任务
	Get(ctx context.Context) (task.Task, error)
	// Add 添加任务
	Add(ctx context.Context, t task.Task) error
	// Release 释放任务
	Release(ctx context.Context, t task.Task) error
	// Stop 停止任务
	Stop(ctx context.Context, id int64) error
	UpdateNextTime(ctx context.Context, id int64, next time.Time) error
	UpdateUtime(ctx context.Context, id int64) error
}

// HistoryDAO 任务执行历史
type HistoryDAO interface {
	Add(ctx context.Context, t task.Task, status int) error
}
