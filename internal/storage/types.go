package storage

import (
	"context"
	"github.com/ecodeclub/ecron/internal/task"
	"time"
)

//go:generate mockgen -source=./types.go -package=daomocks -destination=./mocks/dao.mock.go

type TaskCfgRepository interface {
	// Add 添加任务
	Add(ctx context.Context, t task.Task) error
	// Stop 停止任务
	Stop(ctx context.Context, id int64) error
	// UpdateNextTime 更新下次执行时间
	UpdateNextTime(ctx context.Context, id int64, next time.Time) error
}

// ExecutionDAO 任务执行情况
type ExecutionDAO interface {
	// Upsert 记录任务执行状态和进度
	Upsert(ctx context.Context, id int64, status task.ExecStatus, progress uint8) (int64, error)
}
