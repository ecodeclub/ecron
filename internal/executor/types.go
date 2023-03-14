package executor

import (
	"context"
	"time"

	"github.com/ecodeclub/ecron/internal/task"
)

type EventType string

const (
	ExecuteReady   EventType = "ready"
	ExecuteWaiting EventType = "waiting"
	ExecuteRunning EventType = "running"
	ExecuteSuccess EventType = "success"
	ExecuteFailed  EventType = "failed"
)

type Event struct {
	Type  EventType     // 事件类型
	Delay time.Duration // 下一次查询的时间
}

// Executor 执行器，它是用户任务逻辑在该系统的映射
type Executor interface {
	// Execute 执行任务
	Execute(ctx context.Context, t *task.Task) Event
}
