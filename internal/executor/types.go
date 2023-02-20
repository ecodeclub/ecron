package executor

import (
	"github.com/gotomicro/ecron/internal/task"
)

type EventType string

type Event struct {
	Type EventType
}

const (
	// EventTypeWaiting 等待当前正在执行的任务运行完成
	EventTypeWaiting = "waiting"
	// EventTypeFailed 当前正在执行的任务运行失败
	EventTypeFailed = "failed"
	// EventTypeSuccess 当前正在执行的任务运行成功
	EventTypeSuccess = "success"
)

// Executor 执行器，它是用户任务逻辑在该系统的映射
type Executor interface {
	// Execute 执行任务
	Execute(t *task.Task) (<-chan Event, error)
}
