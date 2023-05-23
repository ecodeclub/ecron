package executor

import (
	"github.com/ecodeclub/ecron/internal/task"
)

// Executor 执行器，它是用户任务逻辑在该系统的映射
type Executor interface {
	// Execute 执行任务
	Execute(t *task.Task) <-chan task.Event
}
