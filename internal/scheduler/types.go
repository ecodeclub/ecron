package scheduler

import (
	"sync"
	"time"

	"github.com/gorhill/cronexpr"
	"github.com/gotomicro/ecron/internal/executor"
	"github.com/gotomicro/ecron/internal/storage"
	"github.com/gotomicro/ecron/internal/task"
	"github.com/gotomicro/ekit/queue"
)

type Scheduler struct {
	s          storage.Storager
	tasks      map[string]scheduledTask
	executors  map[string]executor.Executor
	mux        sync.Mutex
	readyTasks *queue.DelayQueue[execution]
	taskEvents chan task.Event
}

type scheduledTask struct {
	task       *task.Task
	executeId  int64
	executor   executor.Executor
	expr       *cronexpr.Expression
	stopped    bool
	taskEvents chan task.Event
}

type execution struct {
	*scheduledTask
	time time.Time
}

func (e execution) Delay() time.Duration {
	//return e.time.Sub(time.Now())
	return time.Until(e.time)
}
