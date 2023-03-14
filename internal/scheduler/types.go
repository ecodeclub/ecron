package scheduler

import (
	"context"
	"sync"
	"time"

	"github.com/ecodeclub/ecron/internal/executor"
	"github.com/ecodeclub/ecron/internal/storage"
	"github.com/ecodeclub/ecron/internal/task"
	"github.com/gorhill/cronexpr"
	"github.com/gotomicro/ekit/queue"
)

type Scheduler struct {
	s             storage.Storager
	tasks         map[string]scheduledTask
	executors     map[string]executor.Executor
	mux           sync.Mutex
	readyTasks    *queue.DelayQueue[execution]
	taskEvents    chan task.Event
	executeEvents chan executeEvent
}

type scheduledTask struct {
	ctx       context.Context
	task      *task.Task
	executeId int64
	executor  executor.Executor
	expr      *cronexpr.Expression
	stopped   bool
}

type executeEvent struct {
	task  scheduledTask
	event executor.Event
}

type execution struct {
	*scheduledTask
	time time.Time
}

func (e execution) Delay() time.Duration {
	//return e.time.Sub(time.Now())
	return time.Until(e.time)
}
