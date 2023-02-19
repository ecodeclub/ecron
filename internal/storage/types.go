package storage

import (
	"context"

	"github.com/gotomicro/ecron/internal/task"
)

type EventType string

const (
	// EventTypePreempted 抢占了一个任务
	EventTypePreempted = "preempted"
	// EventTypeDeleted 某一个任务被删除了
	EventTypeDeleted      = "deleted"
	EventTypeCreated      = "created"
	EventTypeNotPreempted = "not-preempted"
	EventTypeRunnable     = "runnable"
	EventTypeEnd          = "end"
)

type Storager interface {
	// Events
	// ctx 结束的时候，Storage 也要结束
	// 实现者需要处理 taskEvents
	Events(ctx context.Context, taskEvents <-chan task.Event) (<-chan Event, error)
	TaskDAO
}

type TaskDAO interface {
	Get(ctx context.Context, status string) (*task.Task, error)
	//GetMulti(ctx context.Context, status string, epoch int64) []*task.Task
	Add(ctx context.Context, t *task.Task, dao ...any) (int64, error)
	Update(ctx context.Context, t *task.Task, dao ...any) error
	Delete(ctx context.Context, taskId int64) error
}

type Event struct {
	Type EventType
	Task *task.Task
}
