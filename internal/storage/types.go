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
	EventTypeDeleted = "deleted"
)

type Storage interface {
	// Events
	// ctx 结束的时候，Storage 也要结束
	// 实现者需要处理 taskEvents
	Events(ctx context.Context, taskEvents <-chan task.Event) (<-chan Event, error)
	TaskDAO
}

type TaskDAO interface {
	Add(t *task.Task) error
	Update(t *task.Task, status map[string]string) error
	Delete(name string) error
}

type Event struct {
	Type EventType
	Task *task.Task
}

func NewEvent(event Event) Event {
	return Event{}
}
