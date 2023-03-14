package storage

import (
	"context"

	"github.com/ecodeclub/ecron/internal/task"
)

type EventType string

const (
	// EventTypePreempted 抢占了一个任务
	EventTypePreempted = "preempted"
	// EventTypeDeleted 某一个任务被删除了
	EventTypeDeleted  = "deleted"
	EventTypeCreated  = "created"
	EventTypeRunnable = "runnable"
	EventTypeEnd      = "end"
)

type Storager interface {
	// Events
	// ctx 结束的时候，Storage 也要结束
	// 实现者需要处理 taskEvents
	Events(ctx context.Context, taskEvents <-chan task.Event) (<-chan Event, error)
	TaskDAO
}

type TaskDAO interface {
	Get(ctx context.Context, taskId int64) (*task.Task, error)
	Add(ctx context.Context, t *task.Task) (int64, error)
	AddExecution(ctx context.Context, taskId int64) (int64, error)
	Update(ctx context.Context, t *task.Task) error
	CompareAndUpdateTaskStatus(ctx context.Context, taskId int64, old, new string) error
	CompareAndUpdateTaskExecutionStatus(ctx context.Context, taskId int64, old, new string) error
	Delete(ctx context.Context, taskId int64) error
}

type Event struct {
	Type EventType
	Task *task.Task
}

type Status struct {
	ExpectStatus string
	UseStatus    string
}
