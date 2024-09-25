package http

import (
	"fmt"
	"time"
)

type Registry struct {
	tasks map[string]EcronTask
}

func NewRegistry() *Registry {
	return &Registry{
		tasks: make(map[string]EcronTask),
	}
}

func (r *Registry) Register(tasks ...EcronTask) error {
	for _, t := range tasks {
		if _, exist := r.tasks[t.Name()]; exist {
			return fmt.Errorf("duplicated task: %s", t.Name())
		}
		r.tasks[t.Name()] = t
	}
	return nil
}

type EcronTask struct {
	Task
	Cron     string
	Timeout  time.Duration // 任务的预计执行时长
	Interval time.Duration // 探查间隔
}

func NewTask(t Task, cron string, timeout, interval time.Duration) EcronTask {
	return EcronTask{
		Task:     t,
		Cron:     cron,
		Timeout:  timeout,
		Interval: interval,
	}
}
