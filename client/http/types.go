package http

import "time"

type Task interface {
	Execute() (Status, int)
	Status() (Status, int)
	Stop() error
	Name() string
}

type EcronTask struct {
	Task
	Cron     string
	Timeout  time.Duration // 任务的执行时长
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

type Status string

const (
	StatusSuccess Status = "SUCCESS"
	StatusFailed  Status = "FAILED"
	StatusRunning Status = "RUNNING"
)
