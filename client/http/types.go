package http

type Task interface {
	Execute(t Task) (Status, int)
	Status(t Task) (Status, int)
	Stop(t Task) error
	Name() string
}

type Status string

const (
	StatusSuccess Status = "SUCCESS"
	StatusFailed  Status = "FAILED"
	StatusRunning Status = "RUNNING"
)
