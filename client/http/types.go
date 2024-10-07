package http

//go:generate mockgen -source=./types.go -package=taskmocks -destination=./mocks/task.mock.go
type Task interface {
	Execute() (Status, int)
	Status() (Status, int)
	Stop() error
	Name() string
}

type Status string

const (
	StatusSuccess Status = "SUCCESS"
	StatusFailed  Status = "FAILED"
	StatusRunning Status = "RUNNING"
)
