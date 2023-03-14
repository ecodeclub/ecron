package client

import "github.com/ecodeclub/ecron/internal/executor"

type ClientMode int

const (
	PollingMode = iota + 1
	EventMode
)

type Client interface {
	Init(mode ClientMode) error
	Heartbeat() error
	InitTask(name string) error
	DeleteTask(name string) error
	GetStatus(name string) (executor.EventType, error)
}

func NewClient(mode ClientMode) Client {
	switch mode {
	case PollingMode:
		return &Polling{}
	}
	return nil
}
