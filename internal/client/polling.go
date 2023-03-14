package client

import (
	"github.com/ecodeclub/ecron/internal/executor"
)

type Polling struct {
}

func (p *Polling) Init(mode ClientMode) error {
	//TODO implement me
	panic("implement me")
}

func (p *Polling) Heartbeat() error {
	//TODO implement me
	panic("implement me")
}

func (p *Polling) InitTask(name string) error {
	//TODO implement me
	panic("implement me")
}

func (p *Polling) DeleteTask(name string) error {
	//TODO implement me
	panic("implement me")
}

func (p *Polling) GetStatus(name string) (executor.EventType, error) {
	//TODO implement me
	panic("implement me")
}
