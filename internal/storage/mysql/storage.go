package mysql

import (
	"context"
	"github.com/gotomicro/ecron/internal/storage"
	"github.com/gotomicro/ecron/internal/task"
)

type Storage struct {
	events chan storage.Event
}

func (s *Storage) Add(t *task.Task) error {
	//TODO implement me
	panic("implement me")
}

func (s *Storage) Update(t *task.Task) error {
	//TODO implement me
	panic("implement me")
}

func (s *Storage) Delete(name string) error {
	//TODO implement me
	panic("implement me")
}

func (s *Storage) Events(ctx context.Context, taskEvents <-chan task.Event) (<-chan storage.Event, error) {
	go func() {
		for {
			select {
			case <-ctx.Done():
			case event := <-taskEvents:
				switch event.Type {
				case task.EventTypeRunning:
				case task.EventTypeSuccess:
				case task.EventTypeFailed:
				}
			}
		}
	}()
	return s.events, nil
}
