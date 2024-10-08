package http

import (
	"fmt"
)

type Registry struct {
	tasks map[string]Task
}

func NewRegistry() *Registry {
	return &Registry{
		tasks: make(map[string]Task),
	}
}

func (r *Registry) Register(tasks ...Task) error {
	for _, t := range tasks {
		if _, exist := r.tasks[t.Name()]; exist {
			return fmt.Errorf("duplicated task: %s", t.Name())
		}
		r.tasks[t.Name()] = t
	}
	return nil
}

func (r *Registry) GetTask(name string) (Task, bool) {
	t, ok := r.tasks[name]
	return t, ok
}
