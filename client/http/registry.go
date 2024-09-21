package http

type Registry struct {
	tasks map[string]EcronTask
}

func NewRegistry() *Registry {
	return &Registry{
		tasks: make(map[string]EcronTask),
	}
}

func (r *Registry) Register(tasks ...EcronTask) {
	for _, t := range tasks {
		if _, exist := r.tasks[t.Task.Name()]; exist {
			panic("duplicated task: " + t.Task.Name())
		}
		r.tasks[t.Task.Name()] = t
	}
}
