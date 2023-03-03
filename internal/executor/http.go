package executor

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/gotomicro/ecron/internal/task"
)

type HttpExec struct{}

type RequestConf struct {
	Url     string
	Body    string
	Header  string
	Timeout int64
}

func NewHttpExec() *HttpExec {
	return &HttpExec{}
}

func (h *HttpExec) Execute(t *task.Task) <-chan task.Event {
	te := make(chan task.Event)
	go func() {
		te <- task.Event{Task: *t, Type: task.EventTypeRunning}
	}()

	go func() {
		err := h.req(t.Parameters)
		taskEvent := task.Event{Type: task.EventTypeFailed}
		if err == nil {
			taskEvent.Type = task.EventTypeSuccess
		}
		select {
		case te <- taskEvent:
			log.Println("task status update after execute")
		}
	}()

	return te
}

func (h *HttpExec) req(config string) error {
	req := &RequestConf{}
	err := json.Unmarshal([]byte(config), req)
	if err != nil {
		return err
	}
	resp, err := http.Get(req.Url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	log.Println("request status: ", resp.Status)
	return nil
}
