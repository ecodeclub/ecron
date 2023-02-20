package http

import (
	"errors"
	"fmt"
	"github.com/gotomicro/ecron/internal/executor"
	"github.com/gotomicro/ecron/internal/task"
	"github.com/stretchr/testify/assert"
	"io"
	"net/http"
	"sync"
	"testing"
)

func TestExecutor_Execute(t *testing.T) {
	type args struct {
		t *task.Task
	}
	tests := []struct {
		name       string
		args       args
		wantStatus []executor.EventType
		wantErr    error
	}{
		{
			name:    "nil task",
			args:    args{t: nil},
			wantErr: errors.New(`非法任务`),
		},
		{
			name: "illegal config",
			args: args{t: &task.Task{Config: task.Config{
				Name: "Illegal Config",
				Cron: "",
				Type: "",
				Retry: struct {
					Need  bool
					Count int
				}{},
				Executor: []byte(`{"url":1,"method":{"id":3}}`),
			}}},
			wantErr: errors.New(`非法配置`),
		},
		{
			name: "illegal method",
			args: args{t: &task.Task{Config: task.Config{
				Name: "Illegal Config",
				Cron: "",
				Type: "",
				Retry: struct {
					Need  bool
					Count int
				}{},
				Executor: []byte(`{"url":"http://127.0.0.1:8000/404","method":"run"}`),
			}}},
			wantErr: errors.New(`非法方法`),
		},
		{
			name: "service 404",
			args: args{t: &task.Task{Config: task.Config{
				Name: "Service404",
				Cron: "",
				Type: "",
				Retry: struct {
					Need  bool
					Count int
				}{},
				Executor: []byte(`{"url":"http://127.0.0.1:8000/404","method":"post"}`),
			}}},
			wantStatus: []executor.EventType{
				executor.EventTypeFailed,
			},
			wantErr: nil,
		},
		{
			name: "empty body",
			args: args{t: &task.Task{Config: task.Config{
				Name: "EmptyBody",
				Cron: "",
				Type: "",
				Retry: struct {
					Need  bool
					Count int
				}{},
				Executor: []byte(`{"url":"http://127.0.0.1:8000/illegal-body","method":"post"}`),
			}}},
			wantStatus: []executor.EventType{
				executor.EventTypeFailed,
			},
			wantErr: nil,
		},
		{
			name: "success once",
			args: args{t: &task.Task{Config: task.Config{
				Name: "SucOnce",
				Cron: "",
				Type: "",
				Retry: struct {
					Need  bool
					Count int
				}{},
				Executor: []byte(`{"url":"http://127.0.0.1:8000/suc-once","method":"post"}`),
			}}},
			wantStatus: []executor.EventType{
				executor.EventTypeSuccess,
			},
			wantErr: nil,
		},
		{
			name: "success twice",
			args: args{t: &task.Task{Config: task.Config{
				Name: "SucTwice",
				Cron: "",
				Type: "",
				Retry: struct {
					Need  bool
					Count int
				}{},
				Executor: []byte(`{"url":"http://127.0.0.1:8000/suc-twice","method":"post"}`),
			}}},
			wantStatus: []executor.EventType{
				executor.EventTypeWaiting,
				executor.EventTypeSuccess,
			},
			wantErr: nil,
		},
		{
			name: "fail twice",
			args: args{t: &task.Task{Config: task.Config{
				Name: "FailTwice",
				Cron: "",
				Type: "",
				Retry: struct {
					Need  bool
					Count int
				}{},
				Executor: []byte(`{"url":"http://127.0.0.1:8000/fail-twice","method":"post"}`),
			}}},
			wantStatus: []executor.EventType{
				executor.EventTypeWaiting,
				executor.EventTypeFailed,
			},
			wantErr: nil,
		},
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		wg.Done()
		if err := runTestSrv(); err != nil {
			fmt.Printf(`test srv run error: %v`, err)
		}
	}()
	wg.Wait()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &Executor{
				client: &http.Client{},
			}
			event, err := e.Execute(tt.args.t)
			assert.Equal(t, tt.wantErr, err)
			if err != nil {
				return
			}
			for i := 0; i < len(tt.wantStatus); i++ {
				rv := <-event
				assert.Equal(t, tt.wantStatus[i], rv.Type)
			}
		})
	}
}

func runTestSrv() error {
	http.HandleFunc("/", handle404)
	http.HandleFunc("/illegal-body", handleIllegalBody)
	http.HandleFunc("/suc-once", handleOnceSuc)
	http.HandleFunc("/suc-twice", handleTwiceSuc)
	http.HandleFunc("/fail-twice", handleTwiceFail)
	return http.ListenAndServe("127.0.0.1:8000", nil)
}

func handle404(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNotFound)
}

func handleIllegalBody(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func handleOnceSuc(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write([]byte(`{"status":2}`)); err != nil {
		fmt.Printf(`write back err: %v`, err)
	}
}
func handleTwiceSuc(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		fmt.Printf(`write back err: %v`, err)
		return
	}
	switch string(body) {
	case `2`:
		if _, err = w.Write([]byte(`{"status":2}`)); err != nil {
			fmt.Printf(`write back err: %v`, err)
		}
	default:
		if _, err = w.Write([]byte(`{"status":1,"payload":"2"}`)); err != nil {
			fmt.Printf(`write back err: %v`, err)
		}
	}
}
func handleTwiceFail(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		fmt.Printf(`write back err: %v`, err)
		return
	}
	switch string(body) {
	case `2`:
		if _, err = w.Write([]byte(`{"status":3}`)); err != nil {
			fmt.Printf(`write back err: %v`, err)
		}
	default:
		if _, err = w.Write([]byte(`{"status":1,"payload":"2"}`)); err != nil {
			fmt.Printf(`write back err: %v`, err)
		}
	}
}
