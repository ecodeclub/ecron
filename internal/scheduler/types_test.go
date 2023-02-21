package scheduler

import (
	"context"
	"errors"
	"fmt"
	"github.com/gotomicro/ecron/internal/executor"
	ehttp "github.com/gotomicro/ecron/internal/executor/http"
	"github.com/gotomicro/ecron/internal/storage"
	"github.com/gotomicro/ecron/internal/task"
	"github.com/gotomicro/ekit/queue"
	"github.com/stretchr/testify/assert"
	"golang.org/x/sync/errgroup"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"
)

func TestScheduler_Start(t *testing.T) {
	type fields struct {
		s               *testStorage
		tasks           map[string]scheduledTask
		executors       map[task.Type]executor.Executor
		readyTasks      *queue.DelayQueue[execution]
		taskEvents      chan task.Event
		scheduledEvents chan scheduledEvent
	}
	type args struct {
		ctx   context.Context
		tasks []task.Task
	}
	tests := []struct {
		name       string
		fields     fields
		args       args
		wantStatus []task.Event
		wantErr    []error
	}{
		{
			name: "error storage",
			fields: fields{
				s:     &testStorage{ch: make(chan storage.Event, 3), isErrTest: true},
				tasks: make(map[string]scheduledTask),
				executors: map[task.Type]executor.Executor{
					task.TypeHTTP: ehttp.NewExecutor(),
				},
				readyTasks:      queue.NewDelayQueue[execution](10),
				taskEvents:      make(chan task.Event),
				scheduledEvents: make(chan scheduledEvent, 3),
			},
			args: args{
				ctx:   context.Background(),
				tasks: nil,
			},
			wantStatus: nil,
			wantErr: []error{
				errors.New(`监听存储层失败`),
			},
		},
		{
			name: "http-suc-once",
			fields: fields{
				s:     &testStorage{ch: make(chan storage.Event, 3)},
				tasks: make(map[string]scheduledTask),
				executors: map[task.Type]executor.Executor{
					task.TypeHTTP: ehttp.NewExecutor(),
				},
				readyTasks:      queue.NewDelayQueue[execution](20),
				taskEvents:      make(chan task.Event),
				scheduledEvents: make(chan scheduledEvent),
			},
			args: args{
				ctx: context.Background(),
				tasks: []task.Task{
					{Config: task.Config{
						Name: "http-suc-once",
						Cron: "*/5 * * * * * *",
						Type: task.TypeHTTP,
						Retry: struct {
							Need  bool
							Count int
						}{},
						Executor: []byte(`{"url":"http://127.0.0.1:8000/suc-once","method":"post"}`),
					}},
				},
			},
			wantStatus: []task.Event{
				{
					TaskName: "http-suc-once",
					Type:     task.EventTypeSuccess,
				},
			},
			wantErr: []error{
				context.Canceled,
				nil,
			},
		},
	}
	errTaskNameNotEqual := errors.New(`task name not equal`)
	errTaskTypeNotEqual := errors.New(`task type not equal`)
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
			ctx, cancel := context.WithCancel(tt.args.ctx)
			g := new(errgroup.Group)
			s := &Scheduler{
				s:               tt.fields.s,
				tasks:           tt.fields.tasks,
				executors:       tt.fields.executors,
				readyTasks:      *tt.fields.readyTasks,
				taskEvents:      tt.fields.taskEvents,
				scheduledEvents: tt.fields.scheduledEvents,
			}
			g.Go(func() error {
				for i := 0; i < len(tt.args.tasks); i++ {
					tk := tt.args.tasks[i]
					tt.fields.s.ch <- storage.Event{
						Type: task.EventTypePreempted,
						Task: &tk,
					}
				}
				return nil
			})
			g.Go(func() error {
				defer cancel()
				for i := 0; i < len(tt.wantStatus); i++ {
					event := <-tt.fields.taskEvents
					fmt.Printf(`recv count(%v) at %v`, i, time.Now())
					if tt.wantStatus[i].TaskName != event.TaskName {
						return errTaskNameNotEqual
					}
					if tt.wantStatus[i].Type != event.Type {
						return errTaskTypeNotEqual
					}
				}
				return nil
			})
			err := s.Start(ctx)
			assert.Equal(t, tt.wantErr[0], err)
			if err != context.Canceled {
				return
			}
			err = g.Wait()
			assert.Equal(t, tt.wantErr[1], err)
		})
	}
}

type testStorage struct {
	ch        chan storage.Event
	isErrTest bool
}

func (s *testStorage) Events(ctx context.Context, taskEvents <-chan task.Event) (<-chan storage.Event, error) {
	if s.isErrTest {
		return nil, errors.New(`监听存储层失败`)
	}
	return s.ch, nil
}

func (s *testStorage) Add(t *task.Task) error {
	//TODO implement me
	panic("implement me")
}

func (s *testStorage) Update(t *task.Task) error {
	//TODO implement me
	panic("implement me")
}

func (s *testStorage) Delete(name string) error {
	//TODO implement me
	panic("implement me")
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
