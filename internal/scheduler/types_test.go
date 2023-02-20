package scheduler

import (
	"context"
	"github.com/gotomicro/ecron/internal/executor"
	"github.com/gotomicro/ecron/internal/storage"
	"github.com/gotomicro/ecron/internal/task"
	"github.com/gotomicro/ekit/queue"
	"sync"
	"testing"
)

func TestScheduler_Start(t *testing.T) {
	type fields struct {
		s               storage.Storage
		tasks           map[string]scheduledTask
		executors       map[string]executor.Executor
		mux             sync.Mutex
		readyTasks      queue.DelayQueue[execution]
		taskEvents      chan task.Event
		scheduledEvents chan scheduledEvent
	}
	type args struct {
		ctx context.Context
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr error
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Scheduler{
				s:               tt.fields.s,
				tasks:           tt.fields.tasks,
				executors:       tt.fields.executors,
				mux:             tt.fields.mux,
				readyTasks:      tt.fields.readyTasks,
				taskEvents:      tt.fields.taskEvents,
				scheduledEvents: tt.fields.scheduledEvents,
			}
			if err := s.Start(tt.args.ctx); (err != nil) != tt.wantErr {
				t.Errorf("Start() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
