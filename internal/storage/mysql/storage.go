package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/gotomicro/ecron/internal/errs"
	"github.com/gotomicro/ecron/internal/storage"
	"github.com/gotomicro/ecron/internal/task"
	"strings"
	"time"
)

type Storage struct {
	events chan storage.Event
}

func NewStorage() *Storage {
	return &Storage{
		events: make(chan storage.Event, 1),
	}
}

func (s *Storage) Add(t *task.Task) error {
	sql := "INSERT INTO task (name, cron, type, retry_count, retry_time, progress, state, node, task_job_type, post_guid) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
	values := []any{&t.Name, &t.Cron, &t.Type, &t.RetryCount, &t.RetryTime, &t.Progress, &t.State, &t.Node.ID, &t.TaskJobType, &t.PostGUID}
	result, err := S.DB().ExecContext(context.Background(), sql, values...)
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected != 1 {
		msg := fmt.Sprintf("expected to affect 1 row, affected %d", affected)
		return errs.NewAddTaskTypeError(msg)
	}

	t.ID, err = result.LastInsertId()

	fmt.Printf("Create task success %v\n", t)
	fmt.Printf("task id = %d", t.ID)

	event := storage.Event{
		Type: storage.EventTypePreempted,
		Task: t,
	}
	s.events <- event

	return nil
}

func (s *Storage) Update(t *task.Task, fields map[string]string) error {
	values := make([]any, len(fields))
	sb := strings.Builder{}
	sb.WriteString("UPDATE `task` SET ")
	count := 0
	for k, v := range fields {
		sb.WriteString(k)
		sb.WriteString(" = ")
		sb.WriteRune('?')
		if count != len(fields)-1 {
			sb.WriteRune(',')
		}
		count++
		values = append(values, v)
	}

	sb.WriteString(" WHERE id = ?")
	values = append(values, t.ID)
	sql := sb.String()
	result, err := S.DB().ExecContext(context.Background(), sql, values...)
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected != 1 {
		msg := fmt.Sprintf("expected to affect 1 row, affected %d", affected)
		return errs.NewUpdateTaskTypeError(msg)
	}

	fmt.Printf("Update task success %v\n", t)

	return nil
}

func (s *Storage) Delete(name string) error {
	t := &task.Task{}
	err := S.db.QueryRowContext(context.Background(), "SELECT id FROM task WHERE name=?", name).Scan(&t.Name, &t.Cron, &t.Type, &t.RetryCount, &t.RetryTime, &t.Progress, &t.State, &t.Node.ID, &t.TaskJobType, &t.PostGUID, &t.CreateAt, &t.UpdateAt, &t.DeleteAt)
	switch {
	case err == sql.ErrNoRows:
		return sql.ErrNoRows
	case err != nil:
		return err
	default:
	}

	sql := "UPDATE task SET delete_at = ? FROM name = ?"
	result, err := S.DB().ExecContext(context.Background(), sql, time.Now().Format("2006-01-02 15:04:05"), name)
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected != 1 {
		msg := fmt.Sprintf("expected to affect 1 row, affected %d", affected)
		return errs.NewDeleteTaskTypeError(msg)
	}

	event := storage.Event{
		Type: storage.EventTypePreempted,
		Task: t,
	}
	s.events <- event
	return nil
}

func (s *Storage) Events(ctx context.Context, taskEvents <-chan task.Event) (<-chan storage.Event, error) {
	go func() {
		for {
			select {
			case <-ctx.Done():
			case event := <-taskEvents:
				switch event.Type {
				case task.EventTypeRunning:
					currentEvent := <-s.events
					currentTask := currentEvent.Task
					status := make(map[string]string, 1)
					status["status"] = task.EventTypeRunning
					s.Update(currentTask, status)
				case task.EventTypeSuccess:
					currentEvent := <-s.events
					currentTask := currentEvent.Task
					status := make(map[string]string, 1)
					status["status"] = task.EventTypeSuccess
					s.Update(currentTask, status)
				case task.EventTypeFailed:
					currentEvent := <-s.events
					currentTask := currentEvent.Task
					status := make(map[string]string, 1)
					status["status"] = task.EventTypeFailed
					s.Update(currentTask, status)
				}
			}
		}
	}()
	return s.events, nil
}
