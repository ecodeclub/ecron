package mysql

import (
	"github.com/ecodeclub/ecron/internal/task"
	"time"
)

type TaskInfo struct {
	ID   int64 `gorm:"primary_key;auto_increment"`
	Name string
	// 任务类型
	Type         string
	Cron         string
	Executor     string
	Owner        string
	Status       int8
	Cfg          string
	NextExecTime int64
	Ctime        int64
	Utime        int64
}

func (TaskInfo) TableName() string {
	return "task_info"
}

func toEntity(t task.Task) TaskInfo {
	return TaskInfo{
		ID:       t.ID,
		Name:     t.Name,
		Type:     t.Type.String(),
		Cron:     t.CronExp,
		Executor: t.Executor,
		Cfg:      t.Cfg,
		Ctime:    t.Ctime.UnixMilli(),
		Utime:    t.Utime.UnixMilli(),
		Owner:    t.Owner,
	}
}

func toTask(t TaskInfo) task.Task {
	return task.Task{
		ID:       t.ID,
		Name:     t.Name,
		Type:     task.Type(t.Type),
		Executor: t.Executor,
		Cfg:      t.Cfg,
		CronExp:  t.Cron,
		Ctime:    time.UnixMilli(t.Ctime),
		Utime:    time.UnixMilli(t.Utime),
		Owner:    t.Owner,
	}
}

const (
	TaskStatusWaiting  = int8(1) // 等待调度
	TaskStatusRunning  = int8(2) // 正在执行
	TaskStatusPaused   = int8(3) // 任务中断
	TaskStatusFinished = int8(4) // 任务结束
)

// Execution 任务执行记录
type Execution struct {
	ID  int64 `gorm:"primary_key;auto_increment"`
	Tid int64 `gorm:"uniqueIndex:idx_tid"`
	// 任务执行进度
	Progress uint8
	Status   uint8
	Ctime    int64
	Utime    int64
}

func (Execution) TableName() string {
	return "execution"
}
