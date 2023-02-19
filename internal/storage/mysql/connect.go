package mysql

import "time"

type TaskInfo struct {
	Id              int64 `eorm:"auto_increment,primary_key"`
	Name            string
	SchedulerStatus string
	Epoch           int64
	Cron            string
	Type            string
	Config          string
	BaseColumns
}

type TaskExecution struct {
	Id            int64 `eorm:"auto_increment,primary_key"`
	TaskId        int64
	ExecuteStatus string
	BaseColumns
}

type BaseColumns struct {
	CreateTime time.Time `eorm:"-"`
	UpdateTime time.Time `eorm:"-"`
}
