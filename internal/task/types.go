package task

import (
	"github.com/robfig/cron/v3"
	"time"
)

type Task struct {
	ID       int64
	Name     string
	Type     Type
	Executor string
	Cfg      string
	CronExp  string
	Ctime    time.Time
	Utime    time.Time
}

type Type string

const (
	TypeLocal = "LocalTask"
	TypeHttp  = "HttpTask"
	TypeGrpc  = "GrpcTask"
)

func (t Type) String() string {
	switch t {
	case TypeLocal:
		return "LocalTask"
	case TypeHttp:
		return "HttpTask"
	case TypeGrpc:
		return "GrpcTask"
	default:
		return "UnknownTask"
	}
}

var parser = cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)

func (t Task) NextTime() (time.Time, error) {
	s, err := parser.Parse(t.CronExp)
	if err != nil {
		return time.Time{}, err
	}
	return s.Next(time.Now()), nil
}

type TaskExecRecord struct {
	ID     int64
	Tid    int
	Status Status
	Ctime  time.Time
	Utime  time.Time
}

type Status uint8

const (
	TaskHistoryStatusStart   = 1 // 开始执行
	TaskHistoryStatusFail    = 2 // 执行出错
	TaskHistoryStatusSuccess = 3 // 执行成功
)
