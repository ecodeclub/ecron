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
	Version  int
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
	return string(t)
}

var parser = cron.NewParser(
	cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor,
)

func (t Task) NextTime() (time.Time, error) {
	s, err := parser.Parse(t.CronExp)
	if err != nil {
		return time.Time{}, err
	}
	return s.Next(time.Now()), nil
}

type Execution struct {
	ID     int64
	Tid    int
	Status ExecStatus
	Ctime  time.Time
	Utime  time.Time
}

type ExecStatus uint8

const (
	ExecStatusUnknown ExecStatus = iota
	ExecStatusStarted
	ExecStatusSuccess
	ExecStatusFailed
	ExecStatusDeadlineExceeded
	ExecStatusCancelled
)

func (s ExecStatus) ToUint8() uint8 {
	return uint8(s)
}
