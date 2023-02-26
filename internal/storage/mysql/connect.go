package mysql

import (
	"database/sql/driver"
	"errors"
	"time"
)

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
	CreateTime *NullTime
	UpdateTime *NullTime
}

type NullTime struct {
	Time  time.Time
	Valid bool // Valid is true if Time is not NULL
}

func (nt NullTime) Value() (driver.Value, error) {
	if !nt.Valid {
		return nil, nil
	}
	return nt.Time.Format("2006-01-02 15:04:05"), nil
}

// Scan implements the Scanner interface.
func (nt *NullTime) Scan(value interface{}) error {
	switch v := value.(type) {
	case []uint8:
		t, err := time.ParseInLocation("2006-01-02 15:04:05", string(v), time.Local)
		if err != nil {
			return err
		}
		nt.Time = t
		nt.Valid = true
	default:
		return errors.New("unsupported type")
	}
	return nil
}
