package mysql

import (
	"github.com/gotomicro/ecron/internal/task"
	"testing"
	"time"

	db "github.com/gotomicro/ecron/pkg/db/mysql"
)

func TestAddTask(t *testing.T) {
	// 初始化 store 层
	if err := initStore(); err != nil {
		panic(err)
	}
	storage := NewStorage()
	storage.Add(&task.Task{
		Config: task.Config{
			Name: "test",
			Cron: "2023年02月10日10:22:01",
			Retry: task.Retry{
				RetryCount: 1,
				RetryTime:  10,
			},
			Type: task.TypeHTTP,
		},
		Progress: "39%",
		State:    "1",
		Node: task.Node{
			ID: 129,
		},
		TaskJobType: task.EventTypeCreated,
		PostGUID:    21789,
	})
}

func initStore() error {
	dbOptions := &db.MySQLOptions{
		Host:                  "localhost:3306",
		Username:              "ecron",
		Password:              "ecron1234",
		Database:              "ecron",
		MaxIdleConnections:    100,
		MaxOpenConnections:    100,
		MaxConnectionLifeTime: 10 * time.Second,
		LogLevel:              4,
	}

	ins, err := db.NewMySQL(dbOptions)
	if err != nil {
		return err
	}

	_ = NewStore(ins)
	return err
}
