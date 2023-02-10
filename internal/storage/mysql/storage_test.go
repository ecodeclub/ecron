package mysql

import (
	"context"
	"testing"

	"github.com/gotomicro/ecron/internal/task"
)

// 在启动main之后，可以添加任务观察抢占情况
func TestStorage_Add(t *testing.T) {
	s := &Storage{}
	err := s.Add(context.TODO(), &task.Task{Config: task.Config{
		Name:       "Simple Task2",
		Cron:       "*/5 * * * * * *", // every 5s
		Type:       task.TypeHTTP,
		Parameters: `{"url": "http://www.baidu.com", "body": "{\"key\": \"value\"}", "timeout": 30}`,
	}})
	if err != nil {
		t.Fatal(err)
	}
}
