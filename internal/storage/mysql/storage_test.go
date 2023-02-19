package mysql

import (
	"context"
	"testing"
	"time"

	"github.com/gotomicro/ecron/internal/task"
)

// 在启动main之后，可以添加任务观察抢占情况
func TestStorage_Add(t *testing.T) {
	s := NewMysqlStorage(context.TODO(), "root:@tcp(localhost:3306)/ecron", time.Minute, time.Minute)
	_, err := s.Add(context.TODO(), &task.Task{Config: task.Config{
		Name:       "Simple Task1",
		Cron:       "*/5 * * * * * *", // every 5s
		Type:       task.TypeHTTP,
		Parameters: `{"url": "http://www.baidu.com", "body": "{\"key\": \"value\"}", "timeout": 30}`,
	}}, TaskInfo{})
	if err != nil {
		t.Fatal(err)
	}
}
