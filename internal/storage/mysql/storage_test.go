package mysql

import (
	"context"
	"log"
	"testing"

	"github.com/gotomicro/ecron/internal/storage"
	"github.com/gotomicro/ecron/internal/task"
	"github.com/gotomicro/eorm"
	"github.com/stretchr/testify/assert"
)

// 在启动main之后，可以添加任务观察抢占情况
func TestStorage_Add(t *testing.T) {
	s := NewMysqlStorage("root:@tcp(localhost:3306)/ecron")
	tId, err := s.Add(context.TODO(), &task.Task{Config: task.Config{
		Name:       "Simple Task1",
		Cron:       "*/5 * * * * * *", // every 5s
		Type:       task.TypeHTTP,
		Parameters: `{"url": "http://www.baidu.com", "body": "{\"key\": \"value\"}", "timeout": 30}`,
	}})
	assert.Nil(t, err)
	assert.NotEmpty(t, tId)
	//eId, err := s.AddExecution(context.TODO(), tId)
	//if err != nil {
	//	t.Fatal(err)
	//}
	//assert.Nil(t, err)
	//assert.NotEmpty(t, eId)
}

func TestStorage_Get(t *testing.T) {
	s := NewMysqlStorage("root:@tcp(localhost:3306)/ecron")
	tt, err := s.Get(context.TODO())
	assert.Nil(t, err)
	log.Println(*tt[0])
}

func TestStorage_Update(t *testing.T) {
	s := NewMysqlStorage("root:@tcp(localhost:3306)/ecron")
	err := s.Update(context.TODO(), &task.Task{})
	assert.Nil(t, err)
}

func TestStorage_Refresh(t *testing.T) {
	ctx := context.TODO()
	s := NewMysqlStorage("root:@tcp(localhost:3306)/ecron")
	tk, err := eorm.NewSelector[TaskInfo](s.db).
		Select().From(&TaskInfo{}).
		Where(eorm.C("SchedulerStatus").EQ(storage.EventTypePreempted)).
		Get(ctx)
	assert.Nil(t, err)
	s.refresh(ctx, tk.Id, tk.Epoch)
}
