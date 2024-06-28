package lib

import (
	"context"
	"fmt"
	"github.com/ecodeclub/ecron/internal/task"
	"github.com/ecodeclub/ecron/lib/stroage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/semaphore"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"testing"
	"time"
)

func TestLocal(t *testing.T) {
	db, err := gorm.Open(mysql.Open("root:root@tcp(localhost:13316)/ecron"))
	require.NoError(t, err)
	td, hd := stroage.NewMySqlDAO(db, 1, time.Second*5)
	limiter := semaphore.NewWeighted(3)
	s := NewScheduler(td, hd, time.Second*5, limiter)
	register := NewLocalRegister(td)
	err = register.RegisterTask(context.Background(), "test", "*/5 * * * * ?", func(ctx context.Context, t task.Task) error {
		fmt.Println(time.Now().UnixMilli(), "执行任务", t.ID)
		return nil
	})
	s.RegisterExecutor(register.Exec)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err = s.Schedule(ctx)
	require.NoError(t, err)
	assert.NoError(t, err)
	time.Sleep(time.Minute * 10)
}
