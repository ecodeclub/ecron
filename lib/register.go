package lib

import (
	"context"
	"github.com/ecodeclub/ecron/internal/executor"
	"github.com/ecodeclub/ecron/internal/storage"
	"github.com/ecodeclub/ecron/internal/task"
	"log"
)

type LocalRegister struct {
	Exec *executor.LocalExecutor
	dao  storage.TaskDAO
}

func NewLocalRegister(dao storage.TaskDAO) *LocalRegister {
	e := executor.NewLocalExecutor()
	return &LocalRegister{Exec: e, dao: dao}
}

func (l *LocalRegister) RegisterTask(ctx context.Context, name string, cron string, fn func(ctx context.Context, t task.Task) error) error {
	// 先向数据库插入一条记录
	err := l.dao.Add(ctx, task.Task{
		Name:     name,
		CronExp:  cron,
		Type:     task.TypeLocal,
		Executor: l.Exec.Name(),
	})
	if err != nil {
		log.Println("注册任务失败", err)
		return err
	}
	// 向执行器注册任务
	l.Exec.RegisterFunc(name, fn)
	return nil
}
