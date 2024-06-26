package lib

import (
	"context"
	"github.com/ecodeclub/ecron/internal/executor"
	"github.com/ecodeclub/ecron/internal/storage"
	"github.com/ecodeclub/ecron/internal/task"
	"log"
)

type LocalRegister struct {
	exec *executor.LocalExecutor
	dao  storage.TaskDAO
}

func NewLocalRegister(dao storage.TaskDAO) *LocalRegister {
	e := executor.NewLocalExecutor()
	return &LocalRegister{exec: e, dao: dao}
}

func (l *LocalRegister) RegisterTask(ctx context.Context, name string, cron string, fn func(ctx context.Context, t task.Task) error) error {
	// 先向数据库插入一条记录
	err := l.dao.Add(ctx, task.Task{
		Name:    name,
		CronExp: cron,
		Type:    task.TypeLocal,
	})
	if err != nil {
		log.Println("注册任务失败", err)
		return err
	}
	// 向执行器注册任务
	l.exec.RegisterFunc(name, fn)
	return nil
}
