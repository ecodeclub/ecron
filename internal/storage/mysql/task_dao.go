package mysql

import (
	"context"
	"github.com/ecodeclub/ecron/internal/errs"
	"github.com/ecodeclub/ecron/internal/task"
	"gorm.io/gorm"
	"math/rand"
	"time"
)

type GormTaskDAO struct {
	db              *gorm.DB
	batchSize       int
	refreshInterval time.Duration
	randIndex       func(num int) int
}

func NewGormTaskDAO(db *gorm.DB, batchSize int, refreshInterval time.Duration) *GormTaskDAO {
	return &GormTaskDAO{
		db:              db,
		batchSize:       batchSize,
		refreshInterval: refreshInterval,
		randIndex: func(num int) int {
			return rand.Intn(num)
		},
	}
}

func (g *GormTaskDAO) Release(ctx context.Context, t task.Task) error {
	return g.db.WithContext(ctx).Model(&TaskInfo{}).
		Where("id = ? AND version = ?", t.ID, t.Version).
		Updates(map[string]interface{}{
			"status": TaskStatusWaiting,
			"utime":  time.Now().UnixMilli(),
		}).Error
}

func (g *GormTaskDAO) UpdateUtime(ctx context.Context, id int64) error {
	return g.db.WithContext(ctx).Model(&TaskInfo{}).
		Where("id = ?", id).Updates(map[string]any{
		"utime": time.Now().UnixMilli(),
	}).Error
}

func (g *GormTaskDAO) Stop(ctx context.Context, id int64) error {
	return g.db.WithContext(ctx).Model(&TaskInfo{}).
		Where("id = ?", id).Updates(map[string]any{
		"status": TaskStatusFinished,
		"utime":  time.Now().UnixMilli(),
	}).Error
}

func (g *GormTaskDAO) UpdateNextTime(ctx context.Context, id int64, next time.Time) error {
	return g.db.WithContext(ctx).Model(&TaskInfo{}).
		Where("id = ?", id).Updates(map[string]any{
		"next_exec_time": next.UnixMilli(),
	}).Error
}

func (g *GormTaskDAO) Add(ctx context.Context, t task.Task) error {
	te := g.toEntity(t)
	now := time.Now().UnixMilli()
	te.Status = TaskStatusWaiting
	te.Ctime = now
	te.Utime = now
	return g.db.WithContext(ctx).Create(&te).Error
}

func (g *GormTaskDAO) Preempt(ctx context.Context) (task.Task, error) {
	for {
		now := time.Now()
		// 续约的最晚时间
		t := now.UnixMilli() - g.refreshInterval.Milliseconds()
		var tasks []TaskInfo
		// 一次取一批
		err := g.db.WithContext(ctx).Model(&TaskInfo{}).
			Where("status = ? AND next_exec_time <= ?", TaskStatusWaiting, now.UnixMilli()).
			Or("status = ? AND utime < ?", TaskStatusRunning, t).
			Find(&tasks).Limit(g.batchSize).Error
		if err != nil {
			return task.Task{}, err
		}
		if len(tasks) < 1 {
			return task.Task{}, errs.ErrNoExecutableTask
		}

		i := g.randIndex(len(tasks))
		for j := 0; j < len(tasks); j++ {
			idx := (i + j) % len(tasks)
			taskInfo := tasks[idx]
			res := g.preemptTask(ctx, &taskInfo)
			if res.Error != nil {
				return task.Task{}, res.Error
			}
			if res.RowsAffected > 0 {
				return g.toTask(taskInfo), nil
			}
			continue
		}
		continue
	}
}

func (g *GormTaskDAO) preemptTask(ctx context.Context, task *TaskInfo) *gorm.DB {
	res := g.db.WithContext(ctx).Model(&TaskInfo{}).
		Where("id = ? AND version = ?", task.ID, task.Version).
		Updates(map[string]interface{}{
			"status":  TaskStatusRunning,
			"utime":   time.Now().UnixMilli(),
			"version": task.Version + 1,
		})
	if res.RowsAffected > 0 {
		// 抢到了，要返回任务自增后的version
		task.Version++
	}
	return res
}

func (g *GormTaskDAO) toEntity(t task.Task) TaskInfo {
	return TaskInfo{
		ID:       t.ID,
		Name:     t.Name,
		Type:     t.Type.String(),
		Cron:     t.CronExp,
		Executor: t.Executor,
		Cfg:      t.Cfg,
		Ctime:    t.Ctime.UnixMilli(),
		Utime:    t.Utime.UnixMilli(),
	}
}

func (g *GormTaskDAO) toTask(t TaskInfo) task.Task {
	return task.Task{
		ID:       t.ID,
		Name:     t.Name,
		Type:     task.Type(t.Type),
		Executor: t.Executor,
		Cfg:      t.Cfg,
		CronExp:  t.Cron,
		Version:  t.Version,
		Ctime:    time.UnixMilli(t.Ctime),
		Utime:    time.UnixMilli(t.Utime),
	}
}
