package mysql

import (
	"context"
	"github.com/ecodeclub/ecron/internal/task"
	"gorm.io/gorm"
	"math/rand"
	"time"
)

type GormTaskDAO struct {
	db              *gorm.DB
	batchSize       int
	refreshInterval time.Duration
}

func NewDAO(db *gorm.DB, batchSize int, refreshInterval time.Duration) *GormTaskDAO {
	return &GormTaskDAO{db: db, batchSize: batchSize, refreshInterval: refreshInterval}
}

func (g *GormTaskDAO) Release(ctx context.Context, t task.Task) error {
	return g.db.WithContext(ctx).Model(&TaskInfo{}).
		Where("id = ?", t.ID).
		Updates(map[string]interface{}{
			"status": TaskStatusWaiting,
			"utime":  time.Now().UnixMilli(),
		}).Error
}

func (g *GormTaskDAO) UpdateUtime(ctx context.Context, id int64) error {
	return g.db.WithContext(ctx).Model(&TaskInfo{}).
		Where("id =?", id).Updates(map[string]any{
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
	te.Ctime = now
	te.Utime = now
	return g.db.WithContext(ctx).Create(&te).Error
}

func (g *GormTaskDAO) Get(ctx context.Context) (task.Task, error) {
	for {
		now := time.Now()
		// 续约的最晚时间
		t := now.UnixMilli() - g.refreshInterval.Milliseconds()
		var tasks []TaskInfo
		// 一次取一批
		err := g.db.WithContext(ctx).Model(&TaskInfo{}).
			Where("(status = ? AND exec_exec_time <= ?) OR (status = ? AND utime < ?)",
				TaskStatusWaiting, now, TaskStatusRunning, t).
			Find(&tasks).Limit(g.batchSize).Error
		if err != nil {
			// 没有任务
			return task.Task{}, err
		}
		// 随机抢一个任务, i 的取值范围 [0, len(tasks))
		i := rand.Intn(len(tasks))
		ta := tasks[i]
		res := g.db.WithContext(ctx).Model(&TaskInfo{}).
			Where("id = ? AND version = ?", ta.ID, ta.version).
			Updates(map[string]interface{}{
				"status":  TaskStatusRunning,
				"utime":   now,
				"version": ta.version + 1,
			})

		if res.Error != nil {
			return task.Task{}, res.Error
		}
		if res.RowsAffected == 0 {
			continue
		}
		return g.toTask(ta), nil
	}
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
		Ctime:    time.UnixMilli(t.Ctime),
		Utime:    time.UnixMilli(t.Utime),
	}
}
