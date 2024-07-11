package mysql

import (
	"context"
	"errors"
	"github.com/ecodeclub/ecron/internal/task"
	"gorm.io/gorm"
	"math/rand"
	"time"
)

var ErrNoExecutableTask = errors.New("当前没有可执行的任务")

type GormTaskDAO struct {
	db              *gorm.DB
	batchSize       int
	refreshInterval time.Duration
}

func NewGormTaskDAO(db *gorm.DB, batchSize int, refreshInterval time.Duration) *GormTaskDAO {
	return &GormTaskDAO{db: db, batchSize: batchSize, refreshInterval: refreshInterval}
}

func (g *GormTaskDAO) Release(ctx context.Context, t task.Task) error {
	return g.db.WithContext(ctx).Model(&TaskInfo{}).
		// 不要释放了别人的任务
		Where("id = ? AND Version = ?", t.ID, t.Version).
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
		tasks := make([]TaskInfo, g.batchSize)
		// 一次取一批
		err := g.db.WithContext(ctx).Model(&TaskInfo{}).
			Where("status = ? AND next_exec_time <= ?", TaskStatusWaiting, now).
			Or("status = ? AND utime < ?", TaskStatusRunning, t).
			Find(&tasks).Limit(g.batchSize).Error
		if err != nil {
			return task.Task{}, err
		}
		if len(tasks) < 1 {
			return task.Task{}, ErrNoExecutableTask
		}

		// 随机抢一个任务, i 的取值范围 [0, len(tasks))
		i := rand.Intn(len(tasks))

		//i := 0 // 这一句是为了下面测试 res.RowsAffected == 0 而写的

		taskInfo := tasks[i]
		res := g.preemptTask(ctx, &taskInfo)
		if res.Error != nil {
			return task.Task{}, res.Error
		}
		if res.RowsAffected == 0 {
			var j int
			if i == len(tasks)-1 {
				// 如果 i 取到了最后一条，那么从这一批次的第一条开始往后取
				j = -1
			}
			for j = i + 1; j < len(tasks); j++ {
				taskInfo = tasks[j]
				res = g.preemptTask(ctx, &taskInfo)
				if res.Error != nil {
					return task.Task{}, res.Error
				}
				if res.RowsAffected > 0 {
					return g.toTask(taskInfo), nil
				}
				continue
			}
			// 这一批一个都抢不到，参与下一轮
			// TODO: 优化下一批抢的细节
			continue
		}
		return g.toTask(taskInfo), nil
	}
}

func (g *GormTaskDAO) preemptTask(ctx context.Context, task *TaskInfo) *gorm.DB {
	// 自增后的version也要返回，释放任务需要用到
	task.Version = task.Version + 1
	return g.db.WithContext(ctx).Model(&TaskInfo{}).
		Where("id = ? AND Version = ?", task.ID, task.Version).
		Updates(map[string]interface{}{
			"status":  TaskStatusRunning,
			"utime":   time.Now().UnixMilli(),
			"Version": task.Version,
		})
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
