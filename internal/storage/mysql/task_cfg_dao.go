package mysql

import (
	"context"
	"github.com/ecodeclub/ecron/internal/task"
	"gorm.io/gorm"
	"time"
)

type GormTaskCfgRepository struct {
	db *gorm.DB
}

func NewGormTaskCfgRepository(db *gorm.DB) *GormTaskCfgRepository {
	return &GormTaskCfgRepository{
		db: db,
	}
}

func (g *GormTaskCfgRepository) Add(ctx context.Context, t task.Task) error {
	te := toEntity(t)
	now := time.Now().UnixMilli()
	te.Status = TaskStatusWaiting
	te.Ctime = now
	te.Utime = now
	return g.db.WithContext(ctx).Create(&te).Error
}

func (g *GormTaskCfgRepository) Stop(ctx context.Context, id int64) error {
	return g.db.WithContext(ctx).Model(&TaskInfo{}).
		Where("id = ?", id).Updates(map[string]any{
		"status": TaskStatusFinished,
		"utime":  time.Now().UnixMilli(),
	}).Error
}

func (g *GormTaskCfgRepository) UpdateNextTime(ctx context.Context, id int64, next time.Time) error {
	return g.db.WithContext(ctx).Model(&TaskInfo{}).
		Where("id = ?", id).Updates(map[string]any{
		"next_exec_time": next.UnixMilli(),
	}).Error
}
