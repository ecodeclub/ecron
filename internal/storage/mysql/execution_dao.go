package mysql

import (
	"context"
	"github.com/ecodeclub/ecron/internal/task"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"time"
)

type GormExecutionDAO struct {
	db *gorm.DB
}

func NewGormExecutionDAO(db *gorm.DB) *GormExecutionDAO {
	return &GormExecutionDAO{db: db}
}

func (h *GormExecutionDAO) InsertExecStatus(ctx context.Context, id int64, status task.ExecStatus) (int64, error) {
	now := time.Now().UnixMilli()
	exec := Execution{
		Tid:      id,
		Status:   status.ToUint8(),
		Progress: 0,
		Ctime:    now,
		Utime:    now,
	}
	err := h.db.WithContext(ctx).Clauses(clause.OnConflict{
		DoUpdates: clause.Assignments(map[string]any{
			"status": status.ToUint8(),
			"utime":  now,
		}),
	}).Create(&exec).Error
	return exec.ID, err
}

func (h *GormExecutionDAO) UpdateProgress(ctx context.Context, id int64, progress uint8) error {
	return h.db.WithContext(ctx).Model(&Execution{}).Where("id=?", id).Updates(map[string]any{
		"progress": progress,
		"utime":    time.Now().UnixMilli(),
	}).Error
}
