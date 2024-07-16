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

func (h *GormExecutionDAO) InsertExecStatus(ctx context.Context, id int64, status task.ExecStatus) error {
	now := time.Now().UnixMilli()
	return h.db.WithContext(ctx).Clauses(clause.OnConflict{
		DoUpdates: clause.Assignments(map[string]any{
			"status": status.ToUint8(),
			"utime":  now,
		}),
	}).Create(&Execution{
		Tid:    id,
		Status: status.ToUint8(),
		Ctime:  now,
		Utime:  now,
	}).Error
}
