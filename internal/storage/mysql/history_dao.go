package mysql

import (
	"context"
	"github.com/ecodeclub/ecron/internal/task"
	"gorm.io/gorm"
	"time"
)

type GormHistoryDAO struct {
	db *gorm.DB
}

func NewGormHistoryDAO(db *gorm.DB) *GormHistoryDAO {
	return &GormHistoryDAO{db: db}
}

func (h *GormHistoryDAO) Add(ctx context.Context, id int64, status task.ExecStatus) error {
	var th TaskExecHistory
	now := time.Now().UnixMilli()
	th.Tid = id
	th.Status = status.ToUint8()
	th.Ctime = now
	th.Utime = now
	return h.db.WithContext(ctx).Create(&th).Error
}
