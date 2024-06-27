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

func (h *GormHistoryDAO) Add(ctx context.Context, t task.Task, status int) error {
	var th TaskExecHistory
	th.Tid = t.ID
	th.Status = status
	now := time.Now().UnixMilli()
	th.Ctime = now
	th.Utime = now
	return h.db.WithContext(ctx).Create(&th).Error
}
