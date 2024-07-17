package stroage

import (
	"github.com/ecodeclub/ecron/internal/storage"
	"github.com/ecodeclub/ecron/internal/storage/mysql"
	"gorm.io/gorm"
	"time"
)

// NewMySqlDAO 提供DAO的mysql实现
func NewMySqlDAO(db *gorm.DB, batchSize int, refreshInterval time.Duration) (storage.TaskDAO, storage.ExecutionDAO) {
	return mysql.NewGormTaskDAO(db, batchSize, refreshInterval), mysql.NewGormExecutionDAO(db)
}
