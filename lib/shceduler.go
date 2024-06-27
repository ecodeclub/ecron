package lib

import (
	"github.com/ecodeclub/ecron/internal/scheduler"
	"github.com/ecodeclub/ecron/internal/storage"
	"golang.org/x/sync/semaphore"
	"time"
)

func NewScheduler(dao storage.TaskDAO, history storage.HistoryDAO, refreshInterval time.Duration, limiter *semaphore.Weighted) *scheduler.PreemptScheduler {
	return scheduler.NewPreemptScheduler(dao, history, refreshInterval, limiter)
}
