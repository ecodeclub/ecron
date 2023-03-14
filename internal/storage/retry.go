package storage

import "time"

type RetryStrategy interface {
	Next() (time.Duration, bool)
	GetMaxRetry() int64
}

type RefreshIntervalRetry struct {
	Interval time.Duration
	Max      int64
	cnt      int64
}

func (r *RefreshIntervalRetry) Next() (time.Duration, bool) {
	r.cnt++
	return r.Interval, r.cnt <= r.Max
}

func (r *RefreshIntervalRetry) GetMaxRetry() int64 {
	return r.Max
}
