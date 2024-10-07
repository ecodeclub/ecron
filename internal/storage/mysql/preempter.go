package mysql

import (
	"context"
	"errors"
	"github.com/ecodeclub/ecron/internal/errs"
	"github.com/ecodeclub/ecron/internal/preempt"
	"github.com/ecodeclub/ecron/internal/task"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

//go:generate mockgen -source=./preempter.go -package=daomysqlmocks -destination=./mocks/preempter.mock.go

type Preempter struct {
	taskRepository taskRepository
	// with options
	refreshTimeout  time.Duration
	refreshInterval time.Duration
	buffSize        uint8
	maxRetryTimes   uint8
	retrySleepTime  time.Duration
	randIndex       func(num int) int
}

func NewPreempter(db *gorm.DB, batchSize int, refreshInterval time.Duration) *Preempter {
	taskRepository := newGormTaskRepository(db, batchSize, refreshInterval)
	return newPreempter(taskRepository)
}

// newPreempter 用于测试
func newPreempter(tr taskRepository) *Preempter {
	return &Preempter{
		taskRepository:  tr,
		refreshTimeout:  2 * time.Second,
		refreshInterval: time.Second * 5,
		buffSize:        10,
		maxRetryTimes:   3,
		retrySleepTime:  time.Second,
		randIndex: func(num int) int {
			return rand.Intn(num)
		},
	}
}

func (p *Preempter) Preempt(ctx context.Context) (preempt.TaskLeaser, error) {
	t, err := p.taskRepository.TryPreempt(ctx, func(ctx context.Context, tasks []task.Task) (task.Task, error) {
		size := len(tasks)
		index := p.randIndex(size)
		var err error
		for i := index % size; i < size; i++ {
			t := tasks[i]
			value := uuid.New().String()
			err = p.taskRepository.PreemptTask(ctx, t.ID, t.Owner, value)
			switch {
			case err == nil:
				t.Owner = value
				return t, nil
			case errors.Is(err, ErrFailedToPreempt):
				continue
			default:
				return task.Task{}, err
			}
		}
		return task.Task{}, preempt.ErrNoTaskToPreempt
	})
	if err != nil {
		return nil, err
	}
	var b atomic.Bool
	b.Store(false)
	return &taskLeaser{
		t:               t,
		taskRepository:  p.taskRepository,
		refreshTimeout:  p.refreshTimeout,
		refreshInterval: p.refreshInterval,
		buffSize:        p.buffSize,
		maxRetryTimes:   p.maxRetryTimes,
		retrySleepTime:  p.retrySleepTime,
		done:            make(chan struct{}),
		ones:            sync.Once{},
		hasDone:         b,
	}, err
}

type taskLeaser struct {
	t               task.Task
	taskRepository  taskRepository
	refreshTimeout  time.Duration
	refreshInterval time.Duration
	buffSize        uint8
	maxRetryTimes   uint8
	retrySleepTime  time.Duration
	ones            sync.Once
	done            chan struct{}
	hasDone         atomic.Bool
}

func (d *taskLeaser) Refresh(ctx context.Context) error {
	if d.hasDone.Load() {
		return preempt.ErrLeaserHasRelease
	}
	return d.taskRepository.RefreshTask(ctx, d.t.ID, d.t.Owner)
}

func (d *taskLeaser) Release(ctx context.Context) error {

	if d.hasDone.Load() {
		return preempt.ErrLeaserHasRelease
	}
	d.ones.Do(func() {
		d.hasDone.Store(true)
		close(d.done)
	})
	return d.taskRepository.ReleaseTask(ctx, d.t.ID, d.t.Owner)
}

func (d *taskLeaser) GetTask() task.Task {
	return d.t
}

func (d *taskLeaser) AutoRefresh(ctx context.Context) (s <-chan preempt.Status, err error) {
	if d.hasDone.Load() {
		return nil, preempt.ErrLeaserHasRelease
	}
	sch := make(chan preempt.Status, d.buffSize)
	go func() {
		defer close(sch)
		ticker := time.NewTicker(d.refreshInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				send2Ch(sch, preempt.NewDefaultStatus(d.refreshTask(ctx)))
			case <-d.done:
				send2Ch(sch, preempt.NewDefaultStatus(preempt.ErrLeaserHasRelease))
				return
			case <-ctx.Done():
				send2Ch(sch, preempt.NewDefaultStatus(ctx.Err()))
				return
			}
		}
	}()

	return sch, nil
}

// refreshTask 控制重试和真的执行刷新,续约间隔 > 当次续约（含重试）的所有时间
func (d *taskLeaser) refreshTask(ctx context.Context) error {
	var i = 0
	var err error
	for {
		if ctx.Err() != nil {
			return err
		}
		if i >= int(d.maxRetryTimes) {
			// 这个分支是超时一定次数失败了
			return err
		}
		ctx1, cancel := context.WithTimeout(ctx, d.refreshTimeout)
		err = d.Refresh(ctx1)
		cancel()
		switch {
		case err == nil:
			return nil
		case errors.Is(err, context.DeadlineExceeded):
			i++
			time.Sleep(d.retrySleepTime)
		default:
			return err
		}
	}
}

func send2Ch(ch chan<- preempt.Status, st preempt.Status) {
	select {
	case ch <- st:
	default:

	}
}

var (
	ErrFailedToPreempt = errors.New("抢占失败")
	ErrTaskNotHold     = errors.New("未持有任务")
)

type taskRepository interface {
	//TryPreempt 抢占接口，会返回一批task给f方法
	TryPreempt(ctx context.Context, f func(ctx context.Context, ts []task.Task) (task.Task, error)) (task.Task, error)
	// PreemptTask 获取一个任务
	PreemptTask(ctx context.Context, tid int64, oldOwner string, newOwner string) error
	// ReleaseTask 释放任务
	ReleaseTask(ctx context.Context, tid int64, owner string) error
	// RefreshTask 续约
	RefreshTask(ctx context.Context, tid int64, owner string) error
}

type gormTaskRepository struct {
	db              *gorm.DB
	batchSize       int
	refreshInterval time.Duration
}

func newGormTaskRepository(db *gorm.DB, batchSize int, refreshInterval time.Duration) *gormTaskRepository {
	return &gormTaskRepository{
		db:              db,
		batchSize:       batchSize,
		refreshInterval: refreshInterval,
	}
}

// ReleaseTask 如果释放时释放了别人的抢到的任务怎么办，这里通过Owner控制
func (g *gormTaskRepository) ReleaseTask(ctx context.Context, tid int64, owner string) error {
	res := g.db.WithContext(ctx).Model(&TaskInfo{}).
		Where("id = ? AND owner = ?", tid, owner).
		Updates(map[string]interface{}{
			"status": TaskStatusWaiting,
			"utime":  time.Now().UnixMilli(),
		})

	if res.RowsAffected > 0 {
		return nil
	}
	return ErrTaskNotHold
}

func (g *gormTaskRepository) RefreshTask(ctx context.Context, tid int64, owner string) error {
	res := g.db.WithContext(ctx).Model(&TaskInfo{}).
		Where("id = ? AND owner = ? AND status = ?", tid, owner, TaskStatusRunning).
		Updates(map[string]any{
			"utime": time.Now().UnixMilli(),
		})
	if res.RowsAffected > 0 {
		return nil
	}
	return ErrTaskNotHold
}

func (g *gormTaskRepository) TryPreempt(ctx context.Context, f func(ctx context.Context, ts []task.Task) (task.Task, error)) (task.Task, error) {
	now := time.Now()
	var zero = task.Task{}
	// 续约的最晚时间
	t := now.UnixMilli() - g.refreshInterval.Milliseconds()
	var tasks []TaskInfo
	// 一次取一批
	err := g.db.WithContext(ctx).Model(&TaskInfo{}).
		Where("status = ? AND next_exec_time <= ?", TaskStatusWaiting, now.UnixMilli()).
		Or("status = ? AND utime < ?", TaskStatusRunning, t).
		Find(&tasks).Limit(g.batchSize).Error
	if err != nil {
		return zero, err
	}
	if len(tasks) < 1 {
		return zero, errs.ErrNoExecutableTask
	}
	ts := make([]task.Task, len(tasks))
	for i := 0; i < len(tasks); i++ {
		ts[i] = toTask(tasks[i])
	}
	return f(ctx, ts)
}

func (g *gormTaskRepository) PreemptTask(ctx context.Context, tid int64, oldOwner string, newOwner string) error {
	res := g.db.WithContext(ctx).Model(&TaskInfo{}).
		Where("id = ? AND owner = ?", tid, oldOwner).
		Updates(map[string]interface{}{
			"status": TaskStatusRunning,
			"utime":  time.Now().UnixMilli(),
			"owner":  newOwner,
		})
	if res.RowsAffected > 0 {
		return nil
	}
	return ErrFailedToPreempt
}
