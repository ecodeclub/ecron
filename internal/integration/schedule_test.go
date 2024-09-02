package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/ecodeclub/ecron/internal/executor"
	"github.com/ecodeclub/ecron/internal/integration/startup"
	"github.com/ecodeclub/ecron/internal/scheduler"
	"github.com/ecodeclub/ecron/internal/storage/mysql"
	"github.com/ecodeclub/ecron/internal/task"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"golang.org/x/sync/semaphore"
	"gorm.io/gorm"
	"log/slog"
	"testing"
	"time"
)

type SchedulerTestSuite struct {
	suite.Suite
	db     *gorm.DB
	s      *scheduler.PreemptScheduler
	logger *slog.Logger
}

func TestScheduler(t *testing.T) {
	suite.Run(t, new(SchedulerTestSuite))
}

func (s *SchedulerTestSuite) SetupSuite() {
	s.db = startup.InitDB()
	taskDAO := mysql.NewGormTaskDAO(s.db, 10, time.Second*5)
	executionDAO := mysql.NewGormExecutionDAO(s.db)
	limiter := semaphore.NewWeighted(100)
	s.logger = startup.InitLogger()
	s.s = scheduler.NewPreemptScheduler(taskDAO, executionDAO, time.Second*5, limiter, s.logger)
}

func (s *SchedulerTestSuite) TearDownTest() {
	// 清空所有数据库，并将自增主键恢复为1
	err := s.db.Exec("TRUNCATE TABLE `task_info`").Error
	assert.NoError(s.T(), err)
	s.db.Exec("TRUNCATE TABLE `execution`")
}

func (s *SchedulerTestSuite) TestScheduleLocalTask() {
	local := executor.NewLocalExecutor(s.logger)
	s.s.RegisterExecutor(local)
	t := s.T()
	now := time.Now()
	testCases := []struct {
		name   string
		before func(t *testing.T)
		after  func(t *testing.T)
		ctxFn  func(t *testing.T) context.Context
	}{
		{
			name: "找不到任务的执行器",
			before: func(t *testing.T) {
				// 先往数据库插入一条任务
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				err := s.db.WithContext(ctx).Create(mysql.TaskInfo{
					ID:       1,
					Name:     "Task1",
					Type:     task.TypeLocal,
					Cron:     "@every 10s",
					Executor: "non-existed executor",
					Status:   mysql.TaskStatusWaiting,
					// 一秒钟前就要执行了
					NextExecTime: now.Add(-1 * time.Second).UnixMilli(),
				}).Error
				require.NoError(t, err)
				// 注册执行函数
				local.RegisterFunc("Task1", func(ctx context.Context, t task.Task) error {
					fmt.Println("执行任务了", t.ID)
					return nil
				})
			},
			after: func(t *testing.T) {},
			ctxFn: func(t *testing.T) context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				go func() {
					time.Sleep(time.Second * 1)
					cancel()
				}()
				return ctx
			},
		},
		{
			name: "执行任务成功",
			before: func(t *testing.T) {
				// 先往数据库插入一条任务
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				err := s.db.WithContext(ctx).Create(mysql.TaskInfo{
					ID:           2,
					Name:         "Task2",
					Type:         task.TypeLocal,
					Cron:         "@every 10s",
					Executor:     local.Name(),
					Status:       mysql.TaskStatusWaiting,
					NextExecTime: now.Add(-1 * time.Second).UnixMilli(),
				}).Error
				require.NoError(t, err)
				// 注册执行函数
				local.RegisterFunc("Task2", func(ctx context.Context, t task.Task) error {
					fmt.Println("执行任务了", t.ID)
					return nil
				})
			},
			after: func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
				defer cancel()
				// 验证任务的状态、更新时间和下一次的执行时间
				var taskInfo mysql.TaskInfo
				err := s.db.WithContext(ctx).Model(&mysql.TaskInfo{}).
					Where("id = ?", 2).Find(&taskInfo).Error
				require.NoError(t, err)
				// 只有任务很快执行完，这个断言才能成立
				assert.Equal(t, mysql.TaskStatusWaiting, taskInfo.Status)
				assert.True(t, taskInfo.Utime > now.UnixMilli())
				assert.True(t, taskInfo.NextExecTime > time.Now().UnixMilli())

				// 验证任务的执行记录不为空，只有1条记录
				var execution []mysql.Execution
				err = s.db.WithContext(ctx).
					Where("tid = ?", 2).Find(&execution).Error
				require.NoError(t, err)
				assert.Len(t, execution, 1)
				assert.True(t, execution[0].Status == uint8(task.ExecStatusSuccess))

			},
			ctxFn: func(t *testing.T) context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				go func() {
					time.Sleep(time.Second * 1)
					cancel()
				}()
				return ctx
			},
		},
		{
			name: "执行任务失败",
			before: func(t *testing.T) {
				// 先往数据库插入一条任务
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				err := s.db.WithContext(ctx).Create(mysql.TaskInfo{
					ID:           3,
					Name:         "Task3",
					Type:         task.TypeLocal,
					Cron:         "@every 10s",
					Executor:     local.Name(),
					Status:       mysql.TaskStatusWaiting,
					NextExecTime: now.Add(-1 * time.Second).UnixMilli(),
				}).Error
				require.NoError(t, err)
				// 注册执行函数
				local.RegisterFunc("Task3", func(ctx context.Context, t task.Task) error {
					fmt.Println("执行任务了", t.ID)
					return errors.New("执行任务失败")
				})
			},
			after: func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
				defer cancel()
				// 验证任务的状态、更新时间和下一次的执行时间
				var taskInfo mysql.TaskInfo
				err := s.db.WithContext(ctx).Model(&mysql.TaskInfo{}).
					Where("id = ?", 3).Find(&taskInfo).Error
				require.NoError(t, err)
				// 只有任务很快执行完，这个断言才能成立
				assert.Equal(t, mysql.TaskStatusWaiting, taskInfo.Status)
				assert.True(t, taskInfo.Utime > now.UnixMilli())
				assert.True(t, taskInfo.NextExecTime > time.Now().UnixMilli())

				var history []mysql.Execution
				err = s.db.WithContext(ctx).
					Where("tid = ?", 3).Find(&history).Error
				require.NoError(t, err)
				assert.Len(t, history, 1)
				assert.True(t, history[0].Status == uint8(task.ExecStatusFailed))
			},
			ctxFn: func(t *testing.T) context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				go func() {
					time.Sleep(time.Second * 1)
					cancel()
				}()
				return ctx
			},
		},
		{
			name: "抢到了其它节点续约失败的任务，并且执行任务成功",
			before: func(t *testing.T) {
				// 先往数据库插入一条任务
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				err := s.db.WithContext(ctx).Create(mysql.TaskInfo{
					ID:           4,
					Name:         "Task4",
					Type:         task.TypeLocal,
					Cron:         "@every 10s",
					Executor:     local.Name(),
					Status:       mysql.TaskStatusRunning,
					NextExecTime: now.Add(-1 * time.Second).UnixMilli(),
					// 每5秒执行一次续约，最晚的续约是在5秒前完成
					Utime: now.Add(-6 * time.Second).UnixMilli(),
				}).Error
				require.NoError(t, err)
				// 注册执行函数
				local.RegisterFunc("Task4", func(ctx context.Context, t task.Task) error {
					fmt.Println("执行任务了", t.ID)
					return nil
				})
			},
			after: func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
				defer cancel()
				// 验证任务的状态、更新时间和下一次的执行时间
				var taskInfo mysql.TaskInfo
				err := s.db.WithContext(ctx).Model(&mysql.TaskInfo{}).
					Where("id = ?", 4).Find(&taskInfo).Error
				require.NoError(t, err)
				// 只有任务很快执行完，这个断言才能成立
				assert.Equal(t, mysql.TaskStatusWaiting, taskInfo.Status)
				assert.True(t, taskInfo.Utime > now.UnixMilli())
				assert.True(t, taskInfo.NextExecTime > time.Now().UnixMilli())

				var history []mysql.Execution
				err = s.db.WithContext(ctx).
					Where("tid = ?", 4).Find(&history).Error
				require.NoError(t, err)
				assert.Len(t, history, 1)
				assert.True(t, history[0].Status == uint8(task.ExecStatusSuccess))

			},
			ctxFn: func(t *testing.T) context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				go func() {
					time.Sleep(time.Second * 1)
					cancel()
				}()
				return ctx
			},
		},
		{
			name: "任务执行超时",
			before: func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				err := s.db.WithContext(ctx).Create(mysql.TaskInfo{
					ID:           5,
					Name:         "Task5",
					Type:         task.TypeLocal,
					Cron:         "@every 10s",
					Executor:     local.Name(),
					Status:       mysql.TaskStatusRunning,
					NextExecTime: now.Add(-1 * time.Second).UnixMilli(),
					Utime:        now.Add(-6 * time.Second).UnixMilli(),
				}).Error
				require.NoError(t, err)
				// 注册执行函数
				local.RegisterFunc("Task5", func(ctx context.Context, t task.Task) error {
					fmt.Println("执行任务了", t.ID)
					return context.DeadlineExceeded
				})
			},
			after: func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
				defer cancel()
				var taskInfo mysql.TaskInfo
				err := s.db.WithContext(ctx).Model(&mysql.TaskInfo{}).
					Where("id = ?", 5).Find(&taskInfo).Error
				require.NoError(t, err)
				// 只有任务很快执行完，这个断言才能成立
				assert.Equal(t, mysql.TaskStatusWaiting, taskInfo.Status)
				assert.True(t, taskInfo.Utime > now.UnixMilli())
				assert.True(t, taskInfo.NextExecTime > time.Now().UnixMilli())

				var history []mysql.Execution
				err = s.db.WithContext(ctx).
					Where("tid = ?", 5).Find(&history).Error
				require.NoError(t, err)
				assert.Len(t, history, 1)
				assert.True(t, history[0].Status == uint8(task.ExecStatusDeadlineExceeded))

			},
			ctxFn: func(t *testing.T) context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				go func() {
					time.Sleep(time.Second * 1)
					cancel()
				}()
				return ctx
			},
		},
		{
			name: "任务执行取消",
			before: func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				err := s.db.WithContext(ctx).Create(mysql.TaskInfo{
					ID:           6,
					Name:         "Task6",
					Type:         task.TypeLocal,
					Cron:         "@every 10s",
					Executor:     local.Name(),
					Status:       mysql.TaskStatusRunning,
					NextExecTime: now.Add(-1 * time.Second).UnixMilli(),
					// 每5秒执行一次续约，最晚的续约是在5秒前完成
					Utime: now.Add(-6 * time.Second).UnixMilli(),
				}).Error
				require.NoError(t, err)
				local.RegisterFunc("Task6", func(ctx context.Context, t task.Task) error {
					fmt.Println("执行任务了", t.ID)
					return context.Canceled
				})
			},
			after: func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
				defer cancel()
				// 验证任务的状态、更新时间和下一次的执行时间
				var taskInfo mysql.TaskInfo
				err := s.db.WithContext(ctx).Model(&mysql.TaskInfo{}).
					Where("id = ?", 6).Find(&taskInfo).Error
				require.NoError(t, err)
				// 只有任务很快执行完，这个断言才能成立
				assert.Equal(t, mysql.TaskStatusWaiting, taskInfo.Status)
				assert.True(t, taskInfo.Utime > now.UnixMilli())
				assert.True(t, taskInfo.NextExecTime > time.Now().UnixMilli())

				var history []mysql.Execution
				err = s.db.WithContext(ctx).
					Where("tid = ?", 6).Find(&history).Error
				require.NoError(t, err)
				assert.Len(t, history, 1)
				assert.True(t, history[0].Status == uint8(task.ExecStatusCancelled))

			},
			ctxFn: func(t *testing.T) context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				go func() {
					time.Sleep(time.Second * 1)
					cancel()
				}()
				return ctx
			},
		},
	}
	for _, tc := range testCases {
		// 只能一次执行一个测试用例，不然抢任务时抢到的可能是同一个
		t.Run(tc.name, func(t *testing.T) {
			tc.before(t)
			// 通过context强制让调度器退出
			err := s.s.Schedule(tc.ctxFn(t))
			assert.Equal(t, context.Canceled, err)
			tc.after(t)
		})
	}
}

func (s *SchedulerTestSuite) TestScheduleHttpTask() {
	httpExec := executor.NewHttpExecutor(s.logger)
	s.s.RegisterExecutor(httpExec)

	go func() {
		startup.StartHttpServer("8080")
	}()
	// 等待http server初始化完成
	time.Sleep(time.Second * 1)

	t := s.T()
	now := time.Now()
	testCases := []struct {
		name   string
		before func(t *testing.T)
		after  func(t *testing.T)
		ctxFn  func(t *testing.T) context.Context
	}{
		{
			name: "找不到任务的执行器",
			before: func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				err := s.db.WithContext(ctx).Create(mysql.TaskInfo{
					ID:       1,
					Name:     "Task1",
					Type:     task.TypeHttp,
					Cron:     "@every 10s",
					Executor: "non-existed executor",
					Status:   mysql.TaskStatusWaiting,
					// 一秒钟前就要执行了
					NextExecTime: now.Add(-1 * time.Second).UnixMilli(),
				}).Error
				require.NoError(t, err)
			},
			after: func(t *testing.T) {},
			ctxFn: func(t *testing.T) context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				go func() {
					time.Sleep(time.Second * 1)
					cancel()
				}()
				return ctx
			},
		},
		{
			name: "业务方直接返回执行成功",
			before: func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				err := s.db.WithContext(ctx).Create(mysql.TaskInfo{
					ID:           2,
					Name:         "Task2",
					Type:         task.TypeHttp,
					Cron:         "@every 10s",
					Executor:     httpExec.Name(),
					Status:       mysql.TaskStatusWaiting,
					NextExecTime: now.Add(-1 * time.Second).UnixMilli(),
					Cfg: marshal(t, executor.HttpCfg{
						Url:         "http://localhost:8080/success",
						TaskTimeout: time.Second * 5,
					}),
				}).Error
				require.NoError(t, err)
			},
			after: func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
				defer cancel()
				// 验证任务的状态、更新时间和下一次的执行时间
				var taskInfo mysql.TaskInfo
				err := s.db.WithContext(ctx).Model(&mysql.TaskInfo{}).
					Where("id = ?", 2).Find(&taskInfo).Error
				require.NoError(t, err)
				// 只有任务很快执行完，这个断言才能成立
				assert.Equal(t, mysql.TaskStatusWaiting, taskInfo.Status)
				assert.True(t, taskInfo.Utime > now.UnixMilli())
				assert.True(t, taskInfo.NextExecTime > time.Now().UnixMilli())

				// 验证任务的执行记录不为空，只有1条记录，以及任务执行进度
				var execution []mysql.Execution
				err = s.db.WithContext(ctx).
					Where("tid = ?", 2).Find(&execution).Error
				require.NoError(t, err)
				assert.Len(t, execution, 1)
				assert.True(t, execution[0].Status == uint8(task.ExecStatusSuccess))
				assert.True(t, execution[0].Progress == uint8(100))
			},
			ctxFn: func(t *testing.T) context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				go func() {
					time.Sleep(time.Second * 3)
					cancel()
				}()
				return ctx
			},
		},
		{
			name: "业务方直接返回执行失败",
			before: func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				err := s.db.WithContext(ctx).Create(mysql.TaskInfo{
					ID:       3,
					Name:     "Task3",
					Type:     task.TypeHttp,
					Cron:     "@every 10s",
					Executor: httpExec.Name(),
					Status:   mysql.TaskStatusWaiting,
					Cfg: marshal(t, executor.HttpCfg{
						Url:         "http://localhost:8080/failed",
						TaskTimeout: time.Second * 5,
					}),
					NextExecTime: now.Add(-1 * time.Second).UnixMilli(),
				}).Error
				require.NoError(t, err)
			},
			after: func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
				defer cancel()
				// 验证任务的状态、更新时间和下一次的执行时间
				var taskInfo mysql.TaskInfo
				err := s.db.WithContext(ctx).Model(&mysql.TaskInfo{}).
					Where("id = ?", 3).Find(&taskInfo).Error
				require.NoError(t, err)
				// 只有任务很快执行完，这个断言才能成立
				assert.Equal(t, mysql.TaskStatusWaiting, taskInfo.Status)
				assert.True(t, taskInfo.Utime > now.UnixMilli())
				assert.True(t, taskInfo.NextExecTime > time.Now().UnixMilli())

				var execution []mysql.Execution
				err = s.db.WithContext(ctx).
					Where("tid = ?", 3).Find(&execution).Error
				require.NoError(t, err)
				assert.Len(t, execution, 1)
				assert.True(t, execution[0].Status == uint8(task.ExecStatusFailed))
				assert.True(t, execution[0].Progress == uint8(0))
			},
			ctxFn: func(t *testing.T) context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				go func() {
					time.Sleep(time.Second * 3)
					cancel()
				}()
				return ctx
			},
		},
		{
			name: "抢到了其它节点续约失败的任务，并且执行任务成功",
			before: func(t *testing.T) {
				// 先往数据库插入一条任务
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				err := s.db.WithContext(ctx).Create(mysql.TaskInfo{
					ID:       4,
					Name:     "Task4",
					Type:     task.TypeHttp,
					Cron:     "@every 10s",
					Executor: httpExec.Name(),
					Status:   mysql.TaskStatusRunning,
					Cfg: marshal(t, executor.HttpCfg{
						Url:         "http://localhost:8080/success",
						TaskTimeout: time.Second * 5,
					}),
					NextExecTime: now.Add(-1 * time.Second).UnixMilli(),
					// 每5秒执行一次续约，最晚的续约是在5秒前完成
					Utime: now.Add(-6 * time.Second).UnixMilli(),
				}).Error
				require.NoError(t, err)
			},
			after: func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
				defer cancel()
				// 验证任务的状态、更新时间和下一次的执行时间
				var taskInfo mysql.TaskInfo
				err := s.db.WithContext(ctx).Model(&mysql.TaskInfo{}).
					Where("id = ?", 4).Find(&taskInfo).Error
				require.NoError(t, err)
				// 只有任务很快执行完，这个断言才能成立
				assert.Equal(t, mysql.TaskStatusWaiting, taskInfo.Status)
				assert.True(t, taskInfo.Utime > now.UnixMilli())
				assert.True(t, taskInfo.NextExecTime > time.Now().UnixMilli())

				var execution []mysql.Execution
				err = s.db.WithContext(ctx).
					Where("tid = ?", 4).Find(&execution).Error
				require.NoError(t, err)
				assert.Len(t, execution, 1)
				assert.True(t, execution[0].Status == uint8(task.ExecStatusSuccess))
				assert.True(t, execution[0].Progress == uint8(100))

			},
			ctxFn: func(t *testing.T) context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				go func() {
					time.Sleep(time.Second * 3)
					cancel()
				}()
				return ctx
			},
		},
		{
			name: "发起HTTP调用超时",
			before: func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				err := s.db.WithContext(ctx).Create(mysql.TaskInfo{
					ID:       5,
					Name:     "Task5",
					Type:     task.TypeHttp,
					Cron:     "@every 10s",
					Executor: httpExec.Name(),
					Status:   mysql.TaskStatusRunning,
					Cfg: marshal(t, executor.HttpCfg{
						Url:         "http://localhost:8080/timeout",
						TaskTimeout: time.Second * 5,
					}),
					NextExecTime: now.Add(-1 * time.Second).UnixMilli(),
					Utime:        now.Add(-6 * time.Second).UnixMilli(),
				}).Error
				require.NoError(t, err)
			},
			after: func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
				defer cancel()
				var taskInfo mysql.TaskInfo
				err := s.db.WithContext(ctx).Model(&mysql.TaskInfo{}).
					Where("id = ?", 5).Find(&taskInfo).Error
				require.NoError(t, err)
				// 只有任务很快执行完，这个断言才能成立
				assert.Equal(t, mysql.TaskStatusWaiting, taskInfo.Status)
				assert.True(t, taskInfo.Utime > now.UnixMilli())
				assert.True(t, taskInfo.NextExecTime > time.Now().UnixMilli())

				var execution []mysql.Execution
				err = s.db.WithContext(ctx).
					Where("tid = ?", 5).Find(&execution).Error
				require.NoError(t, err)
				assert.Len(t, execution, 1)
				assert.True(t, execution[0].Status == uint8(task.ExecStatusDeadlineExceeded))
				assert.True(t, execution[0].Progress == uint8(0))
			},
			ctxFn: func(t *testing.T) context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				go func() {
					time.Sleep(time.Second * 10)
					cancel()
				}()
				return ctx
			},
		},
		{
			name: "业务方错误",
			before: func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				err := s.db.WithContext(ctx).Create(mysql.TaskInfo{
					ID:           6,
					Name:         "Task6",
					Type:         task.TypeHttp,
					Cron:         "@every 10s",
					Executor:     httpExec.Name(),
					Status:       mysql.TaskStatusWaiting,
					NextExecTime: now.Add(-1 * time.Second).UnixMilli(),
					Cfg: marshal(t, executor.HttpCfg{
						Url:         "http://localhost:8080/error",
						TaskTimeout: time.Second * 5,
					}),
				}).Error
				require.NoError(t, err)
			},
			after: func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
				defer cancel()
				var taskInfo mysql.TaskInfo
				err := s.db.WithContext(ctx).Model(&mysql.TaskInfo{}).
					Where("id = ?", 6).Find(&taskInfo).Error
				require.NoError(t, err)
				// 只有任务很快执行完，这个断言才能成立
				assert.Equal(t, mysql.TaskStatusWaiting, taskInfo.Status)
				assert.True(t, taskInfo.Utime > now.UnixMilli())
				assert.True(t, taskInfo.NextExecTime > time.Now().UnixMilli())

				var execution []mysql.Execution
				err = s.db.WithContext(ctx).
					Where("tid = ?", 6).Find(&execution).Error
				require.NoError(t, err)
				assert.Len(t, execution, 1)
				assert.True(t, execution[0].Status == uint8(task.ExecStatusFailed))
				assert.True(t, execution[0].Progress == uint8(0))
			},
			ctxFn: func(t *testing.T) context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				go func() {
					time.Sleep(time.Second * 3)
					cancel()
				}()
				return ctx
			},
		},
		{
			name: "开启任务探查，最终执行成功",
			before: func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				err := s.db.WithContext(ctx).Create(mysql.TaskInfo{
					ID:       7,
					Name:     "Task7",
					Type:     task.TypeHttp,
					Cron:     "@every 10s",
					Executor: httpExec.Name(),
					Status:   mysql.TaskStatusWaiting,
					Cfg: marshal(t, executor.HttpCfg{
						Url:         "http://localhost:8080/explore_success",
						TaskTimeout: time.Second * 5,
						// 每个1秒探查一次
						ExploreInterval: time.Second,
					}),
					NextExecTime: now.Add(-1 * time.Second).UnixMilli(),
				}).Error
				require.NoError(t, err)
			},
			after: func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
				defer cancel()
				// 验证任务的状态、更新时间和下一次的执行时间
				var taskInfo mysql.TaskInfo
				err := s.db.WithContext(ctx).Model(&mysql.TaskInfo{}).
					Where("id = ?", 7).Find(&taskInfo).Error
				require.NoError(t, err)
				// 只有任务很快执行完，这个断言才能成立
				assert.Equal(t, mysql.TaskStatusWaiting, taskInfo.Status)
				assert.True(t, taskInfo.Utime > now.UnixMilli())
				assert.True(t, taskInfo.NextExecTime > time.Now().UnixMilli())

				var execution []mysql.Execution
				err = s.db.WithContext(ctx).
					Where("tid = ?", 7).Find(&execution).Error
				require.NoError(t, err)
				assert.Len(t, execution, 1)
				assert.Equal(t, uint8(task.ExecStatusSuccess), execution[0].Status)
				assert.Equal(t, uint8(100), execution[0].Progress)
			},
			ctxFn: func(t *testing.T) context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				go func() {
					time.Sleep(time.Second * 10)
					cancel()
				}()
				return ctx
			},
		},
		{
			name: "开启任务探查，最终执行失败",
			before: func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				err := s.db.WithContext(ctx).Create(mysql.TaskInfo{
					ID:       8,
					Name:     "Task8",
					Type:     task.TypeHttp,
					Cron:     "@every 10s",
					Executor: httpExec.Name(),
					Status:   mysql.TaskStatusWaiting,
					Cfg: marshal(t, executor.HttpCfg{
						Url:         "http://localhost:8080/explore_failed",
						TaskTimeout: time.Second * 5,
						// 每个1秒探查一次
						ExploreInterval: time.Second,
					}),
					NextExecTime: now.Add(-1 * time.Second).UnixMilli(),
				}).Error
				require.NoError(t, err)
			},
			after: func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
				defer cancel()
				// 验证任务的状态、更新时间和下一次的执行时间
				var taskInfo mysql.TaskInfo
				err := s.db.WithContext(ctx).Model(&mysql.TaskInfo{}).
					Where("id = ?", 8).Find(&taskInfo).Error
				require.NoError(t, err)
				// 只有任务很快执行完，这个断言才能成立
				assert.Equal(t, mysql.TaskStatusWaiting, taskInfo.Status)
				assert.True(t, taskInfo.Utime > now.UnixMilli())
				assert.True(t, taskInfo.NextExecTime > time.Now().UnixMilli())

				var execution []mysql.Execution
				err = s.db.WithContext(ctx).
					Where("tid = ?", 8).Find(&execution).Error
				require.NoError(t, err)
				assert.Len(t, execution, 1)
				assert.Equal(t, uint8(task.ExecStatusFailed), execution[0].Status)
				assert.Equal(t, uint8(50), execution[0].Progress)
			},
			ctxFn: func(t *testing.T) context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				go func() {
					time.Sleep(time.Second * 10)
					cancel()
				}()
				return ctx
			},
		},
		{
			name: "开启任务探查，超过任务最大执行时长",
			before: func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				err := s.db.WithContext(ctx).Create(mysql.TaskInfo{
					ID:       9,
					Name:     "Task9",
					Type:     task.TypeHttp,
					Cron:     "@every 10s",
					Executor: httpExec.Name(),
					Status:   mysql.TaskStatusWaiting,
					Cfg: marshal(t, executor.HttpCfg{
						Url:         "http://localhost:8080/running",
						TaskTimeout: time.Second * 3,
						// 每个1秒探查一次
						ExploreInterval: time.Second,
					}),
					NextExecTime: now.Add(-1 * time.Second).UnixMilli(),
				}).Error
				require.NoError(t, err)
			},
			after: func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
				defer cancel()
				// 验证任务的状态、更新时间和下一次的执行时间
				var taskInfo mysql.TaskInfo
				err := s.db.WithContext(ctx).Model(&mysql.TaskInfo{}).
					Where("id = ?", 9).Find(&taskInfo).Error
				require.NoError(t, err)
				// 只有任务很快执行完，这个断言才能成立
				assert.Equal(t, mysql.TaskStatusWaiting, taskInfo.Status)
				assert.True(t, taskInfo.Utime > now.UnixMilli())
				assert.True(t, taskInfo.NextExecTime > time.Now().UnixMilli())

				var execution []mysql.Execution
				err = s.db.WithContext(ctx).
					Where("tid = ?", 9).Find(&execution).Error
				require.NoError(t, err)
				assert.Len(t, execution, 1)
				assert.Equal(t, uint8(task.ExecStatusDeadlineExceeded), execution[0].Status)
				assert.Equal(t, uint8(10), execution[0].Progress)
			},
			ctxFn: func(t *testing.T) context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				go func() {
					time.Sleep(time.Second * 10)
					cancel()
				}()
				return ctx
			},
		},
		{
			name: "开启任务探查，主动取消任务执行",
			before: func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				err := s.db.WithContext(ctx).Create(mysql.TaskInfo{
					ID:       10,
					Name:     "Task10",
					Type:     task.TypeHttp,
					Cron:     "@every 10s",
					Executor: httpExec.Name(),
					Status:   mysql.TaskStatusWaiting,
					Cfg: marshal(t, executor.HttpCfg{
						Url:         "http://localhost:8080/running",
						TaskTimeout: time.Minute,
						// 每个1秒探查一次
						ExploreInterval: time.Second,
					}),
					NextExecTime: now.Add(-1 * time.Second).UnixMilli(),
				}).Error
				require.NoError(t, err)
			},
			after: func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
				defer cancel()
				// 验证任务的状态、更新时间和下一次的执行时间
				var taskInfo mysql.TaskInfo
				err := s.db.WithContext(ctx).Model(&mysql.TaskInfo{}).
					Where("id = ?", 10).Find(&taskInfo).Error
				require.NoError(t, err)
				// 只有任务很快执行完，这个断言才能成立
				assert.Equal(t, mysql.TaskStatusWaiting, taskInfo.Status)
				assert.True(t, taskInfo.Utime > now.UnixMilli())
				assert.True(t, taskInfo.NextExecTime > time.Now().UnixMilli())

				var execution []mysql.Execution
				err = s.db.WithContext(ctx).
					Where("tid = ?", 10).Find(&execution).Error
				require.NoError(t, err)
				assert.Len(t, execution, 1)
				assert.Equal(t, uint8(task.ExecStatusCancelled), execution[0].Status)
				assert.Equal(t, uint8(10), execution[0].Progress)
			},
			ctxFn: func(t *testing.T) context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				go func() {
					time.Sleep(time.Second * 5)
					cancel()
				}()
				return ctx
			},
		},
	}
	for _, tc := range testCases {
		// 只能一次执行一个测试用例，不然抢任务时抢到的可能是同一个
		t.Run(tc.name, func(t *testing.T) {
			tc.before(t)
			// 通过context强制让调度器退出
			err := s.s.Schedule(tc.ctxFn(t))
			assert.Equal(t, context.Canceled, err)
			tc.after(t)
		})
	}
}

func marshal(t *testing.T, cfg executor.HttpCfg) string {
	res, err := json.Marshal(cfg)
	require.NoError(t, err)
	return string(res)
}
