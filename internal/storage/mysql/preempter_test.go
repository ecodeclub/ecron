package mysql

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/ecodeclub/ecron/internal/errs"
	"github.com/ecodeclub/ecron/internal/preempt"
	daomysqlmocks "github.com/ecodeclub/ecron/internal/storage/mysql/mocks"
	"github.com/ecodeclub/ecron/internal/task"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"golang.org/x/net/context"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"testing"
	"time"
)

func TestPreempt_Preempt(t *testing.T) {
	testCases := []struct {
		name     string
		mock     func(ctrl *gomock.Controller) taskRepository
		wantErr  error
		ctxFn    func() context.Context
		wantTask preempt.TaskLeaser
	}{
		{
			name: "抢占成功",
			mock: func(ctrl *gomock.Controller) taskRepository {
				td := daomysqlmocks.NewMocktaskRepository(ctrl)

				td.EXPECT().TryPreempt(gomock.Any(), gomock.Any()).Return(task.Task{
					ID: 1,
				}, nil)

				return td
			},
			wantTask: &taskLeaser{
				t: task.Task{
					ID: 1,
				},
			},
			ctxFn: func() context.Context {
				return context.Background()
			},
		},
		{
			name: "抢占失败，没有任务了",
			mock: func(ctrl *gomock.Controller) taskRepository {
				td := daomysqlmocks.NewMocktaskRepository(ctrl)

				td.EXPECT().TryPreempt(gomock.Any(), gomock.Any()).Return(task.Task{}, ErrFailedToPreempt)

				return td
			},
			wantErr: ErrFailedToPreempt,
			ctxFn: func() context.Context {
				return context.Background()
			},
		},
		{
			name: "抢占失败，超时",
			mock: func(ctrl *gomock.Controller) taskRepository {
				td := daomysqlmocks.NewMocktaskRepository(ctrl)

				td.EXPECT().TryPreempt(gomock.Any(), gomock.Any()).Return(task.Task{}, context.DeadlineExceeded)

				return td
			},
			wantErr: context.DeadlineExceeded,
			ctxFn: func() context.Context {

				ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond*1)
				defer cancel()
				return ctx
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			td := tc.mock(ctrl)

			preempter := newPreempter(td)
			fn := tc.ctxFn()
			t1, err := preempter.Preempt(fn)
			assert.Equal(t, tc.wantErr, err)
			if err == nil {
				assert.Equal(t, tc.wantTask.GetTask(), t1.GetTask())
			}
		})
	}
}

func TestPreempt_TaskLeaser_AutoRefresh(t *testing.T) {
	testCases := []struct {
		name          string
		mock          func(ctrl *gomock.Controller) taskRepository
		wantLeaserErr error
		wantErr       error
		ctxFn         func() context.Context
	}{
		{
			name: "UpdateUtime error",
			mock: func(ctrl *gomock.Controller) taskRepository {
				td := daomysqlmocks.NewMocktaskRepository(ctrl)

				t := task.Task{
					ID:    1,
					Owner: "tom",
				}
				td.EXPECT().TryPreempt(gomock.Any(), gomock.Any()).Return(t, nil)
				//td.EXPECT().PreemptTask(gomock.Any(), t, "1-appid").Return(nil)
				td.EXPECT().RefreshTask(gomock.Any(), t.ID, t.Owner).AnyTimes().Return(errors.New("UpdateUtime error"))
				td.EXPECT().ReleaseTask(gomock.Any(), t.ID, t.Owner).AnyTimes().Return(nil)

				return td
			},
			ctxFn: func() context.Context {
				return context.Background()
			},
			wantErr: errors.New("UpdateUtime error"),
		},
		{
			name: "context超时了",
			mock: func(ctrl *gomock.Controller) taskRepository {
				td := daomysqlmocks.NewMocktaskRepository(ctrl)

				t := task.Task{
					ID:    1,
					Owner: "tom",
				}
				td.EXPECT().TryPreempt(gomock.Any(), gomock.Any()).Return(t, nil)
				td.EXPECT().RefreshTask(gomock.Any(), t.ID, t.Owner).AnyTimes().Return(context.DeadlineExceeded)
				td.EXPECT().ReleaseTask(gomock.Any(), t.ID, t.Owner).AnyTimes().Return(nil)

				return td
			},
			wantErr: context.DeadlineExceeded,
			ctxFn: func() context.Context {
				return context.Background()
			},
		},
		{
			name: "context被取消",
			mock: func(ctrl *gomock.Controller) taskRepository {
				td := daomysqlmocks.NewMocktaskRepository(ctrl)

				t := task.Task{
					ID:    1,
					Owner: "tom",
				}
				td.EXPECT().TryPreempt(gomock.Any(), gomock.Any()).Times(1).Return(t, nil)

				td.EXPECT().RefreshTask(gomock.Any(), t.ID, t.Owner).AnyTimes().Return(nil)
				td.EXPECT().ReleaseTask(gomock.Any(), t.ID, t.Owner).AnyTimes().Return(nil)

				return td
			},
			wantErr: context.Canceled,
			ctxFn: func() context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				go func() {
					time.Sleep(time.Second * 11)
					cancel()
				}()
				return ctx
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			td := tc.mock(ctrl)

			preempter := newPreempter(td)
			// 这里的l有问题
			l, err := preempter.Preempt(context.Background())
			assert.NoError(t, err)

			ctxFn := tc.ctxFn()
			sch, err2 := l.AutoRefresh(ctxFn)
			assert.Equal(t, tc.wantLeaserErr, err2)
			var shouldContinue = true
			for shouldContinue {
				select {
				case s, ok := <-sch:
					if ok && s.Err() != nil {
						assert.Equal(t, tc.wantErr, s.Err())
						err1 := l.Release(ctxFn)
						assert.NoError(t, err1)
						shouldContinue = false
					}
				}
			}
		})
	}
}

func TestPreempt_TaskLeaser_Release(t *testing.T) {
	testCases := []struct {
		name    string
		mock    func(ctrl *gomock.Controller) taskRepository
		wantErr error
		ctxFn   func() context.Context
	}{
		{
			name: "正常结束（release）",
			mock: func(ctrl *gomock.Controller) taskRepository {
				td := daomysqlmocks.NewMocktaskRepository(ctrl)

				t := task.Task{
					ID:    1,
					Owner: "tom",
				}
				td.EXPECT().TryPreempt(gomock.Any(), gomock.Any()).Return(t, nil)

				td.EXPECT().RefreshTask(gomock.Any(), t.ID, t.Owner).AnyTimes().Return(nil)
				td.EXPECT().ReleaseTask(gomock.Any(), t.ID, t.Owner).AnyTimes().Return(nil)

				return td
			},
			wantErr: preempt.ErrLeaserHasRelease,
			ctxFn: func() context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				go func() {
					time.Sleep(time.Second * 60)
					cancel()
				}()
				return ctx
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			td := tc.mock(ctrl)

			preempter := newPreempter(td)
			l, err := preempter.Preempt(context.Background())
			assert.NoError(t, err)

			ctxFn := tc.ctxFn()
			sch, err2 := l.AutoRefresh(ctxFn)
			assert.NoError(t, err2)
			go func() {
				var shouldContinue = true
				for shouldContinue {
					select {
					case s, ok := <-sch:
						if ok && s.Err() != nil {
							assert.Equal(t, tc.wantErr, s.Err())
							err1 := l.Release(ctxFn)
							assert.NoError(t, err1)
							shouldContinue = false
						}
					}
				}
			}()
			time.Sleep(time.Second * 6)
			err1 := l.Release(ctxFn)
			assert.NoError(t, err1)

			time.Sleep(time.Second * 12)
			select {
			case _, ok := <-sch:
				// 这里代表 sch已经被关闭了
				assert.Equal(t, ok, false)
			}

		})
	}
}

func TestGormTaskRepository_TryPreempt(t *testing.T) {
	testCases := []struct {
		name            string
		batchSize       int
		refreshInterval time.Duration
		sqlMock         func(t *testing.T) *sql.DB
		selectMock      func(ctx context.Context, ts []task.Task) (task.Task, error)
		wantTask        task.Task
		wantErr         error
	}{
		{
			name:            "查询数据库错误",
			batchSize:       10,
			refreshInterval: 10 * time.Second,
			sqlMock: func(t *testing.T) *sql.DB {
				mockDB, mock, err := sqlmock.New()
				require.NoError(t, err)
				mock.ExpectQuery("^SELECT \\* FROM `task_info`").
					WillReturnError(errors.New("select error"))
				return mockDB
			},
			wantTask: task.Task{},
			wantErr:  errors.New("select error"),
		},
		{
			name:            "当前没有可以执行的任务",
			batchSize:       10,
			refreshInterval: 10 * time.Second,
			sqlMock: func(t *testing.T) *sql.DB {
				mockDB, mock, err := sqlmock.New()
				require.NoError(t, err)
				// 返回0行
				mock.ExpectQuery("^SELECT \\* FROM `task_info`").WillReturnRows(sqlmock.NewRows(nil))
				return mockDB
			},
			wantTask: task.Task{},
			wantErr:  errs.ErrNoExecutableTask,
		},
		{
			name:            "获取任务并抢占成功",
			batchSize:       10,
			refreshInterval: 10 * time.Second,
			sqlMock: func(t *testing.T) *sql.DB {
				mockDB, mock, err := sqlmock.New()
				require.NoError(t, err)
				// 查询结果只返回一条任务
				values := [][]driver.Value{
					{
						1,
						"test1",
						task.TypeLocal,
						"*/5 * * * * ?",
						"local",
						5,
						"owner",
						TaskStatusWaiting,
						"",
						time.Now().UnixMilli(),
						time.Now().UnixMilli(),
						time.Now().UnixMilli(),
					},
				}
				rows := sqlmock.NewRows([]string{
					"id", "name", "type",
					"cron", "executor", "Version", "Owner", "Status", "cfg",
					"next_exec_time", "ctime", "utime",
				}).AddRows(values...)
				mock.ExpectQuery("^SELECT \\* FROM `task_info`").WillReturnRows(rows)
				mock.ExpectExec("UPDATE `task_info`").WillReturnResult(sqlmock.NewResult(1, 1))
				return mockDB
			},
			wantTask: task.Task{
				ID:       1,
				Name:     "test1",
				Type:     task.TypeLocal,
				Executor: "local",
				Cfg:      "",
				Owner:    "owner",
				CronExp:  "*/5 * * * * ?",
			},
			selectMock: func(ctx context.Context, ts []task.Task) (task.Task, error) {
				for _, t2 := range ts {
					t := t2
					if t.ID == 1 {
						return t, nil
					}
				}
				return task.Task{}, ErrFailedToPreempt
			},
			wantErr: nil,
		},
		{
			name:            "获取任务并抢占失败",
			batchSize:       10,
			refreshInterval: 10 * time.Second,
			sqlMock: func(t *testing.T) *sql.DB {
				mockDB, mock, err := sqlmock.New()
				require.NoError(t, err)
				// 查询结果只返回一条任务
				values := [][]driver.Value{
					{
						0,
						"test1",
						task.TypeLocal,
						"*/5 * * * * ?",
						"local",
						5, "owner", TaskStatusWaiting,
						"",
						time.Now().UnixMilli(),
						time.Now().UnixMilli(),
						time.Now().UnixMilli(),
					},
				}
				rows := sqlmock.NewRows([]string{
					"id", "name", "type",
					"cron", "executor", "Version", "Owner", "Status", "cfg",
					"next_exec_time", "ctime", "utime",
				}).AddRows(values...)
				mock.ExpectQuery("^SELECT \\* FROM `task_info`").WillReturnRows(rows)
				mock.ExpectExec("UPDATE `task_info`").WillReturnResult(sqlmock.NewResult(1, 1))
				return mockDB
			},
			selectMock: func(ctx context.Context, ts []task.Task) (task.Task, error) {
				for _, t2 := range ts {
					t := t2
					if t.ID == 1 {
						return t, nil
					}
				}
				return task.Task{}, ErrFailedToPreempt
			},
			wantErr: ErrFailedToPreempt,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			sqlDB := tc.sqlMock(t)
			db, err := gorm.Open(mysql.New(mysql.Config{
				Conn:                      sqlDB,
				SkipInitializeWithVersion: true,
			}), &gorm.Config{
				DisableAutomaticPing:   true,
				SkipDefaultTransaction: true,
			})
			require.NoError(t, err)

			dao := newGormTaskRepository(db, tc.batchSize, tc.refreshInterval)

			res, err := dao.TryPreempt(context.Background(), tc.selectMock)
			if err != nil {
				assert.Equal(t, tc.wantErr, err)
				return
			}
			assert.Equal(t, tc.wantTask.ID, res.ID)
			assert.Equal(t, tc.wantTask.Name, res.Name)
			assert.Equal(t, tc.wantTask.Type, res.Type)
			assert.Equal(t, tc.wantTask.Executor, res.Executor)
			assert.Equal(t, tc.wantTask.CronExp, res.CronExp)
			assert.Equal(t, tc.wantTask.Owner, res.Owner)
			assert.True(t, res.Ctime.UnixMilli() > 0)
			assert.True(t, res.Utime.UnixMilli() > 0)
		})
	}
}

func TestGormTaskRepository_TryPreemptTask(t *testing.T) {

	zero := task.Task{
		ID:    2,
		Owner: "tom",
	}
	newOwner := "jack"
	testCases := []struct {
		name            string
		batchSize       int
		refreshInterval time.Duration
		sqlMock         func(t *testing.T) *sql.DB

		wantErr  error
		f        func(ctx context.Context, ts []task.Task) (task.Task, error)
		wantTask task.Task
	}{
		{
			name:            "抢占成功",
			batchSize:       10,
			refreshInterval: 10 * time.Second,
			sqlMock: func(t *testing.T) *sql.DB {
				mockDB, mock, err := sqlmock.New()
				require.NoError(t, err)
				rows := sqlmock.NewRows([]string{"id", "name", "type", "cron", "executor", "owner", "status", "cfg", "next_exec_time", "ctime", "utime"}).
					AddRow(1, "Task 1", "Type 1", "*/5 * * * *", "Executor 1", "other", 1, "{}", 1631990400, 1631990400, 1631990400).
					AddRow(2, "Task 2", "Type 2", "0 0 * * *", "Executor 2", "tom", 0, "{}", 1631990400, 1631990400, 1631990400)
				mock.ExpectQuery("SELECT .* FROM  `task_info`").WillReturnRows(rows)
				return mockDB
			},
			f: func(ctx context.Context, ts []task.Task) (task.Task, error) {
				for _, t2 := range ts {
					t2 := t2
					if t2.ID == zero.ID && t2.Owner == zero.Owner {
						return task.Task{
							ID:    zero.ID,
							Owner: newOwner,
						}, nil
					}
				}
				return task.Task{}, preempt.ErrNoTaskToPreempt
			},
			wantTask: task.Task{
				ID:    2,
				Owner: newOwner,
			},
		},
		{
			name:            "没有任务可以抢占了",
			batchSize:       10,
			refreshInterval: 10 * time.Second,
			sqlMock: func(t *testing.T) *sql.DB {
				mockDB, mock, err := sqlmock.New()
				require.NoError(t, err)
				rows := sqlmock.NewRows([]string{"id", "name", "type", "cron", "executor", "owner", "status", "cfg", "next_exec_time", "ctime", "utime"}).
					AddRow(1, "Task 1", "Type 1", "*/5 * * * *", "Executor 1", "other", 1, "{}", 1631990400, 1631990400, 1631990400).
					AddRow(2, "Task 2", "Type 2", "0 0 * * *", "Executor 2", "jack", 0, "{}", 1631990400, 1631990400, 1631990400)
				mock.ExpectQuery("SELECT .* FROM  `task_info`").WillReturnRows(rows)
				return mockDB
			},
			f: func(ctx context.Context, ts []task.Task) (task.Task, error) {
				for _, t2 := range ts {
					t2 := t2
					if t2.ID == zero.ID && t2.Owner == zero.Owner {
						t2.Owner = newOwner
						return t2, nil
					}
				}
				return task.Task{}, preempt.ErrNoTaskToPreempt
			},

			wantErr: preempt.ErrNoTaskToPreempt,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			sqlDB := tc.sqlMock(t)
			db, err := gorm.Open(mysql.New(mysql.Config{
				Conn:                      sqlDB,
				SkipInitializeWithVersion: true,
			}), &gorm.Config{
				DisableAutomaticPing:   true,
				SkipDefaultTransaction: true,
			})
			require.NoError(t, err)

			dao := newGormTaskRepository(db, tc.batchSize, tc.refreshInterval)
			tryPreempt, err := dao.TryPreempt(context.Background(), tc.f)
			assert.Equal(t, tc.wantErr, err)
			if err == nil {
				assert.Equal(t, tc.wantTask, tryPreempt)
				return
			}
		})
	}
}

func TestGormTaskRepository_PreemptTask(t *testing.T) {

	zero := task.Task{
		ID:    1,
		Owner: "tom",
	}
	testCases := []struct {
		name            string
		batchSize       int
		refreshInterval time.Duration
		sqlMock         func(t *testing.T) *sql.DB
		tid             int64
		old             string
		new             string
		wantErr         error
	}{
		{
			name:            "抢占成功",
			batchSize:       10,
			refreshInterval: 10 * time.Second,
			sqlMock: func(t *testing.T) *sql.DB {
				mockDB, mock, err := sqlmock.New()
				require.NoError(t, err)
				//mock.ExpectExec("UPDATE `task_info`").WithArgs(zero.ID, zero.Owner).WillReturnResult(sqlmock.NewResult(1, 1))
				mock.ExpectExec("UPDATE `task_info`").
					WithArgs("jack", sqlmock.AnyArg(), sqlmock.AnyArg(), zero.ID, zero.Owner).WillReturnResult(sqlmock.NewResult(1, 1))
				return mockDB
			},
			tid:     zero.ID,
			old:     zero.Owner,
			new:     "jack",
			wantErr: nil,
		},
		{
			name:            "抢占失败",
			batchSize:       10,
			refreshInterval: 10 * time.Second,
			sqlMock: func(t *testing.T) *sql.DB {
				mockDB, mock, err := sqlmock.New()
				require.NoError(t, err)
				mock.ExpectExec("UPDATE `task_info`").WithArgs("jack", sqlmock.AnyArg(), sqlmock.AnyArg(), zero.ID, zero.Owner).WillReturnResult(sqlmock.NewResult(0, 0))
				return mockDB
			},
			tid:     zero.ID,
			old:     "jack",
			new:     "tom",
			wantErr: ErrFailedToPreempt,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			sqlDB := tc.sqlMock(t)
			db, err := gorm.Open(mysql.New(mysql.Config{
				Conn:                      sqlDB,
				SkipInitializeWithVersion: true,
			}), &gorm.Config{
				DisableAutomaticPing:   true,
				SkipDefaultTransaction: true,
			})
			require.NoError(t, err)

			dao := newGormTaskRepository(db, tc.batchSize, tc.refreshInterval)

			err = dao.PreemptTask(context.Background(), tc.tid, tc.old, tc.new)
			assert.Equal(t, tc.wantErr, err)
		})
	}
}

func TestGormTaskRepository_RefreshTask(t *testing.T) {

	zero := task.Task{
		ID:    1,
		Owner: "tom",
	}
	testCases := []struct {
		name            string
		batchSize       int
		refreshInterval time.Duration
		sqlMock         func(t *testing.T) *sql.DB
		tid             int64
		owner           string
		wantErr         error
		status          int8
	}{
		{
			name:            "续约成功",
			batchSize:       10,
			refreshInterval: 10 * time.Second,
			sqlMock: func(t *testing.T) *sql.DB {
				mockDB, mock, err := sqlmock.New()
				require.NoError(t, err)
				//mock.ExpectExec("UPDATE `task_info`").WithArgs(zero.ID, zero.Owner).WillReturnResult(sqlmock.NewResult(1, 1))
				mock.ExpectExec("UPDATE `task_info`").
					WithArgs(sqlmock.AnyArg(), zero.ID, zero.Owner, TaskStatusRunning).WillReturnResult(sqlmock.NewResult(1, 1))
				return mockDB
			},
			tid:     zero.ID,
			owner:   zero.Owner,
			status:  TaskStatusRunning,
			wantErr: nil,
		},
		{
			name:            "续约失败",
			batchSize:       10,
			refreshInterval: 10 * time.Second,
			sqlMock: func(t *testing.T) *sql.DB {
				mockDB, mock, err := sqlmock.New()
				require.NoError(t, err)
				mock.ExpectExec("UPDATE `task_info`").WithArgs(sqlmock.AnyArg(), zero.ID, zero.Owner, TaskStatusRunning).WillReturnResult(sqlmock.NewResult(0, 0))
				return mockDB
			},
			tid:     zero.ID,
			owner:   "jack",
			status:  TaskStatusRunning,
			wantErr: ErrTaskNotHold,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			sqlDB := tc.sqlMock(t)
			db, err := gorm.Open(mysql.New(mysql.Config{
				Conn:                      sqlDB,
				SkipInitializeWithVersion: true,
			}), &gorm.Config{
				DisableAutomaticPing:   true,
				SkipDefaultTransaction: true,
			})
			require.NoError(t, err)
			dao := newGormTaskRepository(db, tc.batchSize, tc.refreshInterval)
			err = dao.RefreshTask(context.Background(), tc.tid, tc.owner)
			if err != nil {
				assert.Equal(t, tc.wantErr, err)
				return
			}
		})
	}
}

func TestGormTaskRepository_ReleaseTask(t *testing.T) {

	zero := task.Task{
		ID:    1,
		Owner: "tom",
	}
	testCases := []struct {
		name            string
		batchSize       int
		refreshInterval time.Duration
		sqlMock         func(t *testing.T) *sql.DB
		tid             int64
		owner           string
		wantErr         error
	}{
		{
			name:            "解除成功",
			batchSize:       10,
			refreshInterval: 10 * time.Second,
			sqlMock: func(t *testing.T) *sql.DB {
				mockDB, mock, err := sqlmock.New()
				require.NoError(t, err)
				//mock.ExpectExec("UPDATE `task_info`").WithArgs(zero.ID, zero.Owner).WillReturnResult(sqlmock.NewResult(1, 1))
				mock.ExpectExec("UPDATE `task_info`").
					WithArgs(TaskStatusWaiting, sqlmock.AnyArg(), zero.ID, zero.Owner).WillReturnResult(sqlmock.NewResult(1, 1))
				return mockDB
			},
			tid:     zero.ID,
			owner:   zero.Owner,
			wantErr: nil,
		},
		{
			name:            "解除失败",
			batchSize:       10,
			refreshInterval: 10 * time.Second,
			sqlMock: func(t *testing.T) *sql.DB {
				mockDB, mock, err := sqlmock.New()
				require.NoError(t, err)
				mock.ExpectExec("UPDATE `task_info`").WithArgs(TaskStatusWaiting, sqlmock.AnyArg(), zero.ID, zero.Owner).WillReturnResult(sqlmock.NewResult(0, 0))
				return mockDB
			},
			tid:     zero.ID,
			owner:   "jack",
			wantErr: ErrTaskNotHold,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			sqlDB := tc.sqlMock(t)
			db, err := gorm.Open(mysql.New(mysql.Config{
				Conn:                      sqlDB,
				SkipInitializeWithVersion: true,
			}), &gorm.Config{
				DisableAutomaticPing:   true,
				SkipDefaultTransaction: true,
			})
			require.NoError(t, err)
			dao := newGormTaskRepository(db, tc.batchSize, tc.refreshInterval)
			err = dao.ReleaseTask(context.Background(), tc.tid, tc.owner)
			if err != nil {
				assert.Equal(t, tc.wantErr, err)
				return
			}
		})
	}
}
