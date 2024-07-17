package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/ecodeclub/ecron/internal/errs"
	"github.com/ecodeclub/ecron/internal/task"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"testing"
	"time"
)

func TestGormTaskDAO_GormReturn(t *testing.T) {
	db, err := gorm.Open(mysql.Open("root:root@tcp(localhost:13316)/ecron"))
	if err != nil {
		require.NoError(t, err)
	}
	for i := 0; i < 3; i++ {
		var taskInfo TaskInfo
		taskInfo.ID = int64(i)
		taskInfo.Status = int8(i)
		db.Create(&taskInfo)
	}
	var res []TaskInfo
	err = db.Model(&TaskInfo{}).Where("id > 0").Find(&res).Error
	require.NoError(t, err)
	assert.Equal(t, 3, len(res))

	var taskInfos []TaskInfo
	err = db.Model(&TaskInfo{}).Where("id > 10").Find(&taskInfos).Error
	assert.NoError(t, err)
	// 没有符合查询条件的数据时，不会返回 err
	assert.Equal(t, 0, len(taskInfos))
}

func TestGormTaskDAO_Preempt(t *testing.T) {
	testCases := []struct {
		name            string
		batchSize       int
		refreshInterval time.Duration
		randIndex       func(num int) int
		sqlMock         func(t *testing.T) *sql.DB
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
			name:            "randInx返回 0，抢占成功",
			batchSize:       10,
			refreshInterval: 10 * time.Second,
			randIndex: func(num int) int {
				return 0
			},
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
						5, TaskStatusWaiting,
						"",
						time.Now().UnixMilli(),
						time.Now().UnixMilli(),
						time.Now().UnixMilli(),
					},
				}
				rows := sqlmock.NewRows([]string{
					"id", "name", "type",
					"cron", "executor", "Version", "Status", "cfg",
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
				CronExp:  "*/5 * * * * ?",
				Version:  6,
			},
			wantErr: nil,
		},
		{
			name:            "randInx返回 len(tasks)/2，第一次抢占成功",
			batchSize:       10,
			refreshInterval: 10 * time.Second,
			randIndex: func(num int) int {
				return 3 / 2
			},
			sqlMock: func(t *testing.T) *sql.DB {
				mockDB, mock, err := sqlmock.New()
				require.NoError(t, err)
				values := [][]driver.Value{
					{
						2,
						"test2",
						task.TypeLocal,
						"*/5 * * * * ?",
						"local",
						10,
						TaskStatusWaiting,
						"",
						time.Now().UnixMilli(),
						time.Now().UnixMilli(),
						time.Now().UnixMilli(),
					},
					{
						3,
						"test3",
						task.TypeLocal,
						"*/5 * * * * ?",
						"local",
						10,
						TaskStatusWaiting,
						"",
						time.Now().UnixMilli(),
						time.Now().UnixMilli(),
						time.Now().UnixMilli(),
					},
					{
						4,
						"test4",
						task.TypeLocal,
						"*/5 * * * * ?",
						"local",
						10,
						TaskStatusWaiting,
						"",
						time.Now().UnixMilli(),
						time.Now().UnixMilli(),
						time.Now().UnixMilli(),
					},
				}
				rows := sqlmock.NewRows([]string{
					"id", "name", "type",
					"cron", "executor", "Version", "taskStatus", "cfg",
					"next_exec_time", "ctime", "utime",
				}).AddRows(values...)

				mock.ExpectQuery("^SELECT \\* FROM `task_info`").WillReturnRows(rows)
				mock.ExpectExec("UPDATE `task_info`").WillReturnResult(sqlmock.NewResult(3, 1))
				return mockDB
			},
			wantTask: task.Task{
				ID:       3,
				Name:     "test3",
				Type:     task.TypeLocal,
				Executor: "local",
				Cfg:      "",
				CronExp:  "*/5 * * * * ?",
				Version:  11,
			},
			wantErr: nil,
		},
		{
			name:            "randInx返回 len(tasks)-1，第一次抢占失败，第二次成功",
			batchSize:       10,
			refreshInterval: 10 * time.Second,
			randIndex: func(num int) int {
				return 4 - 1
			},
			sqlMock: func(t *testing.T) *sql.DB {
				mockDB, mock, err := sqlmock.New()
				require.NoError(t, err)
				values := [][]driver.Value{
					{
						5,
						"test5",
						task.TypeLocal,
						"*/5 * * * * ?",
						"local",
						10,
						TaskStatusWaiting,
						"",
						time.Now().UnixMilli(),
						time.Now().UnixMilli(),
						time.Now().UnixMilli(),
					},
					{
						6,
						"test6",
						task.TypeLocal,
						"*/5 * * * * ?",
						"local",
						10,
						TaskStatusWaiting,
						"",
						time.Now().UnixMilli(),
						time.Now().UnixMilli(),
						time.Now().UnixMilli(),
					},
					{
						7,
						"test7",
						task.TypeLocal,
						"*/5 * * * * ?",
						"local",
						10,
						TaskStatusWaiting,
						"",
						time.Now().UnixMilli(),
						time.Now().UnixMilli(),
						time.Now().UnixMilli(),
					},
					{
						8,
						"test8",
						task.TypeLocal,
						"*/5 * * * * ?",
						"local",
						10,
						TaskStatusWaiting,
						"",
						time.Now().UnixMilli(),
						time.Now().UnixMilli(),
						time.Now().UnixMilli(),
					},
				}
				rows := sqlmock.NewRows([]string{
					"id", "name", "type",
					"cron", "executor", "Version", "taskStatus", "cfg",
					"next_exec_time", "ctime", "utime",
				}).AddRows(values...)

				mock.ExpectQuery("^SELECT \\* FROM `task_info`").WillReturnRows(rows)

				// 预期第一次抢占失败
				mock.ExpectExec("UPDATE `task_info`").WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectExec("UPDATE `task_info`").WillReturnResult(sqlmock.NewResult(5, 1))
				return mockDB
			},
			wantTask: task.Task{
				ID:       5,
				Name:     "test5",
				Type:     task.TypeLocal,
				Executor: "local",
				Cfg:      "",
				CronExp:  "*/5 * * * * ?",
				Version:  11,
			},
			wantErr: nil,
		},
		{
			name:            "第一批全部失败，但抢到了第二批的任务",
			batchSize:       10,
			refreshInterval: 10 * time.Second,
			randIndex: func(num int) int {
				return 1
			},
			sqlMock: func(t *testing.T) *sql.DB {
				mockDB, mock, err := sqlmock.New()
				require.NoError(t, err)
				values1 := [][]driver.Value{
					{
						9,
						"test9",
						task.TypeLocal,
						"*/5 * * * * ?",
						"local",
						10,
						TaskStatusWaiting,
						"",
						time.Now().UnixMilli(),
						time.Now().UnixMilli(),
						time.Now().UnixMilli(),
					},
					{
						10,
						"test10",
						task.TypeLocal,
						"*/5 * * * * ?",
						"local",
						10,
						TaskStatusWaiting,
						"",
						time.Now().UnixMilli(),
						time.Now().UnixMilli(),
						time.Now().UnixMilli(),
					},
					{
						11,
						"test11",
						task.TypeLocal,
						"*/5 * * * * ?",
						"local",
						10,
						TaskStatusWaiting,
						"",
						time.Now().UnixMilli(),
						time.Now().UnixMilli(),
						time.Now().UnixMilli(),
					},
				}
				rows1 := sqlmock.NewRows([]string{
					"id", "name", "type",
					"cron", "executor", "Version", "taskStatus", "cfg",
					"next_exec_time", "ctime", "utime",
				}).AddRows(values1...)

				mock.ExpectQuery("^SELECT \\* FROM `task_info`").WillReturnRows(rows1)

				// 第一批全部失败
				mock.ExpectExec("UPDATE `task_info`").WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectExec("UPDATE `task_info`").WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectExec("UPDATE `task_info`").WillReturnResult(sqlmock.NewResult(0, 0))

				values2 := [][]driver.Value{
					{
						12,
						"test12",
						task.TypeLocal,
						"*/5 * * * * ?",
						"local",
						10,
						TaskStatusWaiting,
						"",
						time.Now().UnixMilli(),
						time.Now().UnixMilli(),
						time.Now().UnixMilli(),
					},
					{
						13,
						"test13",
						task.TypeLocal,
						"*/5 * * * * ?",
						"local",
						10,
						TaskStatusWaiting,
						"",
						time.Now().UnixMilli(),
						time.Now().UnixMilli(),
						time.Now().UnixMilli(),
					},
					{
						14,
						"test14",
						task.TypeLocal,
						"*/5 * * * * ?",
						"local",
						10,
						TaskStatusWaiting,
						"",
						time.Now().UnixMilli(),
						time.Now().UnixMilli(),
						time.Now().UnixMilli(),
					},
				}
				rows2 := sqlmock.NewRows([]string{
					"id", "name", "type",
					"cron", "executor", "Version", "taskStatus", "cfg",
					"next_exec_time", "ctime", "utime",
				}).AddRows(values2...)
				mock.ExpectQuery("^SELECT \\* FROM `task_info`").WillReturnRows(rows2)
				// 第二批第一条成功
				mock.ExpectExec("UPDATE `task_info`").WillReturnResult(sqlmock.NewResult(13, 1))
				return mockDB
			},
			wantTask: task.Task{
				ID:       13,
				Name:     "test13",
				Type:     task.TypeLocal,
				Executor: "local",
				Cfg:      "",
				CronExp:  "*/5 * * * * ?",
				Version:  11,
			},
			wantErr: nil,
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

			dao := NewGormTaskDAO(db, tc.batchSize, tc.refreshInterval)
			dao.randIndex = tc.randIndex

			res, err := dao.Preempt(context.Background())
			if err != nil {
				assert.Equal(t, tc.wantErr, err)
				return
			}
			assert.Equal(t, tc.wantTask.ID, res.ID)
			assert.Equal(t, tc.wantTask.Name, res.Name)
			assert.Equal(t, tc.wantTask.Type, res.Type)
			assert.Equal(t, tc.wantTask.Executor, res.Executor)
			assert.Equal(t, tc.wantTask.CronExp, res.CronExp)
			assert.Equal(t, tc.wantTask.Version, res.Version)
			assert.True(t, res.Ctime.UnixMilli() > 0)
			assert.True(t, res.Utime.UnixMilli() > 0)
		})
	}
}

func TestGormTaskDAO_Add(t *testing.T) {
	testCases := []struct {
		name            string
		batchSize       int
		refreshInterval time.Duration
		sqlMock         func(t *testing.T) *sql.DB
		in              task.Task
		wantErr         error
	}{
		{
			name:            "插入成功",
			batchSize:       10,
			refreshInterval: time.Minute,
			sqlMock: func(t *testing.T) *sql.DB {
				mockDB, mock, err := sqlmock.New()
				require.NoError(t, err)
				mock.ExpectExec("INSERT INTO `task_info` .*").
					WillReturnResult(sqlmock.NewResult(1, 1))
				return mockDB
			},
			in: task.Task{
				Name: "test",
			},
			wantErr: nil,
		},
		{
			name:            "插入失败",
			batchSize:       10,
			refreshInterval: time.Minute,
			sqlMock: func(t *testing.T) *sql.DB {
				mockDB, mock, err := sqlmock.New()
				require.NoError(t, err)
				mock.ExpectExec("INSERT INTO `task_info` .*").
					WillReturnError(errors.New("mock db error"))
				return mockDB
			},
			in: task.Task{
				Name: "test",
			},
			wantErr: errors.New("mock db error"),
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
			dao := NewGormTaskDAO(db, tc.batchSize, tc.refreshInterval)
			require.NoError(t, err)
			err = dao.Add(context.Background(), tc.in)
			assert.Equal(t, tc.wantErr, err)
		})
	}
}

func TestGormTaskDAO_UpdateNextTime(t *testing.T) {
	testCases := []struct {
		name            string
		batchSize       int
		refreshInterval time.Duration
		sqlMock         func(t *testing.T) *sql.DB
		id              int64
		next            time.Time
		wantErr         error
	}{
		{
			name:            "更新成功",
			batchSize:       10,
			refreshInterval: time.Minute,
			sqlMock: func(t *testing.T) *sql.DB {
				mockDB, mock, err := sqlmock.New()
				require.NoError(t, err)
				mock.ExpectExec("UPDATE `task_info`").
					WillReturnResult(sqlmock.NewResult(1, 1))
				return mockDB
			},
			id:      1,
			next:    time.Now().Add(time.Hour),
			wantErr: nil,
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
			dao := NewGormTaskDAO(db, tc.batchSize, tc.refreshInterval)
			require.NoError(t, err)
			err = dao.UpdateNextTime(context.Background(), tc.id, tc.next)
			assert.Equal(t, tc.wantErr, err)
		})
	}
}
