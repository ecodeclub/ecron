package mysql

import (
	"context"
	"database/sql"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/ecodeclub/ecron/internal/task"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"testing"
	"time"
)

func TestGormExecutionDAO_InsertExecStatus(t *testing.T) {
	testCase := []struct {
		name       string
		sqlMock    func(t *testing.T) *sql.DB
		id         int64
		taskStatus task.ExecStatus
		wantErr    error
		wantID     int64
	}{
		{
			name: "启动任务，insert一条记录",
			sqlMock: func(t *testing.T) *sql.DB {
				mockDB, mock, err := sqlmock.New()
				require.NoError(t, err)
				mock.ExpectExec("INSERT INTO `execution` .* ON DUPLICATE KEY UPDATE").
					WillReturnResult(sqlmock.NewResult(1, 1))
				return mockDB
			},
			id:         1,
			taskStatus: task.ExecStatusStarted,
			wantErr:    nil,
			wantID:     1,
		},
		{
			name: "任务执行成功，更新执行记录",
			sqlMock: func(t *testing.T) *sql.DB {
				mockDB, mock, err := sqlmock.New()
				require.NoError(t, err)
				mock.ExpectExec("INSERT INTO `execution` .* ON DUPLICATE KEY UPDATE").
					WillReturnResult(sqlmock.NewResult(2, 1))
				return mockDB
			},
			id:         1,
			taskStatus: task.ExecStatusSuccess,
			wantErr:    nil,
			wantID:     2,
		},
	}
	for _, tc := range testCase {
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
			dao := NewGormExecutionDAO(db)
			id, err := dao.InsertExecStatus(context.Background(), tc.id, tc.taskStatus)
			assert.Equal(t, tc.wantErr, err)
			assert.Equal(t, tc.wantID, id)
		})
	}
}

func TestGormExecutionDAO_UpdateProgress(t *testing.T) {
	testCases := []struct {
		name     string
		sqlMock  func(t *testing.T) *sql.DB
		id       int64
		progress int
		wantErr  error
	}{
		{
			name: "更新任务进度成功",
			sqlMock: func(t *testing.T) *sql.DB {
				mockDB, mock, err := sqlmock.New()
				require.NoError(t, err)
				mock.ExpectExec("UPDATE `execution` .*").
					WillReturnResult(sqlmock.NewResult(1, 1))
				return mockDB
			},
			id:       1,
			progress: 10,
			wantErr:  nil,
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
			dao := NewGormExecutionDAO(db)
			err = dao.UpdateProgress(context.Background(), tc.id, uint8(tc.progress))
			assert.Equal(t, tc.wantErr, err)
		})
	}
}

func TestGormExecutionDAO_UpdateProgress1(t *testing.T) {
	db, err := gorm.Open(mysql.Open("root:root@tcp(localhost:13316)/ecron"))
	require.NoError(t, err)
	now := time.Now().UnixMilli()
	db.Create(&Execution{
		ID:       1,
		Tid:      2,
		Status:   2,
		Progress: 0,
		Ctime:    now,
		Utime:    now,
	})
	err = db.Model(&Execution{}).Where("id=?", 1).Updates(map[string]any{
		"progress": 100,
		"utime":    time.Now().UnixMilli(),
	}).Error
	require.NoError(t, err)
	var res Execution
	err = db.Model(&Execution{}).Where("id=?", 1).First(&res).Error
	require.NoError(t, err)
	assert.Equal(t, uint8(100), res.Progress)
}
