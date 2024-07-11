package mysql

import (
	"context"
	"database/sql"
	"errors"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/ecodeclub/ecron/internal/task"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"testing"
)

func TestGormHistoryDAO_Add(t *testing.T) {
	testCase := []struct {
		name       string
		sqlMock    func(t *testing.T) *sql.DB
		id         int64
		taskStatus task.ExecStatus
		wantErr    error
	}{
		{
			name: "插入成功",
			sqlMock: func(t *testing.T) *sql.DB {
				mockDB, mock, err := sqlmock.New()
				require.NoError(t, err)
				mock.ExpectExec("INSERT INTO `task_exec_history` .*").
					WillReturnResult(sqlmock.NewResult(1, 1))
				return mockDB
			},
			id:         1,
			taskStatus: task.TaskExecStatusStarted,
			wantErr:    nil,
		},
		{
			name: "插入失败",
			sqlMock: func(t *testing.T) *sql.DB {
				mockDB, mock, err := sqlmock.New()
				require.NoError(t, err)
				mock.ExpectExec("INSERT INTO `task_exec_history` .*").
					WillReturnError(errors.New("mock db error"))
				return mockDB
			},
			id:         1,
			taskStatus: task.TaskExecStatusStarted,
			wantErr:    errors.New("mock db error"),
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
			dao := NewGormHistoryDAO(db)
			err = dao.Add(context.Background(), tc.id, tc.taskStatus)
			assert.Equal(t, tc.wantErr, err)
		})
	}
}
