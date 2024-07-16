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
)

func TestGormExecutionDAO_InsertExecStatus(t *testing.T) {
	testCase := []struct {
		name       string
		sqlMock    func(t *testing.T) *sql.DB
		id         int64
		taskStatus task.ExecStatus
		wantErr    error
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
		},
		{
			name: "任务执行成功，更新执行记录",
			sqlMock: func(t *testing.T) *sql.DB {
				mockDB, mock, err := sqlmock.New()
				require.NoError(t, err)
				mock.ExpectExec("INSERT INTO `execution` .* ON DUPLICATE KEY UPDATE").
					WillReturnResult(sqlmock.NewResult(1, 1))
				return mockDB
			},
			id:         1,
			taskStatus: task.ExecStatusSuccess,
			wantErr:    nil,
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
			err = dao.InsertExecStatus(context.Background(), tc.id, tc.taskStatus)
			assert.Equal(t, tc.wantErr, err)
		})
	}
}
