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
	"time"
)

func TestTaskCfgRepository_Add(t *testing.T) {
	testCases := []struct {
		name    string
		sqlMock func(t *testing.T) *sql.DB
		in      task.Task
		wantErr error
	}{
		{
			name: "插入成功",
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
			name: "插入失败",
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
			dao := NewGormTaskCfgRepository(db)
			require.NoError(t, err)
			err = dao.Add(context.Background(), tc.in)
			assert.Equal(t, tc.wantErr, err)
		})
	}
}

func TestTaskCfgRepository_UpdateNextTime(t *testing.T) {
	testCases := []struct {
		name    string
		sqlMock func(t *testing.T) *sql.DB
		id      int64
		next    time.Time
		wantErr error
	}{
		{
			name: "更新成功",
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
			dao := NewGormTaskCfgRepository(db)
			require.NoError(t, err)
			err = dao.UpdateNextTime(context.Background(), tc.id, tc.next)
			assert.Equal(t, tc.wantErr, err)
		})
	}
}
