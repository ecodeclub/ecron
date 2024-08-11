package internal

import (
	"context"
	"github.com/ecodeclub/ecron/internal/storage"
	daomocks "github.com/ecodeclub/ecron/internal/storage/mocks"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
	"log/slog"
	"os"
	"testing"
)

func TestReportor_Report(t *testing.T) {
	testCases := []struct {
		name       string
		mock       func(ctrl *gomock.Controller) storage.ExecutionDAO
		result     string
		wantStatus State
	}{
		{
			name: "上报进度成功",
			mock: func(ctrl *gomock.Controller) storage.ExecutionDAO {
				dao := daomocks.NewMockExecutionDAO(ctrl)
				dao.EXPECT().UpdateProgress(gomock.Any(), int64(1), 100).Return(nil)
				return dao
			},
			result:     `{"id":1,"status":"SUCCESS","process":100}`,
			wantStatus: StateSuccess,
		},
		{
			name: "返回结果解析失败",
			mock: func(ctrl *gomock.Controller) storage.ExecutionDAO {
				dao := daomocks.NewMockExecutionDAO(ctrl)
				return dao
			},
			result:     `{"id":1,"status":"SUCCESS","process":10`,
			wantStatus: StateUnknown,
		},
		{
			name: "任务执行失败",
			mock: func(ctrl *gomock.Controller) storage.ExecutionDAO {
				dao := daomocks.NewMockExecutionDAO(ctrl)
				dao.EXPECT().UpdateProgress(gomock.Any(), int64(1), 10).Return(nil)
				return dao
			},
			result:     `{"id":1,"status":"FAILED","process":10}`,
			wantStatus: StateFailed,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
			reportor := NewReportor(tc.mock(ctrl), logger)
			status := reportor.Report(context.Background(), tc.result)
			assert.Equal(t, tc.wantStatus, status)
		})
	}
}
