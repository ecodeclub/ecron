package executor

import (
	"encoding/json"
	"github.com/stretchr/testify/require"
	"testing"
)

// 这个执行不了的，请跑集成测试
//func TestHttpExecutor_Run(t *testing.T) {
//	testCases := []struct {
//		name         string
//		id           int64
//		inTask       task.Task
//		path         string
//		status       int
//		reqReturnErr error
//		wantErr      error
//	}{
//		//		{
//		//			name: "任务配置格式错误",
//		//			inTask: task.Task{
//		//				ID: 1,
//		//				Cfg: `{
//		//dfasfdfads
//		//}
//		//`,
//		//			},
//		//			wantErr: errs.ErrWrongTaskCfg,
//		//		},
//		{
//			name: "发起任务请求失败",
//			inTask: task.Task{
//				ID: 1,
//				Cfg: marshal(t, HttpCfg{
//					Method: "GET",
//					Url:    "http://localhost:8080/failed",
//				}),
//			},
//			path:         "/failed",
//			status:       http.StatusBadRequest,
//			reqReturnErr: errors.New("发起任务请求失败"),
//			wantErr:      errs.ErrRequestExecuteFailed,
//		},
//		{
//			name: "任务执行失败",
//			inTask: task.Task{
//				ID: 1,
//				Cfg: marshal(t, HttpCfg{
//					Method: "GET",
//					Url:    "http://localhost:8080/failed",
//				}),
//			},
//			path:         "/failed",
//			status:       http.StatusBadRequest,
//			reqReturnErr: nil,
//			wantErr:      errs.ErrExecuteTaskFailed,
//		},
//		{
//			name: "任务执行成功",
//			inTask: task.Task{
//				ID: 1,
//				Cfg: marshal(t, HttpCfg{
//					Method: "GET",
//					Url:    "http://localhost:8080/success",
//				}),
//			},
//			path:         "/success",
//			status:       http.StatusOK,
//			reqReturnErr: nil,
//			wantErr:      nil,
//		},
//	}
//	for _, tc := range testCases {
//		t.Run(tc.name, func(t *testing.T) {
//			defer gock.Off()
//			gock.New("http://localhost:8080").
//				Post(tc.path).Reply(tc.status).SetError(tc.reqReturnErr)
//
//			logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
//			exec := NewHttpExecutor(logger,nil)
//			err := exec.Run(context.Background(), tc.inTask, 1)
//			assert.Equal(t, tc.wantErr, err)
//		})
//	}
//}

func marshal(t *testing.T, cfg HttpCfg) string {
	res, err := json.Marshal(cfg)
	require.NoError(t, err)
	return string(res)
}
