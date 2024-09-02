package executor

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/ecodeclub/ecron/internal/errs"
	"github.com/ecodeclub/ecron/internal/task"
	"github.com/h2non/gock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"log/slog"
	"net/http"
	"os"
	"testing"
	"time"
)

func TestHttpExecutor_Run(t *testing.T) {
	testCases := []struct {
		name           string
		inTask         task.Task
		statusCode     int
		respBody       string
		respErr        error
		wantErr        error
		wantTaskStatus task.ExecStatus
	}{
		{
			name: "任务配置格式错误",
			inTask: task.Task{
				ID: 1,
				Cfg: `{
		dfasfdfads
		}
		`,
			},
			wantErr:        errs.ErrInCorrectConfig,
			wantTaskStatus: task.ExecStatusFailed,
		},
		{
			name: "执行失败，业务方响应码不是200",
			inTask: task.Task{
				ID: 2,
				Cfg: marshal(t, HttpCfg{
					Url: "http://localhost:8080/test_run",
				}),
			},
			statusCode:     http.StatusBadRequest,
			wantErr:        errs.ErrRequestFailed,
			wantTaskStatus: task.ExecStatusFailed,
		},
		{
			name: "业务方返回任务执行成功",
			inTask: task.Task{
				ID: 3,
				Cfg: marshal(t, HttpCfg{
					Url: "http://localhost:8080/test_run",
				}),
			},
			respBody:       `{"eid":1,"status":"SUCCESS","progress":100}`,
			statusCode:     http.StatusOK,
			wantErr:        nil,
			wantTaskStatus: task.ExecStatusSuccess,
		},
		{
			name: "业务方返回任务执行失败",
			inTask: task.Task{
				ID: 4,
				Cfg: marshal(t, HttpCfg{
					Url: "http://localhost:8080/test_run",
				}),
			},
			respBody:       `{"eid":1,"status":"FAILED","progress":0}`,
			statusCode:     http.StatusOK,
			wantErr:        nil,
			wantTaskStatus: task.ExecStatusFailed,
		},
		{
			name: "业务方返回任务执行中",
			inTask: task.Task{
				ID: 5,
				Cfg: marshal(t, HttpCfg{
					Url: "http://localhost:8080/test_run",
				}),
			},
			respBody:       `{"eid":1,"status":"RUNNING","progress":10}`,
			statusCode:     http.StatusOK,
			wantErr:        nil,
			wantTaskStatus: task.ExecStatusRunning,
		},
		{
			name: "发起http请求失败",
			inTask: task.Task{
				ID: 6,
				Cfg: marshal(t, HttpCfg{
					Url: "http://localhost:8080/test_run",
				}),
			},
			respBody:       `{"eid":1,"status":"RUNNING","progress":10}`,
			respErr:        errors.New("发起任务执行请求失败"),
			statusCode:     http.StatusOK,
			wantErr:        errs.ErrRequestFailed,
			wantTaskStatus: task.ExecStatusFailed,
		},
		{
			name: "发起http请求失败，调用超时",
			inTask: task.Task{
				ID: 7,
				Cfg: marshal(t, HttpCfg{
					Url: "http://localhost:8080/test_run",
				}),
			},
			respBody: `{"eid":1,"status":"RUNNING","progress":10}`,
			respErr: &os.SyscallError{
				Err: MockTimeoutError{
					"模拟HTTP调用超时错误",
				},
			},
			statusCode:     http.StatusOK,
			wantErr:        errs.ErrRequestTimeout,
			wantTaskStatus: task.ExecStatusDeadlineExceeded,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			defer gock.Off()
			gock.New("http://localhost:8080").Post("/test_run").
				MatchHeader("execution_id", "1").
				Reply(tc.statusCode).JSON(tc.respBody).SetError(tc.respErr)

			logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
			exec := NewHttpExecutor(logger)
			status, err := exec.Run(context.Background(), tc.inTask, 1)
			assert.Equal(t, tc.wantErr, err)
			assert.Equal(t, tc.wantTaskStatus, status)
		})
	}
}

func marshal(t *testing.T, cfg HttpCfg) string {
	res, err := json.Marshal(cfg)
	require.NoError(t, err)
	return string(res)
}

type MockTimeoutError struct {
	s string
}

func (m MockTimeoutError) Error() string {
	return m.s
}

func (m MockTimeoutError) Timeout() bool {
	return true
}

func (m MockTimeoutError) Temporary() bool {
	//TODO implement me
	panic("implement me")
}

func TestHttpExecutor_Explore(t *testing.T) {
	testCases := []struct {
		name         string
		maxFailCount int
		inTask       task.Task
		method       string
		statusCode   int
		respBody     string
		respErr      error
		wantResult   Result
	}{
		{
			name:         "一次任务探查成功",
			maxFailCount: 1,
			inTask: task.Task{
				ID: 1,
				Cfg: marshal(t, HttpCfg{

					Url:             "http://localhost:8080/test_explore",
					ExploreInterval: time.Second,
				}),
			},
			respBody:   `{"eid":1,"status":"SUCCESS","progress":100}`,
			statusCode: http.StatusOK,
			wantResult: Result{
				Eid:      1,
				Status:   StatusSuccess,
				Progress: 100,
			},
		},
		{
			name:         "一次任务探查失败",
			maxFailCount: 1,
			inTask: task.Task{
				ID: 2,
				Cfg: marshal(t, HttpCfg{
					Url:             "http://localhost:8080/test_explore",
					ExploreInterval: time.Second,
				}),
			},
			respBody:   `{"eid":1,"status":"FAILED","progress":100}`,
			statusCode: http.StatusOK,
			wantResult: Result{
				Eid:      1,
				Status:   StatusFailed,
				Progress: 100,
			},
		},
		{
			name: "请求超时，达到最大次数",
			// 设置成 0，这样只请求一次就结束任务探查
			maxFailCount: 0,
			inTask: task.Task{
				ID: 3,
				Cfg: marshal(t, HttpCfg{
					Url:             "http://localhost:8080/test_explore",
					ExploreInterval: time.Second,
				}),
			},
			respErr: &os.SyscallError{
				Err: MockTimeoutError{
					"模拟HTTP调用超时错误",
				},
			},
			statusCode: http.StatusOK,
			wantResult: Result{
				Eid:      1,
				Status:   StatusFailed,
				Progress: 0,
			},
		},
		{
			name: "业务方响应码不是200，失败达到最大次数",
			// 设置成 0，这样只请求一次就结束任务探查
			maxFailCount: 0,
			inTask: task.Task{
				ID: 4,
				Cfg: marshal(t, HttpCfg{
					Url:             "http://localhost:8080/test_explore",
					ExploreInterval: time.Second,
				}),
			},
			statusCode: http.StatusBadRequest,
			wantResult: Result{
				Eid:      1,
				Status:   StatusFailed,
				Progress: 0,
			},
		},
		{
			name: "解析探查结果失败，达到最大次数",
			// 设置成 0，这样只请求一次就结束任务探查
			maxFailCount: 0,
			inTask: task.Task{
				ID: 5,
				Cfg: marshal(t, HttpCfg{
					Url:             "http://localhost:8080/test_explore",
					ExploreInterval: time.Second,
				}),
			},
			respBody:   `{"eid":1,"status":"FAILED","prog`,
			statusCode: http.StatusOK,
			wantResult: Result{
				Eid:      1,
				Status:   StatusFailed,
				Progress: 0,
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			defer gock.Off()
			gock.New("http://localhost:8080").Get("/test_explore").
				MatchHeader("execution_id", "1").
				Reply(tc.statusCode).JSON(tc.respBody).SetError(tc.respErr)

			logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
			exec := NewHttpExecutor(logger)
			exec.maxFailCount = tc.maxFailCount
			ch := exec.Explore(context.Background(), 1, tc.inTask)

			for {
				result := <-ch
				assert.Equal(t, tc.wantResult, result)
				if result.Status == StatusSuccess || result.Status == StatusFailed {
					break
				}
			}
		})
	}
}
