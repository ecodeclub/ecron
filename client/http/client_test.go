package http

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHttpClient_HandleFunc(t *testing.T) {
	myTask := new(MyTask)
	r := NewRegistry()
	err := r.Register(myTask)
	require.NoError(t, err)

	cli := NewHttpClient(r, WithPrefix("/aaa/bbb/ccc/"))
	mux := cli.HttpMutex()

	testCases := []struct {
		name            string
		uri             string
		eid             string
		wantStatusCode  int
		wantExecBody    string
		wantExploreBody string
		wantStopBody    string
	}{
		{
			name:            "未知uri",
			uri:             "/unknown",
			wantStatusCode:  http.StatusBadRequest,
			wantExecBody:    fmt.Sprintf("unkonwn uri: %s", "/unknown"),
			wantExploreBody: fmt.Sprintf("unkonwn uri: %s", "/unknown"),
			wantStopBody:    fmt.Sprintf("unkonwn uri: %s", "/unknown"),
		},
		{
			name:            "未知任务",
			uri:             "/aaa/bbb/ccc/unknown-task",
			wantStatusCode:  http.StatusNotFound,
			wantExecBody:    fmt.Sprintf("task not found: %s", "unknown-task"),
			wantExploreBody: fmt.Sprintf("task not found: %s", "unknown-task"),
			wantStopBody:    fmt.Sprintf("task not found: %s", "unknown-task"),
		},
		{
			name:            "缺少eid",
			uri:             "/aaa/bbb/ccc/my-task",
			wantStatusCode:  http.StatusBadRequest,
			wantExecBody:    "miss header: execution_id",
			wantExploreBody: "miss header: execution_id",
			wantStopBody:    "miss header: execution_id",
		},
		{
			name:            "eid解析错误",
			uri:             "/aaa/bbb/ccc/my-task",
			eid:             "abc",
			wantStatusCode:  http.StatusBadRequest,
			wantExecBody:    fmt.Sprintf("unknown execution_id: %s", "abc"),
			wantExploreBody: fmt.Sprintf("unknown execution_id: %s", "abc"),
			wantStopBody:    fmt.Sprintf("unknown execution_id: %s", "abc"),
		},
		{
			name:           "成功",
			uri:            "/aaa/bbb/ccc/my-task",
			eid:            "1",
			wantStatusCode: http.StatusOK,
			// resp.Body.String()返回的结果会在最后加一个 \n，所以在这里我也加一个
			wantExecBody:    "{\"eid\":1,\"status\":\"RUNNING\",\"progress\":10}\n",
			wantExploreBody: "{\"eid\":1,\"status\":\"SUCCESS\",\"progress\":100}\n",
			wantStopBody:    "ok",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// 执行
			req := httptest.NewRequest(http.MethodPost, tc.uri, nil)
			if tc.eid != "" {
				req.Header.Set(headerExecutionID, tc.eid)
			}
			resp := httptest.NewRecorder()
			mux.ServeHTTP(resp, req)
			assert.Equal(t, tc.wantStatusCode, resp.Code)
			assert.Equal(t, tc.wantExecBody, resp.Body.String())

			// 进度探查
			req = httptest.NewRequest(http.MethodGet, tc.uri, nil)
			if tc.eid != "" {
				req.Header.Set(headerExecutionID, tc.eid)
			}
			resp = httptest.NewRecorder()
			mux.ServeHTTP(resp, req)
			assert.Equal(t, tc.wantStatusCode, resp.Code)
			assert.Equal(t, tc.wantExploreBody, resp.Body.String())

			// 停止
			req = httptest.NewRequest(http.MethodDelete, tc.uri, nil)
			if tc.eid != "" {
				req.Header.Set(headerExecutionID, tc.eid)
			}
			resp = httptest.NewRecorder()
			mux.ServeHTTP(resp, req)
			assert.Equal(t, tc.wantStatusCode, resp.Code)
			assert.Equal(t, tc.wantStopBody, resp.Body.String())
		})
	}
}

type MyTask struct {
}

func (m *MyTask) Execute() (Status, int) {
	return StatusRunning, 10
}

func (m *MyTask) Status() (Status, int) {
	return StatusSuccess, 100
}

func (m *MyTask) Stop() error {
	return nil
}

func (m *MyTask) Name() string {
	return "my-task"
}
