package http

import (
	"context"
	"github.com/ecodeclub/ekit/net/httpx"
	"github.com/h2non/gock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"net/http"
	"testing"
	"time"
)

func TestHttpClient_HttpMutex(t *testing.T) {
	// 模拟ecron任务注册接口返回
	gock.New("http://www.ecron:80").Post(uriRegisterTask).
		Reply(http.StatusOK).JSON(`
{
	"code": 0,
	"msg": "ok"
}
`)

	myTask := new(MyTask)

	r := NewRegistry()
	r.Register(NewTask(myTask, "@everry 5s", time.Minute, time.Second))
	cli := NewHttpClient(r, "http://localhost:8080", "http://www.ecron:80", WithPrefix("/aaa/bbb/ccc"))
	mux, err := cli.HttpMutex()
	gock.Off() // 关闭模拟
	require.NoError(t, err)
	go func() {
		err := http.ListenAndServe(":8080", mux)
		require.NoError(t, err)
	}()
	time.Sleep(time.Second)

	// 模拟ecron向用户发起任务调用和探查
	var resp result
	err = httpx.NewRequest(context.Background(), http.MethodPost, "http://localhost:8080/aaa/bbb/ccc/my-task").
		Client(http.DefaultClient).
		AddHeader(headerExecutionID, "1").
		Do().
		JSONScan(&resp)
	require.NoError(t, err)
	assert.Equal(t, int64(1), resp.Eid)
	assert.Equal(t, StatusRunning, resp.Status)
	assert.Equal(t, 10, resp.Progress)
	// 探查
	err = httpx.NewRequest(context.Background(), http.MethodGet, "http://localhost:8080/aaa/bbb/ccc/my-task").
		Client(http.DefaultClient).
		AddHeader(headerExecutionID, "1").
		Do().
		JSONScan(&resp)
	require.NoError(t, err)
	assert.Equal(t, int64(1), resp.Eid)
	assert.Equal(t, StatusSuccess, resp.Status)
	assert.Equal(t, 100, resp.Progress)

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
