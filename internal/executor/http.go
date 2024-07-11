package executor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/ecodeclub/ecron/internal/task"
	"log/slog"
	"net/http"
)

type HttpExecutor struct {
}

func NewHttpExecutor() *HttpExecutor {
	return &HttpExecutor{}
}

func (h *HttpExecutor) Name() string {
	return "HTTP"
}

func (h *HttpExecutor) Run(ctx context.Context, t task.Task) error {
	var req HttpCfg
	err := json.Unmarshal([]byte(t.Cfg), &req)
	if err != nil {
		slog.Error("任务配置信息错误", err)
		return err
	}
	if req.Method != http.MethodGet {
		return errors.New("任务配置信息有误，不是GET方法")
	}
	resp, err := http.Get(req.Url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	// TODO: 处理响应
	fmt.Println(resp)

	return nil
}
