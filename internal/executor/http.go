package executor

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/ecodeclub/ecron/internal/errs"
	"github.com/ecodeclub/ecron/internal/task"
	"log/slog"
	"net/http"
	"time"
)

type HttpExecutor struct {
	client *http.Client
	logger *slog.Logger
}

func NewHttpExecutor(logger *slog.Logger) Executor {
	return &HttpExecutor{
		client: &http.Client{
			Timeout: time.Second * 5,
		},
		logger: logger,
	}
}

func (h *HttpExecutor) Name() string {
	return "HTTP"
}

func (h *HttpExecutor) Run(ctx context.Context, t task.Task) error {
	var req HttpCfg
	err := json.Unmarshal([]byte(t.Cfg), &req)
	if err != nil {
		h.logger.Error("任务配置信息错误",
			slog.Int64("ID", t.ID), slog.String("Cfg", t.Cfg))
		return errs.ErrWrongTaskCfg
	}

	request, err := http.NewRequest(req.Method, req.Url, bytes.NewBuffer([]byte(req.Body)))
	if err != nil {
		return err
	}
	request.Header = req.Header

	resp, err := h.client.Do(request)
	if err != nil {
		h.logger.Error("发起任务执行请求失败",
			slog.Int64("ID", t.ID), slog.Any("error", err))
		return errs.ErrRequestExecuteFailed
	}

	if resp.StatusCode != http.StatusOK {
		return errs.ErrExecuteTaskFailed
	}

	return nil
}

type HttpCfg struct {
	Method string      `json:"method"`
	Url    string      `json:"url"`
	Header http.Header `json:"header"`
	Body   string      `json:"body"`
}
