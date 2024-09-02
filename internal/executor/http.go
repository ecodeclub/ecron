package executor

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/ecodeclub/ecron/internal/errs"
	"github.com/ecodeclub/ecron/internal/task"
	"log/slog"
	"net/http"
	"os"
	"time"
)

type HttpExecutor struct {
	logger *slog.Logger
	client *http.Client
	// 任务探查最大失败次数
	maxFailCount int
}

func NewHttpExecutor(logger *slog.Logger) *HttpExecutor {
	return &HttpExecutor{
		client: &http.Client{
			// http调用超时配置
			Timeout: time.Second * 5,
		},
		logger:       logger,
		maxFailCount: 5,
	}
}

func (h *HttpExecutor) Name() string {
	return "HTTP"
}

func (h *HttpExecutor) Run(ctx context.Context, t task.Task, eid int64) (task.ExecStatus, error) {
	cfg, err := h.parseCfg(t.Cfg)
	if err != nil {
		h.logger.Error("任务配置信息错误",
			slog.Int64("ID", t.ID), slog.String("Cfg", t.Cfg))
		return task.ExecStatusFailed, errs.ErrInCorrectConfig
	}

	result, err := h.request(http.MethodPost, cfg, eid)
	if errors.Is(err, errs.ErrRequestTimeout) {
		h.logger.Error("发起任务请求超时",
			slog.Int64("execution_id", eid))
		return task.ExecStatusDeadlineExceeded, errs.ErrRequestTimeout
	}
	if err != nil {
		h.logger.Error("发起任务请求超时", slog.Int64("task_id", t.ID),
			slog.Int64("execution_id", eid), slog.Any("error", err))
		return task.ExecStatusFailed, errs.ErrRequestFailed
	}

	switch result.Status {
	case StatusSuccess:
		return task.ExecStatusSuccess, nil
	case StatusRunning:
		return task.ExecStatusRunning, nil
	default:
		return task.ExecStatusFailed, nil
	}
}

func (h *HttpExecutor) Explore(ctx context.Context, eid int64, t task.Task) <-chan Result {
	resultChan := make(chan Result, 1)
	go h.explore(ctx, resultChan, t, eid)
	return resultChan
}

func (h *HttpExecutor) explore(ctx context.Context, ch chan Result, t task.Task, eid int64) {
	defer close(ch)

	failCount := 0
	cfg, _ := h.parseCfg(t.Cfg)
	ticker := time.NewTicker(cfg.ExploreInterval)

	for failCount < h.maxFailCount {
		select {
		case <-ctx.Done():
			// 通知业务方取消任务执行
			h.cancelExec(cfg, eid)
			return
		case <-ticker.C:
			result, err := h.request(http.MethodGet, cfg, eid)
			if err != nil {
				failCount++
				continue
			}
			ch <- result
			if result.Status != StatusRunning {
				return
			}
		}
	}
	// failCount >= h.maxFailCount，任务执行失败
	h.logger.Error("探查任务执行进度失败，达到最大错误次数", slog.Int64("execution_id", eid))
	ch <- Result{
		Eid:    eid,
		Status: StatusFailed,
	}
}

func (h *HttpExecutor) TaskTimeout(t task.Task) time.Duration {
	result, err := h.parseCfg(t.Cfg)
	if err != nil || result.TaskTimeout < 0 {
		return time.Minute
	}
	return result.TaskTimeout
}

func (h *HttpExecutor) parseCfg(cfg string) (HttpCfg, error) {
	var result HttpCfg
	err := json.Unmarshal([]byte(cfg), &result)
	return result, err
}

func (h *HttpExecutor) cancelExec(cfg HttpCfg, eid int64) {
	_, err := h.request(http.MethodDelete, cfg, eid)
	if err != nil {
		h.logger.Error("通知业务方停止执行任务失败", slog.Int64("execution_id", eid))
		return
	}
}

func (h *HttpExecutor) request(method string, cfg HttpCfg, eid int64) (Result, error) {
	request, err := http.NewRequest(method, cfg.Url, bytes.NewBuffer([]byte(cfg.Body)))
	if err != nil {
		return Result{}, err
	}

	if cfg.Header == nil {
		request.Header = make(http.Header)
	} else {
		request.Header = cfg.Header
	}
	request.Header.Add("execution_id", fmt.Sprintf("%v", eid))
	request.Header.Add("Content-Type", "application/json")

	resp, err := h.client.Do(request)

	if os.IsTimeout(err) {
		return Result{}, errs.ErrRequestTimeout
	}
	if err != nil {
		return Result{}, err
	}
	if resp.StatusCode != http.StatusOK {
		return Result{}, errs.ErrRequestFailed
	}
	var result Result
	err = json.NewDecoder(resp.Body).Decode(&result)
	_ = resp.Body.Close()
	return result, err
}

type HttpCfg struct {
	// POST Url 执行任务
	// GET Url 查询任务的执行状态
	// DELETE Url 停止执行任务
	Url    string      `json:"url"`
	Header http.Header `json:"header"`
	Body   string      `json:"body"`
	// 预计任务执行时长
	TaskTimeout time.Duration `json:"taskTimeout"`
	// 任务探查间隔
	ExploreInterval time.Duration `json:"exploreInterval"`
}
