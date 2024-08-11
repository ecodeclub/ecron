package executor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/ecodeclub/ecron/internal"
	"github.com/ecodeclub/ecron/internal/errs"
	"github.com/ecodeclub/ecron/internal/task"
	"io"
	"log/slog"
	"net/http"
	"time"
)

type HttpExecutor struct {
	logger   *slog.Logger
	reportor *internal.Reportor
}

func NewHttpExecutor(logger *slog.Logger, reportor *internal.Reportor) Executor {
	return &HttpExecutor{
		logger:   logger,
		reportor: reportor,
	}
}

func (h *HttpExecutor) Name() string {
	return "HTTP"
}

func (h *HttpExecutor) Run(ctx context.Context, t task.Task, eid int64) error {
	var cfg HttpCfg
	// 在scheduler处理过一次，所有这里不处理也没关系
	_ = json.Unmarshal([]byte(t.Cfg), &cfg)

	// 发起任务执行和任务进度探查都使用同一个接口
	// 考虑后续允许用户自己上报任务执行进度，那么它用带着这个eid来上报
	url := fmt.Sprintf("%s?execution_id=%d", cfg.Url, eid)
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	state := h.report(ctx, resp)
	switch state {
	case internal.StateFailed:
		return errs.ErrExecuteTaskFailed
	case internal.StateSuccess:
		return nil
	default:
		return h.checkExecProcess(ctx, cfg, eid)
	}
}

func (h *HttpExecutor) report(ctx context.Context, resp *http.Response) internal.State {
	body, _ := io.ReadAll(resp.Body)
	reportCtx, cancel := context.WithTimeout(ctx, time.Second*3)
	res := h.reportor.Report(reportCtx, string(body))
	cancel()
	return res
}

// TODO: 优化探查的时间间隔策略
func (h *HttpExecutor) checkInterval(taskTimeout time.Duration) time.Duration {
	if taskTimeout >= time.Hour {
		return time.Minute * 10
	}
	if taskTimeout >= time.Minute*30 {
		return time.Minute * 5
	}
	if taskTimeout > time.Minute {
		return time.Second * 10
	}
	if taskTimeout > time.Second*10 {
		return time.Second * 5
	}
	return time.Second
}

func (h *HttpExecutor) checkExecProcess(ctx context.Context, cfg HttpCfg, eid int64) error {
	interval := h.checkInterval(cfg.TaskTimeout)
	ticker := time.NewTicker(interval)
	url := fmt.Sprintf("%s?execution_id=%d", cfg.Url, eid)

	for {
		select {
		case <-ctx.Done():
			h.cancelExec(cfg.Url, eid)
			return ctx.Err()
		case <-ticker.C:
			resp, err := http.Get(url)
			if err != nil || resp.StatusCode != http.StatusOK {
				alive := h.healthCheck(url)
				if alive {
					continue
				} else {
					h.logger.Error("业务方状态异常",
						slog.Any("error", err),
						slog.Any("url", url))
					return errs.ErrExecuteTaskFailed
				}
			}
			defer resp.Body.Close()

			state := h.report(ctx, resp)
			switch state {
			case internal.StateRunning:
				continue
			case internal.StateFailed:
				return errs.ErrExecuteTaskFailed
			case internal.StateSuccess:
				return nil
			default:
				return errs.ErrExecuteTaskFailed
			}
		}
	}
}

func (h *HttpExecutor) healthCheck(url string) bool {
	for i := 1; i <= 3; i++ {
		resp, err := http.Get(url)
		if err != nil || resp.StatusCode != http.StatusOK {
			time.Sleep(time.Second)
			continue
		} else {
			return true
		}
	}
	return false
}

// cancelExec 通知业务方停止执行任务
func (h *HttpExecutor) cancelExec(url string, eid int64) {
	contentType := "application/json"
	jsonBody := fmt.Sprintf(`{"execution_id":"%d"}`, eid)
	_, err := http.Post(url, contentType, bytes.NewReader([]byte(jsonBody)))
	if err != nil {
		h.logger.Error("通知业务方停止任务执行失败",
			slog.Int64("execution_id", eid),
			slog.Any("error", err))
	}
}

type HttpCfg struct {
	Method string      `json:"method"`
	Url    string      `json:"url"`
	Header http.Header `json:"header"`
	Body   string      `json:"body"`
	// 预计任务执行时长
	TaskTimeout time.Duration `json:"taskTimeout"`
}
