package internal

import (
	"context"
	"encoding/json"
	"github.com/ecodeclub/ecron/internal/storage"
	"log/slog"
)

// Reportor 专门负责上报任务执行进度
type Reportor struct {
	dao    storage.ExecutionDAO
	logger *slog.Logger
}

func NewReportor(dao storage.ExecutionDAO, logger *slog.Logger) *Reportor {
	return &Reportor{dao: dao, logger: logger}
}

func (r *Reportor) Report(ctx context.Context, result string) State {
	var res ExecResult
	err := json.Unmarshal([]byte(result), &res)
	if err != nil {
		r.logger.Error("解析结果异常", slog.Any("error", err), slog.String("result", result))
		return StateUnknown
	}
	err = r.dao.UpdateProgress(ctx, res.ID, res.Progress)
	if err != nil {
		slog.Error("上报任务执行进度失败",
			slog.Int64("execution_id", res.ID),
			slog.Any("error", err))
	}
	return res.Status
}

// ExecResult 任务执行结果
type ExecResult struct {
	// execution id
	ID     int64 `json:"id"`
	Status State `json:"status"`
	// 进度
	Progress uint8 `json:"progress"`
}

type State string

const (
	StateUnknown = "UNKNOWN"
	StateSuccess = "SUCCESS"
	StateRunning = "RUNNING"
	StateFailed  = "FAILED"
)
