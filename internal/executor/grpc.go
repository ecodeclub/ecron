package executor

import (
	"context"
	"encoding/json"
	"github.com/ecodeclub/ecron/internal/task"
)

type GrpcExecutor struct {
}

func NewGrpcExecutor() Executor {
	return &GrpcExecutor{}
}

func (g *GrpcExecutor) Name() string {
	return "GRPC"
}

func (g *GrpcExecutor) Run(ctx context.Context, t task.Task) error {
	var req GrpcCfg
	err := json.Unmarshal([]byte(t.Cfg), &req)
	if err != nil {
		return err
	}
	// TODO: 解决 grpc 泛化调用
	panic("implement me")
}

type GrpcCfg struct {
	ServiceName string `json:"service_name"`
	Method      string `json:"method"`
	Port        int    `json:"port"`
}
