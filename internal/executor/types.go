package executor

import (
	"context"
	"github.com/ecodeclub/ecron/internal/task"
	"net/http"
)

//go:generate mockgen -source=./types.go -package=executormocks -destination=./mocks/executor.mock.go
type Executor interface {
	// Name 执行器的名称
	Name() string
	// Run 执行任务
	// ctx 整个调度器的上下文，当有ctx.Done信号时，就要考虑结束任务的执行
	Run(ctx context.Context, t task.Task) error
}

type HttpCfg struct {
	Method string      `json:"method"`
	Url    string      `json:"url"`
	Header http.Header `json:"header"`
	Body   string      `json:"body"`
}

type GrpcCfg struct {
	ServiceName string `json:"service_name"`
	Method      string `json:"method"`
	Port        int    `json:"port"`
}
