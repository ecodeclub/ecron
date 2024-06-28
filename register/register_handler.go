package register

import (
	"encoding/json"
	"github.com/ecodeclub/ecron/internal/executor"
	"github.com/ecodeclub/ecron/internal/storage"
	"github.com/ecodeclub/ecron/internal/task"
	"net/http"
)
import "github.com/gin-gonic/gin"

type RegisterHandler struct {
	dao  storage.TaskDAO
	http *executor.HttpExecutor
	grpc *executor.GrpcExecutor
}

func NewRegisterHandler(dao storage.TaskDAO, http *executor.HttpExecutor, grpc *executor.GrpcExecutor) *RegisterHandler {
	return &RegisterHandler{dao: dao, http: http, grpc: grpc}
}

func (r *RegisterHandler) RegisterRouter(server *gin.Engine) {
	server.POST("/register/task/http", r.RegisterHttpTask)
	server.POST("/register/task/grpc", r.RegisterGrpcTask)
}

func (r *RegisterHandler) RegisterHttpTask(ctx *gin.Context) {
	var req HttpTaskReq
	if err := ctx.ShouldBindJSON(&req); err != nil {
		ctx.JSON(http.StatusOK, gin.H{"error": "请求参数错误"})
		return
	}
	var cfg executor.HttpCfg
	cfg.Method = req.Method
	cfg.Url = req.Url
	res, err := json.Marshal(cfg)
	if err != nil {
		ctx.JSON(http.StatusOK, gin.H{"error": "请求参数错误"})
		return
	}
	err = r.dao.Add(ctx, task.Task{
		Name:     req.Name,
		Type:     task.TypeHttp,
		Executor: r.http.Name(),
		CronExp:  req.Cron,
		Cfg:      string(res),
	})
	if err != nil {
		ctx.JSON(http.StatusOK, gin.H{"error": "注册任务失败"})
		return
	}
	ctx.JSON(http.StatusOK, gin.H{"msg": "OK"})
}

func (r *RegisterHandler) RegisterGrpcTask(ctx *gin.Context) {
	var req GrpcTaskReq
	if err := ctx.ShouldBindJSON(&req); err != nil {
		ctx.JSON(http.StatusOK, gin.H{"error": "请求参数错误"})
		return
	}
	var cfg executor.GrpcCfg
	cfg.ServiceName = req.ServiceName
	cfg.Method = req.Method
	res, err := json.Marshal(cfg)
	if err != nil {
		ctx.JSON(http.StatusOK, gin.H{"error": "请求参数错误"})
		return
	}
	err = r.dao.Add(ctx, task.Task{
		Name:     req.Name,
		Type:     task.TypeGrpc,
		Executor: r.grpc.Name(),
		CronExp:  req.Cron,
		Cfg:      string(res),
	})
	if err != nil {
		ctx.JSON(http.StatusOK, gin.H{"error": "注册任务失败"})
		return
	}
	ctx.JSON(http.StatusOK, gin.H{"msg": "OK"})
}

type HttpTaskReq struct {
	Name   string `json:"name"`
	Cron   string `json:"cron"`
	Method string `json:"method"`
	Url    string `json:"url"`
}

type GrpcTaskReq struct {
	Name        string `json:"name"`
	Cron        string `json:"cron"`
	ServiceName string `json:"service_name"`
	Method      string `json:"method"`
}
