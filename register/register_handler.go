package register

import (
	"github.com/ecodeclub/ecron/internal/executor"
	"github.com/ecodeclub/ecron/internal/storage"
)
import "github.com/gin-gonic/gin"

type RegisterHandler struct {
	dao  storage.TaskDAO
	http executor.HttpExecutor
	grpc executor.GrpcExecutor
}

func (r *RegisterHandler) RegisterRouter(server *gin.Engine) {
	server.POST("/task/http/register", r.RegisterHttpTask)
	server.POST("/task/http/register", r.RegisterGrpcTask)
}

func (r *RegisterHandler) RegisterHttpTask(ctx *gin.Context) {

}

func (r *RegisterHandler) RegisterGrpcTask(ctx *gin.Context) {

}

type RegisterHttpTaskReq struct {
	Name   string `json:"name"`
	Cron   string `json:"cron"`
	Method string `json:"method"`
	Url    string `json:"url"`
}

type RegisterGrocTaskReq struct {
	Name        string `json:"name"`
	Cron        string `json:"cron"`
	ServiceName string `json:"service_name"`
	Method      string `json:"method"`
}
