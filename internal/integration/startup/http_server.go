package startup

import (
	"github.com/ecodeclub/ecron/internal"
	"github.com/gin-gonic/gin"
	"net/http"
	"strconv"
)

func StartHttpServer(port string) {
	server := gin.Default()
	h := new(HttpServer)
	h.RegisterRouter(server)
	err := server.Run(":" + port)
	if err != nil {
		panic(err)
	}
}

// HttpServer 模拟业务方的返回
type HttpServer struct {
}

func (u *HttpServer) RegisterRouter(server *gin.Engine) {
	server.GET("/success", u.Success)
	server.GET("/failed", u.Fail)
	server.GET("/running", u.Running)
	server.POST("/running", u.Running)
	server.GET("/error", u.Error)
}

func (u *HttpServer) Success(ctx *gin.Context) {
	param := ctx.Query("execution_id")
	id, _ := strconv.ParseInt(param, 10, 64)
	ctx.JSON(200, internal.ExecResult{
		ID:       id,
		Status:   internal.StateSuccess,
		Progress: 100,
	})
}

func (u *HttpServer) Running(ctx *gin.Context) {
	param := ctx.Query("execution_id")
	id, _ := strconv.ParseInt(param, 10, 64)
	ctx.JSON(200, internal.ExecResult{
		ID:       id,
		Status:   internal.StateRunning,
		Progress: 10,
	})
}

func (u *HttpServer) Fail(ctx *gin.Context) {
	param := ctx.Query("execution_id")
	id, _ := strconv.ParseInt(param, 10, 64)
	ctx.JSON(200, internal.ExecResult{
		ID:       id,
		Status:   internal.StateFailed,
		Progress: 10,
	})
}

func (u *HttpServer) Error(ctx *gin.Context) {
	ctx.AbortWithStatus(http.StatusServiceUnavailable)
}
