package startup

import (
	"github.com/ecodeclub/ecron/internal/executor"
	"github.com/gin-gonic/gin"
	"net/http"
	"strconv"
	"time"
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
	// 记录当前是第几个请求
	count int
}

func (u *HttpServer) RegisterRouter(server *gin.Engine) {
	server.POST("/success", u.Success) // 一次成功
	server.POST("/failed", u.Fail)     // 一次失败
	server.POST("/error", u.Error)     // 业务方错误
	server.POST("/timeout", u.Timeout) // 业务方超时
	server.POST("/explore_success", u.ExploreSuccess)
	server.GET("/explore_success", u.ExploreSuccess)
	server.POST("/explore_failed", u.ExploreFailed)
	server.GET("/explore_failed", u.ExploreFailed)
	server.POST("/running", u.Running)   // 永远返回执行中
	server.GET("/running", u.Running)    // 永远返回执行中
	server.DELETE("/running", u.Running) // 永远返回执行中
}

func (u *HttpServer) Success(ctx *gin.Context) {
	param := ctx.GetHeader("execution_id")
	id, _ := strconv.ParseInt(param, 10, 64)
	ctx.JSON(200, executor.Result{
		Eid:      id,
		Status:   executor.StatusSuccess,
		Progress: 100,
	})
}

func (u *HttpServer) ExploreSuccess(ctx *gin.Context) {
	param := ctx.GetHeader("execution_id")
	id, _ := strconv.ParseInt(param, 10, 64)
	if u.count == 0 {
		// 发起调用
		ctx.JSON(200, executor.Result{
			Eid:      id,
			Status:   executor.StatusRunning,
			Progress: 10,
		})
		u.count++
		return
	}
	// 探查
	if u.count == 1 {
		ctx.JSON(200, executor.Result{
			Eid:      id,
			Status:   executor.StatusRunning,
			Progress: 50,
		})
		u.count++
		return
	} else {
		ctx.JSON(200, executor.Result{
			Eid:      id,
			Status:   executor.StatusSuccess,
			Progress: 100,
		})
	}
}

func (u *HttpServer) ExploreFailed(ctx *gin.Context) {
	param := ctx.GetHeader("execution_id")
	id, _ := strconv.ParseInt(param, 10, 64)
	if u.count == 0 {
		ctx.JSON(200, executor.Result{
			Eid:      id,
			Status:   executor.StatusRunning,
			Progress: 10,
		})
		u.count++
		return
	} else if u.count == 1 {
		ctx.JSON(200, executor.Result{
			Eid:      id,
			Status:   executor.StatusRunning,
			Progress: 50,
		})
		u.count++
		return
	} else {
		ctx.JSON(200, executor.Result{
			Eid:      id,
			Status:   executor.StatusFailed,
			Progress: 50,
		})
	}
}

func (u *HttpServer) Fail(ctx *gin.Context) {
	param := ctx.GetHeader("execution_id")
	id, _ := strconv.ParseInt(param, 10, 64)
	ctx.JSON(200, executor.Result{
		Eid:      id,
		Status:   executor.StatusFailed,
		Progress: 0,
	})
}

func (u *HttpServer) Error(ctx *gin.Context) {
	ctx.AbortWithStatus(http.StatusServiceUnavailable)
}

func (u *HttpServer) Timeout(ctx *gin.Context) {
	param := ctx.GetHeader("execution_id")
	id, _ := strconv.ParseInt(param, 10, 64)
	// 发起http调用是5秒，这里休眠6秒，客户端就会返回超时
	time.Sleep(time.Second * 6)
	ctx.JSON(200, executor.Result{
		Eid:      id,
		Status:   executor.StatusSuccess,
		Progress: 100,
	})
}

func (u *HttpServer) Running(ctx *gin.Context) {
	param := ctx.GetHeader("execution_id")
	id, _ := strconv.ParseInt(param, 10, 64)
	ctx.JSON(200, executor.Result{
		Eid:      id,
		Status:   executor.StatusRunning,
		Progress: 10,
	})
}
