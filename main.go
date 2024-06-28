package main

import (
	"github.com/ecodeclub/ecron/internal/executor"
	"github.com/ecodeclub/ecron/internal/scheduler"
	mysql2 "github.com/ecodeclub/ecron/internal/storage/mysql"
	"github.com/ecodeclub/ecron/register"
	"github.com/gin-gonic/gin"
	"golang.org/x/sync/semaphore"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"time"
)

func main() {
	db, err := gorm.Open(mysql.Open("root:root@tcp(localhost:13316)/ecron"))
	if err != nil {
		panic(err)
	}
	td := mysql2.NewGormTaskDAO(db, 100, time.Minute*3)
	hd := mysql2.NewGormHistoryDAO(db)
	limiter := semaphore.NewWeighted(100)
	s := scheduler.NewPreemptScheduler(td, hd, time.Minute, limiter)
	he := executor.NewHttpExecutor()
	ge := executor.NewGrpcExecutor()
	s.RegisterExecutor(he, ge)
	server := gin.Default()
	reg := register.NewRegisterHandler(td, he, ge)
	reg.RegisterRouter(server)
	server.Run(":8080")
}
