package main

import (
	"context"
	"log"
	"time"

	"github.com/gotomicro/ecron/internal/scheduler"
	"github.com/gotomicro/ecron/internal/storage/mysql"
)

func main() {
	storeIns := mysql.NewMysqlStorage(context.TODO(), "root:@tcp(localhost:3306)/ecron", 2*time.Second, time.Minute)
	ctx := context.TODO()

	sche := scheduler.NewScheduler(storeIns)
	go func() {
		if err := sche.Start(ctx); err != nil {
			log.Println("scheduler 启动失败：", err)
		}
	}()

	select {}
}
