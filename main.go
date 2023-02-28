package main

import (
	"context"
	"log"
	"time"

	"github.com/gotomicro/ecron/internal/scheduler"
	"github.com/gotomicro/ecron/internal/storage/mysql"
)

func main() {
	ctx := context.TODO()
	storeIns := mysql.NewMysqlStorage("root:@tcp(localhost:3306)/ecron", mysql.WithPreemptInterval(1*time.Second))
	go storeIns.RunPreempt(ctx)
	go storeIns.AutoRefresh(ctx)

	sche := scheduler.NewScheduler(storeIns)
	go func() {
		if err := sche.Start(ctx); err != nil {
			log.Println("scheduler 启动失败：", err)
		}
	}()

	select {}
}
