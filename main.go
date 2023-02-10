package main

import (
	"context"

	"github.com/gotomicro/ecron/internal/scheduler"
	"github.com/gotomicro/ecron/internal/storage/mysql"
)

func main() {
	storeIns := mysql.NewStorage()
	ctx := context.TODO()

	// 在storage 层执行抢占
	go storeIns.RunPreempt(ctx)

	sche := scheduler.NewScheduler(storeIns)
	go sche.Start(ctx)

	select {}
}
