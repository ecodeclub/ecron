package executor

import (
	"context"
	"encoding/json"
	"github.com/ecodeclub/ecron/internal/storage/mysql"
	"github.com/ecodeclub/ecron/pkg/grpc/generic"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"log"
	"strconv"
)

type GrpcExecutor struct {
}

func (g *GrpcExecutor) Name() string {
	return "GRPC"
}

func (g *GrpcExecutor) Run(ctx context.Context, t mysql.TaskInfo) error {
	var req GrpcCfg
	err := json.Unmarshal([]byte(t.Cfg), &req)
	if err != nil {
		return err
	}
	// 怎么发起一个grpc调用
	conn, err := grpc.NewClient(":"+strconv.Itoa(req.Port),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	defer conn.Close()
	if err != nil {
		log.Println("grpc dial err:", err)
	}
	client := generic.NewGpcGenericClient(req.ServiceName, conn)
	if err := client.Init(ctx); err != nil {
		return err
	}
	resp, err := client.InvokeUnaryJson(ctx, req.Method, map[string]any{})
	if err != nil {
		panic(err)
	}

	// 处理resp
	log.Println(resp)
	return nil
}
