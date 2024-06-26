package executor

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/ecodeclub/ecron/internal/task"
	"log"
	"net/http"
)

type HttpExecutor struct {
}

func (h *HttpExecutor) Name() string {
	return "HTTP"
}

func (h *HttpExecutor) Run(ctx context.Context, t task.Task) error {
	var req HttpCfg
	err := json.Unmarshal([]byte(t.Cfg), &req)
	if err != nil {
		log.Println("任务配置信息有误")
		return err
	}
	if req.Method != http.MethodGet {
		return errors.New("任务配置信息有误，不是GET方法")
	}
	resp, err := http.Get(req.Url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	// 怎么处理resp
	log.Println(resp)

	return nil
}
