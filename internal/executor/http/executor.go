package http

import (
	"context"
	"encoding/json"
	"github.com/gotomicro/ecron/internal/scheduler"
	"github.com/gotomicro/ecron/internal/task"
	"io"
	"net/http"
	"strings"
	"time"
)

type ApiConfig struct {
	URL      string            `json:"url" comment:"URL"`
	Method   string            `json:"method" comment:"Method"`
	PayLoad  string            `json:"payload" comment:"PayLoad"`
	Header   map[string]string `json:"header" comment:"Header"`
	Deadline int64             `json:"deadline" comment:"Deadline"` // 以秒为单位 默认5s
}

const (
	StatusRunning = 1 + iota
	StatusSuccess
	StatusFailed
)

type Resp struct {
	// Status 请求执行状态
	Status int `json:"status"`
	// Delay 下次请求的延迟时间 以秒为单位
	Delay int64 `json:"delay"`
	// Payload 如果有值 则轮询中下次查询时会将此参数放在body中发送
	Payload string `json:"payload"`
}

type Executor struct {
	client http.Client
}

func (e *Executor) Execute(t *task.Task) <-chan scheduler.Event {
	event := make(chan scheduler.Event)
	go func(t *task.Task) {
		failed := true
		defer func() {
			if failed {
				event <- scheduler.Event{Type: scheduler.EventTypeFailed}
			}
		}()
		c := &ApiConfig{}
		if err := json.Unmarshal(t.Executor, c); err != nil {
			return
		}
	Next:
		req, err := http.NewRequest(c.Method, c.URL, strings.NewReader(c.PayLoad))
		if err != nil {
			return
		}
		for k, v := range c.Header {
			req.Header.Add(k, v)
		}
		if c.Deadline <= 0 {
			c.Deadline = 5
		}
		ctx, _ := context.WithTimeout(context.Background(), time.Second*time.Duration(c.Deadline))
		resp, err := e.client.Do(req.WithContext(ctx))
		if err != nil {
			return
		}
		defer resp.Body.Close()
		if resp.StatusCode != 200 {
			return
		}
		var msg []byte
		info := &Resp{}
		if msg, err = io.ReadAll(resp.Body); err != nil {
			return
		}
		if err = json.Unmarshal(msg, info); err != nil {
			return
		}
		switch info.Status {
		case StatusSuccess:
			event <- scheduler.Event{Type: scheduler.EventTypeSuccess}
			failed = false
		case StatusRunning:
			event <- scheduler.Event{Type: scheduler.EventTypeWaiting}
			if len(c.PayLoad) > 0 {
				c.PayLoad = info.Payload
			}
			if info.Delay <= 0 {
				info.Delay = 60
			}
			time.Sleep(time.Second * time.Duration(info.Delay))
			goto Next
		}
	}(t)
	return event
}
