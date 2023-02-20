package http

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/gotomicro/ecron/internal/executor"
	"github.com/gotomicro/ecron/internal/task"
	"io"
	"net/http"
	"strings"
	"time"
)

var _ executor.Executor = &Executor{}

var _legalMethod = map[string]struct{}{
	"post":   {},
	"get":    {},
	"put":    {},
	"delete": {},
}

type ApiConfig struct {
	URL      string            `json:"url" comment:"URL"`
	Method   string            `json:"method" comment:"Method"`
	Payload  string            `json:"payload,omitempty" comment:"Payload"`
	Header   map[string]string `json:"header,omitempty" comment:"Header"`
	Deadline int64             `json:"deadline,omitempty" comment:"Deadline"` // 以秒为单位 默认5s
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
	Delay int64 `json:"delay,omitempty"`
	// Payload 如果有值 则轮询中下次查询时会将此参数放在body中发送
	Payload string `json:"payload,omitempty"`
}

type Executor struct {
	client *http.Client
}

func NewExecutor() *Executor {
	return &Executor{
		client: &http.Client{},
	}
}

func (e *Executor) Execute(t *task.Task) (<-chan executor.Event, error) {
	if t == nil {
		return nil, errors.New(`非法任务`)
	}
	event := make(chan executor.Event)
	c := &ApiConfig{}
	if err := json.Unmarshal(t.Executor, c); err != nil {
		return nil, errors.New(`非法配置`)
	}
	if _, ok := _legalMethod[strings.ToLower(c.Method)]; !ok {
		return nil, errors.New(`非法方法`)
	}
	go func(c *ApiConfig) {
		failed := true
		defer func() {
			if failed {
				event <- executor.Event{Type: executor.EventTypeFailed}
			}
			close(event)
		}()
	Next:
		req, err := http.NewRequest(c.Method, c.URL, strings.NewReader(c.Payload))
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
		if resp.StatusCode != http.StatusOK {
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
			event <- executor.Event{Type: executor.EventTypeSuccess}
			failed = false
		case StatusRunning:
			event <- executor.Event{Type: executor.EventTypeWaiting}
			if len(info.Payload) > 0 {
				c.Payload = info.Payload
			}
			if info.Delay <= 0 {
				info.Delay = 60
			}
			time.Sleep(time.Millisecond * time.Duration(info.Delay))
			goto Next
		}
	}(c)
	return event, nil
}
