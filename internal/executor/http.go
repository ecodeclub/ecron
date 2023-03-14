package executor

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/ecodeclub/ecron/internal/task"
)

var _ Executor = &HttpExec{}

var _legalMethod = map[string]struct{}{
	"post":   {},
	"get":    {},
	"put":    {},
	"delete": {},
}

type HttpExec struct {
	client *http.Client
}

type HttpRequestConf struct {
	Url      string            `json:"url" comment:"URL"`
	Method   string            `json:"method" comment:"Method"`
	Payload  string            `json:"payload,omitempty" comment:"Payload"`
	Header   map[string]string `json:"header,omitempty" comment:"Header"`
	Deadline int64             `json:"deadline,omitempty" comment:"Deadline"` // 以秒为单位 默认5s
}

type HttpResponse struct {
	// Status 请求执行状态
	Status string `json:"status"`
	// Delay 下次请求的延迟时间 以秒为单位
	Delay int64 `json:"delay,omitempty"`
	// Payload 如果有值 则轮询中下次查询时会将此参数放在body中发送
	// Payload string `json:"payload,omitempty"`
}

func NewHttpExec(client *http.Client) *HttpExec {
	return &HttpExec{client: client}
}

func (h *HttpExec) Execute(ctx context.Context, t *task.Task) Event {
	event := Event{Type: ExecuteFailed}
	c, err := parseHttpConfig(t.Config.Parameters)
	if err != nil {
		return event
	}
	req, err := http.NewRequest(c.Method, c.Url, strings.NewReader(c.Payload))
	if err != nil {
		return event
	}
	for k, v := range c.Header {
		req.Header.Add(k, v)
	}
	ctx, cancel := context.WithTimeout(ctx, time.Second*time.Duration(c.Deadline))
	defer cancel()
	resp, err := h.client.Do(req.WithContext(ctx))
	if err != nil {
		return event
	}
	defer resp.Body.Close()
	log.Println("request status: ", resp.Status)
	if resp.StatusCode != http.StatusOK {
		return event
	}
	var msg []byte
	info := &HttpResponse{}
	if msg, err = io.ReadAll(resp.Body); err != nil {
		return event
	}
	if err = json.Unmarshal(msg, info); err != nil {
		return event
	}
	event.Type = EventType(info.Status)
	event.Delay = time.Millisecond * time.Duration(info.Delay)
	return event
}

func parseHttpConfig(config string) (*HttpRequestConf, error) {
	c := &HttpRequestConf{}
	if err := json.Unmarshal([]byte(config), c); err != nil {
		return nil, err
	}
	if _, ok := _legalMethod[strings.ToLower(c.Method)]; !ok {
		return nil, errors.New(`非法方法`)
	}
	return c, nil
}
