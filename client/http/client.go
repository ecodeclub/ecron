package http

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
)

const (
	headerExecutionID = "Execution_id"
)

type HttpClient struct {
	registry *Registry
	prefix   string // 本地监听路由
	client   *http.Client
}

type ClientOption func(c *HttpClient)

func WithPrefix(prefix string) ClientOption {
	return func(c *HttpClient) {
		c.prefix = prefix
	}
}

func NewHttpClient(registry *Registry, opts ...ClientOption) *HttpClient {
	c := &HttpClient{
		registry: registry,
		client:   http.DefaultClient,
		prefix:   "/", // 默认监听地址是 /
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

func (c *HttpClient) HttpMutex() http.Handler {
	//mutex := http.NewServeMux()
	//mutex.HandleFunc(c.prefix, c.handleFunc)
	//return mutex
	return c
}

func (c *HttpClient) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// 发起调用 /aaa/bbb/ccc/$task_name
	name, ok := strings.CutPrefix(r.RequestURI, c.prefix)
	if !ok {
		// 不是以 $prefix 开头
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "unkonwn uri: %s", r.RequestURI)
		return
	}

	t, exist := c.registry.GetTask(name)
	if !exist {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "task not found: %s", name)
		return
	}

	header, exist := r.Header[headerExecutionID]
	if !exist {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "miss header: execution_id")
		return
	}

	id := header[0]
	eid, err := strconv.ParseInt(id, 10, 64)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "unknown execution_id: %s", id)
		return
	}

	var status Status
	var progress int
	switch r.Method {
	case http.MethodGet:
		status, progress = t.Status()
	case http.MethodPost:
		status, progress = t.Execute()
	case http.MethodDelete:
		err := t.Stop()
		if err != nil {
			fmt.Fprintf(w, "stop task failed")
		} else {
			fmt.Fprintf(w, "ok")
		}
		return
	default:
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "unsupported method %s", r.Method)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	err = json.NewEncoder(w).Encode(result{
		Eid:      eid,
		Status:   status,
		Progress: progress,
	})
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
	}
}

type result struct {
	Eid      int64  `json:"eid"`
	Status   Status `json:"status"`
	Progress int    `json:"progress"`
}
