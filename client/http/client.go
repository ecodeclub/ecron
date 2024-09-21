package http

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/ecodeclub/ekit/net/httpx"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	uriRegisterTask = "/task/http" // 向ecron的注册任务uri

	headerExecutionID = "Execution_id"

	httpExecutor = "HTTP"
)

type HttpClient struct {
	*Registry
	host     string // 本地地址，如 http://localhost:8080
	endpoint string // ecron服务器地址，如 http://www.ecron.com:80
	prefix   string // 本地监听路由
	client   *http.Client
}

type ClientOption func(c *HttpClient)

func WithPrefix(prefix string) ClientOption {
	return func(c *HttpClient) {
		c.prefix = prefix
	}
}

func NewHttpClient(registry *Registry, host string, endpoint string, opts ...ClientOption) *HttpClient {
	c := &HttpClient{
		Registry: registry,
		host:     host,
		endpoint: endpoint,
		client:   http.DefaultClient,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

func (c *HttpClient) HttpMutex() (http.Handler, error) {
	// prefix 是用户端的路由地址，以 / 开头且以 / 结尾
	if c.prefix == "" {
		c.prefix = "/"
	} else {
		if c.prefix[0] != '/' {
			c.prefix = "/" + c.prefix
		}
		if c.prefix[len(c.prefix)-1] != '/' {
			c.prefix = c.prefix + "/"
		}
	}

	err := c.registerTask()
	if err != nil {
		return nil, err
	}
	mux := c.newServeMux()
	return mux, nil
}

func (c *HttpClient) newServeMux() http.Handler {
	mutex := http.NewServeMux()
	// 监听路由 /aaa/bbb/ccc/
	mutex.HandleFunc(c.prefix, c.handleFunc)
	return mutex
}

func (c *HttpClient) handleFunc(w http.ResponseWriter, r *http.Request) {
	// 发起调用 /aaa/bbb/ccc/$task_name
	name, ok := strings.CutPrefix(r.RequestURI, c.prefix)
	if !ok {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "unkonwn uri: %s", r.RequestURI)
		return
	}

	t, exist := c.tasks[name]
	if !exist {
		w.WriteHeader(http.StatusBadRequest)
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

func (c *HttpClient) registerTask() error {
	req := make([]task, 0, len(c.tasks))

	for name, t := range c.tasks {
		config := httpTaskConfig{
			// http://localhost:8080/aaa/bbb/ccc/$name
			Url:             c.host + c.prefix + name,
			TaskTimeout:     t.Timeout,
			ExploreInterval: t.Interval,
		}
		cfg, err := json.Marshal(&config)
		if err != nil {
			panic("marshal config error")
		}
		item := newTask(t.Name(), t.Cron, string(cfg))
		req = append(req, item)
	}

	var resp response

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	err := httpx.NewRequest(ctx, http.MethodPost, c.endpoint+uriRegisterTask).
		JSONBody(req).
		Client(c.client).
		Do().JSONScan(&resp)

	if err != nil || resp.Code != 0 {
		return errors.New("发起任务注册失败")
	}
	return nil
}

// ecron注册任务接口的返回
type response struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
	Data any    `json:"data"`
}

type task struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Executor string `json:"executor"`
	Cfg      string `json:"cfg"`
	Cron     string `json:"cron"`
}

func newTask(name string, cron string, config string) task {
	return task{
		Name:     name,
		Cron:     cron,
		Type:     "HttpTask",
		Executor: httpExecutor,
		Cfg:      config,
	}
}

type httpTaskConfig struct {
	Url             string        `json:"url"`
	TaskTimeout     time.Duration `json:"taskTimeout"`
	ExploreInterval time.Duration `json:"exploreInterval"`
}
