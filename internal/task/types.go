package task

import "time"

type Type string

const (
	TypeHTTP = "http_task"
)

type EventType string
type TaskJobType string

const (
	// EventTypeCreated 当前task已经被创建，需要通知schedule
	EventTypeCreated = "created"
	// EventTypePreempted 当前调度节点已经抢占了这个任务
	EventTypePreempted = "preempted"
	// EventTypeRunnable 已经到了可运行的时间点
	// 这个时候可能还在等待别的资源
	// 借鉴于进程调度中的概念
	EventTypeRunnable = "runnable"
	// EventTypeRunning 已经找到了目标节点，并且正在运行
	EventTypeRunning = "running"
	// EventTypeFailed 任务运行失败
	EventTypeFailed = "failed"
	// EventTypeSuccess 任务运行成功
	EventTypeSuccess = "success"
)

type Config struct {
	Name string
	Cron string
	Type Type
	Retry
	Executor []byte
}

type Retry struct {
	RetryCount int           //重试次所
	RetryTime  time.Duration //重试时间
	//	statusCode []int         //重试状态码
}

type Task struct {
	Config
	ID          int64       //任务ID
	Progress    string      //任务进度
	State       string      //任务状态0：没有被抢占 1：抢占
	Node        Node        //任务被调度到的节点信息
	TaskError   TaskError   //任务错误内容
	TaskJobType TaskJobType //任务工作类型0：普通任务 1：定时任务
	PostGUID    int64       //任务的唯一ID
	CreateAt    time.Time   //任务创建时间
	UpdateAt    time.Time   //任务修改时间
	DeleteAt    time.Time   //任务删除时间
	TaskEvent   chan Event  //任务事件
}

type Node struct {
	ID       int64     //节点ID
	Name     string    //节点Name
	Addr     string    //节点地址
	Online   bool      //节点是否在线
	CreateAt time.Time //任务创建时间
	UpdateAt time.Time //任务修改时间
	DeleteAt time.Time //任务删除时间
}

type TaskError struct {
	Code    string //task错误状态码
	Message string //task错误信息
}

type Event struct {
	Type EventType
}
