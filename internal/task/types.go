package task

type Type string

const (
	TypeHTTP = "http_task"
)

type EventType string

const (
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
	// EventTypeInit 任务创建成功
	EventTypeInit = "init"
)

type Config struct {
	Name       string
	Cron       string
	Type       Type
	Parameters string
}

// Task 实际上这个表示的是任务的执行信息
// 通过TaskId关联任务详情ßßß
type Task struct {
	Config
	TaskId int64
	Epoch  int64
}

type Event struct {
	Task
	Type EventType
}
