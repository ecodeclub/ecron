package mysql

type TaskInfo struct {
	ID   int64 `gorm:"primary_key;auto_increment"`
	Name string
	// 任务类型
	Type         string
	Cron         string
	Executor     string
	version      int64
	Status       int
	Cfg          string
	NextExecTime int64
	Ctime        int64
	Utime        int64
}

const (
	TaskTypeLocal = "Local_Task"
	TaskTypeHttp  = "HTTP_Task"
	TaskTypeGrpc  = "GRPC_Task"
)

const (
	TaskStatusWaiting  = 1 // 等待调度
	TaskStatusRunning  = 2 // 正在执行
	TaskStatusPaused   = 3 // 任务中断
	TaskStatusFinished = 4 // 任务结束
)

// TaskExecHistory 任务执行记录
type TaskExecHistory struct {
	ID     int64 `gorm:"primary_key;auto_increment"`
	Tid    int64
	Status int
	Ctime  int64
	Utime  int64
}
