package mysql

type TaskInfo struct {
	ID   int64 `gorm:"primary_key;auto_increment"`
	Name string
	// 任务类型
	Type         string
	Cron         string
	Executor     string
	Version      int
	Status       int8
	Cfg          string
	NextExecTime int64
	Ctime        int64
	Utime        int64
}

func (TaskInfo) TableName() string {
	return "task_info"
}

const (
	TaskStatusWaiting  = int8(1) // 等待调度
	TaskStatusRunning  = int8(2) // 正在执行
	TaskStatusPaused   = int8(3) // 任务中断
	TaskStatusFinished = int8(4) // 任务结束
)

// TaskExecHistory 任务执行记录
type TaskExecHistory struct {
	ID     int64 `gorm:"primary_key;auto_increment"`
	Tid    int64
	Status uint8
	Ctime  int64
	Utime  int64
}

func (TaskExecHistory) TableName() string {
	return "task_exec_history"
}
