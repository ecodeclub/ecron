package mysql

type TaskInfo struct {
	Id              int64 `eorm:"auto_increment,primary_key"`
	Name            string
	SchedulerStatus string
	Epoch           int64
	Cron            string
	Type            string
	Config          string
	OccupierId      int64 // 占有该任务的storage
	CandidateId     int64 // 该任务的候选storage
	CreateTime      int64
	UpdateTime      int64
}

type TaskExecution struct {
	Id            int64 `eorm:"auto_increment,primary_key"`
	TaskId        int64
	ExecuteStatus string
	CreateTime    int64
	UpdateTime    int64
}

type StorageInfo struct {
	Id      int64  `eorm:"auto_increment,primary_key"`
	Payload int64  // 该storage的负载
	Status  string // 该storage状态
}
