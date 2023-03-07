package mysql

type TaskInfo struct {
	Id               int64 `eorm:"auto_increment,primary_key"`
	Name             string
	SchedulerStatus  string
	Epoch            int64
	Cron             string
	Type             string
	Config           string
	OccupierId       uint32 // 占有该任务的storage
	OccupierPayload  int64  // 占有该任务的storage的载荷
	CandidateId      uint32 // 该任务的候选storage
	CandidatePayload int64  // 该任务的候选storage的载荷
	BaseColumns
}

type TaskExecution struct {
	Id            int64 `eorm:"auto_increment,primary_key"`
	TaskId        int64
	ExecuteStatus string
	BaseColumns
}

type BaseColumns struct {
	CreateTime int64
	UpdateTime int64
}
