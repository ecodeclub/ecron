package errs

import "errors"

var (
	ErrExecuteTaskFailed    = errors.New("任务执行失败")
	ErrWrongTaskCfg         = errors.New("任务配置信息错误")
	ErrRequestExecuteFailed = errors.New("发起任务执行请求失败")
	ErrUnknownTask          = errors.New("未知的任务，找不到任务的执行入口")

	ErrNoExecutableTask = errors.New("当前没有可执行的任务")
)
