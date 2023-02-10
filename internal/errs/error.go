package errs

import (
	"fmt"
)

func NewUnsupportedTaskTypeError(typ string) error {
	return fmt.Errorf("ecron: 不支持的任务类型 %s", typ)
}

func NewAddTaskTypeError(typ string) error {
	return fmt.Errorf("ecron: 创建任务失败 %s", typ)
}

func NewUpdateTaskTypeError(typ string) error {
	return fmt.Errorf("ecron: 更新任务失败 %s", typ)
}

func NewDeleteTaskTypeError(typ string) error {
	return fmt.Errorf("ecron: 删除任务失败 %s", typ)
}

func NewSelectTaskTypeError(typ string) error {
	return fmt.Errorf("ecron: 查询任务失败 %s", typ)
}
