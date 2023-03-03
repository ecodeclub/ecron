package errs

import (
	"fmt"
)

func NewUnsupportedTaskTypeError(typ string) error {
	return fmt.Errorf("ecron: 不支持的任务类型 %s", typ)
}

func NewCompareAndUpdateAffectZeroError() error {
	return fmt.Errorf("ecron: cas 更新状态异常，状态修改受影响行数为0，请确认update条件")
}

func NewCompareAndUpdateDbError(err error) error {
	return fmt.Errorf("ecron: cas 更新db异常，%w", err)
}

func NewCreateStorageError(err error) error {
	return fmt.Errorf("ecron: 创建storage实例异常，%w", err)
}

func NewAddTaskError(err error) error {
	return fmt.Errorf("ecron: 添加任务异常，%w", err)
}
