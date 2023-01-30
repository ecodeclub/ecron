package errs

import (
	"fmt"
)

func NewUnsupportedTaskTypeError(typ string) error {
	return fmt.Errorf("ecron: 不支持的任务类型 %s", typ)
}
