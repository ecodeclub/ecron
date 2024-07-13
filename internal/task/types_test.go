package task

import (
	"errors"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
	"time"
)

func TestTask_NextTime(t *testing.T) {
	testCases := []struct {
		name     string
		cronExp  string
		wantErr  error
		interval time.Duration
	}{
		{
			name:    "field解析成功",
			cronExp: "0/5 * * * * ?",
			wantErr: nil,
		},
		{
			name:    "Descriptor解析成功",
			cronExp: "@every 1s",
			wantErr: nil,
		},
		{
			name:    "field解析错误",
			cronExp: "*/5 * * *",
			wantErr: errors.New("expected exactly 6 fields"),
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			task := Task{
				CronExp: tc.cronExp,
			}
			res, err := task.NextTime()
			if err != nil {
				assert.True(t, strings.Contains(err.Error(), tc.wantErr.Error()))
				return
			}
			assert.Equal(t, tc.wantErr, err)
			// 预期的下一次执行时间，会大于当前时间
			assert.True(t, res.Sub(time.Now()).Seconds() > 0)
		})
	}
}
