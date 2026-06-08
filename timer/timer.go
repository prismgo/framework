// Package timer 提供定时任务的基础类型，供 console.CronCommand 嵌入使用。
package timer

import "time"

// Timer 描述定时任务的执行间隔。
// Interval 表示重复执行间隔，0 表示不重复（仅执行一次）。
type Timer struct {
	Interval time.Duration
}
