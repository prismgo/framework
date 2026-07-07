// Package ratelimit 定义限流器的公共接口。
//
// 设计原因：遵循框架三层模式（contracts → 实现 → facade），
// 使限流能力可以被 mock 测试或替换实现。
package ratelimit

import (
	"context"
	"time"
)

// Limiter 定义限流器的最小接口。
//
// 实现必须支持固定窗口限流、尝试次数管理和窗口重置。
type Limiter interface {
	// Attempt 在未超限时执行回调，并在回调成功后记录一次尝试。
	//
	// 返回值：
	//   - any: 回调执行结果
	//   - bool: 是否允许执行（true 表示未超限且回调成功）
	//   - error: 缓存或回调错误
	Attempt(ctx context.Context, key string, maxAttempts int, decay time.Duration, callback AttemptFunc) (any, bool, error)

	// TooManyAttempts 判断指定 key 是否已经达到最大尝试次数。
	TooManyAttempts(ctx context.Context, key string, maxAttempts int) (bool, error)

	// Hit 记录一次尝试，默认步长为 1。
	Hit(ctx context.Context, key string, decay time.Duration) (int64, error)

	// Clear 清理尝试次数和 timer，使当前 key 立即恢复可用。
	Clear(ctx context.Context, key string) error
}

// AttemptFunc 是 Attempt 在未超限时执行的回调。
type AttemptFunc func(context.Context) (any, error)
