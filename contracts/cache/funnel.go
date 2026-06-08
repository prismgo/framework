package cache

import (
	"context"
	"time"
)

// FunnelLimiter 是并发限制器的契约。
//
// 用途：使用缓存锁限制同名资源的同时操作数量，防止对同一资源的并发过载。
//
// 使用方式：
//
//	repo.Funnel("send:notification").
//	    Limit(10).
//	    ExpireAfter(time.Second).
//	    BlockFor(5*time.Second).
//	    Then(func() { sendNotification() })
type FunnelLimiter interface {
	// Limit 设置最多允许同时进入关键区的数量。
	Limit(limit int) FunnelLimiter

	// ExpireAfter 设置单个 slot 锁的最长持有时间。
	ExpireAfter(ttl time.Duration) FunnelLimiter

	// BlockFor 设置拿不到 slot 时的最长等待时间。
	BlockFor(wait time.Duration) FunnelLimiter

	// SleepFor 设置等待并发 slot 时两次尝试之间的休眠时间。
	SleepFor(sleep time.Duration) FunnelLimiter

	// Then 在获取到 slot 后执行回调，完成后自动释放。
	Then(ctx context.Context, success func(context.Context) error, failure ...func(context.Context) error) (bool, error)
}
