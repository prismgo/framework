package cache

import (
	"context"
	"time"
)

// Lock 是分布式锁的完整契约。
//
// 用途：通过 Repository.Lock() 创建，支持立即获取、阻塞等待和回调执行模式。
//
// 使用方式：
//
//	lock := repo.Lock("resource:1", 30*time.Second)
//
//	// 模式 1：立即获取，成功后手动释放
//	ok, err := lock.Get(ctx)
//	if ok { defer lock.Release(context.Background()) }
//
//	// 模式 2：阻塞等待，获取后执行回调
//	ok, err := lock.Block(ctx, 10*time.Second, func(ctx context.Context) error {
//	    return processResource(ctx)
//	})
type Lock interface {
	// Get 尝试立即获取锁。
	//
	// 参数 fn 为可选回调，非 nil 时在获取成功后执行回调并自动释放锁。
	// 返回 true 表示获取成功；false 表示锁已被其他调用方持有。
	Get(ctx context.Context, fn ...func(context.Context) error) (bool, error)

	// Block 在 wait 时间内阻塞等待获取锁。
	//
	// 参数 wait 是最长等待时间；fn 在获取成功后执行并自动释放锁。
	// 超过 wait 仍未获取时返回 false 和 ErrLockTimeout。
	Block(ctx context.Context, wait time.Duration, fn func(context.Context) error) (bool, error)

	// Release 释放当前锁实例持有的锁。
	//
	// 释放时校验 token 确保只释放自己获取的锁。
	Release(ctx context.Context) error

	// ForceRelease 不校验 token 直接释放锁。
	ForceRelease(ctx context.Context) error

	// Owner 返回当前锁实例的 owner token。
	//
	// 可用于跨进程恢复锁后释放。
	Owner() string

	// BetweenBlockedAttemptsSleepFor 设置 Block 轮询时的间隔时间。
	BetweenBlockedAttemptsSleepFor(d time.Duration) Lock
}
