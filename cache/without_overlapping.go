package cache

import (
	"context"
	"time"

	cachecontract "github.com/prismgo/framework/contracts/cache"
)

// WithoutOverlappingOptions 描述 WithoutOverlapping helper 的等待与锁 TTL 策略。
type WithoutOverlappingOptions = cachecontract.WithoutOverlappingOptions

// WithoutOverlappingOption 修改 WithoutOverlappingOptions。
type WithoutOverlappingOption = cachecontract.WithoutOverlappingOption

// WithOverlapWait 设置等待获取锁的最长时间。
func WithOverlapWait(wait time.Duration) WithoutOverlappingOption {
	return func(options *WithoutOverlappingOptions) {
		if wait >= 0 {
			options.WaitFor = wait
		}
	}
}

// WithOverlapLock 设置锁的过期时间。
func WithOverlapLock(ttl time.Duration) WithoutOverlappingOption {
	return func(options *WithoutOverlappingOptions) {
		if ttl > 0 {
			options.LockFor = ttl
		}
	}
}

// WithOverlapSleep 设置等待锁时两次尝试之间的间隔。
func WithOverlapSleep(sleep time.Duration) WithoutOverlappingOption {
	return func(options *WithoutOverlappingOptions) {
		if sleep > 0 {
			options.SleepFor = sleep
		}
	}
}

// WithoutOverlapping 使用默认 store 的缓存锁防止同名任务重叠执行。
func WithoutOverlapping(ctx context.Context, key string, fn func(context.Context) error, opts ...WithoutOverlappingOption) (bool, error) {
	return WithoutOverlappingFrom(ctx, "", key, fn, opts...)
}

// WithoutOverlappingFrom 使用指定 store 的缓存锁防止同名任务重叠执行。
func WithoutOverlappingFrom(ctx context.Context, storeName, key string, fn func(context.Context) error, opts ...WithoutOverlappingOption) (bool, error) {
	return Store(storeName).WithoutOverlapping(ctx, key, fn, opts...)
}
