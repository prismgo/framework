package cache

import (
	"context"
	"strconv"
	"strings"
	"time"

	cachecontract "github.com/prismgo/framework/contracts/cache"
)

// FunnelLimiter 使用一组缓存锁限制同名任务的并发数量。
//
// 它对应 Laravel Cache funnel 的常见用法，适合限制同一资源下最多只有
// N 个协程或进程同时进入关键区。
type FunnelLimiter struct {
	repo        *Repository
	name        string
	limit       int
	expireAfter time.Duration
	blockFor    time.Duration
	sleep       time.Duration
}

// newFunnel 创建并发限制器，并设置 Laravel 风格的保守默认值。
func newFunnel(repo *Repository, name string) *FunnelLimiter {
	sleep := 50 * time.Millisecond
	if repo != nil && repo.manager != nil && repo.manager.lockRetrySleep > 0 {
		sleep = repo.manager.lockRetrySleep
	}
	return &FunnelLimiter{
		repo:        repo,
		name:        strings.Trim(strings.TrimSpace(name), ":"),
		limit:       1,
		expireAfter: time.Second,
		sleep:       sleep,
	}
}

// Limit 设置最多允许同时进入关键区的数量。
func (f *FunnelLimiter) Limit(limit int) cachecontract.FunnelLimiter {
	if limit > 0 {
		f.limit = limit
	}
	return f
}

// ExpireAfter 设置单个 slot 锁的最长持有时间。
func (f *FunnelLimiter) ExpireAfter(ttl time.Duration) cachecontract.FunnelLimiter {
	if ttl > 0 {
		f.expireAfter = ttl
	}
	return f
}

// BlockFor 设置拿不到并发 slot 时的最长等待时间。
func (f *FunnelLimiter) BlockFor(wait time.Duration) cachecontract.FunnelLimiter {
	if wait > 0 {
		f.blockFor = wait
	}
	return f
}

// SleepFor 设置等待并发 slot 时两次尝试之间的休眠时间。
func (f *FunnelLimiter) SleepFor(sleep time.Duration) cachecontract.FunnelLimiter {
	if sleep > 0 {
		f.sleep = sleep
	}
	return f
}

// Then 获取并发 slot 后执行 success；拿不到 slot 时可执行 failure。
func (f *FunnelLimiter) Then(ctx context.Context, success func(context.Context) error, failure ...func(context.Context) error) (bool, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	deadline := time.Now().Add(f.blockFor)
	for {
		lock, ok, err := f.acquire(ctx)
		if err != nil || ok {
			if !ok {
				return false, err
			}
			defer func() { _ = lock.Release(context.Background()) }()
			if success == nil {
				return true, nil
			}
			return true, success(ctx)
		}
		if f.blockFor <= 0 || !time.Now().Add(f.sleep).Before(deadline) {
			return f.runFailure(ctx, failure)
		}
		if err := sleepWithContext(ctx, f.sleep); err != nil {
			return false, err
		}
	}
}

// acquire 尝试获取任意一个并发 slot。
func (f *FunnelLimiter) acquire(ctx context.Context) (cachecontract.Lock, bool, error) {
	for slot := 0; slot < f.limit; slot++ {
		lock := f.repo.Lock(f.slotName(slot), f.expireAfter)
		ok, err := lock.Get(ctx)
		if err != nil || ok {
			return lock, ok, err
		}
	}
	return nil, false, nil
}

// runFailure 在未拿到 slot 时执行调用方提供的失败回调。
func (f *FunnelLimiter) runFailure(ctx context.Context, failure []func(context.Context) error) (bool, error) {
	if len(failure) > 0 && failure[0] != nil {
		return false, failure[0](ctx)
	}
	return false, ErrLockTimeout
}

// slotName 生成并发 slot 对应的锁名称。
func (f *FunnelLimiter) slotName(slot int) string {
	if f.limit <= 1 {
		return "funnel:" + f.name
	}
	return "funnel:" + f.name + ":" + strconv.Itoa(slot)
}

// sleepWithContext 在等待过程中响应 context 取消。
func sleepWithContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
