package queue

import (
	"context"
	"errors"
	"fmt"
	"time"

	cachecontract "github.com/prismgo/framework/contracts/cache"
	queuecontract "github.com/prismgo/framework/contracts/queue"
	"github.com/prismgo/framework/ratelimit"
)

// Next 是 middleware 调用下一个处理器的函数。
type Next = queuecontract.Next

// Middleware 包装任务执行逻辑，可用于跳过、互斥、限流等横切能力。
type Middleware = queuecontract.Middleware

// MiddlewareFunc 把普通函数适配成 Middleware。
type MiddlewareFunc func(ctx context.Context, job Job, next Next) error

func (f MiddlewareFunc) Handle(ctx context.Context, job Job, next Next) error {
	return f(ctx, job, next)
}

// ReleaseError 表示 middleware 希望 worker 延迟释放任务，而不是立即记失败。
type ReleaseError struct {
	Delay time.Duration
	Err   error
}

func (e ReleaseError) Error() string {
	if e.Err != nil {
		return e.Err.Error()
	}
	return "queue: release job"
}

func (e ReleaseError) Unwrap() error { return e.Err }

// ReleaseAfter 构造一个带延迟的释放错误。
func ReleaseAfter(delay time.Duration, err error) error {
	if err == nil {
		err = errors.New("queue: release job")
	}
	return ReleaseError{Delay: delay, Err: err}
}

// ReleaseDelay 从错误链中提取延迟释放时间。
func ReleaseDelay(err error) (time.Duration, bool) {
	var release ReleaseError
	if errors.As(err, &release) {
		return release.Delay, true
	}
	return 0, false
}

// FailError 表示任务希望直接失败而不是继续重试。
type FailError struct {
	Err error
}

func (e FailError) Error() string {
	if e.Err != nil {
		return e.Err.Error()
	}
	return "queue: fail job"
}

func (e FailError) Unwrap() error { return e.Err }

// Fail 构造一个立即失败错误。
func Fail(err error) error {
	if err == nil {
		err = errors.New("queue: fail job")
	}
	return FailError{Err: err}
}

func shouldFailError(err error) bool {
	var fail FailError
	return errors.As(err, &fail)
}

// SkipIf 在 predicate 返回 true 时跳过任务。
func SkipIf(predicate func(Job) bool) Middleware {
	return MiddlewareFunc(func(ctx context.Context, job Job, next Next) error {
		if predicate != nil && predicate(job) {
			return ErrSkipped
		}
		return next(ctx)
	})
}

// WithoutOverlapping 使用锁保证同一 key 的任务不会重叠执行。
func WithoutOverlapping(key string, ttl ...time.Duration) *WithoutOverlappingMiddleware {
	middleware := &WithoutOverlappingMiddleware{
		key:          key,
		expireAfter:  time.Minute,
		releaseAfter: time.Minute,
	}
	if len(ttl) > 0 && ttl[0] > 0 {
		middleware.expireAfter = ttl[0]
		middleware.releaseAfter = ttl[0]
	}
	return middleware
}

// WithoutOverlappingMiddleware 是 WithoutOverlapping 的 builder。
type WithoutOverlappingMiddleware struct {
	key          string
	expireAfter  time.Duration
	releaseAfter time.Duration
	dontRelease  bool
	store        cachecontract.Repository
}

// ReleaseAfter 指定拿不到锁时多久后释放回队列。
func (m *WithoutOverlappingMiddleware) ReleaseAfter(delay time.Duration) *WithoutOverlappingMiddleware {
	m.releaseAfter = delay
	m.dontRelease = false
	return m
}

// DontRelease 指定拿不到锁时直接跳过当前任务。
func (m *WithoutOverlappingMiddleware) DontRelease() *WithoutOverlappingMiddleware {
	m.dontRelease = true
	return m
}

// ExpireAfter 指定执行锁的最长持有时间。
func (m *WithoutOverlappingMiddleware) ExpireAfter(ttl time.Duration) *WithoutOverlappingMiddleware {
	if ttl > 0 {
		m.expireAfter = ttl
	}
	return m
}

// Via 指定互斥锁使用的缓存 store。
func (m *WithoutOverlappingMiddleware) Via(store cachecontract.Repository) *WithoutOverlappingMiddleware {
	m.store = store
	return m
}

// Shared 保留 Laravel 风格 API；当前锁 key 默认已在连接范围内共享。
func (m *WithoutOverlappingMiddleware) Shared() *WithoutOverlappingMiddleware {
	return m
}

func (m *WithoutOverlappingMiddleware) Handle(ctx context.Context, job Job, next Next) error {
	if m == nil {
		return next(ctx)
	}
	lock := queueCacheRepositoryFromContext(ctx, m.store, "").Lock(overlapCacheKey(m.key), m.expireAfter)
	ok, err := lock.Get(ctx)
	if err != nil {
		return err
	}
	if !ok {
		if m.dontRelease {
			return ErrSkipped
		}
		return ReleaseAfter(m.releaseAfter, fmt.Errorf("queue: overlap lock exists for %s", m.key))
	}
	defer func() { _ = lock.Release(context.Background()) }()
	return next(ctx)
}

// ThrottlesExceptions 按异常次数节流任务重试，对应 Laravel 的 ThrottlesExceptions middleware。
func ThrottlesExceptions(max int, decay time.Duration) *ThrottlesExceptionsMiddleware {
	return &ThrottlesExceptionsMiddleware{max: max, decay: decay}
}

// ThrottlesExceptionsMiddleware 在任务连续返回错误后延迟释放任务。
type ThrottlesExceptionsMiddleware struct {
	max       int
	decay     time.Duration
	key       string
	backoff   time.Duration
	predicate func(error) bool
	store     cachecontract.Repository
}

// By 指定异常节流 key，默认使用任务类型名。
func (m *ThrottlesExceptionsMiddleware) By(key string) *ThrottlesExceptionsMiddleware {
	m.key = key
	return m
}

// Backoff 指定未达到节流阈值前的释放延迟。
func (m *ThrottlesExceptionsMiddleware) Backoff(delay time.Duration) *ThrottlesExceptionsMiddleware {
	m.backoff = delay
	return m
}

// When 指定哪些错误参与异常节流。
func (m *ThrottlesExceptionsMiddleware) When(predicate func(error) bool) *ThrottlesExceptionsMiddleware {
	m.predicate = predicate
	return m
}

// Via 指定异常节流计数使用的缓存 store。
func (m *ThrottlesExceptionsMiddleware) Via(store cachecontract.Repository) *ThrottlesExceptionsMiddleware {
	m.store = store
	return m
}

func (m *ThrottlesExceptionsMiddleware) Handle(ctx context.Context, job Job, next Next) error {
	if m == nil || m.max <= 0 || m.decay <= 0 {
		return next(ctx)
	}
	key := m.key
	if key == "" {
		if name, err := JobTypeName(job); err == nil {
			key = name
		}
	}
	if key == "" {
		key = "queue:throttle:unknown"
	}
	limiter := m.limiter(ctx)
	limited, err := limiter.TooManyAttempts(ctx, key, m.max)
	if err != nil {
		return err
	}
	if limited {
		seconds, err := limiter.AvailableIn(ctx, key)
		if err != nil {
			return err
		}
		delay := time.Duration(seconds) * time.Second
		if delay <= 0 {
			delay = m.decay
		}
		return ReleaseAfter(delay, fmt.Errorf("queue: throttled exceptions for %s", key))
	}
	err = next(ctx)
	if err == nil {
		_ = limiter.Clear(ctx, key)
		return nil
	}
	if m.predicate != nil && !m.predicate(err) {
		return err
	}
	attempts, hitErr := limiter.Hit(ctx, key, m.decay)
	if hitErr != nil {
		return hitErr
	}
	if attempts >= int64(m.max) {
		return ReleaseAfter(m.decay, err)
	}
	if m.backoff > 0 {
		return ReleaseAfter(m.backoff, err)
	}
	return err
}

func (m *ThrottlesExceptionsMiddleware) limiter(ctx context.Context) *ratelimit.RateLimiter {
	if m == nil {
		return ratelimit.New(queueCacheRepositoryFromContext(ctx, nil, ""))
	}
	return ratelimit.New(queueCacheRepositoryFromContext(ctx, m.store, ""))
}

// RateLimit 在给定窗口内限制任务执行次数，超过后释放回队列。
func RateLimit(key string, max int, every time.Duration) Middleware {
	return MiddlewareFunc(func(ctx context.Context, job Job, next Next) error {
		if max <= 0 || every <= 0 {
			return next(ctx)
		}
		limiter := ratelimit.New(queueCacheRepositoryFromContext(ctx, nil, ""))
		limited, err := limiter.TooManyAttempts(ctx, key, max)
		if err != nil {
			return err
		}
		if limited {
			seconds, err := limiter.AvailableIn(ctx, key)
			if err != nil {
				return err
			}
			delay := time.Duration(seconds) * time.Second
			if delay <= 0 {
				delay = every
			}
			return ReleaseAfter(delay, fmt.Errorf("queue: rate limit exceeded for %s", key))
		}
		if _, err := limiter.Hit(ctx, key, every); err != nil {
			return err
		}
		return next(ctx)
	})
}
