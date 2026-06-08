package ratelimit

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/prismgo/framework/cache"
	cachecontract "github.com/prismgo/framework/contracts/cache"
)

const defaultDecay = time.Minute

// LimiterFunc 根据当前请求返回一组限流规则。
//
// 命名限流器使用 Gin Context 作为输入，便于按用户、租户、IP 或路由参数动态生成 key。
type LimiterFunc func(*gin.Context) []Limit

// AttemptFunc 是 Attempt 在未超限时执行的回调。
type AttemptFunc func(context.Context) (any, error)

// RateLimiter 提供 Laravel RateLimiter 风格的固定窗口限流能力。
//
// 所有状态都写入 contracts/cache.Repository，因此 memory 与 redis store 的行为保持一致。
type RateLimiter struct {
	repo     cachecontract.Repository
	mu       sync.RWMutex
	limiters map[string]LimiterFunc
	hashKeys bool
}

// New 创建一个使用指定缓存仓库的限流器。
func New(repo cachecontract.Repository) *RateLimiter {
	if repo == nil {
		repo = cache.Resolve().Default()
	}
	return &RateLimiter{
		repo:     repo,
		limiters: make(map[string]LimiterFunc),
	}
}

// For 注册命名限流器。
func (r *RateLimiter) For(name string, limiter LimiterFunc) {
	if r == nil || limiter == nil {
		return
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return
	}
	r.mu.Lock()
	r.limiters[name] = limiter
	r.mu.Unlock()
}

// Limiter 返回已注册的命名限流器。
func (r *RateLimiter) Limiter(name string) LimiterFunc {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.limiters[strings.TrimSpace(name)]
}

// ShouldHashKeys 控制中间件生成的请求维度 key 是否使用 SHA1 哈希。
func (r *RateLimiter) ShouldHashKeys(shouldHash bool) {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.hashKeys = shouldHash
	r.mu.Unlock()
}

// Attempt 在未超限时执行回调，并在回调成功后记录一次尝试。
func (r *RateLimiter) Attempt(ctx context.Context, key string, maxAttempts int, decay time.Duration, callback AttemptFunc) (any, bool, error) {
	if callback == nil {
		return nil, false, errors.New("ratelimit: attempt callback is nil")
	}
	if ok, err := r.TooManyAttempts(ctx, key, maxAttempts); err != nil || ok {
		return nil, false, err
	}
	result, err := callback(ctx)
	if err != nil {
		return nil, true, err
	}
	if _, err := r.Hit(ctx, key, decay); err != nil {
		return nil, true, err
	}
	return result, true, nil
}

// TooManyAttempts 判断指定 key 是否已经达到最大尝试次数。
func (r *RateLimiter) TooManyAttempts(ctx context.Context, key string, maxAttempts int) (bool, error) {
	if maxAttempts <= 0 {
		return false, nil
	}
	attempts, err := r.Attempts(ctx, key)
	if err != nil {
		return false, err
	}
	if attempts < int64(maxAttempts) {
		return false, nil
	}
	hasTimer, err := r.repo.Has(ctx, r.timerKey(key))
	if err != nil {
		return false, err
	}
	if hasTimer {
		return true, nil
	}
	return false, r.ResetAttempts(ctx, key)
}

// Hit 记录一次尝试，默认步长为 1。
func (r *RateLimiter) Hit(ctx context.Context, key string, decay time.Duration) (int64, error) {
	return r.Increment(ctx, key, decay, 1)
}

// Increment 按指定步长递增尝试次数，并保证计数器拥有与 timer 一致的 TTL。
func (r *RateLimiter) Increment(ctx context.Context, key string, decay time.Duration, amount ...int64) (int64, error) {
	delta := int64(1)
	if len(amount) > 0 {
		delta = amount[0]
	}
	decay = normalizeDecay(decay)
	key = CleanRateLimiterKey(key)
	timer := r.timerKey(key)
	expiresAt := time.Now().Add(decay).Unix()
	if _, err := r.repo.Add(ctx, timer, expiresAt, decay); err != nil {
		return 0, err
	}
	// 尝试次数 key 是 cache counter，不是普通 cache value。
	//
	// 需求背景：cache 默认 Payload Encoding 切到 msgpack 后，不能再先用 Add 写入普通 value，
	// 再用 Increment 当计数器读取；同一个 key 混用两套语义会导致底层 counter 解码失败。
	// 设计思路：只通过 Increment 写入计数器，再用 Touch 补齐窗口 TTL；Touch 不重写 value，
	// 因此不会把 counter bytes 转回 Payload Encoding value。
	current, err := r.repo.Increment(ctx, key, delta)
	if err != nil {
		return 0, err
	}
	if _, err := r.repo.Touch(ctx, key, decay); err != nil {
		return 0, err
	}
	return current, nil
}

// Decrement 按指定步长递减尝试次数。
func (r *RateLimiter) Decrement(ctx context.Context, key string, amount ...int64) (int64, error) {
	delta := int64(1)
	if len(amount) > 0 {
		delta = amount[0]
	}
	return r.repo.Decrement(ctx, CleanRateLimiterKey(key), delta)
}

// Attempts 返回当前 key 已记录的尝试次数。
func (r *RateLimiter) Attempts(ctx context.Context, key string) (int64, error) {
	// 尝试次数只能通过 counter 路径读取，避免走 Payload Encoding 的 Get。
	//
	// 参数说明：key 是业务限流 key，CleanRateLimiterKey 会统一清理不可见字符。
	// 设计原因：Increment(delta=0) 返回当前 counter 值；缺失 key 会得到 0，符合 Attempts
	// 的业务语义，同时避免 msgpack/json 对 counter bytes 的差异影响限流判断。
	return r.repo.Increment(ctx, CleanRateLimiterKey(key), 0)
}

// ResetAttempts 只清理尝试次数，不清理 timer。
func (r *RateLimiter) ResetAttempts(ctx context.Context, key string) error {
	return r.repo.Forget(ctx, CleanRateLimiterKey(key))
}

// Remaining 返回当前窗口内剩余可用次数。
func (r *RateLimiter) Remaining(ctx context.Context, key string, maxAttempts int) (int, error) {
	attempts, err := r.Attempts(ctx, key)
	if err != nil {
		return 0, err
	}
	remaining := maxAttempts - int(attempts)
	if remaining < 0 {
		return 0, nil
	}
	return remaining, nil
}

// RetriesLeft 是 Remaining 的语义化别名。
func (r *RateLimiter) RetriesLeft(ctx context.Context, key string, maxAttempts int) (int, error) {
	return r.Remaining(ctx, key, maxAttempts)
}

// Clear 清理尝试次数和 timer，使当前 key 立即恢复可用。
func (r *RateLimiter) Clear(ctx context.Context, key string) error {
	key = CleanRateLimiterKey(key)
	if err := r.repo.Forget(ctx, key); err != nil {
		return err
	}
	return r.repo.Forget(ctx, r.timerKey(key))
}

// AvailableIn 返回当前 key 距离恢复可用还需要等待的秒数。
func (r *RateLimiter) AvailableIn(ctx context.Context, key string) (int, error) {
	value, err := r.repo.Get(ctx, r.timerKey(key), int64(0))
	if err != nil {
		return 0, err
	}
	expiresAt, err := asInt64(value)
	if err != nil {
		return 0, err
	}
	wait := int(expiresAt - time.Now().Unix())
	if wait < 0 {
		return 0, nil
	}
	return wait, nil
}

func asInt64(value any) (int64, error) {
	switch v := value.(type) {
	case int64:
		return v, nil
	case int:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case float64:
		return int64(v), nil
	case float32:
		return int64(v), nil
	case string:
		var out int64
		_, err := fmt.Sscan(strings.TrimSpace(v), &out)
		return out, err
	default:
		return 0, fmt.Errorf("ratelimit: expected integer counter, got %T", value)
	}
}

func (r *RateLimiter) timerKey(key string) string {
	return CleanRateLimiterKey(key) + ":timer"
}

func (r *RateLimiter) MiddlewareKey(name, key string) string {
	key = CleanRateLimiterKey(key)
	r.mu.RLock()
	hash := r.hashKeys
	r.mu.RUnlock()
	if hash {
		sum := sha1.Sum([]byte(key))
		key = hex.EncodeToString(sum[:])
	}
	return "ratelimit:" + strings.TrimSpace(name) + ":" + key
}

func normalizeDecay(decay time.Duration) time.Duration {
	if decay <= 0 {
		return defaultDecay
	}
	return decay
}

// CleanRateLimiterKey 规范化限流 key，避免控制字符进入底层缓存。
func CleanRateLimiterKey(key string) string {
	key = strings.TrimSpace(key)
	return strings.Map(func(r rune) rune {
		if r < 32 || r == 127 {
			return -1
		}
		return r
	}, key)
}
