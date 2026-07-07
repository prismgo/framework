package ratelimit

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/prismgo/framework/cache"
	cachecontract "github.com/prismgo/framework/contracts/cache"
	"github.com/prismgo/framework/exception"
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
	key = CleanRateLimiterKey(key)
	attempts, err := r.repo.Increment(ctx, key, 0)
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
	return false, r.repo.Forget(ctx, key)
}

// Hit 记录一次尝试，默认步长为 1。
func (r *RateLimiter) Hit(ctx context.Context, key string, decay time.Duration) (int64, error) {
	return r.Increment(ctx, key, decay, 1)
}

// Increment 按指定步长递增尝试次数，并保证计数器拥有与 timer 一致的 TTL。
//
// 参数 amount 是可选的步长参数，只使用第一个值；多余值会被忽略。
// 设计原因：模拟 Laravel 的可选参数语义，避免强制调用方传入固定值。
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
		return 0, fmt.Errorf("ratelimit: failed to increment counter for key %s: %w", key, err)
	}
	if _, err := r.repo.Touch(ctx, key, decay); err != nil {
		// Touch 失败时，计数器已成功递增，不应返回错误。
		// 使用 exception.Report 记录错误，便于后续排查 TTL 未延长的问题。
		exception.Report(ctx, fmt.Errorf("ratelimit: failed to touch TTL for key %s: %w", key, err), nil)
	}
	return current, nil
}

// Decrement 按指定步长递减尝试次数。
//
// 参数 amount 是可选的步长参数，只使用第一个值；多余值会被忽略。
// 设计原因：模拟 Laravel 的可选参数语义，避免强制调用方传入固定值。
func (r *RateLimiter) Decrement(ctx context.Context, key string, amount ...int64) (int64, error) {
	delta := int64(1)
	if len(amount) > 0 {
		delta = amount[0]
	}
	return r.repo.Decrement(ctx, CleanRateLimiterKey(key), delta)
}

// Attempts 返回当前 key 已记录的尝试次数。
//
// 设计决策：通过 Increment(delta=0) 而非 Get 读取，原因是 cache 底层使用 msgpack 编码，
// Get 会将 counter bytes 解码为 Payload 结构，导致后续 Increment 失败。此方法保证计数器
// 语义一致性，详见 cache 包 msgpack 迁移文档。
func (r *RateLimiter) Attempts(ctx context.Context, key string) (int64, error) {
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
	return max(0, maxAttempts-int(attempts)), nil
}

// RetriesLeft 返回当前窗口内剩余可用次数（含重试次数）。
//
// 设计原因：这是 Remaining 的语义化别名，用于明确表达"剩余重试次数"的业务场景。
// 两者功能完全相同，选择使用哪个取决于代码上下文的语义清晰度。
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
	key = CleanRateLimiterKey(key)
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

// asInt64 将任意类型转换为 int64，浮点数使用四舍五入。
func asInt64(value any) (int64, error) {
	switch v := value.(type) {
	case int64:
		return v, nil
	case int:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case float64:
		return int64(math.Round(v)), nil
	case float32:
		return int64(math.Round(float64(v))), nil
	case string:
		var out int64
		_, err := fmt.Sscan(strings.TrimSpace(v), &out)
		return out, err
	default:
		return 0, fmt.Errorf("ratelimit: expected integer counter, got %T", value)
	}
}

// timerKey 构造 timer 专用 key，使用 "timer:" 前缀避免与用户 key 命名空间碰撞。
//
// 设计原因：如果采用后缀 ":timer"，用户 key "user:123:timer" 的 counter key 会与
// "user:123" 的 timer key 碰撞。前缀方案确保两者完全分离。
// 调用方必须在入口处先调用 CleanRateLimiterKey，此处不再重复清理。
func (r *RateLimiter) timerKey(key string) string {
	return "timer:" + key
}

func (r *RateLimiter) MiddlewareKey(name, key string) string {
	key = CleanRateLimiterKey(key)
	r.mu.RLock()
	hash := r.hashKeys
	r.mu.RUnlock()
	if hash {
		sum := sha256.Sum256([]byte(key))
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
