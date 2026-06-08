package ratelimit

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/prismgo/framework/cache"
	configpkg "github.com/prismgo/framework/config"
	cachecontract "github.com/prismgo/framework/contracts/cache"
)

var defaultLimiter = struct {
	mu      sync.RWMutex
	current *RateLimiter
}{}

func current() *RateLimiter {
	defaultLimiter.mu.RLock()
	current := defaultLimiter.current
	defaultLimiter.mu.RUnlock()
	if current != nil {
		return current
	}

	defaultLimiter.mu.Lock()
	defer defaultLimiter.mu.Unlock()
	if defaultLimiter.current == nil {
		defaultLimiter.current = New(configuredRepository())
	}
	return defaultLimiter.current
}

func configuredRepository() cachecontract.Repository {
	driver := strings.TrimSpace(configpkg.GetString("cache.limiter.driver", ""))
	if driver == "" {
		return cache.Resolve().Default()
	}
	return cache.Store(driver)
}

// Resolve 返回当前全局限流器。
func Resolve() *RateLimiter {
	return current()
}

func For(name string, limiter LimiterFunc) { current().For(name, limiter) }
func Limiter(name string) LimiterFunc      { return current().Limiter(name) }
func ShouldHashKeys(shouldHash bool)       { current().ShouldHashKeys(shouldHash) }
func Attempt(ctx context.Context, key string, maxAttempts int, decay time.Duration, callback AttemptFunc) (any, bool, error) {
	return current().Attempt(ctx, key, maxAttempts, decay, callback)
}
func TooManyAttempts(ctx context.Context, key string, maxAttempts int) (bool, error) {
	return current().TooManyAttempts(ctx, key, maxAttempts)
}
func Hit(ctx context.Context, key string, decay time.Duration) (int64, error) {
	return current().Hit(ctx, key, decay)
}
func Increment(ctx context.Context, key string, decay time.Duration, amount ...int64) (int64, error) {
	return current().Increment(ctx, key, decay, amount...)
}
func Decrement(ctx context.Context, key string, amount ...int64) (int64, error) {
	return current().Decrement(ctx, key, amount...)
}
func Attempts(ctx context.Context, key string) (int64, error) {
	return current().Attempts(ctx, key)
}
func ResetAttempts(ctx context.Context, key string) error {
	return current().ResetAttempts(ctx, key)
}
func Remaining(ctx context.Context, key string, maxAttempts int) (int, error) {
	return current().Remaining(ctx, key, maxAttempts)
}
func RetriesLeft(ctx context.Context, key string, maxAttempts int) (int, error) {
	return current().RetriesLeft(ctx, key, maxAttempts)
}
func Clear(ctx context.Context, key string) error {
	return current().Clear(ctx, key)
}
func AvailableIn(ctx context.Context, key string) (int, error) {
	return current().AvailableIn(ctx, key)
}
