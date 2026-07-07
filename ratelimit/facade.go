package ratelimit

import (
	"context"
	"time"

	"github.com/prismgo/framework/facade"
)

const serviceKey = "ratelimit.default"

// Resolve 从当前 Application 容器解析 RateLimiter。
func Resolve() *RateLimiter {
	return facade.Resolve[*RateLimiter](serviceKey)
}

func For(name string, limiter LimiterFunc) { Resolve().For(name, limiter) }
func Limiter(name string) LimiterFunc      { return Resolve().Limiter(name) }
func ShouldHashKeys(shouldHash bool)       { Resolve().ShouldHashKeys(shouldHash) }
func Attempt(ctx context.Context, key string, maxAttempts int, decay time.Duration, callback AttemptFunc) (any, bool, error) {
	return Resolve().Attempt(ctx, key, maxAttempts, decay, callback)
}
func TooManyAttempts(ctx context.Context, key string, maxAttempts int) (bool, error) {
	return Resolve().TooManyAttempts(ctx, key, maxAttempts)
}
func Hit(ctx context.Context, key string, decay time.Duration) (int64, error) {
	return Resolve().Hit(ctx, key, decay)
}
func Increment(ctx context.Context, key string, decay time.Duration, amount ...int64) (int64, error) {
	return Resolve().Increment(ctx, key, decay, amount...)
}
func Decrement(ctx context.Context, key string, amount ...int64) (int64, error) {
	return Resolve().Decrement(ctx, key, amount...)
}
func Attempts(ctx context.Context, key string) (int64, error) {
	return Resolve().Attempts(ctx, key)
}
func ResetAttempts(ctx context.Context, key string) error {
	return Resolve().ResetAttempts(ctx, key)
}
func Remaining(ctx context.Context, key string, maxAttempts int) (int, error) {
	return Resolve().Remaining(ctx, key, maxAttempts)
}
func RetriesLeft(ctx context.Context, key string, maxAttempts int) (int, error) {
	return Resolve().RetriesLeft(ctx, key, maxAttempts)
}
func Clear(ctx context.Context, key string) error {
	return Resolve().Clear(ctx, key)
}
func AvailableIn(ctx context.Context, key string) (int, error) {
	return Resolve().AvailableIn(ctx, key)
}
