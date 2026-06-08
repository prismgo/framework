package cache

import (
	"context"
	"time"
)

// rememberTyped 实现 Laravel Cache::remember 风格的读穿缓存。
//
// 命中时直接解码返回；未命中时执行 loader，并把 loader 结果写回当前 Repository。
func rememberTyped[T any](ctx context.Context, repo *Repository, key string, ttl time.Duration, loader Loader[T]) (T, error) {
	data, err := repo.getTyped(ctx, key)
	if err == nil {
		var value T
		if err := repo.decode(data, &value); err != nil {
			var zero T
			return zero, err
		}
		return value, nil
	}
	if !isMiss(err) {
		var zero T
		return zero, err
	}
	value, err := loader(ctx)
	if err != nil {
		var zero T
		return zero, err
	}
	if err := repo.Put(ctx, key, value, ttl); err != nil {
		var zero T
		return zero, err
	}
	return value, nil
}
