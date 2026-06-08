package cache

import (
	"context"
	"time"
)

// TaggedRepository 是带标签的缓存操作契约。
//
// 用途：通过 Repository.Tags() 创建，支持按标签批量失效缓存。
//
// 使用方式：
//
//	tagged := repo.Tags("tenant:5", "dictionaries")
//	tagged.Put(ctx, "country_list", data, 0)
//	// 清理标签下的所有缓存
//	tagged.Flush(ctx)
type TaggedRepository interface {
	// Tags 返回当前绑定的标签列表。
	Tags() []string

	Put(ctx context.Context, key string, value any, ttl time.Duration) error
	Set(ctx context.Context, key string, value any, ttl time.Duration) error
	Forever(ctx context.Context, key string, value any) error
	Get(ctx context.Context, key string, fallback ...any) (any, error)
	Has(ctx context.Context, key string) (bool, error)
	Missing(ctx context.Context, key string) (bool, error)
	Remember(ctx context.Context, key string, ttl time.Duration, loader func(context.Context) (any, error)) (any, error)
	RememberForever(ctx context.Context, key string, loader func(context.Context) (any, error)) (any, error)
	Sear(ctx context.Context, key string, loader func(context.Context) (any, error)) (any, error)
	Forget(ctx context.Context, key string) error
	Delete(ctx context.Context, key string) error
	Flush(ctx context.Context) error
	Clear(ctx context.Context) error
}
