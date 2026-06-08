package cache

import (
	"context"
	"time"
)

// MemoRepository 是请求内记忆化缓存的契约。
//
// 用途：对同一个底层 Repository 的读取进行短期内存缓存，避免同一请求内重复查询。
// 写入、删除操作会同步清理本地缓存，保证数据一致性。
//
// 使用方式：
//
//	memo := repo.Memo()
//	user, err := memo.Get(ctx, "user:123")
//	// 同一请求内再次查询直接从内存返回
//	user, err = memo.Get(ctx, "user:123")
type MemoRepository interface {
	// Repository 返回被包装的底层 Repository。
	Repository() Repository

	Get(ctx context.Context, key string, fallback ...any) (any, error)
	Has(ctx context.Context, key string) (bool, error)
	Missing(ctx context.Context, key string) (bool, error)
	Put(ctx context.Context, key string, value any, ttl time.Duration) error
	Set(ctx context.Context, key string, value any, ttl time.Duration) error
	Forever(ctx context.Context, key string, value any) error
	Remember(ctx context.Context, key string, ttl time.Duration, loader func(context.Context) (any, error)) (any, error)
	RememberForever(ctx context.Context, key string, loader func(context.Context) (any, error)) (any, error)
	Sear(ctx context.Context, key string, loader func(context.Context) (any, error)) (any, error)
	Forget(ctx context.Context, key string) error
	Delete(ctx context.Context, key string) error
	Flush(ctx context.Context) error
	Clear(ctx context.Context) error
	Increment(ctx context.Context, key string, delta ...int64) (int64, error)
	Decrement(ctx context.Context, key string, delta ...int64) (int64, error)
}
