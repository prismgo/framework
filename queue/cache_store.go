package queue

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/prismgo/framework/cache"
	configpkg "github.com/prismgo/framework/config"
	cachecontract "github.com/prismgo/framework/contracts/cache"
	"github.com/prismgo/framework/queue/payload"
)

const (
	defaultQueueCacheTTL = time.Minute
	uniqueCachePrefix    = "queue:unique"
	debounceCachePrefix  = "queue:debounce"
	overlapCachePrefix   = "queue:overlap"
)

type queueCacheContextKey string

const queueCacheDriverContextKey queueCacheContextKey = "queue.cache_driver"

// queueCacheStore 返回队列高级能力使用的缓存仓库。
//
// 任务未显式指定 via 时统一回退到 prismgo/cache 的默认 store，避免队列连接
// 自己实现锁、防抖等缓存语义。
func queueCacheStore(name string) cachecontract.Repository {
	name = strings.TrimSpace(name)
	if name == "" {
		return cache.Resolve().Default()
	}
	return cache.Store(name)
}

func queueCacheRepository(store cachecontract.Repository, name string) cachecontract.Repository {
	if repo, ok := store.(*cache.Repository); ok && repo == nil {
		store = nil
	}
	if store != nil {
		return store
	}
	return queueCacheStore(name)
}

func queueCacheRepositoryFromContext(ctx context.Context, store cachecontract.Repository, name string) cachecontract.Repository {
	if store != nil || strings.TrimSpace(name) != "" {
		return queueCacheRepository(store, name)
	}
	return queueCacheStore(queueCacheDriverFromContext(ctx))
}

func queueCacheStoreName(store cachecontract.Repository) string {
	if store == nil {
		return ""
	}
	return store.Name()
}

func queueCacheStoreNameOrDefault(store cachecontract.Repository, fallback string) string {
	if name := queueCacheStoreName(store); name != "" {
		return name
	}
	return strings.TrimSpace(fallback)
}

func withQueueCacheDriver(ctx context.Context, driver string) context.Context {
	driver = strings.TrimSpace(driver)
	if ctx == nil {
		ctx = context.Background()
	}
	if driver == "" {
		return ctx
	}
	return context.WithValue(ctx, queueCacheDriverContextKey, driver)
}

func queueCacheDriverFromContext(ctx context.Context) string {
	if ctx != nil {
		if driver, ok := ctx.Value(queueCacheDriverContextKey).(string); ok && strings.TrimSpace(driver) != "" {
			return strings.TrimSpace(driver)
		}
	}
	return configpkg.GetString("queue.cache_driver", "")
}

func uniqueCacheKey(key string) string {
	return uniqueCachePrefix + ":" + cleanCacheKey(key)
}

func debounceCacheKey(key string) string {
	return debounceCachePrefix + ":" + cleanCacheKey(key)
}

func overlapCacheKey(key string) string {
	return overlapCachePrefix + ":" + cleanCacheKey(key)
}

func cleanCacheKey(key string) string {
	key = strings.Trim(strings.TrimSpace(key), ":")
	return strings.Map(func(r rune) rune {
		if r < 32 || r == 127 {
			return -1
		}
		return r
	}, key)
}

func normalizeQueueCacheTTL(ttl time.Duration) time.Duration {
	if ttl <= 0 {
		return defaultQueueCacheTTL
	}
	return ttl
}

func acquireUnique(ctx context.Context, env *payload.Envelope, store cachecontract.Repository, ttl time.Duration) error {
	if env == nil || env.UniqueKey == "" {
		return nil
	}
	ok, err := queueCacheRepositoryFromContext(ctx, store, env.UniqueVia).Add(ctx, uniqueCacheKey(env.UniqueKey), env.ID, normalizeQueueCacheTTL(ttl))
	if err != nil {
		return err
	}
	if !ok {
		return ErrDuplicate
	}
	return nil
}

func releaseUnique(ctx context.Context, env *payload.Envelope) error {
	if env == nil || env.UniqueKey == "" {
		return nil
	}
	repo := queueCacheRepositoryFromContext(ctx, nil, env.UniqueVia)
	key := uniqueCacheKey(env.UniqueKey)
	value, err := repo.Get(ctx, key, "")
	if err != nil {
		return err
	}
	if current, ok := value.(string); ok && current != "" && env.ID != "" && current != env.ID {
		return nil
	}
	return repo.Forget(ctx, key)
}

func rememberDebounce(ctx context.Context, env *payload.Envelope, store cachecontract.Repository, ttl time.Duration) error {
	if env == nil || env.DebounceKey == "" {
		return nil
	}
	ttl = normalizeQueueCacheTTL(ttl) + defaultQueueCacheTTL
	return queueCacheRepositoryFromContext(ctx, store, env.DebounceVia).Put(ctx, debounceCacheKey(env.DebounceKey), env.ID, ttl)
}

func staleDebounce(ctx context.Context, env *payload.Envelope) (bool, error) {
	if env == nil || env.DebounceKey == "" {
		return false, nil
	}
	value, err := queueCacheRepositoryFromContext(ctx, nil, env.DebounceVia).Get(ctx, debounceCacheKey(env.DebounceKey), "")
	if err != nil {
		return false, err
	}
	id, ok := value.(string)
	if !ok {
		return false, fmt.Errorf("queue: debounce cache value for %s is %T", env.DebounceKey, value)
	}
	return id != "" && id != env.ID, nil
}
