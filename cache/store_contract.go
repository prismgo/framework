package cache

import (
	"context"
	"time"
)

// repositoryStore 将具体 Repository 适配为 contracts/cache.Store。
//
// 需求背景：Repository.GetStore() 需要提供 Laravel getStore() 类似能力，但 PrismGo
// contracts 不能泄漏 gocache 的 StoreInterface，因此通过 Repository 已有公共行为适配。
type repositoryStore struct {
	repo *Repository
}

// Get 读取单个 key 的缓存值。
func (s repositoryStore) Get(ctx context.Context, key string) (any, error) {
	return s.repo.Get(ctx, key)
}

// Many 批量读取 key；未命中 key 由 Repository 返回 nil 值。
func (s repositoryStore) Many(ctx context.Context, keys []string) (map[string]any, error) {
	return s.repo.Many(ctx, keys)
}

// Put 写入单个 key。
func (s repositoryStore) Put(ctx context.Context, key string, value any, ttl time.Duration) error {
	return s.repo.Put(ctx, key, value, ttl)
}

// PutMany 批量写入 key。
func (s repositoryStore) PutMany(ctx context.Context, values map[string]any, ttl time.Duration) error {
	return s.repo.PutMany(ctx, values, ttl)
}

// Add 仅当 key 不存在时写入。
func (s repositoryStore) Add(ctx context.Context, key string, value any, ttl time.Duration) (bool, error) {
	return s.repo.Add(ctx, key, value, ttl)
}

// Increment 按 delta 原子递增计数器。
func (s repositoryStore) Increment(ctx context.Context, key string, delta int64) (int64, error) {
	return s.repo.Increment(ctx, key, delta)
}

// Decrement 按 delta 原子递减计数器。
func (s repositoryStore) Decrement(ctx context.Context, key string, delta int64) (int64, error) {
	return s.repo.Decrement(ctx, key, delta)
}

// Forever 永久写入 key。
func (s repositoryStore) Forever(ctx context.Context, key string, value any) error {
	return s.repo.Forever(ctx, key, value)
}

// Forget 删除单个 key。
func (s repositoryStore) Forget(ctx context.Context, key string) error {
	return s.repo.Forget(ctx, key)
}

// ForgetMany 批量删除 key。
func (s repositoryStore) ForgetMany(ctx context.Context, keys []string) error {
	return s.repo.ForgetMany(ctx, keys)
}

// Flush 清空当前 store。
func (s repositoryStore) Flush(ctx context.Context) error {
	return s.repo.Flush(ctx)
}

// Prefix 返回当前 Repository 的 key 前缀。
func (s repositoryStore) Prefix() string {
	return s.repo.prefix
}
