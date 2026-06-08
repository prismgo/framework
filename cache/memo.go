package cache

import (
	"context"
	"sync"
	"time"

	cachecontract "github.com/prismgo/framework/contracts/cache"
)

// MemoRepository 为一个 Repository 增加请求/任务内记忆化读取。
//
// 它只缓存当前 MemoRepository 实例读取到的原始 payload bytes；任何写入、删除、
// 计数或清空操作都会同步清理本地 memo，避免同一流程内读到旧值。
type MemoRepository struct {
	repo *Repository
	mu   sync.RWMutex
	data map[string][]byte
	miss map[string]struct{}
}

func newMemoRepository(repo *Repository) *MemoRepository {
	return &MemoRepository{
		repo: repo,
		data: map[string][]byte{},
		miss: map[string]struct{}{},
	}
}

// Repository 返回被包装的底层 Repository。
func (m *MemoRepository) Repository() cachecontract.Repository {
	return m.repo
}

// Get 从 memo 或底层 store 读取值。
func (m *MemoRepository) Get(ctx context.Context, key string, fallback ...any) (any, error) {
	data, err := m.getEncoded(ctx, key)
	if err == nil {
		var out any
		if err := m.repo.decode(data, &out); err != nil {
			return nil, err
		}
		return out, nil
	}
	if !isMiss(err) {
		return nil, err
	}
	return resolveAnyDefault(ctx, fallback)
}

// Has 判断 key 是否存在。
func (m *MemoRepository) Has(ctx context.Context, key string) (bool, error) {
	_, err := m.getEncoded(ctx, key)
	if err == nil {
		return true, nil
	}
	if isMiss(err) {
		return false, nil
	}
	return false, err
}

// Missing 判断 key 是否不存在。
func (m *MemoRepository) Missing(ctx context.Context, key string) (bool, error) {
	ok, err := m.Has(ctx, key)
	return !ok, err
}

// Put 写入底层 store 并清理本地 memo。
func (m *MemoRepository) Put(ctx context.Context, key string, value any, ttl time.Duration) error {
	if err := m.repo.Put(ctx, key, value, ttl); err != nil {
		return err
	}
	m.forgetLocal(key)
	return nil
}

// Set 是 Put 的别名。
func (m *MemoRepository) Set(ctx context.Context, key string, value any, ttl time.Duration) error {
	return m.Put(ctx, key, value, ttl)
}

// Forever 永久写入底层 store 并清理本地 memo。
func (m *MemoRepository) Forever(ctx context.Context, key string, value any) error {
	return m.Put(ctx, key, value, 0)
}

// Remember 读取 memo / store；未命中时执行 loader 并写入底层 store。
func (m *MemoRepository) Remember(ctx context.Context, key string, ttl time.Duration, loader func(context.Context) (any, error)) (any, error) {
	value, err := m.Get(ctx, key)
	if err == nil {
		return value, nil
	}
	if !isMiss(err) {
		return nil, err
	}
	value, err = loader(ctx)
	if err != nil {
		return nil, err
	}
	if err := m.Put(ctx, key, value, ttl); err != nil {
		return nil, err
	}
	return value, nil
}

// RememberForever 读取 memo / store；未命中时执行 loader 并永久写入。
func (m *MemoRepository) RememberForever(ctx context.Context, key string, loader func(context.Context) (any, error)) (any, error) {
	return m.Remember(ctx, key, 0, loader)
}

// Sear 是 RememberForever 的别名。
func (m *MemoRepository) Sear(ctx context.Context, key string, loader func(context.Context) (any, error)) (any, error) {
	return m.RememberForever(ctx, key, loader)
}

// Forget 删除底层 store 中的 key 并清理 memo。
func (m *MemoRepository) Forget(ctx context.Context, key string) error {
	if err := m.repo.Forget(ctx, key); err != nil {
		return err
	}
	m.forgetLocal(key)
	return nil
}

// Delete 是 Forget 的别名。
func (m *MemoRepository) Delete(ctx context.Context, key string) error {
	return m.Forget(ctx, key)
}

// Flush 清空底层 store 并清空 memo。
func (m *MemoRepository) Flush(ctx context.Context) error {
	if err := m.repo.Flush(ctx); err != nil {
		return err
	}
	m.clearLocal()
	return nil
}

// Clear 是 Flush 的别名。
func (m *MemoRepository) Clear(ctx context.Context) error {
	return m.Flush(ctx)
}

// Increment 递增计数器并清理 memo。
func (m *MemoRepository) Increment(ctx context.Context, key string, delta ...int64) (int64, error) {
	value, err := m.repo.Increment(ctx, key, delta...)
	if err == nil {
		m.forgetLocal(key)
	}
	return value, err
}

// Decrement 递减计数器并清理 memo。
func (m *MemoRepository) Decrement(ctx context.Context, key string, delta ...int64) (int64, error) {
	value, err := m.repo.Decrement(ctx, key, delta...)
	if err == nil {
		m.forgetLocal(key)
	}
	return value, err
}

func (m *MemoRepository) getEncoded(ctx context.Context, key string) ([]byte, error) {
	m.mu.RLock()
	if data, ok := m.data[key]; ok {
		m.mu.RUnlock()
		return append([]byte(nil), data...), nil
	}
	if _, ok := m.miss[key]; ok {
		m.mu.RUnlock()
		return nil, ErrCacheMiss
	}
	m.mu.RUnlock()

	data, err := m.repo.getTyped(ctx, key)
	m.mu.Lock()
	defer m.mu.Unlock()
	if err == nil {
		m.data[key] = append([]byte(nil), data...)
		delete(m.miss, key)
		return append([]byte(nil), data...), nil
	}
	if isMiss(err) {
		m.miss[key] = struct{}{}
		delete(m.data, key)
	}
	return nil, err
}

func (m *MemoRepository) forgetLocal(key string) {
	m.mu.Lock()
	delete(m.data, key)
	delete(m.miss, key)
	m.mu.Unlock()
}

func (m *MemoRepository) clearLocal() {
	m.mu.Lock()
	m.data = map[string][]byte{}
	m.miss = map[string]struct{}{}
	m.mu.Unlock()
}
