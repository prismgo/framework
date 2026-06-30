package cache

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"time"

	libstore "github.com/eko/gocache/lib/v4/store"
	"github.com/prismgo/framework/routine"
)

// memoryType 是 memory store 暴露给 gocache 的驱动类型名称。
const memoryType = "memory"

// memoryItem 保存单个进程内缓存值及其过期时间。
type memoryItem struct {
	value     any
	expiresAt time.Time
}

// memoryStore 是基于 map 的进程内缓存实现。
// 它实现 gocache StoreInterface，同时额外支持 Touch 和缓存锁能力。
type memoryStore struct {
	// mu 保护 items 的并发读写。
	mu      sync.RWMutex
	items   map[string]memoryItem
	options *libstore.Options
	stop    chan struct{}
	done    chan struct{}
	// closeOnce 保证后台清理 goroutine 只被关闭一次，支持 Manager/Repository 重复 Close。
	closeOnce sync.Once
	locks     *memoryLockProvider
	tags      map[string]map[string]struct{}
}

// newMemoryStore 创建进程内缓存 store，并按需启动过期 key 清理 goroutine。
func newMemoryStore(defaultTTL, cleanupInterval time.Duration) *memoryStore {
	store := &memoryStore{
		items: map[string]memoryItem{},
		options: &libstore.Options{
			Expiration: defaultTTL,
		},
		locks: newMemoryLockProvider(),
		tags:  map[string]map[string]struct{}{},
	}
	if cleanupInterval > 0 {
		store.stop = make(chan struct{})
		store.done = make(chan struct{})
		routine.Task(context.Background(), func(context.Context) error {
			store.janitor(cleanupInterval)
			return nil
		}).
			Component("cache").
			Name("memory.janitor").
			Go()
	}
	return store
}

// Get 从内存中读取 key，未命中或过期时返回 gocache 的 NotFound 错误。
func (s *memoryStore) Get(ctx context.Context, key any) (any, error) {
	_ = ctx
	k, ok := key.(string)
	if !ok {
		return nil, errors.New("cache: memory key must be string")
	}
	s.mu.RLock()
	item, exists := s.items[k]
	s.mu.RUnlock()
	if !exists {
		return nil, libstore.NotFoundWithCause(ErrCacheMiss)
	}
	if expired(item, time.Now()) {
		s.mu.Lock()
		if current, ok := s.items[k]; ok && expired(current, time.Now()) {
			delete(s.items, k)
		}
		s.mu.Unlock()
		return nil, libstore.NotFoundWithCause(ErrCacheMiss)
	}
	return item.value, nil
}

// GetWithTTL 返回缓存值及剩余 TTL；不过期 key 的 TTL 返回 -1。
// 优化：在单次锁持有期间完成所有读取，避免 TOCTOU 竞态。
func (s *memoryStore) GetWithTTL(ctx context.Context, key any) (any, time.Duration, error) {
	_ = ctx
	k, ok := key.(string)
	if !ok {
		return nil, 0, errors.New("cache: memory key must be string")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	item, exists := s.items[k]
	if !exists {
		return nil, 0, libstore.NotFoundWithCause(ErrCacheMiss)
	}

	now := time.Now()
	if expired(item, now) {
		delete(s.items, k)
		return nil, 0, libstore.NotFoundWithCause(ErrCacheMiss)
	}

	if item.expiresAt.IsZero() {
		return item.value, -1, nil
	}
	return item.value, time.Until(item.expiresAt), nil
}

// Set 写入内存缓存，并按调用参数或默认配置计算过期时间。
func (s *memoryStore) Set(ctx context.Context, key any, value any, options ...libstore.Option) error {
	_ = ctx
	k, ok := key.(string)
	if !ok {
		return errors.New("cache: memory key must be string")
	}
	opts := libstore.ApplyOptionsWithDefault(s.options, options...)
	item := memoryItem{value: value}
	if opts.Expiration > 0 {
		item.expiresAt = time.Now().Add(opts.Expiration)
	}
	s.mu.Lock()
	s.items[k] = item
	s.mu.Unlock()
	return nil
}

// Add 仅当 key 不存在或已过期时写入值，判断和写入在同一把锁内完成。
func (s *memoryStore) Add(ctx context.Context, key string, value []byte, ttl time.Duration) (bool, error) {
	_ = ctx
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()

	if item, ok := s.items[key]; ok {
		if !expired(item, now) {
			return false, nil
		}
		delete(s.items, key)
	}

	item := memoryItem{value: value}
	if ttl > 0 {
		item.expiresAt = now.Add(ttl)
	}
	s.items[key] = item
	return true, nil
}

// Increment 原子递增整数计数器；已有 key 的过期时间保持不变。
func (s *memoryStore) Increment(ctx context.Context, key string, delta int64) (int64, error) {
	_ = ctx
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()

	item, ok := s.items[key]
	if ok && expired(item, now) {
		delete(s.items, key)
		ok = false
	}

	var current int64
	if ok {
		var err error
		current, err = decodeCounter(item.value)
		if err != nil {
			return 0, err
		}
	}

	next := current + delta
	item.value = []byte(strconv.FormatInt(next, 10))
	s.items[key] = item
	return next, nil
}

// Pull 原子读取并删除指定 key。
func (s *memoryStore) Pull(ctx context.Context, key string) ([]byte, error) {
	_ = ctx
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()

	item, ok := s.items[key]
	if !ok {
		return nil, libstore.NotFoundWithCause(ErrCacheMiss)
	}
	delete(s.items, key)
	if expired(item, now) {
		return nil, libstore.NotFoundWithCause(ErrCacheMiss)
	}
	return rawBytes(item.value)
}

// GetMany 批量读取内存缓存，未命中的 key 不会出现在返回值中。
// 优化：在一次锁操作内完成所有 key 的读取，避免 N+1 锁竞争问题。
func (s *memoryStore) GetMany(ctx context.Context, keys []string) (map[string][]byte, error) {
	_ = ctx
	if len(keys) == 0 {
		return make(map[string][]byte), nil
	}

	now := time.Now()
	out := make(map[string][]byte, len(keys))

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, key := range keys {
		item, exists := s.items[key]
		if !exists {
			continue
		}

		if expired(item, now) {
			delete(s.items, key)
			continue
		}

		data, err := rawBytes(item.value)
		if err != nil {
			return nil, err
		}
		out[key] = data
	}

	return out, nil
}

// PutMany 批量写入内存缓存。
func (s *memoryStore) PutMany(ctx context.Context, values map[string][]byte, ttl time.Duration) error {
	for key, value := range values {
		if err := s.Set(ctx, key, value, expirationOptions(ttl)...); err != nil {
			return err
		}
	}
	return nil
}

// ForgetMany 批量删除内存缓存。
func (s *memoryStore) ForgetMany(ctx context.Context, keys []string) error {
	for _, key := range keys {
		if err := s.Delete(ctx, key); err != nil {
			return err
		}
	}
	return nil
}

// Delete 删除指定 key；key 不存在时视为成功。
func (s *memoryStore) Delete(ctx context.Context, key any) error {
	_ = ctx
	k, ok := key.(string)
	if !ok {
		return errors.New("cache: memory key must be string")
	}
	s.mu.Lock()
	delete(s.items, k)
	s.mu.Unlock()
	return nil
}

// Invalidate 预留 gocache 标签失效接口；当前 memory store 未实现标签索引。
func (s *memoryStore) Invalidate(ctx context.Context, options ...libstore.InvalidateOption) error {
	_ = ctx
	_ = options
	return nil
}

// Clear 清空当前 memory store 中的全部缓存。
func (s *memoryStore) Clear(ctx context.Context) error {
	_ = ctx
	s.mu.Lock()
	s.items = map[string]memoryItem{}
	s.tags = map[string]map[string]struct{}{}
	s.mu.Unlock()
	return nil
}

// GetType 返回 store 类型名称。
func (s *memoryStore) GetType() string {
	return memoryType
}

// Touch 仅更新已有 key 的过期时间，不读取或重写 value。
func (s *memoryStore) Touch(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	_ = ctx
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	item, ok := s.items[key]
	if !ok || expired(item, now) {
		if ok {
			delete(s.items, key)
		}
		return false, nil
	}
	if ttl > 0 {
		item.expiresAt = now.Add(ttl)
	} else {
		item.expiresAt = time.Time{}
	}
	s.items[key] = item
	return true, nil
}

// GetTagged 读取 tagged cache 对应的真实缓存 key。
func (s *memoryStore) GetTagged(ctx context.Context, prefix string, tags []string, key string) ([]byte, error) {
	value, err := s.Get(ctx, taggedDataKey(prefix, tags, key))
	if err != nil {
		return nil, err
	}
	return rawBytes(value)
}

// PutTagged 写入 tagged cache 并维护标签索引。
func (s *memoryStore) PutTagged(ctx context.Context, prefix string, tags []string, key string, value []byte, ttl time.Duration) error {
	dataKey := taggedDataKey(prefix, tags, key)
	if err := s.Set(ctx, dataKey, value, expirationOptions(ttl)...); err != nil {
		return err
	}
	s.mu.Lock()
	for _, tag := range tags {
		indexKey := tagIndexKey(prefix, tag)
		if s.tags[indexKey] == nil {
			s.tags[indexKey] = map[string]struct{}{}
		}
		s.tags[indexKey][dataKey] = struct{}{}
	}
	s.mu.Unlock()
	return nil
}

// ForgetTagged 删除 tagged cache 中的指定 key，并从当前标签索引移除。
func (s *memoryStore) ForgetTagged(ctx context.Context, prefix string, tags []string, key string) error {
	dataKey := taggedDataKey(prefix, tags, key)
	if err := s.Delete(ctx, dataKey); err != nil {
		return err
	}
	s.mu.Lock()
	for _, tag := range tags {
		indexKey := tagIndexKey(prefix, tag)
		delete(s.tags[indexKey], dataKey)
	}
	s.mu.Unlock()
	return nil
}

// FlushTags 清理任一指定标签关联的全部缓存项。
func (s *memoryStore) FlushTags(ctx context.Context, prefix string, tags []string) error {
	_ = ctx
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, tag := range tags {
		indexKey := tagIndexKey(prefix, tag)
		for dataKey := range s.tags[indexKey] {
			delete(s.items, dataKey)
		}
		delete(s.tags, indexKey)
	}
	return nil
}

// Close 停止 memory store 的后台清理 goroutine。
func (s *memoryStore) Close() error {
	if s.stop == nil {
		return nil
	}
	s.closeOnce.Do(func() {
		close(s.stop)
		<-s.done
	})
	return nil
}

// janitor 按固定间隔清理过期 key。
func (s *memoryStore) janitor(interval time.Duration) {
	defer close(s.done)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.deleteExpired(time.Now())
		case <-s.stop:
			return
		}
	}
}

// deleteExpired 扫描并删除所有已过期的 key。
func (s *memoryStore) deleteExpired(now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for key, item := range s.items {
		if expired(item, now) {
			delete(s.items, key)
		}
	}
}

// expired 判断一个 memoryItem 在给定时间点是否已经过期。
func expired(item memoryItem, now time.Time) bool {
	return !item.expiresAt.IsZero() && !now.Before(item.expiresAt)
}
