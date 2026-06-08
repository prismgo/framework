package cache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	gocache "github.com/eko/gocache/lib/v4/cache"
	libstore "github.com/eko/gocache/lib/v4/store"
	cachecontract "github.com/prismgo/framework/contracts/cache"
	encodingcontract "github.com/prismgo/framework/contracts/encoding"
	encodingpkg "github.com/prismgo/framework/encoding"
)

// Repository 是单个缓存 store 的操作入口。
//
// 它封装 gocache.Cache，并在外层统一处理 key 前缀、Payload Encoding、Touch、
// flexible 刷新去重和锁 key 命名。
type Repository struct {
	// name 是 store 的配置名称，例如 memory / redis。
	name       string
	prefix     string
	codec      encodingcontract.Codec
	cache      *gocache.Cache[any]
	store      libstore.StoreInterface
	toucher    TouchStore
	atomic     AtomicStore
	bulk       BulkStore
	flusher    PrefixFlushStore
	tags       TagStore
	locker     LockProvider
	lockFlush  LockFlushStore
	closer     CloseStore
	lockPrefix string
	manager    *Manager
	err        error
	events     bool

	refreshMu  sync.Mutex
	refreshing map[string]struct{}
}

// newRepository 创建一个可用的 store 操作入口。
func newRepository(name, prefix string, store libstore.StoreInterface, toucher TouchStore, atomic AtomicStore, bulk BulkStore, flusher PrefixFlushStore, tags TagStore, locker LockProvider, lockFlush LockFlushStore, closer CloseStore, lockPrefix string, manager *Manager, events bool) *Repository {
	return &Repository{
		name:       name,
		prefix:     prefix,
		codec:      managerCodec(manager),
		cache:      gocache.New[any](store),
		store:      store,
		toucher:    toucher,
		atomic:     atomic,
		bulk:       bulk,
		flusher:    flusher,
		tags:       tags,
		locker:     locker,
		lockFlush:  lockFlush,
		closer:     closer,
		lockPrefix: lockPrefix,
		manager:    manager,
		events:     events,
		refreshing: make(map[string]struct{}),
	}
}

// managerCodec 返回 repository 应使用的 Payload Encoding codec。
//
// 参数说明：manager 是创建 repository 的 cache manager；正常装配路径会携带严格解析后的
// codec。测试中仍有少量旧 helper 直接构造 repository 或调用包级 encode，此处保留 msgpack
// 默认值，避免这些历史入口绕过第一版默认行为。
func managerCodec(manager *Manager) encodingcontract.Codec {
	if manager != nil && manager.codec != nil {
		return manager.codec
	}
	codec, _ := encodingpkg.Resolve(encodingpkg.NameMsgpack)
	return codec
}

// newErrorRepository 创建一个延迟返回错误的 Repository。
//
// 这样 Store("unknown") 不会立即 panic，后续调用会返回清晰的 error。
func newErrorRepository(name string, err error, manager *Manager) *Repository {
	return &Repository{name: name, err: err, manager: manager, events: true, refreshing: make(map[string]struct{})}
}

// Name 返回当前 Repository 对应的 store 名称。
func (r *Repository) Name() string {
	return r.name
}

// GetStore 返回当前 Repository 包装的 Store 契约。
//
// 设计原因：对齐 Laravel Repository.getStore() 的边界，但只暴露
// contracts/cache.Store，不把 gocache 的 StoreInterface 泄漏给业务代码。
func (r *Repository) GetStore() cachecontract.Store {
	if r == nil || r.err != nil || r.store == nil {
		return nil
	}
	return repositoryStore{repo: r}
}

// Put 将 value 写入当前 store。
//
// 数值语义 value 会直写为十进制 bytes；其他 value 会先按当前 cache Payload Encoding 编码，
// 再交给底层 store；ttl <= 0 时使用不过期语义。
func (r *Repository) Put(ctx context.Context, key string, value any, ttl time.Duration) error {
	if r.err != nil {
		return r.err
	}
	r.dispatch(ctx, EventCacheWriting, CacheEvent{Key: key})
	data, err := r.encode(value)
	if err != nil {
		r.dispatch(ctx, EventCacheWriteFailed, CacheEvent{Key: key, Error: err})
		return err
	}
	options := expirationOptions(ttl)
	if err := r.cache.Set(ctx, r.key(key), data, options...); err != nil {
		r.dispatch(ctx, EventCacheWriteFailed, CacheEvent{Key: key, Error: err})
		return err
	}
	r.dispatch(ctx, EventCacheWritten, CacheEvent{Key: key})
	return nil
}

// Set 是 Put 的别名，便于对齐 Laravel/PSR 风格命名。
func (r *Repository) Set(ctx context.Context, key string, value any, ttl time.Duration) error {
	return r.Put(ctx, key, value, ttl)
}

// Has 判断当前 store 中是否存在指定 key。
func (r *Repository) Has(ctx context.Context, key string) (bool, error) {
	if _, err := r.getTyped(ctx, key); err != nil {
		if isMiss(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// Missing 判断当前 store 中是否不存在指定 key。
func (r *Repository) Missing(ctx context.Context, key string) (bool, error) {
	ok, err := r.Has(ctx, key)
	return !ok, err
}

// Forever 将 value 永久写入当前 store，不受 store 默认 TTL 影响。
func (r *Repository) Forever(ctx context.Context, key string, value any) error {
	return r.Put(ctx, key, value, 0)
}

// Many 批量读取 key；未命中的 key 会保留在结果中且值为 nil。
func (r *Repository) Many(ctx context.Context, keys []string) (map[string]any, error) {
	values, err := r.manyEncoded(ctx, keys)
	if err != nil {
		return nil, err
	}
	out := make(map[string]any, len(keys))
	for _, key := range keys {
		data, ok := values[key]
		if !ok {
			out[key] = nil
			continue
		}
		var value any
		if err := r.decode(data, &value); err != nil {
			return nil, err
		}
		out[key] = value
	}
	return out, nil
}

// GetMultiple 是 Many 的 Laravel/PSR 风格别名。
func (r *Repository) GetMultiple(ctx context.Context, keys []string) (map[string]any, error) {
	return r.Many(ctx, keys)
}

// PutMany 批量写入多个缓存值。
func (r *Repository) PutMany(ctx context.Context, values any, ttl time.Duration) error {
	if r.err != nil {
		return r.err
	}
	encoded, keys, err := r.encodeMany(values)
	if err != nil {
		r.dispatch(ctx, EventCacheWriteFailed, CacheEvent{Error: err})
		return err
	}
	r.dispatch(ctx, EventCacheWriting, CacheEvent{Keys: keys})
	if err := r.putManyEncoded(ctx, encoded, ttl); err != nil {
		r.dispatch(ctx, EventCacheWriteFailed, CacheEvent{Keys: keys, Error: err})
		return err
	}
	r.dispatch(ctx, EventCacheWritten, CacheEvent{Keys: keys})
	return nil
}

// SetMultiple 是 PutMany 的 Laravel/PSR 风格别名。
func (r *Repository) SetMultiple(ctx context.Context, values any, ttl time.Duration) error {
	return r.PutMany(ctx, values, ttl)
}

// Add 仅当 key 不存在时写入 value，并依赖底层 store 保证原子性。
func (r *Repository) Add(ctx context.Context, key string, value any, ttl time.Duration) (bool, error) {
	if r.err != nil {
		return false, r.err
	}
	if r.atomic == nil {
		return false, fmt.Errorf("cache: store %q does not support atomic add", r.name)
	}
	data, err := r.encode(value)
	if err != nil {
		return false, err
	}
	return r.atomic.Add(ctx, r.key(key), data, ttl)
}

// Increment 按 delta 原子递增整数计数器；未传 delta 时默认递增 1。
func (r *Repository) Increment(ctx context.Context, key string, delta ...int64) (int64, error) {
	return r.increment(ctx, key, counterDelta(1, delta))
}

// Decrement 按 delta 原子递减整数计数器；未传 delta 时默认递减 1。
func (r *Repository) Decrement(ctx context.Context, key string, delta ...int64) (int64, error) {
	return r.increment(ctx, key, -counterDelta(1, delta))
}

// Pull 读取当前 store 中的值并立即删除；未命中时可返回 fallback。
func (r *Repository) Pull(ctx context.Context, key string, fallback ...any) (any, error) {
	data, err := r.pullEncoded(ctx, key)
	if err == nil {
		var out any
		if err := r.decode(data, &out); err != nil {
			return nil, err
		}
		return out, nil
	}
	if !isMiss(err) {
		return nil, err
	}
	return resolveAnyDefault(ctx, fallback)
}

// Get 从当前 store 读取缓存并返回 any。
//
// 该方法适合 Laravel 风格快速调用；需要强类型解码时优先使用包级 Get[T] / GetFrom[T]。
func (r *Repository) Get(ctx context.Context, key string, fallback ...any) (any, error) {
	data, err := r.getTyped(ctx, key)
	if err == nil {
		var out any
		if err := r.decode(data, &out); err != nil {
			return nil, err
		}
		return out, nil
	}
	if !isMiss(err) {
		return nil, err
	}
	return resolveAnyDefault(ctx, fallback)
}

// Remember 读取当前 store；未命中时执行 loader，并把结果写入缓存。
func (r *Repository) Remember(ctx context.Context, key string, ttl time.Duration, loader func(context.Context) (any, error)) (any, error) {
	return rememberTyped[any](ctx, r, key, ttl, loader)
}

// RememberForever 读取当前 store；未命中时执行 loader，并永久写入缓存。
func (r *Repository) RememberForever(ctx context.Context, key string, loader func(context.Context) (any, error)) (any, error) {
	return r.Remember(ctx, key, 0, loader)
}

// Sear 是 RememberForever 的 Laravel 风格别名。
func (r *Repository) Sear(ctx context.Context, key string, loader func(context.Context) (any, error)) (any, error) {
	return r.RememberForever(ctx, key, loader)
}

// Flexible 使用当前 store 提供 fresh/stale 两段式热点缓存。
func (r *Repository) Flexible(ctx context.Context, key string, window FlexibleWindow, loader func(context.Context) (any, error)) (any, error) {
	return flexibleTyped[any](ctx, r, key, window, loader)
}

// Touch 延长当前 store 中已有 key 的 TTL，不重新读取或写回 value。
func (r *Repository) Touch(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	if r.err != nil {
		return false, r.err
	}
	if r.toucher == nil {
		return false, fmt.Errorf("cache: store %q does not support touch", r.name)
	}
	return r.toucher.Touch(ctx, r.key(key), ttl)
}

// Forget 从当前 store 删除指定 key。
func (r *Repository) Forget(ctx context.Context, key string) error {
	if r.err != nil {
		return r.err
	}
	r.dispatch(ctx, EventCacheForgetting, CacheEvent{Key: key})
	if err := r.cache.Delete(ctx, r.key(key)); err != nil {
		r.dispatch(ctx, EventCacheForgetFailed, CacheEvent{Key: key, Error: err})
		return err
	}
	r.dispatch(ctx, EventCacheForgotten, CacheEvent{Key: key})
	return nil
}

// Delete 是 Forget 的别名。
func (r *Repository) Delete(ctx context.Context, key string) error {
	return r.Forget(ctx, key)
}

// ForgetMany 批量删除多个 key。
func (r *Repository) ForgetMany(ctx context.Context, keys []string) error {
	if r.err != nil {
		return r.err
	}
	r.dispatch(ctx, EventCacheForgetting, CacheEvent{Keys: keys})
	if err := r.forgetManyEncoded(ctx, keys); err != nil {
		r.dispatch(ctx, EventCacheForgetFailed, CacheEvent{Keys: keys, Error: err})
		return err
	}
	r.dispatch(ctx, EventCacheForgotten, CacheEvent{Keys: keys})
	return nil
}

// DeleteMultiple 是 ForgetMany 的 Laravel/PSR 风格别名。
func (r *Repository) DeleteMultiple(ctx context.Context, keys []string) error {
	return r.ForgetMany(ctx, keys)
}

// Flush 清空当前 Repository 对应 store 中的缓存数据。
func (r *Repository) Flush(ctx context.Context) error {
	if r.err != nil {
		return r.err
	}
	r.dispatch(ctx, EventCacheFlushing, CacheEvent{})
	var err error
	if r.flusher != nil {
		err = r.flusher.Flush(ctx, r.prefix)
	} else {
		err = r.store.Clear(ctx)
	}
	if err != nil {
		r.dispatch(ctx, EventCacheFlushFailed, CacheEvent{Error: err})
		return err
	}
	r.dispatch(ctx, EventCacheFlushed, CacheEvent{})
	return nil
}

// Clear 是 Flush 的别名。
func (r *Repository) Clear(ctx context.Context) error {
	return r.Flush(ctx)
}

// Lock 基于当前 store 创建一个带 TTL 的锁对象。
func (r *Repository) Lock(name string, ttl time.Duration) cachecontract.Lock {
	provider := r.locker
	if r.err != nil {
		provider = errorLockProvider{err: r.err}
	}
	if provider == nil {
		provider = errorLockProvider{err: fmt.Errorf("cache: store %q does not support locks", r.name)}
	}
	return newLock(provider, r.lockKey(name), ttl, r.manager.lockRetrySleep)
}

// RestoreLock 根据已有 owner token 恢复一个可释放的锁对象。
func (r *Repository) RestoreLock(name, owner string) cachecontract.Lock {
	provider := r.locker
	if r.err != nil {
		provider = errorLockProvider{err: r.err}
	}
	if provider == nil {
		provider = errorLockProvider{err: fmt.Errorf("cache: store %q does not support locks", r.name)}
	}
	return newRestoredLock(provider, r.lockKey(name), owner, time.Second, r.manager.lockRetrySleep)
}

// LockWithOwner 创建使用指定 owner token 获取的新锁对象。
func (r *Repository) LockWithOwner(name string, ttl time.Duration, owner string) cachecontract.Lock {
	provider := r.locker
	if r.err != nil {
		provider = errorLockProvider{err: r.err}
	}
	if provider == nil {
		provider = errorLockProvider{err: fmt.Errorf("cache: store %q does not support locks", r.name)}
	}
	return newLockWithOwner(provider, r.lockKey(name), ttl, r.manager.lockRetrySleep, owner)
}

// SupportsFlushingLocks 判断当前 store 是否支持批量清理锁。
func (r *Repository) SupportsFlushingLocks() bool {
	return r != nil && r.err == nil && r.lockFlush != nil
}

// FlushLocks 清理当前 Repository 锁命名空间下的全部锁。
func (r *Repository) FlushLocks(ctx context.Context) error {
	if r.err != nil {
		return r.err
	}
	if r.lockFlush == nil {
		err := fmt.Errorf("cache: store %q does not support flushing locks", r.name)
		r.dispatch(ctx, EventCacheLockFlushFailed, CacheEvent{Error: err})
		return err
	}
	if err := r.lockFlush.FlushLocks(ctx, r.lockPrefix); err != nil {
		r.dispatch(ctx, EventCacheLockFlushFailed, CacheEvent{Error: err})
		return err
	}
	r.dispatch(ctx, EventCacheLocksFlushed, CacheEvent{})
	return nil
}

// SupportsTags 判断当前 store 是否支持 tagged cache。
func (r *Repository) SupportsTags() bool {
	return r != nil && r.err == nil && r.tags != nil
}

// Tags 创建带标签的缓存 Repository。
func (r *Repository) Tags(tags ...string) cachecontract.TaggedRepository {
	return newTaggedRepository(r, tags)
}

// Memo 创建一个请求/任务内记忆化 Repository。
func (r *Repository) Memo() cachecontract.MemoRepository {
	return newMemoRepository(r)
}

// Funnel 创建一个基于当前 store 锁能力的并发限制器。
func (r *Repository) Funnel(name string) cachecontract.FunnelLimiter {
	return newFunnel(r, name)
}

// WithoutOverlapping 使用当前 store 的缓存锁防止同名任务重叠执行。
func (r *Repository) WithoutOverlapping(ctx context.Context, key string, fn func(context.Context) error, opts ...WithoutOverlappingOption) (bool, error) {
	options := WithoutOverlappingOptions{
		WaitFor: 10 * time.Second,
		LockFor: 10 * time.Second,
	}
	if r != nil && r.manager != nil {
		options.SleepFor = r.manager.lockRetrySleep
	}
	if options.SleepFor <= 0 {
		options.SleepFor = 50 * time.Millisecond
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&options)
		}
	}
	return r.Lock("without-overlapping:"+key, options.LockFor).
		BetweenBlockedAttemptsSleepFor(options.SleepFor).
		Block(ctx, options.WaitFor, fn)
}

// Close 释放当前 Repository 持有的外部资源。
func (r *Repository) Close() error {
	if r.closer == nil {
		return nil
	}
	return r.closer.Close()
}

// getTyped 读取当前 store 中的原始 payload bytes。
func (r *Repository) getTyped(ctx context.Context, key string) ([]byte, error) {
	if r.err != nil {
		return nil, r.err
	}
	r.dispatch(ctx, EventCacheRetrieving, CacheEvent{Key: key})
	value, err := r.cache.Get(ctx, r.key(key))
	if err != nil {
		if isStoreNotFound(err) {
			r.dispatch(ctx, EventCacheMissed, CacheEvent{Key: key})
			return nil, ErrCacheMiss
		}
		r.dispatch(ctx, EventCacheMissed, CacheEvent{Key: key, Error: err})
		return nil, err
	}
	switch v := value.(type) {
	case []byte:
		r.dispatch(ctx, EventCacheHit, CacheEvent{Key: key})
		return v, nil
	case string:
		r.dispatch(ctx, EventCacheHit, CacheEvent{Key: key})
		return []byte(v), nil
	default:
		data, err := r.encode(v)
		if err != nil {
			return nil, err
		}
		r.dispatch(ctx, EventCacheHit, CacheEvent{Key: key})
		return data, nil
	}
}

// putEncoded 直接写入已编码的 payload bytes，供 flexible 复用。
func (r *Repository) putEncoded(ctx context.Context, key string, data []byte, ttl time.Duration) error {
	if r.err != nil {
		return r.err
	}
	options := expirationOptions(ttl)
	return r.cache.Set(ctx, r.key(key), data, options...)
}

// manyEncoded 批量读取原始 payload bytes；未命中的 key 不会出现在返回 map 中。
func (r *Repository) manyEncoded(ctx context.Context, keys []string) (map[string][]byte, error) {
	if r.err != nil {
		return nil, r.err
	}
	out := make(map[string][]byte, len(keys))
	if len(keys) == 0 {
		return out, nil
	}
	if r.bulk != nil {
		fullKeys := make([]string, 0, len(keys))
		reverse := make(map[string]string, len(keys))
		for _, key := range keys {
			full := r.key(key)
			fullKeys = append(fullKeys, full)
			reverse[full] = key
		}
		values, err := r.bulk.GetMany(ctx, fullKeys)
		if err != nil {
			return nil, err
		}
		for full, data := range values {
			if key, ok := reverse[full]; ok {
				out[key] = data
			}
		}
		return out, nil
	}
	for _, key := range keys {
		data, err := r.getTyped(ctx, key)
		if err == nil {
			out[key] = data
			continue
		}
		if !isMiss(err) {
			return nil, err
		}
	}
	return out, nil
}

// putManyEncoded 批量写入已经编码好的 payload bytes。
func (r *Repository) putManyEncoded(ctx context.Context, values map[string][]byte, ttl time.Duration) error {
	if r.err != nil {
		return r.err
	}
	if len(values) == 0 {
		return nil
	}
	if r.bulk != nil {
		full := make(map[string][]byte, len(values))
		for key, data := range values {
			full[r.key(key)] = data
		}
		return r.bulk.PutMany(ctx, full, ttl)
	}
	for key, data := range values {
		if err := r.putEncoded(ctx, key, data, ttl); err != nil {
			return err
		}
	}
	return nil
}

// forgetManyEncoded 批量删除 key。
func (r *Repository) forgetManyEncoded(ctx context.Context, keys []string) error {
	if r.err != nil {
		return r.err
	}
	if len(keys) == 0 {
		return nil
	}
	if r.bulk != nil {
		full := make([]string, 0, len(keys))
		for _, key := range keys {
			full = append(full, r.key(key))
		}
		return r.bulk.ForgetMany(ctx, full)
	}
	for _, key := range keys {
		if err := r.cache.Delete(ctx, r.key(key)); err != nil {
			return err
		}
	}
	return nil
}

// increment 调用底层 store 的原子计数能力。
func (r *Repository) increment(ctx context.Context, key string, delta int64) (int64, error) {
	if r.err != nil {
		return 0, r.err
	}
	if r.atomic == nil {
		return 0, fmt.Errorf("cache: store %q does not support counters", r.name)
	}
	return r.atomic.Increment(ctx, r.key(key), delta)
}

// pullEncoded 调用底层 store 的原子取后删除能力。
func (r *Repository) pullEncoded(ctx context.Context, key string) ([]byte, error) {
	if r.err != nil {
		return nil, r.err
	}
	if r.atomic == nil {
		return nil, fmt.Errorf("cache: store %q does not support pull", r.name)
	}
	return r.atomic.Pull(ctx, r.key(key))
}

// key 生成带 store 前缀的缓存 key。
func (r *Repository) key(key string) string {
	key = strings.TrimSpace(key)
	if r.prefix == "" {
		return key
	}
	return r.prefix + ":" + strings.Trim(key, ":")
}

// lockKey 生成带锁前缀的 key，避免普通缓存 key 与锁 key 混用。
func (r *Repository) lockKey(name string) string {
	name = strings.Trim(name, ":")
	if r.lockPrefix == "" {
		return name
	}
	return r.lockPrefix + ":" + name
}

// startRefresh 标记某个 key 正在后台刷新，用于避免 stale 期重复刷新。
func (r *Repository) startRefresh(key string) bool {
	r.refreshMu.Lock()
	defer r.refreshMu.Unlock()
	if _, ok := r.refreshing[key]; ok {
		return false
	}
	r.refreshing[key] = struct{}{}
	return true
}

// finishRefresh 清理某个 key 的后台刷新标记。
func (r *Repository) finishRefresh(key string) {
	r.refreshMu.Lock()
	delete(r.refreshing, key)
	r.refreshMu.Unlock()
}

// encode 把 cache value 转为底层 store 保存的 payload bytes。
//
// 参数说明：value 是 Put/Add/Remember 等 value 语义写入的业务值。数值语义 value 直写为
// 十进制 ASCII bytes；其他 value 使用当前 repository 的 Payload Encoding。
func (r *Repository) encode(value any) ([]byte, error) {
	if data, ok := encodeNumericPayload(value); ok {
		return data, nil
	}
	return r.codec.Marshal(value)
}

// decode 把缓存中的 payload bytes 解码到目标对象。
//
// 参数说明：data 是底层 store 读取出的 payload bytes；dest 是调用方提供的可写目标。
// raw numeric payload 会按目标类型解析；其他 payload 交给当前 Payload Encoding codec。
func (r *Repository) decode(data []byte, dest any) error {
	if text, ok := rawNumericPayload(data); ok {
		if err := decodeNumericPayload(text, dest); err == nil {
			return nil
		} else if numericDecodeTarget(dest) {
			return err
		}
	}
	return r.codec.Unmarshal(data, dest)
}

// encodeNumericPayload 返回数值语义 value 的十进制 bytes。
func encodeNumericPayload(value any) ([]byte, bool) {
	if value == nil {
		return nil, false
	}
	rv := reflect.ValueOf(value)
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return []byte(strconv.FormatInt(rv.Int(), 10)), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return []byte(strconv.FormatUint(rv.Uint(), 10)), true
	case reflect.Float32:
		return []byte(strconv.FormatFloat(rv.Float(), 'g', -1, 32)), true
	case reflect.Float64:
		return []byte(strconv.FormatFloat(rv.Float(), 'g', -1, 64)), true
	case reflect.String:
		text := strings.TrimSpace(rv.String())
		if _, ok := parseRawNumericText(text); ok {
			return []byte(text), true
		}
	}
	return nil, false
}

// rawNumericPayload 判断 bytes 是否是可直接解析的数值 payload。
func rawNumericPayload(data []byte) (string, bool) {
	text := strings.TrimSpace(string(data))
	if _, ok := parseRawNumericText(text); !ok {
		return "", false
	}
	return text, true
}

func parseRawNumericText(text string) (float64, bool) {
	if text == "" {
		return 0, false
	}
	hasDigit := false
	for i := 0; i < len(text); i++ {
		c := text[i]
		switch {
		case c >= '0' && c <= '9':
			hasDigit = true
		case c == '+', c == '-', c == '.', c == 'e', c == 'E':
		default:
			return 0, false
		}
	}
	if !hasDigit {
		return 0, false
	}
	n, err := strconv.ParseFloat(text, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

func decodeNumericPayload(text string, dest any) error {
	rv := reflect.ValueOf(dest)
	if rv.Kind() != reflect.Pointer || rv.IsNil() {
		return errors.New("cache: decode destination must be a non-nil pointer")
	}
	return setNumericPayload(text, rv.Elem())
}

func setNumericPayload(text string, rv reflect.Value) error {
	if !rv.CanSet() {
		return errors.New("cache: decode destination must be settable")
	}
	switch rv.Kind() {
	case reflect.Interface:
		if rv.NumMethod() != 0 {
			return fmt.Errorf("cache: cannot decode numeric payload into %s", rv.Type())
		}
		n, err := strconv.ParseFloat(text, 64)
		if err != nil {
			return err
		}
		rv.Set(reflect.ValueOf(n))
		return nil
	case reflect.String:
		rv.SetString(text)
		return nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		n, err := strconv.ParseInt(text, 10, rv.Type().Bits())
		if err != nil {
			return err
		}
		rv.SetInt(n)
		return nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		n, err := strconv.ParseUint(text, 10, rv.Type().Bits())
		if err != nil {
			return err
		}
		rv.SetUint(n)
		return nil
	case reflect.Float32, reflect.Float64:
		n, err := strconv.ParseFloat(text, rv.Type().Bits())
		if err != nil {
			return err
		}
		rv.SetFloat(n)
		return nil
	default:
		return fmt.Errorf("cache: cannot decode numeric payload into %s", rv.Type())
	}
}

func numericDecodeTarget(dest any) bool {
	rv := reflect.ValueOf(dest)
	if rv.Kind() != reflect.Pointer || rv.IsNil() {
		return false
	}
	switch rv.Elem().Kind() {
	case reflect.Interface, reflect.String, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
		reflect.Float32, reflect.Float64:
		return true
	default:
		return false
	}
}

// jsonEncode 使用标准 JSON 编码值。
//
// 设计原因：rawBytes 仍需要把底层 store 返回的非 []byte/string 值转换成可读 bytes；该场景
// 是 store 适配与测试辅助逻辑，不代表 cache value 的 Payload Encoding 主路径。
func jsonEncode(value any) ([]byte, error) {
	return json.Marshal(value)
}

// encode 保留给既有包内测试使用的默认编码 helper。
//
// 需求背景：历史测试直接调用包级 encode 构造 flexible entry 等内部样本；这些样本验证的是
// 旧 helper 的 JSON 兼容分支，不代表生产默认 codec。生产写入必须走 r.encode。
func encode(value any) ([]byte, error) {
	return jsonEncode(value)
}

// rawBytes 把底层 store 返回的值统一转换为 bytes。
func rawBytes(value any) ([]byte, error) {
	switch v := value.(type) {
	case []byte:
		return v, nil
	case string:
		return []byte(v), nil
	default:
		return jsonEncode(v)
	}
}

// encodeMany 把任意 map[string]T 编码成批量写入需要的 payload bytes。
func (r *Repository) encodeMany(values any) (map[string][]byte, []string, error) {
	if values == nil {
		return map[string][]byte{}, nil, nil
	}
	rv := reflect.ValueOf(values)
	if rv.Kind() != reflect.Map || rv.Type().Key().Kind() != reflect.String {
		return nil, nil, errors.New("cache: PutMany values must be map[string]T")
	}
	out := make(map[string][]byte, rv.Len())
	keys := make([]string, 0, rv.Len())
	iter := rv.MapRange()
	for iter.Next() {
		key := iter.Key().String()
		data, err := r.encode(iter.Value().Interface())
		if err != nil {
			return nil, nil, err
		}
		out[key] = data
		keys = append(keys, key)
	}
	return out, keys, nil
}

// encodeMany 保留给既有包内测试使用的默认批量编码 helper。
//
// 参数说明：values 必须是 map[string]T；返回编码后的 key/value bytes 和原始 key 列表。
// 设计思路：生产路径使用 repository 方法以便携带配置 codec；包级 helper 只服务旧测试中对
// 参数校验分支的覆盖。
func encodeMany(values any) (map[string][]byte, []string, error) {
	repo := &Repository{codec: managerCodec(nil)}
	return repo.encodeMany(values)
}

// expirationOptions 显式传递 TTL，保证 ttl <= 0 时覆盖 store 默认 TTL 为不过期。
func expirationOptions(ttl time.Duration) []libstore.Option {
	if ttl < 0 {
		ttl = 0
	}
	return []libstore.Option{libstore.WithExpiration(ttl)}
}

// counterDelta 解析可选计数步长。
func counterDelta(def int64, delta []int64) int64 {
	if len(delta) == 0 {
		return def
	}
	return delta[0]
}

// decodeCounter 解析当前计数器值，只接受整数格式。
func decodeCounter(value any) (int64, error) {
	data, err := rawBytes(value)
	if err != nil {
		return 0, err
	}
	count, err := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%w: %s", ErrInvalidCounter, strings.TrimSpace(string(data)))
	}
	return count, nil
}

// isStoreNotFound 判断底层 gocache store 是否返回未命中错误。
func isStoreNotFound(err error) bool {
	var nf *libstore.NotFound
	return errors.As(err, &nf)
}

// isMiss 统一判断本包与 gocache 的缓存未命中错误。
func isMiss(err error) bool {
	return errors.Is(err, ErrCacheMiss) || isStoreNotFound(err)
}
