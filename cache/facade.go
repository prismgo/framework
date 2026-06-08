package cache

import (
	"context"
	"time"

	"github.com/prismgo/framework/container"
	cachecontract "github.com/prismgo/framework/contracts/cache"
	containercontract "github.com/prismgo/framework/contracts/container"
	"github.com/prismgo/framework/facade"
)

const serviceKey = "cache.manager"

// manager 从当前 Application 容器解析具体缓存 Manager。
//
// 设计原因：公共 Resolve 对外返回 contracts/cache.Factory；实现包内部仍需要具体
// Manager 来保留泛型 facade 的强类型解码能力。
func manager() *Manager {
	return facade.Resolve[*Manager](serviceKey)
}

// Resolve 从当前 Application 容器解析缓存 Factory 契约。
func Resolve() cachecontract.Factory {
	return manager()
}

// DefaultName 返回当前全局 Manager 的默认 store 名称。
func DefaultName() string {
	return Resolve().DefaultName()
}

// Close 释放当前全局 Manager 已构建 store 持有的外部资源。
func Close() error {
	return Resolve().Close()
}

// Default 返回默认 store 对应的 Repository。
//
// 说明：cache.Default 不是“当前 facade 实例”别名，而是 Laravel Cache manager 下的
// default store 选择器，因此保留为缓存包的业务 API。
func Default() cachecontract.Repository {
	return Resolve().Default()
}

// Store 按名称返回 Repository；name 为空时返回默认 store。
func Store(name string) cachecontract.Repository {
	return Resolve().Store(name)
}

// storeRepository 返回指定 store 的具体 Repository，供泛型 facade 复用。
func storeRepository(name string) *Repository {
	return manager().storeRepository(name)
}

// Name 返回默认 store 对应的 Repository 名称。
func Name() string {
	return Default().Name()
}

// Put 将 value 写入默认 store，并使用 ttl 控制过期时间。
func Put(ctx context.Context, key string, value any, ttl time.Duration) error {
	return Default().Put(ctx, key, value, ttl)
}

// PutFrom 将 value 写入指定 store。
func PutFrom(ctx context.Context, storeName, key string, value any, ttl time.Duration) error {
	return Store(storeName).Put(ctx, key, value, ttl)
}

// Set 是 Put 的别名。
func Set(ctx context.Context, key string, value any, ttl time.Duration) error {
	return Put(ctx, key, value, ttl)
}

// SetFrom 是 PutFrom 的别名。
func SetFrom(ctx context.Context, storeName, key string, value any, ttl time.Duration) error {
	return PutFrom(ctx, storeName, key, value, ttl)
}

// Has 判断默认 store 中是否存在指定 key。
func Has(ctx context.Context, key string) (bool, error) {
	return Default().Has(ctx, key)
}

// HasFrom 判断指定 store 中是否存在指定 key。
func HasFrom(ctx context.Context, storeName, key string) (bool, error) {
	return Store(storeName).Has(ctx, key)
}

// Missing 判断默认 store 中是否不存在指定 key。
func Missing(ctx context.Context, key string) (bool, error) {
	return Default().Missing(ctx, key)
}

// MissingFrom 判断指定 store 中是否不存在指定 key。
func MissingFrom(ctx context.Context, storeName, key string) (bool, error) {
	return Store(storeName).Missing(ctx, key)
}

// Forever 将 value 永久写入默认 store。
func Forever(ctx context.Context, key string, value any) error {
	return Default().Forever(ctx, key, value)
}

// ForeverFrom 将 value 永久写入指定 store。
func ForeverFrom(ctx context.Context, storeName, key string, value any) error {
	return Store(storeName).Forever(ctx, key, value)
}

// Add 仅当 key 不存在时写入 value，并依赖底层 store 保证原子性。
func Add(ctx context.Context, key string, value any, ttl time.Duration) (bool, error) {
	return Default().Add(ctx, key, value, ttl)
}

// AddFrom 仅当 key 不存在时写入指定 store。
func AddFrom(ctx context.Context, storeName, key string, value any, ttl time.Duration) (bool, error) {
	return Store(storeName).Add(ctx, key, value, ttl)
}

// Get 从默认 store 读取并解码为 T，未命中时可返回 fallback。
func Get[T any](ctx context.Context, key string, fallback ...Fallback[T]) (T, error) {
	return GetFrom[T](ctx, "", key, fallback...)
}

// GetFrom 从指定 store 读取并解码为 T。
func GetFrom[T any](ctx context.Context, storeName, key string, fallback ...Fallback[T]) (T, error) {
	repo := storeRepository(storeName)
	value, err := repo.getTyped(ctx, key)
	if err == nil {
		var out T
		if err := repo.decode(value, &out); err != nil {
			return out, err
		}
		return out, nil
	}
	if !isMiss(err) {
		var zero T
		return zero, err
	}
	return resolveDefault(ctx, fallback)
}

// String 从默认 store 读取字符串值。
func String(ctx context.Context, key string, fallback ...Fallback[string]) (string, error) {
	return Get[string](ctx, key, fallback...)
}

// StringFrom 从指定 store 读取字符串值。
func StringFrom(ctx context.Context, storeName, key string, fallback ...Fallback[string]) (string, error) {
	return GetFrom[string](ctx, storeName, key, fallback...)
}

// Integer 从默认 store 读取整数值。
func Integer(ctx context.Context, key string, fallback ...Fallback[int]) (int, error) {
	return Get[int](ctx, key, fallback...)
}

// IntegerFrom 从指定 store 读取整数值。
func IntegerFrom(ctx context.Context, storeName, key string, fallback ...Fallback[int]) (int, error) {
	return GetFrom[int](ctx, storeName, key, fallback...)
}

// Float 从默认 store 读取 float64 值。
func Float(ctx context.Context, key string, fallback ...Fallback[float64]) (float64, error) {
	return Get[float64](ctx, key, fallback...)
}

// FloatFrom 从指定 store 读取 float64 值。
func FloatFrom(ctx context.Context, storeName, key string, fallback ...Fallback[float64]) (float64, error) {
	return GetFrom[float64](ctx, storeName, key, fallback...)
}

// Boolean 从默认 store 读取 bool 值。
func Boolean(ctx context.Context, key string, fallback ...Fallback[bool]) (bool, error) {
	return Get[bool](ctx, key, fallback...)
}

// BooleanFrom 从指定 store 读取 bool 值。
func BooleanFrom(ctx context.Context, storeName, key string, fallback ...Fallback[bool]) (bool, error) {
	return GetFrom[bool](ctx, storeName, key, fallback...)
}

// Many 从默认 store 批量读取并解码为 T。
func Many[T any](ctx context.Context, keys []string, fallback ...Fallback[T]) (map[string]T, error) {
	return ManyFrom[T](ctx, "", keys, fallback...)
}

// ManyFrom 从指定 store 批量读取并解码为 T。
func ManyFrom[T any](ctx context.Context, storeName string, keys []string, fallback ...Fallback[T]) (map[string]T, error) {
	repo := storeRepository(storeName)
	values, err := repo.manyEncoded(ctx, keys)
	if err != nil {
		return nil, err
	}
	out := make(map[string]T, len(keys))
	for _, key := range keys {
		data, ok := values[key]
		if !ok {
			value, err := resolveDefault(ctx, fallback)
			if err != nil && !isMiss(err) {
				return nil, err
			}
			out[key] = value
			continue
		}
		var value T
		if err := repo.decode(data, &value); err != nil {
			return nil, err
		}
		out[key] = value
	}
	return out, nil
}

// GetMultiple 是 Many 的别名。
func GetMultiple[T any](ctx context.Context, keys []string, fallback ...Fallback[T]) (map[string]T, error) {
	return Many[T](ctx, keys, fallback...)
}

// GetMultipleFrom 是 ManyFrom 的别名。
func GetMultipleFrom[T any](ctx context.Context, storeName string, keys []string, fallback ...Fallback[T]) (map[string]T, error) {
	return ManyFrom[T](ctx, storeName, keys, fallback...)
}

// PutMany 批量写入默认 store。
func PutMany(ctx context.Context, values any, ttl time.Duration) error {
	return Default().PutMany(ctx, values, ttl)
}

// PutManyFrom 批量写入指定 store。
func PutManyFrom(ctx context.Context, storeName string, values any, ttl time.Duration) error {
	return Store(storeName).PutMany(ctx, values, ttl)
}

// SetMultiple 是 PutMany 的别名。
func SetMultiple(ctx context.Context, values any, ttl time.Duration) error {
	return PutMany(ctx, values, ttl)
}

// SetMultipleFrom 是 PutManyFrom 的别名。
func SetMultipleFrom(ctx context.Context, storeName string, values any, ttl time.Duration) error {
	return PutManyFrom(ctx, storeName, values, ttl)
}

// Remember 读取默认 store；未命中时执行 loader，并把结果写入缓存。
func Remember[T any](ctx context.Context, key string, ttl time.Duration, loader Loader[T]) (T, error) {
	return RememberFrom(ctx, "", key, ttl, loader)
}

// RememberFrom 读取指定 store；未命中时执行 loader，并把结果写入缓存。
func RememberFrom[T any](ctx context.Context, storeName, key string, ttl time.Duration, loader Loader[T]) (T, error) {
	return rememberTyped(ctx, storeRepository(storeName), key, ttl, loader)
}

// RememberForever 读取默认 store；未命中时执行 loader，并永久写入缓存。
func RememberForever[T any](ctx context.Context, key string, loader Loader[T]) (T, error) {
	return Remember(ctx, key, 0, loader)
}

// RememberForeverFrom 读取指定 store；未命中时执行 loader，并永久写入缓存。
func RememberForeverFrom[T any](ctx context.Context, storeName, key string, loader Loader[T]) (T, error) {
	return RememberFrom(ctx, storeName, key, 0, loader)
}

// Sear 是 RememberForever 的 Laravel 风格别名。
func Sear[T any](ctx context.Context, key string, loader Loader[T]) (T, error) {
	return RememberForever(ctx, key, loader)
}

// SearFrom 是 RememberForeverFrom 的 Laravel 风格别名。
func SearFrom[T any](ctx context.Context, storeName, key string, loader Loader[T]) (T, error) {
	return RememberForeverFrom(ctx, storeName, key, loader)
}

// Flexible 使用默认 store 提供 fresh/stale 两段式热点缓存。
func Flexible[T any](ctx context.Context, key string, window FlexibleWindow, loader Loader[T]) (T, error) {
	return FlexibleFrom(ctx, "", key, window, loader)
}

// FlexibleFrom 使用指定 store 提供 fresh/stale 两段式热点缓存。
func FlexibleFrom[T any](ctx context.Context, storeName, key string, window FlexibleWindow, loader Loader[T]) (T, error) {
	return flexibleTyped(ctx, storeRepository(storeName), key, window, loader)
}

// Touch 延长默认 store 中已存在 key 的 TTL，不重新读取或写回 value。
func Touch(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	return Default().Touch(ctx, key, ttl)
}

// TouchFrom 延长指定 store 中已存在 key 的 TTL。
func TouchFrom(ctx context.Context, storeName, key string, ttl time.Duration) (bool, error) {
	return Store(storeName).Touch(ctx, key, ttl)
}

// Increment 按 delta 原子递增默认 store 中的整数计数器。
func Increment(ctx context.Context, key string, delta ...int64) (int64, error) {
	return Default().Increment(ctx, key, delta...)
}

// IncrementFrom 按 delta 原子递增指定 store 中的整数计数器。
func IncrementFrom(ctx context.Context, storeName, key string, delta ...int64) (int64, error) {
	return Store(storeName).Increment(ctx, key, delta...)
}

// Decrement 按 delta 原子递减默认 store 中的整数计数器。
func Decrement(ctx context.Context, key string, delta ...int64) (int64, error) {
	return Default().Decrement(ctx, key, delta...)
}

// DecrementFrom 按 delta 原子递减指定 store 中的整数计数器。
func DecrementFrom(ctx context.Context, storeName, key string, delta ...int64) (int64, error) {
	return Store(storeName).Decrement(ctx, key, delta...)
}

// Pull 读取默认 store 中的值并立即删除。
func Pull[T any](ctx context.Context, key string, fallback ...Fallback[T]) (T, error) {
	return PullFrom[T](ctx, "", key, fallback...)
}

// PullFrom 从指定 store 中读取值并立即删除。
func PullFrom[T any](ctx context.Context, storeName, key string, fallback ...Fallback[T]) (T, error) {
	repo := storeRepository(storeName)
	data, err := repo.pullEncoded(ctx, key)
	if err == nil {
		var out T
		if err := repo.decode(data, &out); err != nil {
			return out, err
		}
		return out, nil
	}
	if !isMiss(err) {
		var zero T
		return zero, err
	}
	return resolveDefault(ctx, fallback)
}

// Forget 从默认 store 删除指定 key。
func Forget(ctx context.Context, key string) error {
	return Default().Forget(ctx, key)
}

// ForgetFrom 从指定 store 删除指定 key。
func ForgetFrom(ctx context.Context, storeName, key string) error {
	return Store(storeName).Forget(ctx, key)
}

// Delete 是 Forget 的别名。
func Delete(ctx context.Context, key string) error {
	return Forget(ctx, key)
}

// ForgetMany 批量删除默认 store 中的 key。
func ForgetMany(ctx context.Context, keys []string) error {
	return Default().ForgetMany(ctx, keys)
}

// ForgetManyFrom 批量删除指定 store 中的 key。
func ForgetManyFrom(ctx context.Context, storeName string, keys []string) error {
	return Store(storeName).ForgetMany(ctx, keys)
}

// DeleteMultiple 是 ForgetMany 的别名。
func DeleteMultiple(ctx context.Context, keys []string) error {
	return ForgetMany(ctx, keys)
}

// DeleteMultipleFrom 是 ForgetManyFrom 的别名。
func DeleteMultipleFrom(ctx context.Context, storeName string, keys []string) error {
	return ForgetManyFrom(ctx, storeName, keys)
}

// Flush 清空默认 store 中的缓存数据。
func Flush(ctx context.Context) error {
	return Default().Flush(ctx)
}

// FlushFrom 清空指定 store 中的缓存数据。
func FlushFrom(ctx context.Context, storeName string) error {
	return Store(storeName).Flush(ctx)
}

// Clear 是 Flush 的别名。
func Clear(ctx context.Context) error {
	return Flush(ctx)
}

// ClearFrom 是 FlushFrom 的别名。
func ClearFrom(ctx context.Context, storeName string) error {
	return FlushFrom(ctx, storeName)
}

// Lock 基于默认 store 创建一个带 TTL 的锁对象。
func Lock(name string, ttl time.Duration) cachecontract.Lock {
	return Default().Lock(name, ttl)
}

// LockFrom 基于指定 store 创建一个带 TTL 的锁对象。
func LockFrom(storeName, name string, ttl time.Duration) cachecontract.Lock {
	return Store(storeName).Lock(name, ttl)
}

// LockWithOwner 基于默认 store 创建一个使用指定 owner 的锁对象。
func LockWithOwner(name string, ttl time.Duration, owner string) cachecontract.Lock {
	return Default().LockWithOwner(name, ttl, owner)
}

// LockWithOwnerFrom 基于指定 store 创建一个使用指定 owner 的锁对象。
func LockWithOwnerFrom(storeName, name string, ttl time.Duration, owner string) cachecontract.Lock {
	return Store(storeName).LockWithOwner(name, ttl, owner)
}

// RestoreLock 根据已有 owner token 恢复一个可释放的锁对象。
func RestoreLock(name, owner string) cachecontract.Lock {
	return Default().RestoreLock(name, owner)
}

// RestoreLockFrom 根据已有 owner token 恢复指定 store 的锁对象。
func RestoreLockFrom(storeName, name, owner string) cachecontract.Lock {
	return Store(storeName).RestoreLock(name, owner)
}

// FlushLocks 清理默认 store 的锁命名空间。
func FlushLocks(ctx context.Context) error {
	return Default().FlushLocks(ctx)
}

// FlushLocksFrom 清理指定 store 的锁命名空间。
func FlushLocksFrom(ctx context.Context, storeName string) error {
	return Store(storeName).FlushLocks(ctx)
}

// Funnel 基于默认 store 创建一个并发限制器。
func Funnel(name string) cachecontract.FunnelLimiter {
	return Default().Funnel(name)
}

// FunnelFrom 基于指定 store 创建一个并发限制器。
func FunnelFrom(storeName, name string) cachecontract.FunnelLimiter {
	return Store(storeName).Funnel(name)
}

// Tags 基于默认 store 创建 tagged cache 操作入口。
func Tags(tags ...string) cachecontract.TaggedRepository {
	return Default().Tags(tags...)
}

// TagsFrom 基于指定 store 创建 tagged cache 操作入口。
func TagsFrom(storeName string, tags ...string) cachecontract.TaggedRepository {
	return Store(storeName).Tags(tags...)
}

// Memo 基于默认 store 创建请求/任务内记忆化缓存入口。
func Memo() cachecontract.MemoRepository {
	return Default().Memo()
}

// MemoFrom 基于指定 store 创建请求/任务内记忆化缓存入口。
func MemoFrom(storeName string) cachecontract.MemoRepository {
	return Store(storeName).Memo()
}

// ManagerCloseOption 返回缓存 Manager 的关闭选项，供 bootstrap 注册时使用。
func ManagerCloseOption() containercontract.BindingOption {
	return container.WithCloser(func(m *Manager) error {
		return m.Close()
	})
}
