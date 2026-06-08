package cache

import (
	"context"
	"time"
)

// Repository 是缓存操作的完整契约。
//
// 用途：这是业务代码面向的主要缓存接口。每个 Repository 对应一个缓存 store
// （如 memory、redis），通过 Manager.Store(name) 获取。
//
// 使用方式：
//
//	repo := cache.Default()
//	err := repo.Put(ctx, "user:1", user, 10*time.Minute)
//	value, err := repo.Get(ctx, "user:1")
type Repository interface {
	// Name 返回当前 Repository 对应的 store 配置名称。
	Name() string

	// GetStore 返回当前 Repository 包装的底层 Store 契约。
	//
	// 需求背景：Laravel Repository 暴露 getStore()，方便上层在必要时识别
	// 当前仓库背后的缓存后端能力；PrismGo 仅暴露本包 Store 契约，不泄露实现包细节。
	GetStore() Store

	// ---- 基本读写 ----

	// Put 将 value JSON 编码后写入缓存。
	//
	// 参数 ttl 是过期时间；ttl <= 0 时不过期。
	Put(ctx context.Context, key string, value any, ttl time.Duration) error

	// Set 是 Put 的别名。
	Set(ctx context.Context, key string, value any, ttl time.Duration) error

	// Get 读取缓存值并返回 any。
	//
	// 参数 fallback 是未命中时的默认值，可选。
	// 返回 ErrCacheMiss 表示 key 不存在且未提供 fallback。
	Get(ctx context.Context, key string, fallback ...any) (any, error)

	// Has 判断 key 是否存在且值不为 nil。
	Has(ctx context.Context, key string) (bool, error)

	// Missing 判断 key 是否不存在。
	Missing(ctx context.Context, key string) (bool, error)

	// Forever 永久写入缓存。
	Forever(ctx context.Context, key string, value any) error

	// ---- 批量 ----

	// Many 批量读取缓存值。
	//
	// 未命中的 key 在返回 map 中值为 nil。
	Many(ctx context.Context, keys []string) (map[string]any, error)

	// GetMultiple 是 Many 的别名。
	GetMultiple(ctx context.Context, keys []string) (map[string]any, error)

	// PutMany 批量写入缓存值。
	//
	// 参数 values 必须是 map[string]T 类型。
	PutMany(ctx context.Context, values any, ttl time.Duration) error

	// SetMultiple 是 PutMany 的别名。
	SetMultiple(ctx context.Context, values any, ttl time.Duration) error

	// ---- 原子操作 ----

	// Add 仅在 key 不存在时写入 value。
	//
	// 返回 true 表示写入成功（key 原本不存在）。
	Add(ctx context.Context, key string, value any, ttl time.Duration) (bool, error)

	// Increment 原子递增整数计数器。
	//
	// 参数 delta 是可选步长，未传时默认 +1。
	// 返回递增后的值。
	Increment(ctx context.Context, key string, delta ...int64) (int64, error)

	// Decrement 原子递减整数计数器。
	//
	// 参数 delta 是可选步长，未传时默认 -1。
	// 返回递减后的值。
	Decrement(ctx context.Context, key string, delta ...int64) (int64, error)

	// Pull 读取值后立即删除。
	//
	// 参数 fallback 是未命中时的默认值，可选。
	Pull(ctx context.Context, key string, fallback ...any) (any, error)

	// ---- 读后写回 ----

	// Remember 读取缓存；未命中时执行 loader 并将结果写入缓存。
	//
	// 参数 loader 在未命中时调用，其返回值和 error 决定写入内容和是否回退。
	Remember(ctx context.Context, key string, ttl time.Duration, loader func(context.Context) (any, error)) (any, error)

	// RememberForever 读取缓存；未命中时执行 loader 并永久写入。
	RememberForever(ctx context.Context, key string, loader func(context.Context) (any, error)) (any, error)

	// Sear 是 RememberForever 的语义化别名。
	Sear(ctx context.Context, key string, loader func(context.Context) (any, error)) (any, error)

	// ---- 热点保护 ----

	// Flexible 使用 fresh/stale 两段式策略读取缓存。
	//
	// 当缓存处于 stale 窗口时，返回过期值并异步触发刷新，避免缓存击穿。
	Flexible(ctx context.Context, key string, window FlexibleWindow, loader func(context.Context) (any, error)) (any, error)

	// ---- TTL 管理 ----

	// Touch 延长已有 key 的 TTL，不重新读取或写回 value。
	//
	// 返回 true 表示 key 存在且 TTL 已更新。
	Touch(ctx context.Context, key string, ttl time.Duration) (bool, error)

	// ---- 删除 ----

	// Forget 删除单个 key。
	Forget(ctx context.Context, key string) error

	// Delete 是 Forget 的别名。
	Delete(ctx context.Context, key string) error

	// ForgetMany 批量删除。
	ForgetMany(ctx context.Context, keys []string) error

	// DeleteMultiple 是 ForgetMany 的别名。
	DeleteMultiple(ctx context.Context, keys []string) error

	// Flush 清空当前 store 的所有缓存数据。
	Flush(ctx context.Context) error

	// Clear 是 Flush 的别名。
	Clear(ctx context.Context) error

	// ---- 分布式锁 ----

	// Lock 基于当前 store 创建一个带 TTL 的锁对象。
	//
	// 使用方式：
	//
	//	lock := repo.Lock("export:tenant:1", 30*time.Second)
	//	ok, err := lock.Get(ctx, func(ctx context.Context) error {
	//	    return doExport(ctx)
	//	})
	Lock(name string, ttl time.Duration) Lock

	// RestoreLock 根据已有 owner token 恢复一个可释放的锁对象。
	RestoreLock(name, owner string) Lock

	// LockWithOwner 使用指定的 owner token 创建锁对象。
	LockWithOwner(name string, ttl time.Duration, owner string) Lock

	// FlushLocks 清理当前 store 的锁命名空间。
	FlushLocks(ctx context.Context) error

	// SupportsFlushingLocks 判断当前 store 是否支持批量清理锁。
	SupportsFlushingLocks() bool

	// ---- 标签缓存 ----

	// Tags 创建带标签的缓存操作入口。
	//
	// 标签缓存适合按一组业务标签批量失效，例如清理某个租户的所有缓存项。
	//
	// 使用方式：
	//
	//	repo.Tags("tenant:1", "assets").Put(ctx, "logo", data, 0)
	//	repo.Tags("tenant:1").Flush(ctx) // 清理该租户所有资产缓存
	Tags(tags ...string) TaggedRepository

	// SupportsTags 判断当前 store 是否支持 tagged cache。
	SupportsTags() bool

	// ---- 记忆化缓存 ----

	// Memo 创建请求/任务内的记忆化缓存入口。
	//
	// 同一个 MemoRepository 实例在内存中缓存读取结果，避免同一请求内重复查询。
	Memo() MemoRepository

	// ---- 并发限制 ----

	// Funnel 基于当前 store 的锁能力创建并发限制器。
	//
	// 使用方式：
	//
	//	repo.Funnel("api:rate:limit").Limit(5).ExpireAfter(time.Second).Then(func() {
	//	    callExternalAPI()
	//	})
	Funnel(name string) FunnelLimiter

	// ---- 防重叠执行 ----

	// WithoutOverlapping 使用缓存锁防止同名任务重叠执行。
	//
	// 使用方式：
	//
	//	ok, err := repo.WithoutOverlapping(ctx, "reconcile:tenant:1", func(ctx context.Context) error {
	//	    return reconcile(ctx, tenantID)
	//	}, cache.WithOverlapLock(30*time.Second), cache.WithOverlapWait(5*time.Second))
	WithoutOverlapping(ctx context.Context, key string, fn func(context.Context) error, opts ...WithoutOverlappingOption) (bool, error)

	// Close 释放当前 Repository 持有的外部资源。
	Close() error
}

// FlexibleWindow 定义 Flexible 缓存的 fresh/stale 时间窗口。
type FlexibleWindow struct {
	// Fresh 是新数据在缓存中的保鲜时间。
	Fresh time.Duration
	// Stale 是 fresh 过期后仍可使用旧值的时间。
	Stale time.Duration
}

// WithoutOverlappingOptions 配置防重叠执行的行为。
type WithoutOverlappingOptions struct {
	LockFor  time.Duration
	WaitFor  time.Duration
	SleepFor time.Duration
}

// WithoutOverlappingOption 是防重叠执行的函数式选项。
type WithoutOverlappingOption func(*WithoutOverlappingOptions)
