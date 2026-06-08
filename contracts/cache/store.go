// Package cache 定义缓存系统的公共契约。
//
// 本包声明缓存驱动、Repository、锁和工厂的完整接口。
// 具体的内存、Redis、文件驱动实现以及 Manager 编排逻辑由 prismgo/cache 实现包提供。
package cache

import (
	"context"
	"time"
)

// Store 是缓存后端驱动的基础契约。
//
// 用途：自定义缓存后端（如 Redis、Memcached、数据库）必须实现此接口。
// 实现包通过 Store 接口与 Repository 解耦。
//
// 使用方式：
//
//	type MyDBStore struct { db *sql.DB }
//	func (s *MyDBStore) Get(ctx context.Context, key string) (any, error) { ... }
//	// ... 实现其余方法
//
//	repo := cache.Default()
//	store := repo.GetStore()
type Store interface {
	// Get 读取单个 key 的缓存值。
	//
	// 参数 ctx 是调用链上下文。
	// 参数 key 是缓存键名（不含前缀，前缀由 Repository 统一添加）。
	// 返回未命中时返回 ErrCacheMiss 错误。
	Get(ctx context.Context, key string) (any, error)

	// Many 批量读取缓存值。
	//
	// 参数 keys 是缓存键名列表。返回 map 会保留请求中的 key，未命中值为 nil。
	Many(ctx context.Context, keys []string) (map[string]any, error)

	// Put 写入单个缓存值。
	//
	// 参数 ttl 是过期时间；ttl <= 0 时表示不过期。
	Put(ctx context.Context, key string, value any, ttl time.Duration) error

	// PutMany 批量写入缓存值。
	PutMany(ctx context.Context, values map[string]any, ttl time.Duration) error

	// Add 仅在 key 不存在时原子写入 value。
	//
	// 返回 true 表示写入成功（key 原本不存在）。
	Add(ctx context.Context, key string, value any, ttl time.Duration) (bool, error)

	// Increment 按 delta 原子递增整数计数器。
	//
	// 参数 delta 是增量值，为负数时表示递减。
	// 返回递增后的值。
	Increment(ctx context.Context, key string, delta int64) (int64, error)

	// Decrement 按 delta 原子递减整数计数器。
	//
	// 参数 delta 是减量值。
	// 返回递减后的值。
	Decrement(ctx context.Context, key string, delta int64) (int64, error)

	// Forever 将 value 永久写入（无过期时间）。
	Forever(ctx context.Context, key string, value any) error

	// Forget 删除单个缓存 key。
	Forget(ctx context.Context, key string) error

	// ForgetMany 批量删除缓存 key。
	ForgetMany(ctx context.Context, keys []string) error

	// Flush 清空当前 store 中的所有缓存数据。
	Flush(ctx context.Context) error

	// Prefix 返回当前 store 的 key 前缀。
	Prefix() string
}

// TouchStore 是支持仅更新 TTL 的驱动扩展接口。
//
// 自定义驱动实现该接口后，Repository.Touch 直接复用此能力而不需要重新读取
// 和写入缓存值。
//
// 使用方式：实现包会在构造 Repository 时自动识别该扩展能力。
type TouchStore interface {
	// Touch 延长已存在 key 的过期时间。
	//
	// 参数 ttl 是新的过期时间。
	// 返回 true 表示 key 存在且 TTL 已更新；false 表示 key 不存在。
	Touch(ctx context.Context, key string, ttl time.Duration) (bool, error)
}

// AtomicStore 是支持原子写入、计数和取后删除的驱动扩展接口。
//
// 实现后 Repository.Add、Increment、Decrement、Pull 直接委托给底层 store，
// 保证复合操作的原子性。未实现的驱动调用这些方法时返回不支持错误。
type AtomicStore interface {
	Add(ctx context.Context, key string, value []byte, ttl time.Duration) (bool, error)
	Increment(ctx context.Context, key string, delta int64) (int64, error)
	Pull(ctx context.Context, key string) ([]byte, error)
}

// BulkStore 是支持批量读取、写入和删除的驱动扩展接口。
//
// 实现后 Repository.Many、PutMany、ForgetMany 直接复用底层批量能力。
// 未实现时 Repository 逐 key 回退。
type BulkStore interface {
	GetMany(ctx context.Context, keys []string) (map[string][]byte, error)
	PutMany(ctx context.Context, values map[string][]byte, ttl time.Duration) error
	ForgetMany(ctx context.Context, keys []string) error
}

// PrefixFlushStore 是支持按前缀清理缓存的驱动扩展接口。
//
// 实现后 Repository.Flush 传入完整的 key 前缀；未实现时回退到 Clear()。
type PrefixFlushStore interface {
	Flush(ctx context.Context, prefix string) error
}

// TagStore 是支持标签化缓存的驱动扩展接口。
//
// 实现后 Repository.Tags() 可用；未实现时调用 Tags() 返回错误的 TaggedRepository。
type TagStore interface {
	GetTagged(ctx context.Context, prefix string, tags []string, key string) ([]byte, error)
	PutTagged(ctx context.Context, prefix string, tags []string, key string, value []byte, ttl time.Duration) error
	ForgetTagged(ctx context.Context, prefix string, tags []string, key string) error
	FlushTags(ctx context.Context, prefix string, tags []string) error
}

// LockProvider 是支持分布式锁的驱动扩展接口。
//
// 实现后 Repository.Lock() 可用；未实现时返回错误的 Lock 对象。
//
// 使用方式：实现包会在构造 Repository 时自动识别该扩展能力。
type LockProvider interface {
	// Acquire 尝试以 key 和 token 获取锁。
	//
	// 参数 token 是锁拥有者的唯一标识，Release 时校验。
	// 返回 true 表示获取成功。
	Acquire(ctx context.Context, key, token string, ttl time.Duration) (bool, error)

	// Release 按 token 校验并释放锁。
	//
	// 返回 true 表示成功释放；false 表示锁不属于当前 token。
	Release(ctx context.Context, key, token string) (bool, error)

	// ForceRelease 不校验 token 直接释放锁。
	ForceRelease(ctx context.Context, key string) error
}

// LockFlushStore 是支持批量清理锁的驱动扩展接口。
type LockFlushStore interface {
	FlushLocks(ctx context.Context, lockPrefix string) error
}

// CloseStore 是支持释放外部资源的驱动扩展接口。
//
// 连接型驱动实现该接口以关闭网络连接、文件句柄或后台 goroutine。
type CloseStore interface {
	Close() error
}
