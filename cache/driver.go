package cache

import (
	"fmt"
	"strings"
	"sync"

	libstore "github.com/eko/gocache/lib/v4/store"
	cachecontract "github.com/prismgo/framework/contracts/cache"
)

// TouchStore 是支持只更新 TTL 的 store 扩展接口。
type TouchStore = cachecontract.TouchStore

// CloseStore 是支持释放外部资源的 store 扩展接口。
type CloseStore = cachecontract.CloseStore

// AtomicStore 是支持原子写入、计数和取后删除的 store 扩展接口。
type AtomicStore = cachecontract.AtomicStore

// BulkStore 是支持批量读取、写入和删除的 store 扩展接口。
type BulkStore = cachecontract.BulkStore

// PrefixFlushStore 是支持按 Repository 前缀清理缓存的 store 扩展接口。
type PrefixFlushStore = cachecontract.PrefixFlushStore

// TagStore 是支持 Laravel tagged cache 的 store 扩展接口。
type TagStore = cachecontract.TagStore

// LockProvider 抽象不同 store 的互斥锁原子操作。
type LockProvider = cachecontract.LockProvider

// LockFlushStore 是支持批量清理锁 key 的 store 扩展接口。
type LockFlushStore = cachecontract.LockFlushStore

// StoreFactoryContext 是自定义 cache driver 的构造上下文。
//
// 它把 store 配置名、driver 名、原始配置和已经解析好的 key 前缀传给扩展工厂，
// 让业务侧可以像 Laravel Cache::extend 一样按配置构建自己的缓存后端。
type StoreFactoryContext struct {
	// Name 是当前 store 的配置名称，例如 custom。
	Name string
	// Driver 是标准化后的 driver 名称。
	Driver string
	// Config 是当前 store 的完整配置副本。
	Config StoreConfig
	// GlobalPrefix 是 Config.Prefix 解析后的全局 key 前缀。
	GlobalPrefix string
	// StorePrefix 是 StoreConfig.Prefix 解析后的当前 store 前缀。
	StorePrefix string
	// Prefix 是最终写入缓存 key 时使用的完整前缀。
	Prefix string
	// LockPrefix 是最终写入锁 key 时使用的完整前缀。
	LockPrefix string
}

// StoreDriver 描述自定义 driver 返回给 Manager 的底层 store 及其可选能力。
//
// Store 是必需项，必须实现 gocache 的 StoreInterface。其余字段可为空；
// 为空时 Manager 会自动检查 Store 本身是否实现对应扩展接口。
type StoreDriver struct {
	Store     libstore.StoreInterface
	Touch     TouchStore
	Atomic    AtomicStore
	Bulk      BulkStore
	Flush     PrefixFlushStore
	Tags      TagStore
	Lock      LockProvider
	LockFlush LockFlushStore
	Close     CloseStore
}

// NewStoreDriver 使用一个 gocache StoreInterface 创建最小自定义 driver。
//
// 如果 store 同时实现 TouchStore、AtomicStore、PrefixFlushStore、LockProvider
// 或 CloseStore，Manager 会在构建 Repository 时自动启用这些能力。
func NewStoreDriver(store libstore.StoreInterface) StoreDriver {
	return StoreDriver{Store: store}
}

// StoreFactory 是自定义 cache driver 的工厂函数。
type StoreFactory func(StoreFactoryContext) (StoreDriver, error)

var (
	storeFactoryMu sync.RWMutex
	storeFactories = map[string]StoreFactory{}
)

// Extend 注册一个自定义 cache driver 工厂。
//
// 注册后可在 cache.stores.*.driver 中使用该 driver 名称。空名称或 nil 工厂会被忽略，
// 同名注册会覆盖先前工厂，保持和 Laravel Cache::extend 一致的后注册生效语义。
func Extend(name string, factory StoreFactory) {
	registerStoreFactory(name, factory)
}

func registerStoreFactory(name string, factory StoreFactory) {
	name = normalizeDriverName(name)
	if name == "" || factory == nil {
		return
	}
	storeFactoryMu.Lock()
	storeFactories[name] = factory
	storeFactoryMu.Unlock()
}

func lookupStoreFactory(name string) (StoreFactory, bool) {
	name = normalizeDriverName(name)
	storeFactoryMu.RLock()
	factory, ok := storeFactories[name]
	storeFactoryMu.RUnlock()
	return factory, ok
}

func normalizeDriverName(name string) string {
	return strings.ToLower(strings.TrimSpace(name))
}

func (d StoreDriver) normalize() (StoreDriver, error) {
	if d.Store == nil {
		return d, fmt.Errorf("cache: custom driver store is nil")
	}
	if d.Touch == nil {
		if store, ok := d.Store.(TouchStore); ok {
			d.Touch = store
		}
	}
	if d.Atomic == nil {
		if store, ok := d.Store.(AtomicStore); ok {
			d.Atomic = store
		}
	}
	if d.Bulk == nil {
		if store, ok := d.Store.(BulkStore); ok {
			d.Bulk = store
		}
	}
	if d.Flush == nil {
		if store, ok := d.Store.(PrefixFlushStore); ok {
			d.Flush = store
		}
	}
	if d.Tags == nil {
		if store, ok := d.Store.(TagStore); ok {
			d.Tags = store
		}
	}
	if d.Lock == nil {
		if store, ok := d.Store.(LockProvider); ok {
			d.Lock = store
		}
	}
	if d.LockFlush == nil {
		if store, ok := d.Store.(LockFlushStore); ok {
			d.LockFlush = store
		}
	}
	if d.Close == nil {
		if store, ok := d.Store.(CloseStore); ok {
			d.Close = store
		}
	}
	return d, nil
}
