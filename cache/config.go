package cache

import "time"

// Config 描述整个缓存系统的顶层配置。
//
// Default 指向默认 store，Prefix 作为所有缓存 key 的全局前缀，
// Stores 保存各 store 的驱动配置，Lock 和 Flexible 分别控制锁与热点刷新策略。
type Config struct {
	// Default 是默认 store 名称，空值会回退为 memory。
	Default string
	// Encoding 是 cache value 的 Payload Encoding 名称，空值继承 encoding.default。
	Encoding string
	// Prefix 是所有缓存 key 的全局前缀。
	Prefix string
	// Stores 保存全部可用 store 的配置。
	Stores map[string]StoreConfig
	// Lock 控制缓存锁的 key 前缀和等待策略。
	Lock LockConfig
	// Flexible 控制 stale 期后台刷新任务的运行边界。
	Flexible FlexibleConfig
}

// StoreConfig 描述单个缓存 store 的构造参数。
type StoreConfig struct {
	// Driver 支持 memory / redis / file 以及 Extend 注册的自定义 driver；空值会按 memory 处理。
	Driver string
	// Prefix 是当前 store 的 key 前缀，会拼接在 Config.Prefix 后面。
	Prefix string
	// DefaultTTL 是 store 级默认 TTL；0 表示默认不过期。
	DefaultTTL time.Duration
	// CleanupInterval 仅 memory store 使用，用于定期清理过期 key。
	CleanupInterval time.Duration
	// Redis 保存 redis driver 的连接参数。
	Redis RedisConfig
	// File 保存 file driver 的本地目录参数。
	File FileConfig
	// Stores 保存 failover driver 按优先级尝试的子 store 名称。
	Stores []string
	// Events 控制当前 store 是否派发 cache.* 事件；nil 表示默认开启。
	Events *bool
	// Options 保存自定义 driver 的原始扩展参数，内置 driver 会安全忽略。
	Options map[string]any
}

// RedisConfig 描述 Redis store 使用的共享连接。
type RedisConfig struct {
	// Connection selects a named prismgo/redis connection.
	Connection string
}

// FileConfig 描述 file 缓存驱动的本地存储目录。
type FileConfig struct {
	// Path 是缓存数据文件根目录；为空时使用 storage/framework/cache/data。
	Path string
	// LockPath 是缓存锁文件根目录；为空时使用缓存数据目录同级的 locks。
	LockPath string
}

// LockConfig 控制缓存锁的 key 前缀和等待重试节奏。
type LockConfig struct {
	// Prefix 是锁 key 在缓存 key 空间中的二级前缀。
	Prefix string
	// RetrySleep 是 Block 未拿到锁时两次重试之间的等待时间。
	RetrySleep time.Duration
}

// FlexibleConfig 控制 stale 期后台刷新任务的运行边界。
type FlexibleConfig struct {
	// RefreshTimeout 是异步刷新 loader 的最大执行时间。
	RefreshTimeout time.Duration
}
