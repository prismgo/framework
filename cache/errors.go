package cache

import cachecontract "github.com/prismgo/framework/contracts/cache"

var (
	// ErrCacheMiss 表示指定 key 在当前 store 中不存在或已过期。
	ErrCacheMiss = cachecontract.ErrCacheMiss
	// ErrStoreNotFound 表示调用方指定了未注册的 store 名称。
	ErrStoreNotFound = cachecontract.ErrStoreNotFound
	// ErrLockTimeout 表示 Block 等待锁超过了调用方给定的等待时间。
	ErrLockTimeout = cachecontract.ErrLockTimeout
	// ErrLockNotHeld 表示当前 DistributedLock 实例没有持有可释放的锁。
	ErrLockNotHeld = cachecontract.ErrLockNotHeld
	// ErrInvalidCounter 表示计数器已有值不是可递增的整数。
	ErrInvalidCounter = cachecontract.ErrInvalidCounter
	// ErrTagsUnsupported 表示当前 store 不支持 tagged cache。
	ErrTagsUnsupported = cachecontract.ErrTagsUnsupported
)
