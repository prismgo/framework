package cache

import "errors"

var (
	// ErrCacheMiss 表示指定 key 在当前 store 中不存在或已过期。
	ErrCacheMiss = errors.New("cache: key not found")
	// ErrStoreNotFound 表示调用方指定了未注册的 store 名称。
	ErrStoreNotFound = errors.New("cache: store not found")
	// ErrLockTimeout 表示 Block 等待锁超过了调用方给定的等待时间。
	ErrLockTimeout = errors.New("cache: lock wait timeout")
	// ErrLockNotHeld 表示当前 DistributedLock 实例没有持有可释放的锁。
	ErrLockNotHeld = errors.New("cache: lock not held")
	// ErrInvalidCounter 表示计数器已有值不是可递增的整数。
	ErrInvalidCounter = errors.New("cache: counter value is not an integer")
	// ErrTagsUnsupported 表示当前 store 不支持 tagged cache。
	ErrTagsUnsupported = errors.New("cache: store does not support tags")
)
