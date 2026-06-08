package cache

// Factory 是缓存管理器的契约。
//
// 用途：管理多个缓存 store 的生命周期，按名称查找和创建 Repository。
//
// 使用方式：
//
//	mgr, _ := cache.NewManager(config)
//	repo := mgr.Store("redis")
//	repo.Put(ctx, "key", "value", 0)
type Factory interface {
	// Store 返回指定名称的 Repository。
	//
	// 参数 name 为空时返回默认 store。未知 store 返回携带错误的 Repository，
	// 后续调用以普通 error 形式暴露配置问题。
	Store(name string) Repository

	// Default 返回默认 store 的 Repository。
	Default() Repository

	// DefaultName 返回默认 store 的配置名称。
	DefaultName() string

	// Close 释放所有已构建 store 持有的外部资源。
	Close() error
}
