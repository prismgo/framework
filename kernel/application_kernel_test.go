package kernel

import (
	"testing"
	"time"

	"github.com/prismgo/framework/cache"
	"github.com/prismgo/framework/console"
	"github.com/prismgo/framework/container"
	containercontract "github.com/prismgo/framework/contracts/container"
	"github.com/prismgo/framework/timer"
)

func newApplicationKernelForTest(t *testing.T, name string, source ApplicationRegistrySource, dependencies ...BuiltinDependencies) *Kernel {
	t.Helper()
	registry := container.NewContainer()
	// 测试意图：NewApplicationKernel 的生产语义要求 Application source 暴露容器契约；
	// kernel 包单元测试只关注 Kernel 行为，因此这里用隔离 source 补齐该生产前提。
	application := kernelTestApplicationSource{
		ApplicationRegistrySource: source,
		registry:                  registry,
	}
	return NewApplicationKernel(name, application, dependencies...)
}

func useKernelTestContainer(t *testing.T) *container.Container {
	t.Helper()
	registry := container.NewContainer()
	container.SetProvider(func() *container.Container { return registry })
	t.Cleanup(func() { container.SetProvider(nil) })
	return registry
}

func bindKernelCacheManagerForTest(t *testing.T) *cache.Manager {
	t.Helper()
	registry := useKernelTestContainer(t)
	manager, err := cache.NewManager(cache.Config{
		Default:  "memory",
		Encoding: "json",
		Stores: map[string]cache.StoreConfig{
			"memory": {Driver: "memory", CleanupInterval: time.Millisecond},
		},
		Lock: cache.LockConfig{RetrySleep: time.Millisecond},
	})
	if err != nil {
		t.Fatalf("new cache manager: %v", err)
	}
	t.Cleanup(func() { _ = manager.Close() })
	// 测试装配说明：kernel isolation 使用 cache facade 获取分布式锁；
	// 显式绑定 cache.manager，避免依赖旧的全局 fallback 容器。
	if err := registry.Instance("cache.manager", manager); err != nil {
		t.Fatalf("bind cache manager: %v", err)
	}
	return manager
}

type kernelTestApplicationSource struct {
	ApplicationRegistrySource
	registry containercontract.Container
}

func (s kernelTestApplicationSource) Container() containercontract.Container {
	return s.registry
}

func (s kernelTestApplicationSource) CommandFactories() []console.CommandFactory {
	if s.ApplicationRegistrySource == nil {
		return nil
	}
	return s.ApplicationRegistrySource.CommandFactories()
}

func (s kernelTestApplicationSource) StartingCallbacks() []StartingCallback {
	if s.ApplicationRegistrySource == nil {
		return nil
	}
	return s.ApplicationRegistrySource.StartingCallbacks()
}

func (s kernelTestApplicationSource) ScheduleRegistrars() []func(*timer.Schedule) {
	if s.ApplicationRegistrySource == nil {
		return nil
	}
	return s.ApplicationRegistrySource.ScheduleRegistrars()
}

func (s kernelTestApplicationSource) MigrationPaths() []string {
	if s.ApplicationRegistrySource == nil {
		return nil
	}
	return s.ApplicationRegistrySource.MigrationPaths()
}

func (s kernelTestApplicationSource) SeedPaths() []string {
	if s.ApplicationRegistrySource == nil {
		return nil
	}
	return s.ApplicationRegistrySource.SeedPaths()
}
