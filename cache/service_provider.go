package cache

import (
	"context"

	"github.com/prismgo/framework/container"
	containercontract "github.com/prismgo/framework/contracts/container"
	eventcontract "github.com/prismgo/framework/contracts/event"
	providercontract "github.com/prismgo/framework/contracts/provider"
)

// providerApplication 复用公共 provider 契约的最小 Application 视图，避免 cache 包反向 import foundation。
type providerApplication = providercontract.Application

// ServiceProvider 把 cache manager 注册到当前 Application Container。
//
// 需求背景：cache 是 framework default provider。Register 阶段只绑定 lazy factory，
// 不读取配置、不创建 Redis client；跨服务事件桥接放在 Boot 阶段。
type ServiceProvider struct{}

// Name 返回稳定 provider identity，用于生命周期事件、去重和错误消息。
func (ServiceProvider) Name() string { return "cache" }

// Register 注册 cache manager lazy factory，并保留显式注入的 manager。
func (ServiceProvider) Register(app providerApplication) error {
	c := app.Container()
	if c.Bound("cache.manager") {
		return nil
	}
	return c.Singleton("cache.manager", func(containercontract.Resolver) (any, error) {
		cfg, err := buildConfig()
		if err != nil {
			return nil, err
		}
		return NewManager(cfg)
	}, container.WithCloser(func(m *Manager) error {
		return m.Close()
	}))
}

// Boot 安装 cache -> event bus 桥接。
//
// 设计思路：sink 不捕获本次 Boot 的 c 或 dispatcher，而是在每次 cache
// 事件发生时从当前 Application Container 解析 contracts/event.Dispatcher。
func (ServiceProvider) Boot(providerApplication) error {
	UseEventSink(func(ctx context.Context, ev CacheEvent) {
		dispatchCurrentEvent(ctx, ev)
	})
	return nil
}

func dispatchCurrentEvent(ctx context.Context, ev eventcontract.Event) {
	if ev == nil {
		return
	}
	bus, err := container.Make[eventcontract.Dispatcher]("event.dispatcher")
	if err != nil || bus == nil {
		return
	}
	bus.Dispatch(ctx, ev)
}
