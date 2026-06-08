package redis

import (
	"context"
	"fmt"

	"github.com/prismgo/framework/container"
	containercontract "github.com/prismgo/framework/contracts/container"
	eventcontract "github.com/prismgo/framework/contracts/event"
	providercontract "github.com/prismgo/framework/contracts/provider"
)

type providerApplication = providercontract.Application

// ServiceProvider 把共享 Redis Factory 注册到 Application Container。
//
// 需求背景：
// Redis 是 cache、queue、session、horizon 的底层共享能力，不能要求业务应用逐个手动注册。
// 作为 framework default provider 后，框架包可以统一从容器解析 redis/redis.connection。
//
// 设计思路：
// Register 阶段只注册懒加载 factory，不连接 Redis；Boot 阶段只安装事件桥接，把
// PrismGo 管理 Redis client 产生的事件转交给当前 Application 的 event dispatcher。
type ServiceProvider struct{}

// Name 返回稳定 provider identity。
//
// 逻辑说明：
// provider 列表和测试依赖该名称识别 Redis provider。名称保持短字符串 redis，与容器
// 单例 key 对齐，便于排查启动顺序。
func (ServiceProvider) Name() string { return "redis" }

// Register 注册 redis 与 redis.connection 懒加载工厂。
//
// 参数说明：
// app 提供当前 Application 容器。Register 会在容器未绑定时注册 redis Factory 和默认
// redis.connection，已存在绑定则保留调用方显式替换。
//
// 逻辑说明：
// redis 绑定返回 Manager；redis.connection 通过同一个 Manager 解析默认连接。两者都保持
// 懒加载，避免 provider 注册阶段访问网络或要求 Redis 服务已经启动。
func (ServiceProvider) Register(app providerApplication) error {
	c := app.Container()
	if !c.Bound(serviceKey) {
		if err := c.Singleton(serviceKey, func(containercontract.Resolver) (any, error) {
			return NewManagerFromConfig()
		}, container.WithContextCloser(func(ctx context.Context, m *Manager) error {
			return m.Close(ctx)
		})); err != nil {
			return err
		}
	}
	if !c.Bound(defaultConnectionKey) {
		if err := c.Singleton(defaultConnectionKey, func(resolver containercontract.Resolver) (any, error) {
			raw, err := resolver.Make(serviceKey)
			if err != nil {
				return nil, err
			}
			manager, ok := raw.(*Manager)
			if !ok {
				return nil, fmt.Errorf("redis: container %q resolved %T, want *redis.Manager", serviceKey, raw)
			}
			return manager.DefaultConnection()
		}); err != nil {
			return err
		}
	}
	return nil
}

// Boot 安装 Redis 命令事件到 prismgo/event 的桥接。
//
// 需求背景：
// Redis 单命令事件既要支持连接级监听器，也要能进入 prismgo/event；pipeline 批量事件
// 只进入全局 dispatcher，供日志、指标或 Horizon 这类横切能力订阅。
//
// 设计思路：
// 事件发生时再从当前容器解析 event.dispatcher，而不是在 Boot 时持有某个 dispatcher
// 实例。这样测试切换当前 Application 或容器绑定时，Redis 事件会派发到最新上下文。
func (ServiceProvider) Boot(providerApplication) error {
	UseEventSink(func(ctx context.Context, ev eventcontract.Event) {
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
