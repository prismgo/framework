package queue

import (
	"context"
	"fmt"

	"github.com/prismgo/framework/container"
	containercontract "github.com/prismgo/framework/contracts/container"
	eventcontract "github.com/prismgo/framework/contracts/event"
	providercontract "github.com/prismgo/framework/contracts/provider"
	queuecontract "github.com/prismgo/framework/contracts/queue"
)

// providerApplication 复用公共 provider 契约的最小 Application 视图，避免 queue 包反向 import foundation。
type providerApplication = providercontract.Application

// ServiceProvider 把 queue manager 注册到当前 Application Container。
//
// 需求背景：queue 是 framework default provider。Register 阶段只绑定 lazy factory，
// 不创建 Redis/RabbitMQ 连接；跨服务事件桥接放在 Boot 阶段。
type ServiceProvider struct{}

// Name 返回稳定 provider identity，用于生命周期事件、去重和错误消息。
func (ServiceProvider) Name() string { return "queue" }

// Register 注册 queue manager lazy factory，并保留显式注入的 manager。
func (ServiceProvider) Register(app providerApplication) error {
	c := app.Container()
	if !c.Bound(serviceKey) {
		if err := c.Singleton(serviceKey, func(containercontract.Resolver) (any, error) {
			return NewManagerFromConfig()
		}, container.WithCloser(func(m *Manager) error {
			return m.Close()
		})); err != nil {
			return err
		}
	}
	if c.Bound(queuecontract.DispatcherServiceKey) {
		return nil
	}
	return c.Singleton(queuecontract.DispatcherServiceKey,
		func(resolver containercontract.Resolver) (any, error) {
			raw, err := resolver.Make(serviceKey)
			if err != nil {
				return nil, err
			}
			manager, ok := raw.(*Manager)
			if !ok {
				return nil, fmt.Errorf("queue: container %q resolved %T, want *queue.Manager", serviceKey, raw)
			}
			return NewDispatcher(manager), nil
		},
	)
}

// Boot 安装 queue -> event bus 桥接。
//
// 设计思路：sink 不捕获本次 Boot 的 c 或 dispatcher，而是在每次 queue
// 事件发生时从当前 Application Container 解析 contracts/event.Dispatcher。
func (ServiceProvider) Boot(providerApplication) error {
	UseEventSink(func(ctx context.Context, ev Event) {
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
