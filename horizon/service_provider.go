package horizon

import (
	"fmt"

	"github.com/prismgo/framework/console"
	containercontract "github.com/prismgo/framework/contracts/container"
	providercontract "github.com/prismgo/framework/contracts/provider"
	"github.com/prismgo/framework/event"
	frameworkprovider "github.com/prismgo/framework/provider"
	"github.com/prismgo/framework/queue"
)

// providerApplication 复用公共 provider 契约的最小 Application 视图，避免 horizon 包反向 import foundation。
type providerApplication = providercontract.Application

// ServiceProvider 是 Prismgo Horizon 首期推荐接入方式。
//
// 需求背景：issue 10 要求项目通过 bootstrap/provider.go 显式加入 horizon.ServiceProvider{}，
// 不再在 bootstrap/app.go 手动 WithCommands(horizon.CommandFactories()...)。Register 阶段只绑定
// lazy manager factory；Boot 阶段声明 console commands，并把 queue provider bridge 派发到
// prismgo/event 的事件注册给 Horizon collector。
type ServiceProvider struct{}

// Name 返回稳定 provider identity，用于 Application provider repository 去重和生命周期事件。
func (ServiceProvider) Name() string { return "horizon" }

// Register 注册 horizon.manager lazy factory。
//
// 逻辑说明：factory 会在 Resolve 时读取当前 Application 的 queue.manager 与 event.dispatcher，
// 但不会解析 Horizon Store，也不会 ping Redis/RabbitMQ。Store 继续由命令运行时 ResolveStore
// 延迟创建，确保 HTTP-only Boot 不触碰外部 Horizon store。
func (ServiceProvider) Register(app providerApplication) error {
	c := app.Container()
	if c.Bound(managerFacadeKey) {
		return nil
	}
	return c.Singleton(managerFacadeKey, func(resolver containercontract.Resolver) (any, error) {
		return buildProviderManager(resolver)
	})
}

// Boot 声明 Horizon console commands and binds a lazy runtime manager.
//
// 设计思路：provider.Commands 把命令注册延迟到 Console Kernel starting 阶段，因此 HTTP-only
// Application.Boot 不需要创建 Kernel，也不解析 Horizon Store；collector/monitor 启动继续由
// Horizon HTTP/API 或 Horizon commands 的运行时路径触发，避免普通 CLI discovery/list/help
// 因 Redis 不可用而失败。
func (ServiceProvider) Boot(app providerApplication) error {
	if err := frameworkprovider.Commands(horizonCommandDeclarations()...); err != nil {
		return err
	}
	manager, err := buildProviderManager(app.Container())
	if err != nil {
		return fmt.Errorf("horizon provider boot: %w", err)
	}
	if err := app.Container().Instance(managerFacadeKey, manager); err != nil {
		return fmt.Errorf("horizon provider boot: %w", err)
	}
	return nil
}

func buildProviderManager(resolver containercontract.Resolver) (*Manager, error) {
	cfg, err := LoadConfig()
	if err != nil {
		return nil, err
	}
	queueManager, err := resolveProviderQueueManager(resolver)
	if err != nil {
		return nil, err
	}
	dispatcher, err := resolveProviderEventDispatcher(resolver)
	if err != nil {
		return nil, err
	}
	return NewManager(cfg,
		WithStoreFactory(defaultStoreFactory),
		WithQueueManager(NewQueueAdapter(queueManager)),
		WithWorkerRunner(NewQueueWorkerAdapter(queueManager)),
		WithEventDispatcher(dispatcher),
	)
}

func resolveProviderQueueManager(resolver containercontract.Resolver) (*queue.Manager, error) {
	if resolver == nil {
		return nil, fmt.Errorf("horizon: queue manager is not configured")
	}
	raw, err := resolver.Make("queue.manager")
	if err != nil {
		return nil, fmt.Errorf("horizon: queue manager is not configured: %w", err)
	}
	manager, ok := raw.(*queue.Manager)
	if !ok {
		return nil, fmt.Errorf("horizon: queue manager resolved %T, want *queue.Manager", raw)
	}
	if manager == nil {
		return nil, fmt.Errorf("horizon: queue manager is not configured")
	}
	return manager, nil
}

func resolveProviderEventDispatcher(resolver containercontract.Resolver) (*event.Dispatcher, error) {
	if resolver == nil {
		return nil, fmt.Errorf("horizon: event dispatcher is not configured")
	}
	raw, err := resolver.Make("event.dispatcher")
	if err != nil {
		return nil, fmt.Errorf("horizon: event dispatcher is not configured: %w", err)
	}
	dispatcher, ok := raw.(*event.Dispatcher)
	if !ok {
		return nil, fmt.Errorf("horizon: event dispatcher resolved %T, want *event.Dispatcher", raw)
	}
	if dispatcher == nil {
		return nil, fmt.Errorf("horizon: event dispatcher is not configured")
	}
	return dispatcher, nil
}

func horizonCommandDeclarations() []any {
	factories := CommandFactories()
	out := make([]any, 0, len(factories))
	for _, factory := range factories {
		declaration := console.CommandFactory(factory)
		out = append(out, declaration)
	}
	return out
}
