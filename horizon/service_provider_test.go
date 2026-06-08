package horizon

import (
	"context"
	"testing"
	"time"

	"github.com/prismgo/framework/console"
	"github.com/prismgo/framework/container"
	containercontract "github.com/prismgo/framework/contracts/container"
	"github.com/prismgo/framework/event"
	"github.com/prismgo/framework/kernel"
	"github.com/prismgo/framework/queue"
	"github.com/prismgo/framework/timer"
)

func TestServiceProviderRegistersLazyManagerWithoutBootingCollector(t *testing.T) {
	// 需求背景：historical scenario 10 要求 Horizon 通过显式 ServiceProvider 接入。Register 阶段只绑定
	// horizon.manager lazy factory，Boot 阶段只声明命令并绑定 manager；Store/collector 仍由
	// Horizon HTTP/API 或命令运行时延迟启动，不能在 provider boot 时连接 Redis 或创建 memory store。
	ctx := context.Background()
	registry := container.NewContainer()
	container.SetProvider(func() *container.Container { return registry })
	t.Cleanup(func() {
		queue.UseEventSink(nil)
		container.SetProvider(nil)
	})
	source := horizonKernelSource{}
	if err := registry.Instance(kernel.StartingRegistrarKey, kernel.StartingRegistrar(func(callbacks ...kernel.StartingCallback) error {
		source.starting = append(source.starting, callbacks...)
		return nil
	})); err != nil {
		t.Fatalf("bind starting registrar: %v", err)
	}
	t.Setenv("HORIZON_STORE", "memory")
	if err := reloadConfigFacadeForTest(t, registry); err != nil {
		t.Fatalf("reload config facade: %v", err)
	}
	app := horizonProviderTestApp{ctx: ctx, registry: registry}

	queueManager, err := queue.NewManager(queue.Config{Default: "sync"}, queue.NewRegistry())
	if err != nil {
		t.Fatalf("new queue manager: %v", err)
	}
	if err := registry.Singleton("queue.manager", func(containercontract.Resolver) (any, error) { return queueManager, nil }); err != nil {
		t.Fatalf("register queue manager: %v", err)
	}
	bus := event.New()
	if err := registry.Singleton("event.dispatcher", func(containercontract.Resolver) (any, error) { return bus, nil }); err != nil {
		t.Fatalf("register event dispatcher: %v", err)
	}
	if err := (queue.ServiceProvider{}).Boot(app); err != nil {
		t.Fatalf("boot queue provider bridge: %v", err)
	}

	provider := ServiceProvider{}
	if err := provider.Register(app); err != nil {
		t.Fatalf("register horizon provider: %v", err)
	}
	if !registry.Bound("horizon.manager") {
		t.Fatal("horizon.manager factory should be bound before boot")
	}
	if registry.Resolved("horizon.manager") {
		t.Fatal("horizon.manager should stay lazy before boot")
	}
	if err := provider.Boot(app); err != nil {
		t.Fatalf("boot horizon provider: %v", err)
	}
	if err := provider.Register(app); err != nil {
		t.Fatalf("register horizon provider should no-op when manager already exists: %v", err)
	}
	raw, err := registry.Make("horizon.manager")
	if err != nil {
		t.Fatalf("resolve horizon manager: %v", err)
	}
	manager, _ := raw.(*Manager)
	if manager.StoreFactory() == nil {
		t.Fatal("horizon manager should keep default store factory for command runtime")
	}

	if _, err := queue.NewDispatcher(queueManager).Dispatch(ctx, &integrationJob{Value: "provider"}); err != nil {
		t.Fatalf("dispatch provider bridge job: %v", err)
	}
	if manager.CollBound() {
		t.Fatal("collector should not be bound during provider boot")
	}

	appKernel := kernel.New("test", kernel.WithApplicationRegistry(&source))
	if err := appKernel.Call(context.Background(), "list"); err != nil {
		t.Fatalf("trigger kernel starting with list: %v", err)
	}
	names := map[string]bool{}
	for _, command := range appKernel.Commands() {
		names[command.Name] = true
	}
	if !names["horizon"] || !names["horizon:work"] || !names["horizon:install"] || !names["horizon:listen"] || names["horizon:publish"] {
		t.Fatalf("horizon provider command registration names = %#v", names)
	}
}

func TestServiceProviderBootReportsMissingRuntimeDependencies(t *testing.T) {
	ctx := context.Background()
	registry := container.NewContainer()
	app := horizonProviderTestApp{ctx: ctx, registry: registry}
	provider := ServiceProvider{}
	if err := provider.Register(app); err != nil {
		t.Fatalf("register horizon provider: %v", err)
	}
	if err := provider.Boot(app); err == nil {
		t.Fatal("boot should fail when queue manager and event dispatcher are missing")
	}
}

type horizonProviderTestApp struct {
	ctx      context.Context
	registry containercontract.Container
}

type horizonKernelSource struct {
	starting []kernel.StartingCallback
}

func (s *horizonKernelSource) CommandFactories() []console.CommandFactory { return nil }

func (s *horizonKernelSource) StartingCallbacks() []kernel.StartingCallback {
	return append([]kernel.StartingCallback(nil), s.starting...)
}

func (s *horizonKernelSource) ScheduleRegistrars() []func(*timer.Schedule) { return nil }

func (s *horizonKernelSource) MigrationPaths() []string { return nil }

func (s *horizonKernelSource) SeedPaths() []string { return nil }

func (a horizonProviderTestApp) Context() context.Context {
	return a.ctx
}

func (a horizonProviderTestApp) Container() containercontract.Container {
	return a.registry
}

func testNow() time.Time {
	return time.Now().UTC()
}
