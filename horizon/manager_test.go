package horizon

import (
	"context"
	"testing"
	"time"

	queuecontract "github.com/prismgo/framework/contracts/queue"
	"github.com/prismgo/framework/event"
	"github.com/prismgo/framework/queue"
)

func TestNewManagerKeepsExplicitDependencies(t *testing.T) {
	// 需求背景：runtime retry contract 明确 Manager 必须以显式依赖注入为主。本测试用 fake 依赖验证构造器只保存
	// 调用方传入的资源边界，不通过包级 fallback 创建真实生产资源。
	queue := &fakeQueueManager{}
	events := &fakeEventDispatcher{}
	store := &fakeStoreFactory{}
	cfg := Config{Environment: "local", Store: "memory", Prefix: "demo"}

	manager, err := NewManager(cfg, WithQueueManager(queue), WithEventDispatcher(events), WithStoreFactory(store))
	if err != nil {
		t.Fatalf("NewManager returned error: %v", err)
	}

	if manager.Config().Environment != "local" || manager.Config().Store != "memory" {
		t.Fatalf("manager config was not retained: %#v", manager.Config())
	}
	if manager.QueueManager() != queue || manager.EventDispatcher() != events || manager.StoreFactory() != store {
		t.Fatal("manager did not retain explicitly injected dependencies")
	}
}

func TestNilManagerAccessorsAreSafe(t *testing.T) {
	// 设计原因：facade 或测试代码可能在未完成装配时读取 accessor，nil receiver 返回零值能让错误边界更清晰。
	var manager *Manager
	if manager.Config().Environment != "" {
		t.Fatal("nil manager should return empty config")
	}
	if manager.QueueManager() != nil || manager.EventDispatcher() != nil || manager.StoreFactory() != nil ||
		manager.Collector() != nil || manager.Flusher() != nil || manager.CollBound() {
		t.Fatal("nil manager dependencies should be nil")
	}
	manager.Shutdown()
}

func TestQueueAdapterNarrowsQueueManagerBoundary(t *testing.T) {
	// 需求背景：Horizon 通过 contracts/queue.Queue adapter 使用队列能力，生产路径可注入
	// *queue.Manager，但命令层不能访问 queue manager 内部 driver 状态。
	queueManager, err := queue.NewManager(queue.Config{Default: "sync"}, queue.NewRegistry())
	if err != nil {
		t.Fatalf("new queue manager: %v", err)
	}
	adapter := NewQueueAdapter(queueManager)

	connection, err := adapter.Queue("sync")
	if err != nil {
		t.Fatalf("adapter connection: %v", err)
	}
	if size, err := connection.Size(context.Background(), "default"); err != nil || size != 0 {
		t.Fatalf("adapter size = %d, err=%v", size, err)
	}
	if adapter.Failed() == nil {
		t.Fatal("adapter should expose failed store")
	}
	if err := adapter.RequestRestart(context.Background()); err != nil {
		t.Fatalf("adapter request restart: %v", err)
	}

	var nilAdapter *QueueAdapter
	if _, err := nilAdapter.Queue("sync"); err == nil {
		t.Fatal("nil adapter connection should return an error")
	}
	if nilAdapter.Failed() != nil {
		t.Fatal("nil adapter failed store should be nil")
	}
	if err := nilAdapter.RequestRestart(context.Background()); err == nil {
		t.Fatal("nil adapter restart should return an error")
	}
}

func TestManagerRegistersCollectorExplicitlyAndIdempotently(t *testing.T) {
	// 需求背景：historical scenario 03 要求 Horizon collector 只能通过 Manager 显式注册到注入的 event.Dispatcher，
	// NewManager 本身不能产生订阅副作用，重复注册也不能让同一个 queue 事件被重复计数。
	ctx := context.Background()
	bus := event.New()
	store := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})
	manager, err := NewManager(Config{Store: "memory"}, WithEventDispatcher(bus), WithStoreFactory(staticStoreResolver{store: store}))
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	if bus.Has(queue.EventJobQueued) {
		t.Fatal("NewManager should not subscribe collector events automatically")
	}

	if err := manager.RegisterMonitor(ctx); err != nil {
		t.Fatalf("register collector: %v", err)
	}
	for _, name := range []string{
		queue.EventJobQueued,
		queue.EventJobProcessing,
		queue.EventJobProcessed,
		queue.EventJobReleased,
		queue.EventJobFailed,
		queue.EventConsumerStarted,
		queue.EventConsumerStopped,
		queue.EventConsumerStopFailed,
		queue.EventPoisonEnvelope,
	} {
		if !bus.Has(name) {
			t.Fatalf("expected exact listener for %s", name)
		}
	}
	if bus.Has("queue.*") {
		t.Fatal("monitor must not subscribe through queue.* wildcard")
	}
	if err := manager.RegisterMonitor(ctx); err != nil {
		t.Fatalf("register collector twice: %v", err)
	}

	bus.Dispatch(ctx, queue.JobQueued{Connection: "redis", Queue: "default", JobID: "job-1", JobName: "EmailJob"})
	// collector replaces legacy collector as collection entry point; events go to collector async
	time.Sleep(50 * time.Millisecond)
	if !manager.CollBound() {
		t.Fatal("expected collector to be bound after RegisterMonitor")
	}

	missingDispatcher, err := NewManager(Config{})
	if err != nil {
		t.Fatalf("new manager without dispatcher: %v", err)
	}
	if err := missingDispatcher.RegisterMonitor(ctx); err == nil {
		t.Fatal("expected missing dispatcher error")
	}
}

func TestManagerMonitorReadsWorkerSupervisorFromEventContext(t *testing.T) {
	// 需求背景：historical scenario 43 要求普通 queue event 监听路径也能保留 horizon:work runtime 的 supervisor 来源。
	// 逻辑说明：worker wrapper 不修改 queue event 合同，而是把 --supervisor 放入 context；Manager 监听器
	// 从 context 读取该值后写入 CollectorInput.SourceSupervisor。
	ctx := context.Background()
	bus := event.New()
	obs := observabilityPresetConfigOrFull()
	obs.EventMetrics = true
	obs.EventMetricsSampleRate = 1
	obs.QueuedWaitsMax = 0
	store := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})
	manager, err := NewManager(Config{
		Store:         "memory",
		Prefix:        "ctx-prefix",
		Environment:   "testing",
		Observability: obs,
	}, WithEventDispatcher(bus), WithStoreFactory(staticStoreResolver{store: store}))
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	if err := manager.RegisterMonitor(ctx); err != nil {
		t.Fatalf("register monitor: %v", err)
	}

	bus.Dispatch(contextWithWorkerSupervisor(ctx, "ctx-supervisor"), queue.JobProcessed{
		Connection: "redis",
		Queue:      "default",
		JobID:      "job-ctx",
		JobName:    "EmailJob",
		Duration:   time.Millisecond,
	})
	manager.Collector().Stop()
	snapshot := manager.Collector().FlushSnapshot(time.Now())
	if snapshot == nil || len(snapshot.windows) != 1 {
		t.Fatalf("expected collector window from context supervisor event, got %#v", snapshot)
	}
	window := snapshot.windows[0]
	if window.supervisor != "ctx-supervisor" || window.sourcePrefix != "ctx-prefix" || window.environment != "testing" {
		t.Fatalf("unexpected source dimensions from event context: %#v", window)
	}
}

func TestManagerStartsFlusherAndShutdownStopsBackgroundLoops(t *testing.T) {
	// 需求背景：collector/flusher 是新的生产采集链路，RegisterMonitor 必须在 Store 可用时启动 flusher；
	// Shutdown 应安全停止后台循环，供进程退出 best-effort flush 使用。
	ctx := context.Background()
	store := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})
	bus := event.New()
	manager, err := NewManager(
		Config{Store: "memory", Observability: observabilityPresetConfigOrFull()},
		WithEventDispatcher(bus),
		WithStoreFactory(staticStoreResolver{store: store}),
	)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	if manager.Flusher() != nil {
		t.Fatal("flusher should not start before RegisterMonitor")
	}
	if err := manager.RegisterMonitor(ctx); err != nil {
		t.Fatalf("register collector: %v", err)
	}
	if manager.Collector() == nil || manager.Flusher() == nil || !manager.CollBound() {
		t.Fatalf("collector/flusher should be active after register: collector=%v flusher=%v bound=%v",
			manager.Collector(), manager.Flusher(), manager.CollBound())
	}
	manager.Shutdown()
	manager.Shutdown()
}

// fakeQueueManager 表示测试用队列管理器依赖，不连接真实队列。
type fakeQueueManager struct{}

func (f *fakeQueueManager) Queue(string) (queuecontract.Queue, error) { return nil, nil }
func (f *fakeQueueManager) Failed() queue.FailedStore                 { return nil }
func (f *fakeQueueManager) RequestRestart(context.Context) error      { return nil }

// fakeEventDispatcher 表示测试用事件总线依赖，不提供真实事件派发能力。
type fakeEventDispatcher struct{}

func (f *fakeEventDispatcher) ListenFunc(string, func(context.Context, event.Event) error) {}

// fakeStoreFactory 表示测试用 Store 工厂依赖，不创建 Redis 或 memory Store。
type fakeStoreFactory struct{}

func (f *fakeStoreFactory) ResolveStore(context.Context, Config) (Store, error) {
	return nil, nil
}
