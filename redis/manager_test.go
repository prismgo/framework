package redis

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"
	configpkg "github.com/prismgo/framework/config"
	"github.com/prismgo/framework/container"
	containercontract "github.com/prismgo/framework/contracts/container"
	eventcontract "github.com/prismgo/framework/contracts/event"
	rediscontract "github.com/prismgo/framework/contracts/redis"
	goexception "github.com/prismgo/framework/exception"
	"github.com/prismgo/framework/logger"
	goredis "github.com/redis/go-redis/v9"
)

func useRedisTestContainer(t *testing.T) *container.Container {
	t.Helper()
	c := container.NewContainer()
	container.SetProvider(func() *container.Container { return c })
	t.Cleanup(func() { container.SetProvider(nil) })
	return c
}

// TestManagerLazilyResolvesNamedConnections 覆盖 Manager 的核心懒加载语义。
//
// 需求背景：cache/queue/session/horizon 迁移到共享 Redis Factory 后，不能在启动阶段提前连接
// Redis；只有首次请求具体连接时才创建 go-redis client。
func TestManagerLazilyResolvesNamedConnections(t *testing.T) {
	server := miniredis.RunT(t)
	manager := newTestManager(t, server)
	t.Cleanup(func() { _ = manager.Close(context.Background()) })

	if got := manager.Connections(); len(got) != 0 {
		t.Fatalf("connections before resolve = %#v, want empty", got)
	}
	defaultConn, err := manager.DefaultConnection()
	if err != nil {
		t.Fatalf("DefaultConnection error = %v", err)
	}
	cacheConn, err := manager.Connection("cache")
	if err != nil {
		t.Fatalf("Connection(cache) error = %v", err)
	}
	again, err := manager.Connection("cache")
	if err != nil {
		t.Fatalf("Connection(cache) again error = %v", err)
	}
	if cacheConn != again {
		t.Fatal("named connection should be cached")
	}
	if defaultConn.Name() != "default" || cacheConn.Name() != "cache" {
		t.Fatalf("names = %q/%q", defaultConn.Name(), cacheConn.Name())
	}
	if got := manager.Connections(); len(got) != 2 {
		t.Fatalf("connections after resolve = %#v, want 2", got)
	}
	if _, err := manager.Connection("missing"); err == nil {
		t.Fatal("missing connection should return an error")
	}
}

// TestConnectionClientDispatchesSuccessAndFailureEvents 覆盖 typed go-redis 命令的事件边界。
//
// 逻辑说明：PrismGo 管理的 client 通过 go-redis hook 自动产生事件，避免把 go-redis 的
// 完整命令面重新抽象进 PrismGo。
func TestConnectionClientDispatchesSuccessAndFailureEvents(t *testing.T) {
	server := miniredis.RunT(t)
	manager := newTestManager(t, server)
	t.Cleanup(func() { _ = manager.Close(context.Background()) })

	conn, err := manager.Connection("cache")
	if err != nil {
		t.Fatalf("Connection(cache) error = %v", err)
	}

	successes := 0
	failures := 0
	conn.Listen(func(_ context.Context, ev CommandExecuted) {
		successes++
		if ev.Command != "set" || ev.ConnectionName != "cache" {
			t.Fatalf("unexpected success event: %#v", ev)
		}
	})
	conn.ListenForFailures(func(_ context.Context, ev CommandFailed) {
		failures++
		if ev.Command != "get" || ev.ConnectionName != "cache" || ev.Error == nil {
			t.Fatalf("unexpected failure event: %#v", ev)
		}
	})

	if err := conn.Client().Set(context.Background(), "key", "value", 0).Err(); err != nil {
		t.Fatalf("set command error = %v", err)
	}
	if err := conn.Client().Do(context.Background(), "get").Err(); err == nil {
		t.Fatal("invalid get command should fail")
	}
	if successes != 1 || failures != 1 {
		t.Fatalf("events success=%d failure=%d, want 1/1", successes, failures)
	}
}

func TestConnectionEventToggleAffectsExistingClient(t *testing.T) {
	server := miniredis.RunT(t)
	manager := newTestManager(t, server)
	t.Cleanup(func() { _ = manager.Close(context.Background()) })

	conn, err := manager.Connection("cache")
	if err != nil {
		t.Fatalf("Connection(cache) error = %v", err)
	}
	client := conn.Client()
	successes := 0
	conn.Listen(func(_ context.Context, _ CommandExecuted) {
		successes++
	})

	if err := client.Set(context.Background(), "toggle:key", "one", 0).Err(); err != nil {
		t.Fatalf("initial set error = %v", err)
	}
	manager.DisableEvents()
	if err := client.Set(context.Background(), "toggle:key", "two", 0).Err(); err != nil {
		t.Fatalf("disabled set error = %v", err)
	}
	if successes != 1 {
		t.Fatalf("success events after DisableEvents = %d, want 1", successes)
	}
	manager.EnableEvents()
	if err := client.Set(context.Background(), "toggle:key", "three", 0).Err(); err != nil {
		t.Fatalf("reenabled set error = %v", err)
	}
	if successes != 2 {
		t.Fatalf("success events after EnableEvents = %d, want 2", successes)
	}
}

// TestManagerPurgeAndCloseReleaseClients 验证连接重建与关闭生命周期。
//
// 设计思路：Purge 只关闭指定连接并允许后续重新创建；Close 关闭所有已解析连接，供
// Application.CloseContext 的 container closer 使用。
func TestManagerPurgeAndCloseReleaseClients(t *testing.T) {
	server := miniredis.RunT(t)
	manager := newTestManager(t, server)

	conn, err := manager.Connection("cache")
	if err != nil {
		t.Fatalf("Connection(cache) error = %v", err)
	}
	client := conn.Client()
	if err := manager.Purge("cache"); err != nil {
		t.Fatalf("Purge(cache) error = %v", err)
	}
	if err := client.Ping(context.Background()).Err(); !errors.Is(err, goredis.ErrClosed) {
		t.Fatalf("purged client ping error = %v, want redis.ErrClosed", err)
	}
	recreated, err := manager.Connection("cache")
	if err != nil {
		t.Fatalf("Connection(cache) after purge error = %v", err)
	}
	if recreated == conn {
		t.Fatal("purged connection should be recreated")
	}

	if err := manager.Close(context.Background()); err != nil {
		t.Fatalf("Close error = %v", err)
	}
	if err := recreated.Client().Ping(context.Background()).Err(); !errors.Is(err, goredis.ErrClosed) {
		t.Fatalf("closed client ping error = %v, want redis.ErrClosed", err)
	}
}

func TestManagerCloseWithCanceledContextKeepsConnectionsForRetry(t *testing.T) {
	server := miniredis.RunT(t)
	manager := newTestManager(t, server)

	conn, err := manager.Connection("cache")
	if err != nil {
		t.Fatalf("Connection(cache) error = %v", err)
	}
	client := conn.Client()
	canceled, cancel := context.WithCancel(context.Background())
	cancel()

	if err := manager.Close(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("Close(canceled) error = %v, want context.Canceled", err)
	}
	if got := manager.Connections(); got["cache"] != conn {
		t.Fatalf("canceled close should keep cached connection for retry, got %#v", got)
	}
	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("client should remain usable after canceled close: %v", err)
	}

	if err := manager.Close(context.Background()); err != nil {
		t.Fatalf("retry Close error = %v", err)
	}
	if err := client.Ping(context.Background()).Err(); !errors.Is(err, goredis.ErrClosed) {
		t.Fatalf("retried close ping error = %v, want redis.ErrClosed", err)
	}
	if got := manager.Connections(); len(got) != 0 {
		t.Fatalf("connections after successful retry = %#v, want empty", got)
	}
}

func TestSuccessListenerPanicIsReportedWithoutChangingRedisResult(t *testing.T) {
	server := miniredis.RunT(t)
	manager := newTestManager(t, server)
	t.Cleanup(func() { _ = manager.Close(context.Background()) })
	reports := captureRedisReports(t)

	conn, err := manager.Connection("cache")
	if err != nil {
		t.Fatalf("Connection(cache) error = %v", err)
	}
	conn.Listen(func(context.Context, CommandExecuted) {
		panic("success listener panic")
	})

	if err := conn.Client().Set(context.Background(), "panic:key", "value", 0).Err(); err != nil {
		t.Fatalf("set command should keep original result: %v", err)
	}
	report := waitRedisReport(t, reports)
	if !strings.Contains(report.err.Error(), "success listener panic") {
		t.Fatalf("reported err = %v", report.err)
	}
	if report.fields["component"] != "redis" || report.fields["connection"] != "cache" || report.fields["command"] != "set" || report.fields["listener"] != "success" {
		t.Fatalf("reported fields = %#v", report.fields)
	}
}

func TestFailureListenerPanicIsReportedWithoutMaskingRedisError(t *testing.T) {
	server := miniredis.RunT(t)
	manager := newTestManager(t, server)
	t.Cleanup(func() { _ = manager.Close(context.Background()) })
	reports := captureRedisReports(t)

	conn, err := manager.Connection("cache")
	if err != nil {
		t.Fatalf("Connection(cache) error = %v", err)
	}
	conn.ListenForFailures(func(context.Context, CommandFailed) {
		panic("failure listener panic")
	})

	err = conn.Client().Do(context.Background(), "get").Err()
	if err == nil {
		t.Fatal("invalid get command should fail")
	}
	report := waitRedisReport(t, reports)
	if !strings.Contains(report.err.Error(), "failure listener panic") {
		t.Fatalf("reported err = %v", report.err)
	}
	if report.fields["component"] != "redis" || report.fields["connection"] != "cache" || report.fields["command"] != "get" || report.fields["listener"] != "failure" {
		t.Fatalf("reported fields = %#v", report.fields)
	}
}

// TestConfigFromRepositoryReadsDatabaseRedis 覆盖 Laravel 风格 database.redis 配置解析。
//
// 需求背景：Redis 连接配置迁移到 database.redis 后，旧 cache/queue/session/horizon 配置只保存
// connection 名称；这里验证解析层不会再依赖根 redis 命名空间。
func TestConfigFromRepositoryReadsDatabaseRedis(t *testing.T) {
	configpkg.Add("database", func() map[string]any {
		return map[string]any{
			"redis": map[string]any{
				"client": "go-redis",
				"options": map[string]any{
					"prefix": "app",
				},
				"default": map[string]any{
					"host":     "10.0.0.8",
					"port":     "6380",
					"username": "default-user",
					"password": "secret",
					"database": "2",
				},
				"cache": map[string]any{
					"addr": "127.0.0.1:6381",
					"db":   5,
				},
			},
		}
	})
	repo, err := configpkg.NewFromFile(t.TempDir() + "/missing.env")
	if err != nil {
		t.Fatalf("NewFromFile error = %v", err)
	}

	cfg := ConfigFromRepository(repo)
	if cfg.Client != "go-redis" || cfg.DefaultName != DefaultConnectionName {
		t.Fatalf("top-level config = %#v", cfg)
	}
	if cfg.Options["prefix"] != "app" {
		t.Fatalf("options = %#v", cfg.Options)
	}
	def := cfg.Connections["default"]
	if def.address() != "10.0.0.8:6380" || def.Username != "default-user" || def.Password != "secret" || def.DB != 2 {
		t.Fatalf("default redis config = %#v", def)
	}
	cache := cfg.Connections["cache"]
	if cache.address() != "127.0.0.1:6381" || cache.DB != 5 {
		t.Fatalf("cache redis config = %#v", cache)
	}
	if _, err := NewManager(Config{Client: "phpredis"}); err == nil {
		t.Fatal("unsupported redis client should fail")
	}
	defaults := ConfigFromRepository(nil)
	if defaults.Client != "go" || len(defaults.Connections) != 2 {
		t.Fatalf("default config = %#v", defaults)
	}
}

// TestFacadeAndServiceProviderCoverContainerLifecycle 验证 Redis facade 的容器装配路径。
//
// 逻辑说明：ServiceProvider 只注册懒加载 factory，不应立即创建 Redis client；首次通过 facade
// 执行命令时才解析连接，container.Close 再通过 closer 关闭 Manager。
func TestFacadeAndServiceProviderCoverContainerLifecycle(t *testing.T) {
	server := miniredis.RunT(t)
	c := useRedisTestContainer(t)

	if err := c.Singleton(serviceKey, func(containercontract.Resolver) (any, error) {
		return NewManager(Config{
			Connections: map[string]ConnectionConfig{
				"default": {Name: "default", Addr: server.Addr()},
			},
		})
	}, container.WithContextCloser(func(ctx context.Context, manager *Manager) error {
		return manager.Close(ctx)
	})); err != nil {
		t.Fatalf("RegisterFactory error = %v", err)
	}

	if err := c.Singleton(defaultConnectionKey, func(containercontract.Resolver) (any, error) {
		manager := ManagerInstance()
		return manager.DefaultConnection()
	}); err != nil {
		t.Fatalf("RegisterFactory defaultConnection error = %v", err)
	}

	manager := ManagerInstance()
	if factory := Resolve(); factory == nil {
		t.Fatal("Resolve returned nil factory")
	}
	client, err := Client()
	if err != nil {
		t.Fatalf("facade Client error = %v", err)
	}
	if err := client.Set(context.Background(), "facade:key", "value", 0).Err(); err != nil {
		t.Fatalf("facade client Set error = %v", err)
	}
	value, err := client.Get(context.Background(), "facade:key").Result()
	if err != nil || value != "value" {
		t.Fatalf("facade client Get = %q, %v", value, err)
	}
	if err := client.Del(context.Background(), "facade:key").Err(); err != nil {
		t.Fatalf("facade client Del error = %v", err)
	}

	conn, err := manager.DefaultConnection()
	if err != nil {
		t.Fatalf("DefaultConnection error = %v", err)
	}
	if err := container.Close(context.Background()); err != nil {
		t.Fatalf("container close error = %v", err)
	}
	if err := conn.Client().Ping(context.Background()).Err(); !errors.Is(err, goredis.ErrClosed) {
		t.Fatalf("facade manager close ping error = %v, want redis.ErrClosed", err)
	}
}

// TestServiceProviderBootDispatchesCommandEvents 覆盖 provider 事件桥接。
//
// 设计思路：RedisServiceProvider.Boot 不持有启动时的 dispatcher，而是在事件发生时从当前
// container 解析 event.dispatcher，避免多 Application 测试互相串事件。
func TestServiceProviderBootDispatchesCommandEvents(t *testing.T) {
	server := miniredis.RunT(t)
	c := useRedisTestContainer(t)

	bus := &recordingDispatcher{}
	if err := c.Instance("event.dispatcher", eventcontract.Dispatcher(bus)); err != nil {
		t.Fatalf("register dispatcher error = %v", err)
	}
	if err := c.Instance("redis", newTestManager(t, server)); err != nil {
		t.Fatalf("Use manager error = %v", err)
	}
	t.Cleanup(func() { UseEventSink(nil) })
	if err := (ServiceProvider{}).Boot(testProviderApp{container: c}); err != nil {
		t.Fatalf("provider boot error = %v", err)
	}
	client, err := Client()
	if err != nil {
		t.Fatalf("Client error = %v", err)
	}
	if err := client.Set(context.Background(), "event:key", "value", 0).Err(); err != nil {
		t.Fatalf("client set error = %v", err)
	}
	if len(bus.events) != 1 || bus.events[0].Name() != EventCommandExecuted {
		t.Fatalf("events = %#v", bus.events)
	}
}

func TestFacadeClientDispatchesCommandEvents(t *testing.T) {
	server := miniredis.RunT(t)
	c := useRedisTestContainer(t)

	bus := &recordingDispatcher{}
	if err := c.Instance("event.dispatcher", eventcontract.Dispatcher(bus)); err != nil {
		t.Fatalf("register dispatcher error = %v", err)
	}
	if err := c.Instance("redis", newTestManager(t, server)); err != nil {
		t.Fatalf("Use manager error = %v", err)
	}
	t.Cleanup(func() { UseEventSink(nil) })
	if err := (ServiceProvider{}).Boot(testProviderApp{container: c}); err != nil {
		t.Fatalf("provider boot error = %v", err)
	}

	client, err := Client()
	if err != nil {
		t.Fatalf("Client error = %v", err)
	}
	if err := client.Incr(context.Background(), "facade:counter").Err(); err != nil {
		t.Fatalf("facade client incr error = %v", err)
	}
	if len(bus.events) != 1 || bus.events[0].Name() != EventCommandExecuted {
		t.Fatalf("events = %#v", bus.events)
	}
	ev, ok := bus.events[0].(CommandExecutedEvent)
	if !ok {
		t.Fatalf("event type = %T, want CommandExecutedEvent", bus.events[0])
	}
	if ev.Command != "incr" || ev.ConnectionName != "default" {
		t.Fatalf("event = %#v", ev)
	}
}

func TestPipelineDispatchesOneBatchSuccessEvent(t *testing.T) {
	server := miniredis.RunT(t)
	manager := newTestManager(t, server)
	t.Cleanup(func() { _ = manager.Close(context.Background()) })
	UseEventSink(nil)

	conn, err := manager.Connection("cache")
	if err != nil {
		t.Fatalf("Connection(cache) error = %v", err)
	}
	singleEvents := 0
	conn.Listen(func(context.Context, CommandExecuted) { singleEvents++ })
	bus := &recordingDispatcher{}
	UseEventSink(func(ctx context.Context, ev eventcontract.Event) {
		bus.Dispatch(ctx, ev)
	})
	t.Cleanup(func() { UseEventSink(nil) })

	pipe := conn.Client().Pipeline()
	pipe.Set(context.Background(), "pipe:key", "value", 0)
	pipe.Get(context.Background(), "pipe:key")
	if _, err := pipe.Exec(context.Background()); err != nil {
		t.Fatalf("pipeline exec error = %v", err)
	}
	if singleEvents != 0 {
		t.Fatalf("single command events from pipeline = %d, want 0", singleEvents)
	}
	if len(bus.events) != 1 || bus.events[0].Name() != EventCommandBatchExecuted {
		t.Fatalf("events = %#v", bus.events)
	}
	ev, ok := bus.events[0].(CommandBatchExecutedEvent)
	if !ok {
		t.Fatalf("event type = %T, want CommandBatchExecutedEvent", bus.events[0])
	}
	if ev.ConnectionName != "cache" || len(ev.Commands) != 2 || ev.Commands[0].Command != "set" || ev.Commands[1].Command != "get" {
		t.Fatalf("batch event = %#v", ev)
	}
}

func TestTxPipelineDispatchesOneBatchEventWithoutTransactionWrappers(t *testing.T) {
	server := miniredis.RunT(t)
	manager := newTestManager(t, server)
	t.Cleanup(func() { _ = manager.Close(context.Background()) })
	UseEventSink(nil)

	conn, err := manager.Connection("cache")
	if err != nil {
		t.Fatalf("Connection(cache) error = %v", err)
	}
	bus := &recordingDispatcher{}
	UseEventSink(func(ctx context.Context, ev eventcontract.Event) {
		bus.Dispatch(ctx, ev)
	})
	t.Cleanup(func() { UseEventSink(nil) })

	pipe := conn.Client().TxPipeline()
	pipe.Set(context.Background(), "txpipe:key", "value", 0)
	pipe.Get(context.Background(), "txpipe:key")
	if _, err := pipe.Exec(context.Background()); err != nil {
		t.Fatalf("tx pipeline exec error = %v", err)
	}
	if len(bus.events) != 1 || bus.events[0].Name() != EventCommandBatchExecuted {
		t.Fatalf("events = %#v", bus.events)
	}
	ev, ok := bus.events[0].(CommandBatchExecutedEvent)
	if !ok {
		t.Fatalf("event type = %T, want CommandBatchExecutedEvent", bus.events[0])
	}
	if len(ev.Commands) != 2 || ev.Commands[0].Command != "set" || ev.Commands[1].Command != "get" {
		t.Fatalf("tx pipeline commands = %#v, want only set/get", ev.Commands)
	}
}

func TestTxPipelineDispatchesOneBatchFailureEventWithoutTransactionWrappers(t *testing.T) {
	server := miniredis.RunT(t)
	manager := newTestManager(t, server)
	t.Cleanup(func() { _ = manager.Close(context.Background()) })
	UseEventSink(nil)

	conn, err := manager.Connection("cache")
	if err != nil {
		t.Fatalf("Connection(cache) error = %v", err)
	}
	bus := &recordingDispatcher{}
	UseEventSink(func(ctx context.Context, ev eventcontract.Event) {
		bus.Dispatch(ctx, ev)
	})
	t.Cleanup(func() { UseEventSink(nil) })

	pipe := conn.Client().TxPipeline()
	pipe.Set(context.Background(), "txpipe:bad", "value", 0)
	pipe.Do(context.Background(), "get")
	if _, err := pipe.Exec(context.Background()); err == nil {
		t.Fatal("tx pipeline exec should fail")
	}
	if len(bus.events) != 1 || bus.events[0].Name() != EventCommandBatchFailed {
		t.Fatalf("events = %#v", bus.events)
	}
	ev, ok := bus.events[0].(CommandBatchFailedEvent)
	if !ok {
		t.Fatalf("event type = %T, want CommandBatchFailedEvent", bus.events[0])
	}
	if ev.Error == nil || len(ev.Commands) != 2 || ev.Commands[0].Command != "set" || ev.Commands[1].Command != "get" || ev.Commands[1].Error == nil {
		t.Fatalf("tx pipeline failure commands = %#v", ev.Commands)
	}
}

func TestPipelineDispatchesOneBatchFailureEvent(t *testing.T) {
	server := miniredis.RunT(t)
	manager := newTestManager(t, server)
	t.Cleanup(func() { _ = manager.Close(context.Background()) })
	UseEventSink(nil)

	conn, err := manager.Connection("cache")
	if err != nil {
		t.Fatalf("Connection(cache) error = %v", err)
	}
	bus := &recordingDispatcher{}
	UseEventSink(func(ctx context.Context, ev eventcontract.Event) {
		bus.Dispatch(ctx, ev)
	})
	t.Cleanup(func() { UseEventSink(nil) })

	pipe := conn.Client().Pipeline()
	pipe.Set(context.Background(), "pipe:bad", "value", 0)
	pipe.Do(context.Background(), "get")
	if _, err := pipe.Exec(context.Background()); err == nil {
		t.Fatal("pipeline exec should fail")
	}
	if len(bus.events) != 1 || bus.events[0].Name() != EventCommandBatchFailed {
		t.Fatalf("events = %#v", bus.events)
	}
	ev, ok := bus.events[0].(CommandBatchFailedEvent)
	if !ok {
		t.Fatalf("event type = %T, want CommandBatchFailedEvent", bus.events[0])
	}
	if ev.Error == nil || len(ev.Commands) != 2 || ev.Commands[1].Command != "get" || ev.Commands[1].Error == nil {
		t.Fatalf("batch failure event = %#v", ev)
	}
}

// TestServiceProviderRegisterBuildsManagerFromConfig 覆盖 Redis provider 的完整注册路径。
//
// 需求背景：Redis provider 是 framework default provider，业务应用不应手动注册 redis factory。
// Register 只声明懒加载工厂，第一次 Resolve 时才读取 config.default 并构造 Manager。
func TestServiceProviderRegisterBuildsManagerFromConfig(t *testing.T) {
	server := miniredis.RunT(t)
	configpkg.Add("database", func() map[string]any {
		return map[string]any{
			"redis": map[string]any{
				"default": map[string]any{"addr": server.Addr()},
				"cache":   map[string]any{"host": "127.0.0.1", "port": server.Port()},
			},
		}
	})
	repo, err := configpkg.NewFromFile(t.TempDir() + "/missing.env")
	if err != nil {
		t.Fatalf("NewFromFile error = %v", err)
	}

	c := useRedisTestContainer(t)
	if err := c.Instance("config.default", repo); err != nil {
		t.Fatalf("register config error = %v", err)
	}
	provider := ServiceProvider{}
	if provider.Name() != "redis" {
		t.Fatalf("provider name = %q", provider.Name())
	}
	if err := provider.Register(testProviderApp{container: c}); err != nil {
		t.Fatalf("provider register error = %v", err)
	}
	if c.Resolved(serviceKey) {
		t.Fatal("redis service should remain lazy after Register")
	}
	manager, err := NewManagerFromConfig()
	if err != nil {
		t.Fatalf("NewManagerFromConfig error = %v", err)
	}
	if err := manager.Close(context.Background()); err != nil {
		t.Fatalf("close direct manager error = %v", err)
	}
	factory := Resolve()
	if factory == nil {
		t.Fatal("Resolve factory returned nil")
	}
	conn, err := factory.Connection("cache")
	if err != nil {
		t.Fatalf("factory cache connection error = %v", err)
	}
	if err := conn.Client().Ping(context.Background()).Err(); err != nil {
		t.Fatalf("cache ping error = %v", err)
	}
	defaultConn, err := container.Make[rediscontract.Connection](defaultConnectionKey)
	if err != nil {
		t.Fatalf("resolve redis.connection error = %v", err)
	}
	if defaultConn.Name() != "default" {
		t.Fatalf("default connection name = %q", defaultConn.Name())
	}
}

// TestConnectionFromClientAndEventSinkNilBranches 覆盖外部 client 与空监听器分支。
//
// 逻辑说明：NewConnectionFromClient 是测试和高级集成保留入口；空监听器、空事件 sink
// 都应是 no-op，不能影响 Redis 命令执行。
func TestConnectionFromClientAndEventSinkNilBranches(t *testing.T) {
	server := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	conn := NewConnectionFromClient("", client)
	conn.Listen(nil)
	conn.ListenForFailures(nil)
	UseEventSink(nil)
	if conn.Name() != DefaultConnectionName {
		t.Fatalf("connection name = %q", conn.Name())
	}
	if err := conn.Client().Set(context.Background(), "direct:key", "value", 0).Err(); err != nil {
		t.Fatalf("direct set error = %v", err)
	}
	value, err := client.Get(context.Background(), "direct:key").Result()
	if err != nil || value != "value" {
		t.Fatalf("direct get = %q, %v", value, err)
	}
}

func TestConfigFromFacadeStrictRequiresDatabaseRedis(t *testing.T) {
	configpkg.Add("database", func() map[string]any {
		return map[string]any{}
	})
	repo, err := configpkg.NewFromFile(t.TempDir() + "/missing.env")
	if err != nil {
		t.Fatalf("NewFromFile error = %v", err)
	}

	c := useRedisTestContainer(t)
	if err := c.Instance("config.default", repo); err != nil {
		t.Fatalf("register config error = %v", err)
	}

	if _, err := ConfigFromFacadeStrict(); err == nil {
		t.Fatal("ConfigFromFacadeStrict should fail when database.redis is missing")
	}
	if _, err := NewManagerFromConfig(); err == nil {
		t.Fatal("NewManagerFromConfig should fail when database.redis is missing")
	}
}

func TestCommandEventsKeepRawParametersLikeLaravel(t *testing.T) {
	server := miniredis.RunT(t)
	manager := newTestManager(t, server)
	t.Cleanup(func() { _ = manager.Close(context.Background()) })

	conn, err := manager.Connection("cache")
	if err != nil {
		t.Fatalf("Connection(cache) error = %v", err)
	}

	var success CommandExecuted
	var failure CommandFailed
	conn.Listen(func(_ context.Context, ev CommandExecuted) {
		success = ev
	})
	conn.ListenForFailures(func(_ context.Context, ev CommandFailed) {
		failure = ev
	})

	if err := conn.Client().Set(context.Background(), "secret:key", "super-secret", 0).Err(); err != nil {
		t.Fatalf("set command error = %v", err)
	}
	if err := conn.Client().Do(context.Background(), "get", "secret:key", "super-secret").Err(); err == nil {
		t.Fatal("invalid get command should fail")
	}
	if len(success.Parameters) != 2 || anyString(success.Parameters[0]) != "secret:key" || anyString(success.Parameters[1]) != "super-secret" {
		t.Fatalf("success parameters should remain raw like Laravel: %#v", success.Parameters)
	}
	if len(failure.Parameters) != 2 || anyString(failure.Parameters[0]) != "secret:key" || anyString(failure.Parameters[1]) != "super-secret" {
		t.Fatalf("failure parameters should remain raw like Laravel: %#v", failure.Parameters)
	}
}

func TestPipelineFailureEventsKeepRawParametersLikeLaravel(t *testing.T) {
	server := miniredis.RunT(t)
	manager := newTestManager(t, server)
	t.Cleanup(func() { _ = manager.Close(context.Background()) })
	UseEventSink(nil)

	conn, err := manager.Connection("cache")
	if err != nil {
		t.Fatalf("Connection(cache) error = %v", err)
	}
	bus := &recordingDispatcher{}
	UseEventSink(func(ctx context.Context, ev eventcontract.Event) {
		bus.Dispatch(ctx, ev)
	})
	t.Cleanup(func() { UseEventSink(nil) })

	pipe := conn.Client().Pipeline()
	pipe.Set(context.Background(), "pipe:secret", "super-secret", 0)
	pipe.Do(context.Background(), "get", "pipe:secret", "super-secret")
	if _, err := pipe.Exec(context.Background()); err == nil {
		t.Fatal("pipeline exec should fail")
	}
	if len(bus.events) != 1 {
		t.Fatalf("events = %#v", bus.events)
	}
	ev, ok := bus.events[0].(CommandBatchFailedEvent)
	if !ok {
		t.Fatalf("event type = %T, want CommandBatchFailedEvent", bus.events[0])
	}
	if len(ev.Commands) != 2 || len(ev.Commands[0].Parameters) != 2 || anyString(ev.Commands[0].Parameters[0]) != "pipe:secret" || anyString(ev.Commands[0].Parameters[1]) != "super-secret" {
		t.Fatalf("batch command parameters should remain raw like Laravel: %#v", ev.Commands)
	}
}

func TestManagerConnectionSupportsURLAndMappedOptions(t *testing.T) {
	server := miniredis.RunT(t)
	manager, err := NewManager(Config{
		Connections: map[string]ConnectionConfig{
			"default": {
				Name: "default",
				Host: server.Host(),
				Port: server.Port(),
				DB:   3,
				Options: map[string]any{
					"name":          "redis-manager-test",
					"read_timeout":  "4s",
					"write_timeout": "5s",
					"timeout":       "6s",
					"max_retries":   5,
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("NewManager error = %v", err)
	}
	t.Cleanup(func() { _ = manager.Close(context.Background()) })

	conn, err := manager.Connection()
	if err != nil {
		t.Fatalf("Connection error = %v", err)
	}
	client, ok := conn.Client().(*goredis.Client)
	if !ok {
		t.Fatalf("client type = %T, want *redis.Client", conn.Client())
	}
	if client.Options().DB != 3 || client.Options().ClientName != "redis-manager-test" || client.Options().MaxRetries != 5 {
		t.Fatalf("client options = %#v", client.Options())
	}
	if client.Options().DialTimeout != 6*time.Second || client.Options().ReadTimeout != 4*time.Second || client.Options().WriteTimeout != 5*time.Second {
		t.Fatalf("timeouts = %s/%s/%s", client.Options().DialTimeout, client.Options().ReadTimeout, client.Options().WriteTimeout)
	}
	if err := conn.Client().Set(context.Background(), "url:key", "value", 0).Err(); err != nil {
		t.Fatalf("set through url-configured client error = %v", err)
	}
	server.Select(3)
	value, err := server.Get("url:key")
	if err != nil || value != "value" {
		t.Fatalf("server db3 value = %q, %v", value, err)
	}
}

func TestManagerConnectionSupportsRedisURL(t *testing.T) {
	server := miniredis.RunT(t)
	manager, err := NewManager(Config{
		Connections: map[string]ConnectionConfig{
			"default": {
				Name: "default",
				Options: map[string]any{
					"url":  "redis://" + server.Addr() + "/4?max_retries=5&dial_timeout=1s&read_timeout=2s&write_timeout=3s",
					"name": "redis-url-test",
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("NewManager error = %v", err)
	}
	t.Cleanup(func() { _ = manager.Close(context.Background()) })

	conn, err := manager.Connection()
	if err != nil {
		t.Fatalf("Connection error = %v", err)
	}
	client, ok := conn.Client().(*goredis.Client)
	if !ok {
		t.Fatalf("client type = %T, want *redis.Client", conn.Client())
	}
	if client.Options().DB != 4 || client.Options().ClientName != "redis-url-test" || client.Options().MaxRetries != 5 {
		t.Fatalf("client options = %#v", client.Options())
	}
	if client.Options().DialTimeout != time.Second || client.Options().ReadTimeout != 2*time.Second || client.Options().WriteTimeout != 3*time.Second {
		t.Fatalf("timeouts = %s/%s/%s", client.Options().DialTimeout, client.Options().ReadTimeout, client.Options().WriteTimeout)
	}
	if err := conn.Client().Set(context.Background(), "url:key", "value", 0).Err(); err != nil {
		t.Fatalf("set through url-configured client error = %v", err)
	}
	server.Select(4)
	value, err := server.Get("url:key")
	if err != nil || value != "value" {
		t.Fatalf("server db4 value = %q, %v", value, err)
	}
}

func TestManagerConnectionFailsOnInvalidRedisURL(t *testing.T) {
	manager, err := NewManager(Config{
		Connections: map[string]ConnectionConfig{
			"default": {
				Name: "default",
				Options: map[string]any{
					"url": "not-a-redis-url",
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("NewManager error = %v", err)
	}

	if _, err := manager.Connection(); err == nil {
		t.Fatal("Connection should fail on invalid redis url")
	}
}

func TestManagerConnectionSupportsTLSSchemeOption(t *testing.T) {
	server := miniredis.RunT(t)
	manager, err := NewManager(Config{
		Connections: map[string]ConnectionConfig{
			"default": {
				Name: "default",
				Host: server.Host(),
				Port: server.Port(),
				Options: map[string]any{
					"scheme": "tls",
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("NewManager error = %v", err)
	}
	conn, err := manager.Connection()
	if err != nil {
		t.Fatalf("Connection error = %v", err)
	}
	client, ok := conn.Client().(*goredis.Client)
	if !ok {
		t.Fatalf("client type = %T, want *redis.Client", conn.Client())
	}
	if client.Options().TLSConfig == nil {
		t.Fatal("TLSConfig should be set when scheme=tls")
	}
}

type testProviderApp struct {
	container *container.Container
}

func (a testProviderApp) Container() containercontract.Container { return a.container }

type recordingDispatcher struct {
	events []eventcontract.Event
}

func (d *recordingDispatcher) Listen(string, eventcontract.Listener) {}
func (d *recordingDispatcher) ListenFunc(string, func(context.Context, eventcontract.Event) error) {
}
func (d *recordingDispatcher) Subscribe(eventcontract.Subscriber) {}
func (d *recordingDispatcher) Forget(string)                      {}
func (d *recordingDispatcher) Has(string) bool                    { return false }
func (d *recordingDispatcher) Dispatch(_ context.Context, ev eventcontract.Event) {
	d.events = append(d.events, ev)
}

func newTestManager(t *testing.T, server *miniredis.Miniredis) *Manager {
	t.Helper()
	manager, err := NewManager(Config{
		DefaultName: "default",
		Connections: map[string]ConnectionConfig{
			"default": {Name: "default", Addr: server.Addr()},
			"cache":   {Name: "cache", Addr: server.Addr()},
		},
	})
	if err != nil {
		t.Fatalf("NewManager error = %v", err)
	}
	return manager
}

type redisReportedException struct {
	err    error
	fields map[string]any
}

func captureRedisReports(t *testing.T) <-chan redisReportedException {
	t.Helper()
	c := useRedisTestContainer(t)
	reports := make(chan redisReportedException, 2)
	handler := goexception.New()
	handler.Reporters = append(handler.Reporters, func(_ any, err error, fields map[string]any) {
		copied := make(map[string]any, len(fields))
		for key, value := range fields {
			copied[key] = value
		}
		reports <- redisReportedException{err: err, fields: copied}
	})
	if err := c.Instance("exception.handler", handler); err != nil {
		t.Fatalf("bind exception handler: %v", err)
	}
	manager, err := logger.NewManager(logger.Config{
		Default:  "null",
		Channels: map[string]logger.ChannelOptions{"null": {Driver: "null", Level: "debug"}},
	})
	if err != nil {
		t.Fatalf("new logger manager: %v", err)
	}
	if err := c.Instance("logger.manager", manager); err != nil {
		t.Fatalf("bind logger manager: %v", err)
	}
	return reports
}

func waitRedisReport(t *testing.T, reports <-chan redisReportedException) redisReportedException {
	t.Helper()
	select {
	case report := <-reports:
		return report
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for redis listener panic report")
		return redisReportedException{}
	}
}
