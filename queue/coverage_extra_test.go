package queue

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/prismgo/framework/queue/payload"
	"github.com/prismgo/framework/queue/state"
)

func TestFluentOptionsCoverDelayAndCacheViaBuilders(t *testing.T) {
	// 测试目的：链式 DispatchOption API 要和函数式 API 写入同一组内部字段，
	// 否则业务代码通过 fluent 形式配置唯一锁或防抖 store 时会被静默忽略。
	repo := useTestCache(t, "memory").Default()

	options := applyOptions(
		OnConnection("sync").
			DelaySeconds(7).
			Unique("invoice:1", time.Minute).
			UniqueVia(repo).
			UniqueUntilProcessing().
			Debounce("invoice", 2*time.Minute).
			DebounceVia(repo),
	)

	if options.Delay != 7*time.Second {
		t.Fatalf("DelaySeconds() delay = %s, want 7s", options.Delay)
	}
	if options.UniqueKey != "invoice:1" || options.UniqueFor != time.Minute || !options.UniqueUntil {
		t.Fatalf("unique options were not preserved: %#v", options)
	}
	if options.uniqueVia != repo {
		t.Fatalf("unique via store = %#v, want configured repo", options.uniqueVia)
	}
	if options.DebounceKey != "invoice" || options.DebounceFor != 2*time.Minute {
		t.Fatalf("debounce options were not preserved: %#v", options)
	}
	if options.debounceVia != repo {
		t.Fatalf("debounce via store = %#v, want configured repo", options.debounceVia)
	}
}

func TestEventSinkSnapshotAndObserverBoundaries(t *testing.T) {
	// 测试目的：全局 sink 的快照读取、nil 事件短路和 worker 级 observer 注入都属于
	// 事件桥的并发安全边界，需要独立于具体 driver 验证。
	var nilCtx context.Context
	UseEventSink(nil)
	t.Cleanup(func() { UseEventSink(nil) })
	if CurrentEventSink() != nil {
		t.Fatal("CurrentEventSink() should be nil after reset")
	}

	type contextKey string
	const observedKey contextKey = "observed"
	seen := 0
	UseEventSink(func(ctx context.Context, ev Event) {
		seen++
		if got, _ := ctx.Value(observedKey).(string); got != "yes" {
			t.Fatalf("sink context marker = %q, want yes", got)
		}
		if ev.Name() != EventJobProcessed {
			t.Fatalf("event name = %q, want %q", ev.Name(), EventJobProcessed)
		}
	})
	if CurrentEventSink() == nil {
		t.Fatal("CurrentEventSink() should expose the installed sink")
	}

	ctx := contextWithEventObserver(nilCtx, func(ctx context.Context, ev Event) context.Context {
		if ev == nil {
			t.Fatal("observer should not receive nil event")
		}
		return context.WithValue(ctx, observedKey, "yes")
	})
	fire(ctx, JobProcessed{JobID: "job-1"})
	fire(ctx, nil)
	if seen != 1 {
		t.Fatalf("sink calls = %d, want 1", seen)
	}

	base := context.Background()
	if got := contextWithEventObserver(base, nil); got != base {
		t.Fatal("nil observer should return the original context")
	}
	if eventObserverFromContext(nilCtx) != nil {
		t.Fatal("nil context should not contain an observer")
	}
}

func TestRegistryEdgeBranches(t *testing.T) {
	// 测试目的：注册表错误边界要返回显式错误或 no-op，不能 panic；空 payload
	// 反序列化则应只实例化目标 Job，不要求 codec 参与。
	var nilRegistry *Registry
	nilRegistry.RegisterJobType(&testJob{})
	if nilRegistry.Has("anything") {
		t.Fatal("nil registry should never report registered jobs")
	}
	if _, err := nilRegistry.Unmarshal("missing", nil); !errors.Is(err, ErrJobNotRegistered) {
		t.Fatalf("nil registry Unmarshal error = %v, want ErrJobNotRegistered", err)
	}

	registry := NewRegistry()
	registry.RegisterJobType(&testJob{})
	name, err := JobTypeName(&testJob{})
	if err != nil {
		t.Fatalf("JobTypeName(testJob) error = %v", err)
	}
	if !registry.Has(name) {
		t.Fatalf("registry should have %s", name)
	}
	job, err := registry.unmarshalWithCodec(name, nil, nil)
	if err != nil {
		t.Fatalf("unmarshal empty payload: %v", err)
	}
	if _, ok := job.(*testJob); !ok {
		t.Fatalf("unmarshaled job = %T, want *testJob", job)
	}

	registry.registerFactory("nil-factory", func() Job { return nil })
	if _, err := registry.Unmarshal("nil-factory", nil); err == nil {
		t.Fatal("nil factory should return an error")
	}
	if _, err := jobTypeName(reflect.TypeOf(struct{}{})); err == nil {
		t.Fatal("anonymous struct type should be rejected")
	}
	if got := newJobFromType(nil); got != nil {
		t.Fatalf("newJobFromType(nil) = %#v, want nil", got)
	}
}

func TestConfigCastingAndConnectionDefaultsCoverFallbacks(t *testing.T) {
	// 测试目的：配置解析允许来自 env/JSON 的多种标量类型；负数和未知类型
	// 必须稳定回退到调用方给定默认值。
	if got := castInt(int64(12), 1); got != 12 {
		t.Fatalf("castInt(int64) = %d, want 12", got)
	}
	if got := castInt(float64(4.8), 1); got != 4 {
		t.Fatalf("castInt(float64) = %d, want 4", got)
	}
	if got := castInt(int64(-1), 9); got != 9 {
		t.Fatalf("castInt(negative int64) = %d, want fallback", got)
	}
	if got := castInt(float64(-1), 9); got != 9 {
		t.Fatalf("castInt(negative float64) = %d, want fallback", got)
	}
	if got := castInt(struct{}{}, 9); got != 9 {
		t.Fatalf("castInt(unknown) = %d, want fallback", got)
	}
	if got := castString(fmt.Stringer(time.Second)); got != "1s" {
		t.Fatalf("castString(Stringer) = %q, want 1s", got)
	}
	if mapHasKey(nil, "retry_after") {
		t.Fatal("nil map should not report a key")
	}

	cfg := connectionConfigFromMap(map[string]any{
		"driver":      []byte("sync"),
		"queue":       "",
		"retry_after": "15",
		"block_for":   "bad",
	}, "fallback")
	if cfg.Driver != "sync" || cfg.Queue != "fallback" || cfg.RetryAfter != 15*time.Second {
		t.Fatalf("connection config parsed incorrectly: %#v", cfg)
	}
	if !cfg.retryAfterConfigured {
		t.Fatal("retry_after presence should be recorded even when parsed from string")
	}
}

func TestRuntimeHelperErrorBranches(t *testing.T) {
	// 测试目的：runtime helper 遇到 nil runtime/envelope 时应返回清晰错误，
	// 避免 worker 或 driver 边界把配置问题变成 panic。
	if _, err := payloadForQueueEnvelope(nil, nil); err == nil {
		t.Fatal("nil envelope should return an error")
	}
	if _, err := encodeQueueEnvelope(nil, &payload.Envelope{}); err == nil {
		t.Fatal("nil runtime should not encode envelopes")
	}
	if _, err := envelopeFromQueuePayload(nil, nil); err == nil {
		t.Fatal("nil runtime should not decode envelopes")
	}
}

func TestDispatcherNilContractMethods(t *testing.T) {
	// 测试目的：contracts/queue.Dispatcher 的 lifecycle 方法在 nil dispatcher 上
	// 应保持已有的错误/no-op 语义，方便容器装配失败时调用方得到明确反馈。
	var dispatcher *Dispatcher
	if err := dispatcher.RequestRestart(context.Background()); err == nil {
		t.Fatal("nil dispatcher RequestRestart should fail")
	}
	if err := dispatcher.Close(); err != nil {
		t.Fatalf("nil dispatcher Close() error = %v, want nil", err)
	}
}

func TestMiddlewareBuilderAndLimiterBranches(t *testing.T) {
	// 测试目的：middleware builder 的 Via/ReleaseAfter 分支和限流命中分支都只依赖 cache
	// 契约，使用内存 store 即可验证错误类型与延迟语义。
	repo := useTestCache(t, "memory").Default()
	ctx := context.Background()

	overlap := WithoutOverlapping("coverage").ReleaseAfter(3 * time.Second).Via(repo)
	if err := overlap.Handle(ctx, &testJob{}, func(context.Context) error { return nil }); err != nil {
		t.Fatalf("overlap first handle: %v", err)
	}

	throttle := ThrottlesExceptions(1, time.Second).Via(repo)
	err := throttle.Handle(ctx, &testJob{}, func(context.Context) error {
		return errors.New("boom")
	})
	if delay, ok := ReleaseDelay(err); !ok || delay != time.Second {
		t.Fatalf("throttle error = %v, delay = %s, want release after 1s", err, delay)
	}

	limited := RateLimit("coverage-rate", 1, time.Second)
	if err := limited.Handle(ctx, &testJob{}, func(context.Context) error { return nil }); err != nil {
		t.Fatalf("first rate limited call: %v", err)
	}
	err = limited.Handle(ctx, &testJob{}, func(context.Context) error {
		t.Fatal("limited call should not reach next")
		return nil
	})
	if delay, ok := ReleaseDelay(err); !ok || delay <= 0 {
		t.Fatalf("rate limit error = %v, delay = %s, want release error", err, delay)
	}
}

func TestDefaultErrorMessagesAndProviderName(t *testing.T) {
	// 测试目的：nil error 构造器和 provider identity 是公开契约表面，
	// 覆盖默认消息可以防止以后被无意改成空错误。
	if got := ReleaseAfter(0, nil).Error(); got != "queue: release job" {
		t.Fatalf("ReleaseAfter(nil).Error() = %q", got)
	}
	if got := Fail(nil).Error(); got != "queue: fail job" {
		t.Fatalf("Fail(nil).Error() = %q", got)
	}
	if name := (ServiceProvider{}).Name(); name != "queue" {
		t.Fatalf("ServiceProvider.Name() = %q, want queue", name)
	}
}

func TestQueuePageAndBulkClampBoundaries(t *testing.T) {
	// 测试目的：分页和 bulk accepted 归一化保护调用方传入的越界参数，
	// 这些 helper 是失败任务列表和批量投递状态计算的共同边界。
	page := normalizeQueuePage(state.PageRequest{Page: -1, PageSize: state.MaxPageSize + 1})
	if page.Page != 1 || page.PageSize != state.MaxPageSize {
		t.Fatalf("normalized page = %#v", page)
	}
	items := []int{1, 2, 3}
	if got := queuePageSlice(items, state.PageRequest{Page: 3, PageSize: 2}); len(got) != 0 {
		t.Fatalf("out-of-range page = %#v, want empty", got)
	}
	if got := clampBulkAccepted(-1, 3); got != 0 {
		t.Fatalf("clamp negative = %d, want 0", got)
	}
	if got := clampBulkAccepted(9, 3); got != 3 {
		t.Fatalf("clamp overflow = %d, want 3", got)
	}
}

func TestContractDispatchOptionsCoverAdvancedFields(t *testing.T) {
	// 测试目的：跨包只读 contract 转换必须保留高级投递语义，尤其是 batch、
	// unique、debounce、加密和 Horizon metadata。
	deadline := time.Now().Add(time.Hour)
	options := applyOptions(dispatchOptionsFromQueueOptions(contractDispatchOptions{
		connection:    "sync",
		queue:         "critical",
		delay:         time.Second,
		tries:         2,
		maxExceptions: 3,
		timeout:       4 * time.Second,
		failOnTimeout: true,
		encrypted:     true,
		backoff:       []time.Duration{time.Second},
		retryUntil:    deadline,
		batchID:       "batch-1",
		uniqueKey:     "unique-1",
		uniqueFor:     time.Minute,
		uniqueUntil:   true,
		debounceKey:   "debounce-1",
		debounceFor:   2 * time.Minute,
		tags:          []string{"billing"},
		silenced:      true,
	})...)

	if options.MaxExceptions != 3 || !options.FailOnTimeout || !options.Encrypted {
		t.Fatalf("advanced retry/encryption options missing: %#v", options)
	}
	if !options.RetryUntil.Equal(deadline) || options.BatchID != "batch-1" {
		t.Fatalf("deadline or batch option missing: %#v", options)
	}
	if options.UniqueKey != "unique-1" || options.UniqueFor != time.Minute || !options.UniqueUntil {
		t.Fatalf("unique contract options missing: %#v", options)
	}
	if options.DebounceKey != "debounce-1" || options.DebounceFor != 2*time.Minute {
		t.Fatalf("debounce contract options missing: %#v", options)
	}
	if len(options.Tags) != 1 || options.Tags[0] != "billing" || !options.Silenced {
		t.Fatalf("metadata contract options missing: %#v", options)
	}
}

func TestConnectorAndCacheHelperBoundaries(t *testing.T) {
	// 测试目的：connector 入口必须拒绝缺失的规范化配置；cache helper 则要稳定清理
	// 控制字符和默认 TTL，避免 driver 侧生成非法 key。
	if _, err := connectorSpec("sync", nil); err == nil {
		t.Fatal("connectorSpec without _spec should fail")
	}
	if _, err := (SyncConnector{}).Connect(context.Background(), "sync", nil); err == nil {
		t.Fatal("sync connector without _spec should fail")
	}
	if _, err := (SyncConnector{}).Connect(context.Background(), "sync", connectorConfig(ConnectionConfig{Driver: "sync"})); err != nil {
		t.Fatalf("sync connector with spec: %v", err)
	}
	_, err := (RabbitMQConnector{}).Connect(context.Background(), "rabbit", connectorConfig(ConnectionConfig{
		Driver:               "rabbitmq",
		RetryAfter:           time.Second,
		retryAfterConfigured: true,
	}))
	if !errors.Is(err, ErrUnsupportedRetryAfter) {
		t.Fatalf("rabbit retry_after error = %v, want ErrUnsupportedRetryAfter", err)
	}

	if got := cleanCacheKey(":\x00tenant:1\x7f:"); got != "tenant:1" {
		t.Fatalf("cleanCacheKey() = %q, want tenant:1", got)
	}
	if got := normalizeQueueCacheTTL(5 * time.Second); got != 5*time.Second {
		t.Fatalf("normalizeQueueCacheTTL(positive) = %s", got)
	}
}

func TestRuntimeAndRestartStoreBoundaries(t *testing.T) {
	// 测试目的：runtime nil/empty middleware paths and restart stores are lifecycle
	// boundaries used by managers and workers without requiring a transport driver.
	var nilCtx context.Context
	var runtime *Runtime
	runtime.UseMiddleware(MiddlewareFunc(func(context.Context, Job, Next) error { return nil }))
	if snapshot := runtime.middlewareSnapshot(); snapshot != nil {
		t.Fatalf("nil runtime middleware snapshot = %#v, want nil", snapshot)
	}
	runtime = &Runtime{}
	runtime.UseMiddleware()
	if len(runtime.middlewareSnapshot()) != 0 {
		t.Fatal("empty middleware registration should not append")
	}

	memoryRestart := NewMemoryRestartStore()
	at := time.Now()
	if err := memoryRestart.RequestRestart(nilCtx, at); err != nil {
		t.Fatalf("memory restart request: %v", err)
	}
	got, err := memoryRestart.RestartRequestedAt(nilCtx)
	if err != nil || !got.Equal(at) {
		t.Fatalf("memory restart at = %s, err = %v, want %s", got, err, at)
	}
	if err := (*MemoryRestartStore)(nil).RequestRestart(nilCtx, at); err != nil {
		t.Fatalf("nil memory restart request: %v", err)
	}

	useTestCache(t, "memory")
	cacheRestart := NewCacheRestartStore("", "")
	if err := cacheRestart.RequestRestart(nilCtx, at); err != nil {
		t.Fatalf("cache restart request: %v", err)
	}
	cached, err := cacheRestart.RestartRequestedAt(nilCtx)
	if err != nil || cached.IsZero() {
		t.Fatalf("cache restart at = %s, err = %v, want stored time", cached, err)
	}
	if _, err := (*CacheRestartStore)(nil).RestartRequestedAt(nilCtx); err != nil {
		t.Fatalf("nil cache restart read: %v", err)
	}
}

func TestStateStoreAndEncodingErrorBoundaries(t *testing.T) {
	// 测试目的：状态存储 driver 配置错误要在构造期显式返回；restart 时间戳和
	// encrypted payload 解析遇到非法输入时应稳定回落为错误或空时间。
	if _, err := buildFailedStore(Config{Failed: StateStoreConfig{Driver: "bogus"}}, nil); err == nil {
		t.Fatal("unknown failed store driver should fail")
	}
	if _, err := buildBatchStore(Config{Batching: StateStoreConfig{Driver: "bogus"}}, nil); err == nil {
		t.Fatal("unknown batch store driver should fail")
	}
	if _, ok := restartNano("not-a-number"); ok {
		t.Fatal("invalid restart timestamp string should not parse")
	}
	if got, ok := restartNano(float64(12)); !ok || got != 12 {
		t.Fatalf("restartNano(float64) = %d/%v, want 12/true", got, ok)
	}

	manager := useTestCache(t, "memory")
	store := NewCacheRestartStore("", "missing-restart-key")
	got, err := store.RestartRequestedAt(context.Background())
	if err != nil || !got.IsZero() {
		t.Fatalf("missing cache restart = %s, err = %v, want zero time", got, err)
	}
	if err := manager.Default().Forever(context.Background(), "bad-restart-key", "bad"); err != nil {
		t.Fatalf("seed bad restart key: %v", err)
	}
	store = NewCacheRestartStore("", "bad-restart-key")
	got, err = store.RestartRequestedAt(context.Background())
	if err != nil || !got.IsZero() {
		t.Fatalf("bad cache restart = %s, err = %v, want zero time", got, err)
	}

	raw, err := encryptedRawMessage("token")
	if err != nil {
		t.Fatalf("encryptedRawMessage: %v", err)
	}
	if token, err := encryptedToken(raw); err != nil || token != "token" {
		t.Fatalf("encryptedToken = %q, err = %v, want token", token, err)
	}
	if _, err := encryptedToken(payload.Payload(`{bad-json`)); err == nil {
		t.Fatal("invalid encrypted token payload should fail")
	}
}

func TestPendingJobAndDispatcherLifecycleBranches(t *testing.T) {
	// 测试目的：chain metadata 序列化和 dispatcher lifecycle 的正常路径都应保持
	// 与 Manager runtime 一致，不需要真实外部 transport。
	var nilCtx context.Context
	manager, err := NewManager(Config{Default: "sync"}, NewRegistry())
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	deadline := time.Now().Add(time.Hour).Truncate(time.Second)
	pending, err := manager.pendingJob(&testJob{Key: "next"}, applyOptions(
		OnQueue("mail"),
		Tries(3),
		MaxExceptions(2),
		Timeout(4*time.Second),
		FailOnTimeout(),
		Backoff(time.Second, 2*time.Second),
		RetryUntil(deadline),
		Unique("u", time.Minute),
		UniqueUntilProcessing(),
		Debounce("d", 2*time.Minute),
	))
	if err != nil {
		t.Fatalf("pending job: %v", err)
	}
	if pending.Queue != "mail" || pending.MaxTries != 3 || pending.MaxExceptions != 2 {
		t.Fatalf("pending retry metadata = %#v", pending)
	}
	if !pending.FailOnTimeout || pending.TimeoutSec != 4 || len(pending.BackoffSec) != 2 {
		t.Fatalf("pending timeout/backoff metadata = %#v", pending)
	}
	if pending.RetryUntil != deadline.Unix() || pending.UniqueKey != "u" || !pending.UniqueUntil {
		t.Fatalf("pending deadline/unique metadata = %#v", pending)
	}
	if pending.DebounceKey != "d" || pending.DebounceForSec != 120 {
		t.Fatalf("pending debounce metadata = %#v", pending)
	}

	dispatcher := NewDispatcher(manager)
	if err := dispatcher.RequestRestart(nilCtx); err != nil {
		t.Fatalf("dispatcher request restart: %v", err)
	}
	if err := dispatcher.Close(); err != nil {
		t.Fatalf("dispatcher close: %v", err)
	}
	if _, err := manager.Queue("sync"); !errors.Is(err, ErrManagerClosed) {
		t.Fatalf("closed manager queue error = %v, want ErrManagerClosed", err)
	}
}
