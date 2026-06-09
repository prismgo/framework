package queue

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/prismgo/framework/cache"
	configpkg "github.com/prismgo/framework/config"
	"github.com/prismgo/framework/container"
	cachecontract "github.com/prismgo/framework/contracts/cache"
	queuecontract "github.com/prismgo/framework/contracts/queue"
	encodingpkg "github.com/prismgo/framework/encoding"
	encryptionpkg "github.com/prismgo/framework/encryption"
	"github.com/prismgo/framework/exception"
	"github.com/prismgo/framework/queue/payload"
	redisqueue "github.com/prismgo/framework/queue/redis"
	"github.com/prismgo/framework/queue/state"
	"github.com/prismgo/framework/ratelimit"
	prismredis "github.com/prismgo/framework/redis"
)

var testLog = struct {
	sync.Mutex
	items []string
	hits  map[string]int
}{hits: map[string]int{}}

func testQueueEncrypter(t *testing.T) *encryptionpkg.Encrypter {
	t.Helper()
	secret := "base64:" + base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{7}, 32))
	encrypter, err := encryptionpkg.New(encryptionpkg.Config{Key: secret, Cipher: encryptionpkg.CipherAES256GCM})
	if err != nil {
		t.Fatalf("queue test encrypter: %v", err)
	}
	return encrypter
}

type testJob struct {
	Key      string `json:"key"`
	FailTill int    `json:"fail_till,omitempty"`
	Skip     bool   `json:"skip,omitempty"`
	SleepMS  int    `json:"sleep_ms,omitempty"`
}

func (j *testJob) Handle(context.Context) error {
	if j.SleepMS > 0 {
		time.Sleep(time.Duration(j.SleepMS) * time.Millisecond)
	}
	testLog.Lock()
	defer testLog.Unlock()
	testLog.hits[j.Key]++
	testLog.items = append(testLog.items, j.Key)
	if j.FailTill > 0 && testLog.hits[j.Key] <= j.FailTill {
		return fmt.Errorf("fail %s", j.Key)
	}
	return nil
}

func (j *testJob) Middleware() []Middleware {
	if j.Skip {
		return []Middleware{SkipIf(func(Job) bool { return true })}
	}
	return nil
}

type retryJob struct {
	Key      string `json:"key"`
	FailTill int    `json:"fail_till,omitempty"`
}

func (j *retryJob) Tries() int { return 3 }
func (j *retryJob) Backoff() []time.Duration {
	return []time.Duration{time.Second, 2 * time.Second}
}
func (j *retryJob) Timeout() time.Duration { return time.Second }
func (j *retryJob) Handle(ctx context.Context) error {
	return (&testJob{Key: j.Key, FailTill: j.FailTill}).Handle(ctx)
}

type panicJob struct{}

func (panicJob) Handle(context.Context) error {
	panic("boom")
}

type uniqueTestJob struct {
	Key string `json:"key"`
}

func (j *uniqueTestJob) Handle(ctx context.Context) error {
	return (&testJob{Key: j.Key}).Handle(ctx)
}
func (j *uniqueTestJob) UniqueID() string         { return "unique-provider" }
func (j *uniqueTestJob) UniqueFor() time.Duration { return time.Second }
func (j *uniqueTestJob) DebounceID() string       { return "debounce-provider" }
func (j *uniqueTestJob) DebounceFor() time.Duration {
	return time.Millisecond
}

type cacheUniqueViaJob struct {
	Key string `json:"key"`
}

func (j *cacheUniqueViaJob) Handle(ctx context.Context) error {
	return (&testJob{Key: j.Key}).Handle(ctx)
}
func (j *cacheUniqueViaJob) UniqueID() string         { return "via:unique:" + j.Key }
func (j *cacheUniqueViaJob) UniqueFor() time.Duration { return time.Minute }
func (j *cacheUniqueViaJob) UniqueVia() cachecontract.Repository {
	return cache.Store("redis")
}

type cacheDebounceViaJob struct {
	Key   string `json:"key"`
	Group string `json:"group"`
}

func (j *cacheDebounceViaJob) Handle(ctx context.Context) error {
	return (&testJob{Key: j.Key}).Handle(ctx)
}
func (j *cacheDebounceViaJob) DebounceID() string { return "via:debounce:" + j.Group }
func (j *cacheDebounceViaJob) DebounceFor() time.Duration {
	return 10 * time.Millisecond
}
func (j *cacheDebounceViaJob) DebounceVia() cachecontract.Repository {
	return cache.Store("redis")
}

type badMarshalJob struct {
	Ch chan int `json:"ch"`
}

func (badMarshalJob) Handle(context.Context) error { return nil }

type failedHookJob struct {
	Key string `json:"key"`
}

func (j *failedHookJob) Handle(context.Context) error {
	return fmt.Errorf("hook fail %s", j.Key)
}

func (j *failedHookJob) Failed(_ context.Context, err error) {
	testLog.Lock()
	defer testLog.Unlock()
	testLog.items = append(testLog.items, "failed:"+j.Key+":"+err.Error())
}

type failNowJob struct {
	Key string `json:"key"`
}

func (j *failNowJob) Handle(context.Context) error {
	return Fail(fmt.Errorf("fail now %s", j.Key))
}

type encryptedTestJob struct {
	Key string `json:"key"`
}

func (j *encryptedTestJob) ShouldEncrypt() bool { return true }

func (j *encryptedTestJob) Handle(ctx context.Context) error {
	return (&testJob{Key: j.Key}).Handle(ctx)
}

func resetTestLog() {
	testLog.Lock()
	defer testLog.Unlock()
	testLog.items = nil
	testLog.hits = map[string]int{}
}

func useTestCache(t *testing.T, defaultStore string, redisAddr ...string) *cache.Manager {
	t.Helper()
	registry := useQueueTestContainer(t)
	stores := map[string]cache.StoreConfig{
		"memory": {Driver: "memory", CleanupInterval: time.Millisecond},
	}
	if len(redisAddr) > 0 && redisAddr[0] != "" {
		redisManager, err := prismredis.NewManager(prismredis.Config{
			DefaultName: "default",
			Connections: map[string]prismredis.ConnectionConfig{
				"default": {Name: "default", Addr: redisAddr[0]},
			},
		})
		if err != nil {
			t.Fatalf("redis NewManager error = %v", err)
		}
		if err := registry.Instance("redis", redisManager); err != nil {
			t.Fatalf("bind redis manager: %v", err)
		}
		t.Cleanup(func() { _ = redisManager.Close(context.Background()) })
		stores["redis"] = cache.StoreConfig{Driver: "redis", Redis: cache.RedisConfig{Connection: "default"}}
	}
	if defaultStore == "" {
		defaultStore = "memory"
	}
	manager, err := cache.NewManager(cache.Config{
		Default: defaultStore,
		Prefix:  "queue_test_cache",
		Stores:  stores,
		Lock:    cache.LockConfig{RetrySleep: time.Millisecond},
	})
	if err != nil {
		t.Fatalf("new cache manager: %v", err)
	}
	if err := registry.Instance("cache.manager", manager); err != nil {
		t.Fatalf("bind cache manager: %v", err)
	}
	if err := registry.Instance("config.default", configpkg.New()); err != nil {
		t.Fatalf("bind config: %v", err)
	}
	if err := registry.Instance("exception.handler", exception.New(exception.WithLogging(false))); err != nil {
		t.Fatalf("bind exception handler: %v", err)
	}
	t.Cleanup(func() {
		_ = manager.Close()
	})
	return manager
}

func useQueueTestContainer(t *testing.T) *container.Container {
	t.Helper()
	registry := container.NewContainer()
	container.SetProvider(func() *container.Container { return registry })
	t.Cleanup(func() { container.SetProvider(nil) })
	return registry
}

func bindQueueManagerForTest(t *testing.T, manager *Manager) *container.Container {
	t.Helper()
	registry := useQueueTestContainer(t)
	if err := registry.Instance(serviceKey, manager); err != nil {
		t.Fatalf("bind queue manager: %v", err)
	}
	return registry
}

func bindQueueConfigInRegistry(t *testing.T, registry *container.Container, cfg *configpkg.Config) {
	t.Helper()
	if err := registry.Instance("config.default", cfg); err != nil {
		t.Fatalf("bind config: %v", err)
	}
}

func containsString(values []string, needle string) bool {
	for _, value := range values {
		if value == needle {
			return true
		}
	}
	return false
}

func mustJobName(job Job) string {
	name, err := JobTypeName(job)
	if err != nil {
		panic(err)
	}
	return name
}

func testJobName() string {
	return mustJobName(&testJob{})
}

func newTestRegistry() *Registry {
	registry := NewRegistry()
	RegisterTypeTo[*testJob](registry)
	return registry
}

func newSyncManager() *Manager {
	manager, err := NewManager(Config{Default: "sync"}, newTestRegistry())
	if err != nil {
		panic(err)
	}
	return manager
}

func TestSyncDispatchConcurrentPushesDrainAllJobs(t *testing.T) {
	resetTestLog()
	manager := newSyncManager()
	const jobs = 200
	start := make(chan struct{})
	var wg sync.WaitGroup
	errs := make(chan error, jobs)

	// 需求背景：sync transport 在 job runner drain 期间仍可能收到并发 Push；drain owner
	// 退出前必须确认队列为空，否则任务会残留到下一次 Push 才执行。
	for i := 0; i < jobs; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := NewDispatcher(manager).Dispatch(context.Background(), &testJob{Key: fmt.Sprintf("sync-concurrent-%03d", i), SleepMS: 1})
			errs <- err
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("dispatch returned error: %v", err)
		}
	}

	testLog.Lock()
	executed := 0
	for key, hits := range testLog.hits {
		if strings.HasPrefix(key, "sync-concurrent-") {
			executed += hits
		}
	}
	testLog.Unlock()
	if executed != jobs {
		t.Fatalf("executed sync jobs = %d, want %d", executed, jobs)
	}
	queueConn, err := manager.Queue("sync")
	if err != nil {
		t.Fatalf("resolve sync queue: %v", err)
	}
	size, err := queueConn.Size(context.Background(), "default")
	if err != nil {
		t.Fatalf("sync queue size: %v", err)
	}
	if size != 0 {
		t.Fatalf("sync queue size = %d, want 0", size)
	}
}

func newRedisManager(t *testing.T) (*Manager, *miniredis.Miniredis) {
	return newRedisManagerWithTiming(t, time.Second, 0)
}

func newRedisManagerWithTiming(t *testing.T, retryAfter time.Duration, blockFor time.Duration) (*Manager, *miniredis.Miniredis) {
	t.Helper()
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	useTestCache(t, "redis", srv.Addr())
	client := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	conn := redisqueue.NewRedisQueueFromClient(client, redisqueue.RedisOptions{
		Prefix:     "queue_test",
		RetryAfter: retryAfter,
		BlockFor:   blockFor,
		FailedTTL:  time.Hour,
		Codec:      encodingpkg.JSON(),
	})
	failed := redisqueue.NewRedisFailedStoreFromClient(client, redisqueue.RedisOptions{Prefix: "queue_test", FailedTTL: time.Hour, Codec: encodingpkg.JSON()})
	manager := newRuntimeBackedManagerForTest("redis", "default", map[string]queuecontract.Queue{"redis": conn}, failed, NewMemoryBatchStore(), newTestRegistry())
	t.Cleanup(func() {
		_ = manager.Close()
		srv.Close()
	})
	return manager, srv
}

func mustQueueEnvelope(t *testing.T, manager *Manager, connection string) *redisqueue.RedisQueue {
	t.Helper()
	queueConn, err := manager.Queue(connection)
	if err != nil {
		t.Fatalf("resolve queue %q: %v", connection, err)
	}
	redisQueue, ok := queueConn.(*redisqueue.RedisQueue)
	if !ok {
		t.Fatalf("queue %q type = %T, want *redisqueue.RedisQueue", connection, queueConn)
	}
	return redisQueue
}

func reservedEnvelope(job queuecontract.ReservedJob) *payload.Envelope {
	if carrier, ok := job.(interface{ envelope() *payload.Envelope }); ok {
		return carrier.envelope()
	}
	var env payload.Envelope
	if err := payload.QueueCodec(encodingpkg.JSON()).Unmarshal(job.Payload(), &env); err == nil {
		return &env
	}
	if err := payload.QueueCodec(nil).Unmarshal(job.Payload(), &env); err == nil {
		return &env
	}
	return nil
}

func TestFacadeExposesManagerMethods(t *testing.T) {
	resetTestLog()
	ctx := context.Background()
	manager := newSyncManager()
	bindQueueManagerForTest(t, manager)

	if Resolve() != manager {
		t.Fatal("expected facade to use configured manager")
	}
	if Failed() == nil {
		t.Fatal("expected failed store")
	}
	manager.connectionSpecs["facade-custom"] = ConnectionConfig{Driver: "facade-custom-driver"}
	facadeConnector := &capturingConnector{queue: &contractOnlyQueue{}}
	Extend("facade-custom-driver", facadeConnector)
	if _, err := manager.Queue("facade-custom"); err != nil {
		t.Fatalf("facade extend queue: %v", err)
	}
	if facadeConnector.calls.Load() != 1 {
		t.Fatalf("facade connector calls = %d, want 1", facadeConnector.calls.Load())
	}

	UseMiddleware(MiddlewareFunc(func(ctx context.Context, _ Job, next Next) error {
		testLog.Lock()
		testLog.items = append(testLog.items, "facade-before")
		testLog.Unlock()
		return next(ctx)
	}))
	if _, err := Dispatch(ctx, &testJob{Key: "facade-dispatch"}); err != nil {
		t.Fatalf("facade dispatch: %v", err)
	}
	testLog.Lock()
	items := append([]string(nil), testLog.items...)
	testLog.Unlock()
	if !containsString(items, "facade-before") || !containsString(items, "facade-dispatch") {
		t.Fatalf("facade middleware or dispatch did not run: %v", items)
	}

	envelopeJob := &testJob{Key: "facade-envelope"}
	opts := normalizeDispatchOptions(manager.runtime, envelopeJob, DispatchOptions{})
	env, err := newDispatcherPayloadFactory(manager.runtime).MakeEnvelope(envelopeJob, payload.EnvelopeOptions{Queue: opts.Queue})
	if err != nil {
		t.Fatalf("make envelope: %v", err)
	}
	failedRetryID := "facade-failed"
	if err := manager.Failed().Record(ctx, payload.FailedJob{ID: failedRetryID, Connection: "", Queue: env.Queue, JobID: env.ID, JobName: env.Name, Envelope: *env, FailedAt: time.Now()}); err != nil {
		t.Fatalf("record failed retry: %v", err)
	}
	if err := NewDispatcher(manager).RetryFailed(ctx, failedRetryID); err != nil {
		t.Fatalf("retry failed: %v", err)
	}
	queueConn, err := manager.Queue("")
	if err != nil {
		t.Fatalf("resolve default queue: %v", err)
	}
	size, err := queueConn.Size(ctx, "default")
	if err != nil {
		t.Fatalf("connection size: %v", err)
	}
	if size != 0 {
		t.Fatalf("sync retry queue size = %d, want immediate execution", size)
	}
	testLog.Lock()
	retried := containsString(testLog.items, "facade-envelope")
	testLog.Unlock()
	if !retried {
		t.Fatal("sync retry should execute retried job immediately")
	}
	if _, err := manager.Failed().Find(ctx, failedRetryID); !errors.Is(err, ErrEmpty) {
		t.Fatalf("retry should forget failed record, got %v", err)
	}

	status, err := Batch(&testJob{Key: "facade-batch"}).Name("facade-batch").Dispatch(ctx)
	if err != nil {
		t.Fatalf("facade batch dispatch: %v", err)
	}
	if err := CancelBatch(ctx, status.ID); err != nil {
		t.Fatalf("facade cancel batch: %v", err)
	}
	latest, err := GetBatchStatus(ctx, status.ID)
	if err != nil {
		t.Fatalf("facade batch status: %v", err)
	}
	if !latest.Cancelled {
		t.Fatal("expected cancelled batch")
	}

	manual := payload.BatchStatus{ID: "facade-manual", Total: 1, Pending: 1}
	if err := manager.createBatch(ctx, manual); err != nil {
		t.Fatalf("create manual batch: %v", err)
	}
	if err := MarkBatchJob(ctx, manual.ID, false); err != nil {
		t.Fatalf("facade mark batch job: %v", err)
	}
	latest, err = GetBatchStatus(ctx, manual.ID)
	if err != nil {
		t.Fatalf("facade manual batch status: %v", err)
	}
	if latest.Pending != 0 || latest.Processed != 1 || latest.Failed != 1 {
		t.Fatalf("manual batch status = %+v", latest)
	}

	if err := RequestRestart(ctx); err != nil {
		t.Fatalf("facade request restart: %v", err)
	}
	if !manager.restartRequested(ctx, time.Now().Add(-time.Second)) {
		t.Fatal("expected restart request")
	}
	if err := Close(); err != nil {
		t.Fatalf("facade close: %v", err)
	}
}

func TestDispatchOptionChainAppliesOptionsInOrder(t *testing.T) {
	retryUntil := time.Unix(1700000000, 0)
	backoff := []time.Duration{5 * time.Second, 30 * time.Second, time.Minute}

	opts := applyOptions(
		OnConnection("sync").
			OnConnection("redis").
			OnQueue("orders").
			Delay(30*time.Second).
			Tries(5).
			MaxExceptions(2).
			Timeout(20*time.Second).
			FailOnTimeout().
			Encrypt().
			Backoff(backoff...).
			RetryUntil(retryUntil).
			Unique("sync-order:10001", 10*time.Minute).
			UniqueUntilProcessing().
			Debounce("sync-order:10001", time.Minute),
	)

	if opts.Connection != "redis" {
		t.Fatalf("connection = %q, want redis", opts.Connection)
	}
	if opts.Queue != "orders" {
		t.Fatalf("queue = %q, want orders", opts.Queue)
	}
	if opts.Delay != 30*time.Second {
		t.Fatalf("delay = %v, want 30s", opts.Delay)
	}
	if opts.Tries != 5 {
		t.Fatalf("tries = %d, want 5", opts.Tries)
	}
	if opts.MaxExceptions != 2 {
		t.Fatalf("max exceptions = %d, want 2", opts.MaxExceptions)
	}
	if opts.Timeout != 20*time.Second {
		t.Fatalf("timeout = %v, want 20s", opts.Timeout)
	}
	if !opts.FailOnTimeout {
		t.Fatal("expected fail on timeout")
	}
	if !opts.Encrypted {
		t.Fatal("expected encrypted option")
	}
	if fmt.Sprint(opts.Backoff) != fmt.Sprint(backoff) {
		t.Fatalf("backoff = %v, want %v", opts.Backoff, backoff)
	}
	if !opts.RetryUntil.Equal(retryUntil) {
		t.Fatalf("retry until = %v, want %v", opts.RetryUntil, retryUntil)
	}
	if opts.UniqueKey != "sync-order:10001" || opts.UniqueFor != 10*time.Minute || !opts.UniqueUntil {
		t.Fatalf("unique options = key:%q ttl:%v until:%v", opts.UniqueKey, opts.UniqueFor, opts.UniqueUntil)
	}
	if opts.DebounceKey != "sync-order:10001" || opts.DebounceFor != time.Minute {
		t.Fatalf("debounce options = key:%q ttl:%v", opts.DebounceKey, opts.DebounceFor)
	}
}

func TestDispatchOptionChainCanStartFromAnyOption(t *testing.T) {
	retryUntil := time.Unix(1700000300, 0)
	cases := []struct {
		name   string
		option DispatchOption
		check  func(*testing.T, DispatchOptions)
	}{
		{
			name:   "on connection",
			option: OnConnection("redis").OnQueue("orders"),
			check: func(t *testing.T, opts DispatchOptions) {
				t.Helper()
				if opts.Connection != "redis" {
					t.Fatalf("connection = %q, want redis", opts.Connection)
				}
			},
		},
		{
			name:   "on queue",
			option: OnQueue("orders").Tries(3),
			check: func(t *testing.T, opts DispatchOptions) {
				t.Helper()
				if opts.Tries != 3 {
					t.Fatalf("tries = %d, want 3", opts.Tries)
				}
			},
		},
		{
			name:   "delay",
			option: Delay(10 * time.Second).Timeout(time.Second).OnQueue("delay"),
			check: func(t *testing.T, opts DispatchOptions) {
				t.Helper()
				if opts.Delay != 10*time.Second {
					t.Fatalf("delay = %v, want 10s", opts.Delay)
				}
			},
		},
		{
			name:   "delay seconds",
			option: DelaySeconds(7).OnConnection("redis").OnQueue("delay-seconds"),
			check: func(t *testing.T, opts DispatchOptions) {
				t.Helper()
				if opts.Delay != 7*time.Second {
					t.Fatalf("delay = %v, want 7s", opts.Delay)
				}
			},
		},
		{
			name:   "tries",
			option: Tries(4).Backoff(time.Second).OnQueue("tries"),
			check: func(t *testing.T, opts DispatchOptions) {
				t.Helper()
				if opts.Tries != 4 {
					t.Fatalf("tries = %d, want 4", opts.Tries)
				}
			},
		},
		{
			name:   "max exceptions",
			option: MaxExceptions(2).RetryUntil(retryUntil).OnQueue("max-exceptions"),
			check: func(t *testing.T, opts DispatchOptions) {
				t.Helper()
				if opts.MaxExceptions != 2 {
					t.Fatalf("max exceptions = %d, want 2", opts.MaxExceptions)
				}
			},
		},
		{
			name:   "timeout",
			option: Timeout(5 * time.Second).FailOnTimeout().OnQueue("timeout"),
			check: func(t *testing.T, opts DispatchOptions) {
				t.Helper()
				if opts.Timeout != 5*time.Second {
					t.Fatalf("timeout = %v, want 5s", opts.Timeout)
				}
			},
		},
		{
			name:   "fail on timeout",
			option: FailOnTimeout().OnQueue("timeouts"),
			check: func(t *testing.T, opts DispatchOptions) {
				t.Helper()
				if !opts.FailOnTimeout {
					t.Fatal("expected fail on timeout")
				}
			},
		},
		{
			name:   "encrypt",
			option: Encrypt().OnQueue("secret"),
			check: func(t *testing.T, opts DispatchOptions) {
				t.Helper()
				if !opts.Encrypted {
					t.Fatal("expected encrypted option")
				}
			},
		},
		{
			name:   "backoff",
			option: Backoff(time.Second, 2*time.Second).Tries(3).OnQueue("backoff"),
			check: func(t *testing.T, opts DispatchOptions) {
				t.Helper()
				if fmt.Sprint(opts.Backoff) != fmt.Sprint([]time.Duration{time.Second, 2 * time.Second}) {
					t.Fatalf("backoff = %v, want [1s 2s]", opts.Backoff)
				}
			},
		},
		{
			name:   "retry until",
			option: RetryUntil(retryUntil).Tries(5).OnQueue("retry-until"),
			check: func(t *testing.T, opts DispatchOptions) {
				t.Helper()
				if !opts.RetryUntil.Equal(retryUntil) {
					t.Fatalf("retry until = %v, want %v", opts.RetryUntil, retryUntil)
				}
			},
		},
		{
			name:   "unique",
			option: Unique("unique:1", time.Minute).OnQueue("unique"),
			check: func(t *testing.T, opts DispatchOptions) {
				t.Helper()
				if opts.UniqueKey != "unique:1" || opts.UniqueFor != time.Minute {
					t.Fatalf("unique = key:%q ttl:%v", opts.UniqueKey, opts.UniqueFor)
				}
			},
		},
		{
			name:   "unique via",
			option: UniqueVia(nil).OnQueue("unique-via"),
			check: func(t *testing.T, opts DispatchOptions) {
				t.Helper()
				if opts.Queue != "unique-via" {
					t.Fatalf("queue = %q, want unique-via", opts.Queue)
				}
			},
		},
		{
			name:   "unique until processing",
			option: UniqueUntilProcessing().OnQueue("until"),
			check: func(t *testing.T, opts DispatchOptions) {
				t.Helper()
				if !opts.UniqueUntil {
					t.Fatal("expected unique until processing")
				}
			},
		},
		{
			name:   "debounce",
			option: Debounce("debounce:1", time.Minute).OnQueue("debounce"),
			check: func(t *testing.T, opts DispatchOptions) {
				t.Helper()
				if opts.DebounceKey != "debounce:1" || opts.DebounceFor != time.Minute {
					t.Fatalf("debounce = key:%q ttl:%v", opts.DebounceKey, opts.DebounceFor)
				}
			},
		},
		{
			name:   "debounce via",
			option: DebounceVia(nil).OnQueue("debounce-via"),
			check: func(t *testing.T, opts DispatchOptions) {
				t.Helper()
				if opts.Queue != "debounce-via" {
					t.Fatalf("queue = %q, want debounce-via", opts.Queue)
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opts := applyOptions(tc.option)
			if opts.Queue == "" {
				t.Fatal("expected chained OnQueue to apply")
			}
			tc.check(t, opts)
		})
	}
}

func TestSyncDispatchRunsImmediatelyWithMiddlewareAndChain(t *testing.T) {
	resetTestLog()
	manager := newSyncManager()
	manager.UseMiddleware(MiddlewareFunc(func(ctx context.Context, job Job, next Next) error {
		testLog.Lock()
		testLog.items = append(testLog.items, "before")
		testLog.Unlock()
		err := next(ctx)
		testLog.Lock()
		testLog.items = append(testLog.items, "after")
		testLog.Unlock()
		return err
	}))

	id, err := manager.Chain(&testJob{Key: "first"}, &testJob{Key: "second"}).Dispatch(context.Background())
	if err != nil {
		t.Fatalf("dispatch chain: %v", err)
	}
	if id == "" {
		t.Fatal("expected job id")
	}

	testLog.Lock()
	defer testLog.Unlock()
	want := []string{"before", "first", "after", "before", "second", "after"}
	if fmt.Sprint(testLog.items) != fmt.Sprint(want) {
		t.Fatalf("items = %v, want %v", testLog.items, want)
	}
}

func TestSyncBatchTracksProgressAndSkipMiddleware(t *testing.T) {
	resetTestLog()
	manager := newSyncManager()
	status, err := manager.Batch(&testJob{Key: "done"}, &testJob{Key: "skip", Skip: true}).Name("daily").Dispatch(context.Background())
	if err != nil {
		t.Fatalf("dispatch batch: %v", err)
	}
	status, err = manager.BatchStatus(context.Background(), status.ID)
	if err != nil {
		t.Fatalf("batch status: %v", err)
	}
	if status.Total != 2 || status.Pending != 0 || status.Processed != 2 || status.Failed != 0 || status.FinishedAt.IsZero() {
		t.Fatalf("unexpected batch status: %#v", status)
	}
}

func TestRedisWorkerRetriesThenDeletesReservedJob(t *testing.T) {
	resetTestLog()
	manager, _ := newRedisManager(t)
	_, err := NewDispatcher(manager).Dispatch(context.Background(), &testJob{Key: "retry", FailTill: 1}, Backoff(10*time.Millisecond), Tries(2))
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}

	worker := NewWorker(manager)
	if err := worker.Work(context.Background(), WorkerOptions{Once: true, Tries: 2}); err != nil {
		t.Fatalf("first work: %v", err)
	}
	time.Sleep(20 * time.Millisecond)
	if err := worker.Work(context.Background(), WorkerOptions{Once: true, Tries: 2}); err != nil {
		t.Fatalf("second work: %v", err)
	}

	page, err := manager.Failed().Page(context.Background(), state.PageRequest{Page: 1, PageSize: 10})
	if err != nil {
		t.Fatalf("failed page: %v", err)
	}
	items := page.Items
	if len(items) != 0 {
		t.Fatalf("expected no failed jobs, got %v", items)
	}
	if size, _ := mustQueueEnvelope(t, manager, "redis").Size(context.Background(), "default"); size != 0 {
		t.Fatalf("queue size = %d, want 0", size)
	}
}

func TestRedisWorkerRecordsFailedAndRetryFailedRequeues(t *testing.T) {
	resetTestLog()
	manager, _ := newRedisManager(t)
	_, err := NewDispatcher(manager).Dispatch(context.Background(), &testJob{Key: "dead", FailTill: 5}, Tries(1), MaxExceptions(2))
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if err := NewWorker(manager).Work(context.Background(), WorkerOptions{Once: true, Tries: 1}); err != nil {
		t.Fatalf("work: %v", err)
	}
	page, err := manager.Failed().Page(context.Background(), state.PageRequest{Page: 1, PageSize: 10})
	if err != nil {
		t.Fatalf("failed page: %v", err)
	}
	items := page.Items
	testJobName, _ := JobTypeName(&testJob{})
	if len(items) != 1 || items[0].JobName != testJobName {
		t.Fatalf("unexpected failed jobs: %#v", items)
	}
	if items[0].Envelope.Exceptions != 1 {
		t.Fatalf("failed envelope exceptions = %d, want 1", items[0].Envelope.Exceptions)
	}
	if err := NewDispatcher(manager).RetryFailed(context.Background(), items[0].ID); err != nil {
		t.Fatalf("retry failed: %v", err)
	}
	if _, err := manager.Failed().Find(context.Background(), items[0].ID); !errors.Is(err, ErrEmpty) {
		t.Fatalf("expected failed job removed, got %v", err)
	}
	queueConn := mustQueueEnvelope(t, manager, "redis")
	reserved, err := queueConn.Pop(context.Background(), []string{"default"})
	if err != nil {
		t.Fatalf("pop retried job: %v", err)
	}
	env := reservedEnvelope(reserved)
	if env == nil {
		t.Fatal("expected retried envelope")
	}
	if env.Exceptions != 0 {
		t.Fatalf("retried envelope exceptions = %d, want 0", env.Exceptions)
	}
}

func TestRedisDelayDebounceAndUnique(t *testing.T) {
	resetTestLog()
	manager, _ := newRedisManager(t)
	if _, err := NewDispatcher(manager).Dispatch(context.Background(), &testJob{Key: "old"}, Debounce("customer:1", 10*time.Millisecond)); err != nil {
		t.Fatalf("dispatch old: %v", err)
	}
	if _, err := NewDispatcher(manager).Dispatch(context.Background(), &testJob{Key: "new"}, Debounce("customer:1", 10*time.Millisecond)); err != nil {
		t.Fatalf("dispatch new: %v", err)
	}
	time.Sleep(20 * time.Millisecond)
	worker := NewWorker(manager)
	if err := worker.Work(context.Background(), WorkerOptions{MaxJobs: 2, StopWhenEmpty: true}); err != nil {
		t.Fatalf("work debounce: %v", err)
	}

	testLog.Lock()
	if fmt.Sprint(testLog.items) != "[new]" {
		t.Fatalf("debounced items = %v, want [new]", testLog.items)
	}
	testLog.Unlock()

	if _, err := NewDispatcher(manager).Dispatch(context.Background(), &testJob{Key: "unique"}, Unique("job:unique", time.Minute), Delay(10*time.Millisecond)); err != nil {
		t.Fatalf("dispatch unique: %v", err)
	}
	if _, err := NewDispatcher(manager).Dispatch(context.Background(), &testJob{Key: "dup"}, Unique("job:unique", time.Minute)); !errors.Is(err, ErrDuplicate) {
		t.Fatalf("expected duplicate unique error, got %v", err)
	}
	time.Sleep(20 * time.Millisecond)
	if err := worker.Work(context.Background(), WorkerOptions{Once: true}); err != nil {
		t.Fatalf("work unique: %v", err)
	}
	if _, err := NewDispatcher(manager).Dispatch(context.Background(), &testJob{Key: "unique2"}, Unique("job:unique", time.Minute)); err != nil {
		t.Fatalf("unique lock should be released: %v", err)
	}
}

func TestAdvancedCacheViaProviders(t *testing.T) {
	resetTestLog()
	manager, _ := newRedisManager(t)

	if _, err := NewDispatcher(manager).Dispatch(context.Background(), &cacheUniqueViaJob{Key: "one"}, Delay(10*time.Millisecond)); err != nil {
		t.Fatalf("dispatch unique via: %v", err)
	}
	if _, err := NewDispatcher(manager).Dispatch(context.Background(), &cacheUniqueViaJob{Key: "one"}); !errors.Is(err, ErrDuplicate) {
		t.Fatalf("expected duplicate from cache unique via, got %v", err)
	}
	if ok, err := cache.Store("redis").Has(context.Background(), uniqueCacheKey("via:unique:one")); err != nil || !ok {
		t.Fatalf("unique key should be stored in redis cache, ok=%v err=%v", ok, err)
	}
	time.Sleep(20 * time.Millisecond)
	if err := NewWorker(manager).Work(context.Background(), WorkerOptions{Once: true}); err != nil {
		t.Fatalf("work unique via: %v", err)
	}

	if _, err := NewDispatcher(manager).Dispatch(context.Background(), &cacheDebounceViaJob{Key: "old", Group: "stats"}); err != nil {
		t.Fatalf("dispatch debounce old: %v", err)
	}
	if _, err := NewDispatcher(manager).Dispatch(context.Background(), &cacheDebounceViaJob{Key: "new", Group: "stats"}); err != nil {
		t.Fatalf("dispatch debounce new: %v", err)
	}
	if ok, err := cache.Store("redis").Has(context.Background(), debounceCacheKey("via:debounce:stats")); err != nil || !ok {
		t.Fatalf("debounce key should be stored in redis cache, ok=%v err=%v", ok, err)
	}
	time.Sleep(20 * time.Millisecond)
	if err := NewWorker(manager).Work(context.Background(), WorkerOptions{MaxJobs: 2, StopWhenEmpty: true}); err != nil {
		t.Fatalf("work debounce via: %v", err)
	}
}

func TestQueueCacheDriverConfigUsesConfiguredStore(t *testing.T) {
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	defer srv.Close()
	useTestCache(t, "memory", srv.Addr())

	client := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	conn := redisqueue.NewRedisQueueFromClient(client, redisqueue.RedisOptions{Prefix: "queue_cache_driver", Codec: encodingpkg.JSON()})
	manager := newRuntimeBackedManagerForTest("redis", "default", map[string]queuecontract.Queue{"redis": conn}, NewMemoryFailedStore(), nil, newTestRegistry())
	manager.runtime.cacheDriver = "redis"
	t.Cleanup(func() { _ = manager.Close() })

	if _, err := NewDispatcher(manager).Dispatch(context.Background(), &testJob{Key: "configured"}, Unique("configured", time.Minute), Delay(time.Minute)); err != nil {
		t.Fatalf("dispatch configured cache driver: %v", err)
	}
	if ok, err := cache.Store("redis").Has(context.Background(), uniqueCacheKey("configured")); err != nil || !ok {
		t.Fatalf("redis cache should hold unique key, ok=%v err=%v", ok, err)
	}
	if ok, err := cache.Default().Has(context.Background(), uniqueCacheKey("configured")); err != nil || ok {
		t.Fatalf("default memory cache should not hold unique key, ok=%v err=%v", ok, err)
	}
}

func TestWorkerStopConditionsAndTimeoutFailure(t *testing.T) {
	resetTestLog()
	manager, _ := newRedisManager(t)
	if err := NewWorker(manager).Work(context.Background(), WorkerOptions{StopWhenEmpty: true}); err != nil {
		t.Fatalf("stop when empty: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := NewWorker(manager).Work(ctx, WorkerOptions{}); err != nil {
		t.Fatalf("cancelled worker: %v", err)
	}
}

func TestWorkerStopsWhenRestartRequested(t *testing.T) {
	conn := &notifyingConnection{SyncConnection: NewSyncConnection(), popped: make(chan struct{})}
	manager := newRuntimeBackedManagerForTest("sync", "default", map[string]queuecontract.Queue{"sync": conn}, NewMemoryFailedStore(), nil, newTestRegistry())
	done := make(chan error, 1)
	go func() {
		done <- NewWorker(manager).Work(context.Background(), WorkerOptions{Sleep: 5 * time.Millisecond})
	}()
	select {
	case <-conn.popped:
	case <-time.After(time.Second):
		t.Fatal("worker did not start polling")
	}
	if err := manager.RequestRestart(context.Background()); err != nil {
		t.Fatalf("request restart: %v", err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("worker returned error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("worker did not stop after restart signal")
	}
}

func TestRedisRestartSignalStore(t *testing.T) {
	manager, _ := newRedisManager(t)
	started := time.Now()
	if manager.restartRequested(context.Background(), started) {
		t.Fatal("unexpected restart before signal")
	}
	time.Sleep(time.Millisecond)
	if err := manager.RequestRestart(context.Background()); err != nil {
		t.Fatalf("request restart: %v", err)
	}
	if !manager.restartRequested(context.Background(), started) {
		t.Fatal("expected restart signal to be visible")
	}
	if manager.restartRequested(context.Background(), time.Now().Add(time.Second)) {
		t.Fatal("future worker start should ignore stale restart signal")
	}
}

func TestManagerRestartErrorAndFallbackBranches(t *testing.T) {
	if err := (*Manager)(nil).RequestRestart(context.Background()); err == nil {
		t.Fatal("expected nil manager restart error")
	}

	missing := newRuntimeBackedManagerForTest("missing", "default", nil, nil, nil, nil)
	if err := missing.RequestRestart(context.Background()); err != nil {
		t.Fatalf("independent restart store should not require a queue connection: %v", err)
	}
	if !missing.restartRequested(context.Background(), time.Now().Add(-time.Second)) {
		t.Fatal("missing connection should still read independent restart store")
	}
	if (*Manager)(nil).restartRequested(context.Background(), time.Now()) {
		t.Fatal("nil manager should not request restart")
	}

	fallback := newRuntimeBackedManagerForTest("bare", "default", map[string]queuecontract.Queue{"bare": bareQueue{}}, NewMemoryFailedStore(), nil, newTestRegistry())
	started := time.Now()
	if err := fallback.RequestRestart(context.TODO()); err != nil {
		t.Fatalf("fallback restart: %v", err)
	}
	if !fallback.restartRequested(context.TODO(), started) {
		t.Fatal("expected fallback restart timestamp")
	}
	if fallback.restartRequested(context.Background(), time.Now().Add(time.Second)) {
		t.Fatal("future worker start should ignore fallback restart timestamp")
	}

	readerErr := newRuntimeBackedManagerForTest("missing", "default", nil, nil, nil, nil)
	readerErr.runtime.restart = errorRestartStore{}
	if readerErr.restartRequested(context.Background(), time.Now().Add(-time.Second)) {
		t.Fatal("restart store read error should not stop worker")
	}
}

func TestRegistryAndManagerErrorPaths(t *testing.T) {
	registry := NewRegistry()
	if registry.Has("missing") {
		t.Fatal("unexpected registered job")
	}
	if _, err := registry.Marshal(nil); err == nil {
		t.Fatal("expected marshal nil error")
	}
	if _, err := registry.Unmarshal("missing", nil); !errors.Is(err, ErrJobNotRegistered) {
		t.Fatalf("expected ErrJobNotRegistered, got %v", err)
	}
	if _, err := NewManager(Config{Default: "missing", Connections: map[string]ConnectionConfig{"sync": {Driver: "sync"}}}, registry); err == nil {
		t.Fatal("expected missing default connection error")
	}
	badManager := &Manager{connectionSpecs: map[string]ConnectionConfig{"bad": {Driver: "bad"}}}
	if _, err := badManager.Queue("bad"); err == nil {
		t.Fatal("expected unknown driver error")
	}
}

func TestCustomConnectorBuildsQueueWithOptions(t *testing.T) {
	resetTestLog()
	driverName := "custom-queue-driver"
	connectionSpec := ConnectionConfig{
		Driver:     "CUSTOM-QUEUE-DRIVER",
		Queue:      "emails",
		Prefix:     "custom-prefix",
		RetryAfter: 7 * time.Second,
		BlockFor:   time.Second,
		Options: map[string]any{
			"endpoint": "in-memory",
		},
	}

	manager, err := NewManager(Config{
		Default: "sync",
	}, newTestRegistry())
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	t.Cleanup(func() { _ = manager.Close() })
	connector := &capturingConnector{queue: &contractOnlyQueue{}}
	manager.connectionSpecs["primary"] = connectionSpec
	Extend(driverName, connector)

	if _, err := NewDispatcher(manager).Dispatch(context.Background(), &testJob{Key: "custom-driver"}, OnConnection("primary").OnQueue("emails")); err != nil {
		t.Fatalf("dispatch custom driver: %v", err)
	}
	if connector.calls.Load() != 1 {
		t.Fatalf("custom connector calls = %d, want 1", connector.calls.Load())
	}
	if connector.name != "primary" {
		t.Fatalf("custom connector name = %q, want primary", connector.name)
	}
	spec, ok := connector.config["_spec"].(ConnectionConfig)
	if !ok {
		t.Fatalf("custom connector config missing _spec: %#v", connector.config)
	}
	if spec.Queue != "emails" || spec.Prefix != "custom-prefix" || spec.Options["endpoint"] != "in-memory" {
		t.Fatalf("unexpected custom connector spec: %#v", spec)
	}
	if err := NewWorker(manager).Work(context.Background(), WorkerOptions{Connection: "primary", Queues: []string{"emails"}, Once: true}); err != nil {
		t.Fatalf("work custom driver: %v", err)
	}
	testLog.Lock()
	hits := testLog.hits["custom-driver"]
	testLog.Unlock()
	if hits != 1 {
		t.Fatalf("custom job hits = %d, want 1", hits)
	}
}

func TestPackageExtendBeforeManagerCreationRegistersCustomConnector(t *testing.T) {
	driverName := "custom-queue-before-manager"
	connector := &capturingConnector{queue: &contractOnlyQueue{}}
	Extend(driverName, connector)

	manager, err := NewManager(Config{
		Default: "primary",
		Connections: map[string]ConnectionConfig{
			"primary": {Driver: strings.ToUpper(driverName), Queue: "emails"},
		},
	}, newTestRegistry())
	if err != nil {
		t.Fatalf("new manager with package connector: %v", err)
	}
	t.Cleanup(func() { _ = manager.Close() })

	if got := connector.calls.Load(); got != 1 {
		t.Fatalf("connector calls = %d, want 1", got)
	}
	if connector.name != "primary" {
		t.Fatalf("connector name = %q, want primary", connector.name)
	}
}

func TestPackageExtendAfterManagerCreationBeforeFirstQueue(t *testing.T) {
	driverName := "custom-queue-late-before-first"
	manager := newSyncManager()
	manager.connectionSpecs["late"] = ConnectionConfig{Driver: driverName}

	connector := &capturingConnector{queue: &contractOnlyQueue{}}
	Extend(driverName, connector)

	if _, err := manager.Queue("late"); err != nil {
		t.Fatalf("late package connector queue: %v", err)
	}
	if got := connector.calls.Load(); got != 1 {
		t.Fatalf("connector calls = %d, want 1", got)
	}
}

func TestPackageExtendIgnoresEmptyNilAndReplacesByNormalizedName(t *testing.T) {
	emptyConnector := &capturingConnector{queue: &contractOnlyQueue{}}
	Extend("", emptyConnector)
	if _, ok := lookupConnector(""); ok {
		t.Fatal("empty connector name should be ignored")
	}

	nilDriver := "custom-queue-nil-ignored"
	Extend(nilDriver, nil)
	if _, ok := lookupConnector(nilDriver); ok {
		t.Fatal("nil connector should be ignored")
	}

	driverName := "custom-queue-replace"
	first := &capturingConnector{queue: &contractOnlyQueue{}}
	second := &capturingConnector{queue: &contractOnlyQueue{}}
	Extend(driverName, first)
	Extend(strings.ToUpper(driverName), second)

	manager := newSyncManager()
	manager.connectionSpecs["replace"] = ConnectionConfig{Driver: driverName}
	if queueConn, err := manager.Queue("replace"); err != nil {
		t.Fatalf("replacement queue: %v", err)
	} else if queueConn != second.queue {
		t.Fatalf("queue instance = %#v, want replacement %#v", queueConn, second.queue)
	}
	if got := first.calls.Load(); got != 0 {
		t.Fatalf("first connector calls = %d, want 0", got)
	}
	if got := second.calls.Load(); got != 1 {
		t.Fatalf("second connector calls = %d, want 1", got)
	}
}

func TestPackageExtendReplacementDoesNotAffectCachedConnection(t *testing.T) {
	driverName := "custom-queue-cached-replace"
	firstQueue := &contractOnlyQueue{}
	secondQueue := &contractOnlyQueue{}
	first := &capturingConnector{queue: firstQueue}
	second := &capturingConnector{queue: secondQueue}
	Extend(driverName, first)

	manager := newSyncManager()
	manager.connectionSpecs["cached"] = ConnectionConfig{Driver: driverName}
	queueConn, err := manager.Queue("cached")
	if err != nil {
		t.Fatalf("initial cached queue: %v", err)
	}
	Extend(driverName, second)
	again, err := manager.Queue("cached")
	if err != nil {
		t.Fatalf("cached queue after replacement: %v", err)
	}
	if queueConn != firstQueue || again != firstQueue {
		t.Fatalf("cached queue changed: first=%#v again=%#v want %#v", queueConn, again, firstQueue)
	}
	if got := first.calls.Load(); got != 1 {
		t.Fatalf("first connector calls = %d, want 1", got)
	}
	if got := second.calls.Load(); got != 0 {
		t.Fatalf("second connector calls = %d, want 0", got)
	}
}

func TestWorkerWorkCreatesPopSessionPerCallAndWorkQueueReusesProvidedSession(t *testing.T) {
	conn := &countingPopSessionQueue{}
	manager := newRuntimeBackedManagerForTest("pop", "default", map[string]queuecontract.Queue{"pop": conn}, NewMemoryFailedStore(), nil, newTestRegistry())
	worker := NewWorker(manager)
	options := WorkerOptions{Connection: "pop", Queues: []string{"default"}, Once: true}
	if err := worker.Work(context.Background(), options); err != nil {
		t.Fatalf("first Work: %v", err)
	}
	if err := worker.Work(context.Background(), options); err != nil {
		t.Fatalf("second Work: %v", err)
	}
	if conn.sessions != 2 {
		t.Fatalf("Worker.Work should create a pop session per call, got %d", conn.sessions)
	}
	if conn.closedSessions != 2 {
		t.Fatalf("Worker.Work should close each created pop session, got %d closes", conn.closedSessions)
	}
	session := conn.NewPopSession()
	if err := worker.WorkQueue(context.Background(), session, options); err != nil {
		t.Fatalf("first WorkQueue: %v", err)
	}
	if err := worker.WorkQueue(context.Background(), session, options); err != nil {
		t.Fatalf("second WorkQueue: %v", err)
	}
	if conn.sessions != 3 {
		t.Fatalf("WorkQueue should reuse the provided queue session, got %d sessions", conn.sessions)
	}
	if conn.closedSessions != 2 {
		t.Fatalf("WorkQueue should not close caller-owned queue session, got %d closes", conn.closedSessions)
	}
}

func TestWorkerMultiQueueNonBlockingPassChecksSecondaryBeforePrimaryBlock(t *testing.T) {
	// 需求背景：多队列 worker 配置 high,low 时，high 为空不能先消耗完整 block_for，
	// 否则 low 已经 ready 的任务会被高优先级空队列放大延迟。
	//
	// 设计思路：fake queue 记录 PopWaitMode；如果 worker 直接允许等待 high，
	// 测试会直接失败。正确路径应先对 high/low 使用一次 PopNoWait，并命中 low。
	conn := &multiQueueProbeQueue{readyQueue: "low"}
	manager := newRuntimeBackedManagerForTest("probe", "high", map[string]queuecontract.Queue{"probe": conn}, NewMemoryFailedStore(), nil, newTestRegistry())
	worker := NewWorker(manager)
	reserved, err := worker.popReserved(context.Background(), conn, WorkerOptions{Connection: "probe", Queues: []string{"high", "low"}})
	if err != nil {
		t.Fatalf("pop reserved: %v", err)
	}
	if reserved == nil || reserved.ID() != "low-job" {
		t.Fatalf("reserved=%#v, want low-job", reserved)
	}
	want := []multiQueueProbePop{{queues: []string{"high", "low"}, wait: queuecontract.PopNoWait}}
	if !reflect.DeepEqual(conn.calls, want) {
		t.Fatalf("pop calls=%#v, want %#v", conn.calls, want)
	}
}

func TestWorkerWorkNormalizesQueuesBeforeConsumerIntentAndPop(t *testing.T) {
	// 需求背景：Worker.Work 的 consumer intent 和实际 Pop 必须使用同一份队列语义；
	// 否则 RabbitMQ/Redis 生命周期事件会监听原始输入，实际消费却发生在归一化后的队列。
	conn := &intentAndPopQueue{}
	manager := newRuntimeBackedManagerForTest("probe", "default", map[string]queuecontract.Queue{"probe": conn}, NewMemoryFailedStore(), nil, newTestRegistry())
	worker := NewWorker(manager)

	err := worker.Work(context.Background(), WorkerOptions{
		Connection: "probe",
		Queues:     []string{" ", "jobs", "jobs"},
		Once:       true,
	})
	if err != nil {
		t.Fatalf("work: %v", err)
	}

	want := []string{"jobs"}
	if !reflect.DeepEqual(conn.intentQueues, want) {
		t.Fatalf("consumer intent queues=%#v, want %#v", conn.intentQueues, want)
	}
	if !reflect.DeepEqual(conn.popQueues, want) {
		t.Fatalf("pop queues=%#v, want %#v", conn.popQueues, want)
	}
}

func TestWorkerSessionUsesSameResolvedConnectionForIntentAndPopSession(t *testing.T) {
	// 需求背景：WorkerSession 的 consumer intent 必须来自 Begin 解析到的原始连接，
	// Pop 则来自同一个连接创建的 worker-local session，避免生命周期资源分裂。
	conn := &workerSessionProbeQueue{}
	manager := newRuntimeBackedManagerForTest("probe", "default", map[string]queuecontract.Queue{"probe": conn}, NewMemoryFailedStore(), nil, newTestRegistry())
	worker := NewWorker(manager)

	session, err := worker.Begin(context.Background(), WorkerOptions{Connection: "probe", Queues: []string{"jobs"}})
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := session.Activate(context.Background()); err != nil {
		t.Fatalf("activate: %v", err)
	}
	if err := session.Work(context.Background()); err != nil {
		t.Fatalf("work: %v", err)
	}
	if err := session.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if conn.sessions != 1 {
		t.Fatalf("pop sessions=%d, want 1", conn.sessions)
	}
	if conn.intentCalls != 1 {
		t.Fatalf("consumer intent calls=%d, want 1", conn.intentCalls)
	}
	if conn.popCalls != 1 {
		t.Fatalf("session pop calls=%d, want 1", conn.popCalls)
	}
	if conn.closedSessions != 1 {
		t.Fatalf("closed sessions=%d, want 1", conn.closedSessions)
	}
	if conn.releases != 1 {
		t.Fatalf("consumer releases=%d, want 1", conn.releases)
	}
}

func TestWorkerSessionActivateIsIdempotent(t *testing.T) {
	// 需求背景：Horizon monitor 和 worker loop 可能多次触发 Activate；底层 driver
	// 只能获得一次 consumer intent，否则引用计数和 stopped 事件会失衡。
	conn := &workerSessionProbeQueue{}
	manager := newRuntimeBackedManagerForTest("probe", "default", map[string]queuecontract.Queue{"probe": conn}, NewMemoryFailedStore(), nil, newTestRegistry())
	worker := NewWorker(manager)

	session, err := worker.Begin(context.Background(), WorkerOptions{Connection: "probe", Queues: []string{"jobs"}})
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := session.Activate(context.Background()); err != nil {
		t.Fatalf("first activate: %v", err)
	}
	if err := session.Activate(context.Background()); err != nil {
		t.Fatalf("second activate: %v", err)
	}
	if conn.intentCalls != 1 {
		t.Fatalf("consumer intent calls=%d, want 1", conn.intentCalls)
	}
}

func TestWorkerSessionRunOnceForcesOnceAndSkipsConsumerIntent(t *testing.T) {
	// 需求背景：WorkerSession.Work 由 Horizon 外层循环调用，每次只能消费一轮，并且
	// 已由 session 生命周期持有 consumer intent，单轮 Work 不能重复获取。
	conn := &workerSessionProbeQueue{jobOnFirstPop: true}
	manager := newRuntimeBackedManagerForTest("probe", "default", map[string]queuecontract.Queue{"probe": conn}, NewMemoryFailedStore(), nil, newTestRegistry())
	worker := NewWorker(manager)

	session, err := worker.Begin(context.Background(), WorkerOptions{Connection: "probe", Queues: []string{"jobs"}, StopWhenEmpty: true})
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := session.Work(context.Background()); err != nil {
		t.Fatalf("work: %v", err)
	}
	if conn.intentCalls != 0 {
		t.Fatalf("consumer intent calls=%d, want 0 without Activate", conn.intentCalls)
	}
	if conn.popCalls != 1 {
		t.Fatalf("pop calls=%d, want 1 forced Once run", conn.popCalls)
	}
}

type countingPopSessionQueue struct {
	bareQueue
	sessions       int
	closedSessions int
}

func (q *countingPopSessionQueue) NewPopSession() queuecontract.Queue {
	q.sessions++
	return &countingPopSessionView{countingPopSessionQueue: q, id: q.sessions}
}

type countingPopSessionView struct {
	*countingPopSessionQueue
	id int
}

func (v *countingPopSessionView) Close() error {
	v.closedSessions++
	return nil
}

type multiQueueProbePop struct {
	queues []string
	wait   queuecontract.PopWaitMode
}

type multiQueueProbeQueue struct {
	bareQueue
	readyQueue string
	calls      []multiQueueProbePop
}

func (q *multiQueueProbeQueue) Pop(_ context.Context, queues []string, wait ...queuecontract.PopWaitMode) (queuecontract.ReservedJob, error) {
	mode := queuecontract.PopWaitAvailable
	if len(wait) > 0 {
		mode = wait[0]
	}
	q.calls = append(q.calls, multiQueueProbePop{queues: append([]string(nil), queues...), wait: mode})
	for _, queueName := range queues {
		if queueName == q.readyQueue {
			return &testReservedJob{env: &payload.Envelope{ID: "low-job", Name: testJobName(), Queue: queueName, Payload: []byte(`{"key":"low"}`)}}, nil
		}
	}
	return nil, ErrEmpty
}

type intentAndPopQueue struct {
	bareQueue
	intentQueues []string
	popQueues    []string
}

func (q *intentAndPopQueue) AcquireConsumerIntent(queues []string) (func() error, error) {
	q.intentQueues = append([]string(nil), queues...)
	return func() error { return nil }, nil
}

func (q *intentAndPopQueue) Pop(_ context.Context, queues []string, _ ...queuecontract.PopWaitMode) (queuecontract.ReservedJob, error) {
	q.popQueues = append([]string(nil), queues...)
	return nil, ErrEmpty
}

type workerSessionProbeQueue struct {
	bareQueue
	sessions       int
	closedSessions int
	intentCalls    int
	releases       int
	popCalls       int
	jobOnFirstPop  bool
}

func (q *workerSessionProbeQueue) NewPopSession() queuecontract.Queue {
	q.sessions++
	return &workerSessionProbeView{parent: q}
}

func (q *workerSessionProbeQueue) AcquireConsumerIntent([]string) (func() error, error) {
	q.intentCalls++
	return func() error {
		q.releases++
		return nil
	}, nil
}

type workerSessionProbeView struct {
	bareQueue
	parent *workerSessionProbeQueue
}

func (v *workerSessionProbeView) Pop(context.Context, []string, ...queuecontract.PopWaitMode) (queuecontract.ReservedJob, error) {
	v.parent.popCalls++
	if v.parent.jobOnFirstPop && v.parent.popCalls == 1 {
		return &testReservedJob{env: &payload.Envelope{ID: "session-job", Name: testJobName(), Queue: "jobs", Payload: []byte(`{"key":"session"}`)}}, nil
	}
	return nil, ErrEmpty
}

func (v *workerSessionProbeView) Close() error {
	v.parent.closedSessions++
	return nil
}

func TestCustomConnectorUnknownNilAndError(t *testing.T) {
	manager := newSyncManager()
	manager.connectionSpecs["custom"] = ConnectionConfig{Driver: "custom-queue-ignored-nil"}
	Extend("custom-queue-ignored-nil", nil)
	if _, err := manager.Queue("custom"); err == nil || !strings.Contains(err.Error(), "unknown driver") {
		t.Fatalf("ignored custom connector err = %v, want unknown driver", err)
	}

	manager.connectionSpecs["custom-error"] = ConnectionConfig{Driver: "custom-queue-error"}
	Extend("custom-queue-error", errorConnector{err: errors.New("factory failed")})
	if _, err := manager.Queue("custom-error"); err == nil || !strings.Contains(err.Error(), "factory failed") {
		t.Fatalf("factory error connector err = %v, want factory failed", err)
	}
}

func TestManagerQueueDeduplicatesConcurrentConnectByName(t *testing.T) {
	manager := newSyncManager()
	manager.connectionSpecs["custom"] = ConnectionConfig{Driver: "custom-queue-dedupe"}
	connector := &blockingConnector{queue: &contractOnlyQueue{}, started: make(chan struct{}), release: make(chan struct{})}
	Extend("custom-queue-dedupe", connector)

	start := make(chan struct{})
	entered := make(chan struct{}, 8)
	results := make(chan queuecontract.Queue, 8)
	errs := make(chan error, 8)
	for range 8 {
		go func() {
			<-start
			entered <- struct{}{}
			queueConn, err := manager.Queue("custom")
			if err != nil {
				errs <- err
				return
			}
			results <- queueConn
		}()
	}
	close(start)
	for range 8 {
		<-entered
	}
	<-connector.started
	time.Sleep(20 * time.Millisecond)
	if connector.calls.Load() != 1 {
		t.Fatalf("connector calls while build in-flight = %d, want 1", connector.calls.Load())
	}
	close(connector.release)
	for range 8 {
		select {
		case err := <-errs:
			t.Fatalf("Queue error: %v", err)
		case got := <-results:
			if got != connector.queue {
				t.Fatalf("queue instance = %#v, want %#v", got, connector.queue)
			}
		}
	}
	if connector.calls.Load() != 1 {
		t.Fatalf("connector calls = %d, want 1", connector.calls.Load())
	}
}

func TestManagerQueueSharesBuildErrorAndAllowsRetry(t *testing.T) {
	manager := newSyncManager()
	manager.connectionSpecs["custom"] = ConnectionConfig{Driver: "custom-queue-retry"}
	connector := &flakyConnector{err: errors.New("factory failed"), queue: &contractOnlyQueue{}, started: make(chan struct{}), release: make(chan struct{})}
	Extend("custom-queue-retry", connector)

	start := make(chan struct{})
	entered := make(chan struct{}, 6)
	errCh := make(chan error, 6)
	for range 6 {
		go func() {
			<-start
			entered <- struct{}{}
			_, err := manager.Queue("custom")
			errCh <- err
		}()
	}
	close(start)
	for range 6 {
		<-entered
	}
	<-connector.started
	time.Sleep(20 * time.Millisecond)
	if connector.calls.Load() != 1 {
		t.Fatalf("connector calls while failed build in-flight = %d, want 1", connector.calls.Load())
	}
	close(connector.release)
	for range 6 {
		if err := <-errCh; err == nil || !strings.Contains(err.Error(), "factory failed") {
			t.Fatalf("concurrent Queue error = %v, want factory failed", err)
		}
	}

	connector.mu.Lock()
	connector.err = nil
	connector.started = make(chan struct{})
	connector.release = make(chan struct{})
	connector.mu.Unlock()
	go func() {
		<-connector.started
		close(connector.release)
	}()
	queueConn, err := manager.Queue("custom")
	if err != nil {
		t.Fatalf("retry Queue error: %v", err)
	}
	if queueConn != connector.queue {
		t.Fatalf("retry queue instance = %#v, want %#v", queueConn, connector.queue)
	}
	if connector.calls.Load() != 2 {
		t.Fatalf("connector calls after retry = %d, want 2", connector.calls.Load())
	}
	cached, err := manager.Queue("custom")
	if err != nil {
		t.Fatalf("cached Queue error: %v", err)
	}
	if cached != connector.queue {
		t.Fatalf("cached queue instance = %#v, want %#v", cached, connector.queue)
	}
	if connector.calls.Load() != 2 {
		t.Fatalf("connector calls after cache hit = %d, want 2", connector.calls.Load())
	}
}

func TestBuildConfigReadsConfiguredConnections(t *testing.T) {
	registry := useQueueTestContainer(t)
	configpkg.Add("queue", func() map[string]any {
		return map[string]any{
			"default":      "custom",
			"queue":        "critical",
			"cache_driver": "redis",
			"failed": map[string]any{
				"ttl":    "45",
				"prefix": "failed-prefix",
			},
			"batching": map[string]any{
				"prefix": "batch-prefix",
			},
			"connections": map[string]any{
				"custom": map[string]any{
					"driver":      "custom-config-driver",
					"queue":       "jobs",
					"prefix":      "custom-prefix",
					"retry_after": "12",
					"block_for":   3,
					"addr":        "127.0.0.1:6380",
					"database":    "4",
					"endpoint":    "in-memory",
				},
				"ignored": "not-a-map",
			},
		}
	})
	cfg := configpkg.New()
	if err := cfg.ReloadFromFile(filepath.Join(t.TempDir(), ".env")); err != nil {
		t.Fatalf("reload config: %v", err)
	}
	bindQueueConfigInRegistry(t, registry, cfg)

	built := BuildConfig()
	if built.Default != "custom" {
		t.Fatalf("unexpected top-level queue config: %#v", built)
	}
	if built.FailedTTL != 45*time.Second {
		t.Fatalf("failed ttl = %s, want 45s", built.FailedTTL)
	}
	if built.Failed.Prefix != "failed-prefix" || built.Batching.Prefix != "batch-prefix" {
		t.Fatalf("state prefixes = failed:%q batch:%q", built.Failed.Prefix, built.Batching.Prefix)
	}
	if len(built.Connections) != 1 {
		t.Fatalf("connections = %#v, want only custom map item", built.Connections)
	}
	custom := built.Connections["custom"]
	if custom.Driver != "custom-config-driver" || custom.Queue != "jobs" || custom.Prefix != "custom-prefix" {
		t.Fatalf("unexpected custom connection config: %#v", custom)
	}
	if custom.RetryAfter != 12*time.Second || custom.BlockFor != 3*time.Second {
		t.Fatalf("unexpected custom durations: retry=%s block=%s", custom.RetryAfter, custom.BlockFor)
	}
	if custom.Options["addr"] != "127.0.0.1:6380" || custom.Options["database"] != "4" {
		t.Fatalf("custom raw redis-like options not preserved: %#v", custom.Options)
	}
	if custom.Options["endpoint"] != "in-memory" {
		t.Fatalf("custom options not preserved: %#v", custom.Options)
	}
}

func TestMemoryFailedStoreAndMiddlewareHelpers(t *testing.T) {
	store := NewMemoryFailedStore()
	failed := payload.FailedJob{ID: "failed-1", JobID: "job-1", JobName: testJobName(), FailedAt: time.Now().Add(time.Second)}
	if err := store.Record(context.Background(), failed); err != nil {
		t.Fatalf("record: %v", err)
	}
	if err := store.Record(context.Background(), payload.FailedJob{JobID: "job-0", JobName: testJobName(), FailedAt: time.Now()}); err != nil {
		t.Fatalf("record with generated id: %v", err)
	}
	if got, err := store.Find(context.Background(), "failed-1"); err != nil || got.JobID != "job-1" {
		t.Fatalf("find = %#v, %v", got, err)
	}
	if got, err := store.Find(context.Background(), "job-0"); err != nil || got.JobID != "job-0" {
		t.Fatalf("find generated id = %#v, %v", got, err)
	}
	if all := mustFailedItems(t, store); len(all) != 2 || all[0].JobID != "job-0" {
		t.Fatalf("unexpected all result = %#v", all)
	}
	if err := store.Forget(context.Background(), "failed-1"); err != nil {
		t.Fatalf("forget: %v", err)
	}
	if all := mustFailedItems(t, store); len(all) != 1 || all[0].JobID != "job-0" {
		t.Fatalf("expected one remaining failed job, got %#v", all)
	}
	if err := store.Flush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if cloneEnvelope(nil) != nil {
		t.Fatal("expected nil envelope clone")
	}

	delay, ok := ReleaseDelay(ReleaseAfter(time.Second, errors.New("busy")))
	if !ok || delay != time.Second {
		t.Fatalf("release delay = %v %v", delay, ok)
	}
	if delay, ok := ReleaseDelay(errors.New("other")); ok || delay != 0 {
		t.Fatalf("unexpected release delay = %v %v", delay, ok)
	}
	releaseErr := ReleaseAfter(time.Second, nil)
	if releaseErr.Error() == "" {
		t.Fatal("expected release error string")
	}
	var typed ReleaseError
	if !errors.As(releaseErr, &typed) || typed.Unwrap() == nil {
		t.Fatal("expected release unwrap")
	}
}

func TestFacadeConfigEventsAndPackageBuilders(t *testing.T) {
	resetTestLog()
	manager := newSyncManager()
	registry := bindQueueManagerForTest(t, manager)
	RegisterType[*testJob]()
	if !DefaultRegistry().Has(testJobName()) {
		t.Fatal("expected facade job registered")
	}
	if Resolve() != manager {
		t.Fatal("expected facade manager")
	}
	if resolved := Resolve(); resolved == nil {
		t.Fatal("resolve facade returned nil")
	}
	if _, err := Dispatch(context.Background(), &testJob{Key: "facade"}); err != nil {
		t.Fatalf("facade dispatch: %v", err)
	}
	if _, err := Later(context.Background(), 1, &testJob{Key: "later"}); err != nil {
		t.Fatalf("later: %v", err)
	}
	if _, err := Chain(&testJob{Key: "chain-a"}, &testJob{Key: "chain-b"}).Options(OnQueue("default")).Dispatch(context.Background()); err != nil {
		t.Fatalf("package chain: %v", err)
	}
	status, err := Batch(&testJob{Key: "batch"}).Options(OnConnection("sync")).Name("prismgo").Dispatch(context.Background())
	if err != nil {
		t.Fatalf("package batch: %v", err)
	}
	if err := manager.CancelBatch(context.Background(), status.ID); err != nil {
		t.Fatalf("cancel batch: %v", err)
	}
	bindQueueConfigInRegistry(t, registry, configpkg.New())
	if cfg := BuildConfig(); cfg.Default == "" || cfg.Connections["redis"].Driver != "redis" {
		t.Fatalf("unexpected config: %#v", cfg)
	}
	if appManager, err := NewManagerFromConfig(); err != nil || appManager == nil {
		t.Fatalf("new app manager: %v", err)
	}
	if err := (ServiceProvider{}).Register(providerTestApp{registry: registry}); err != nil {
		t.Fatalf("register service provider: %v", err)
	}

	var names []string
	UseEventSink(func(_ context.Context, ev Event) { names = append(names, ev.Name()) })
	fire(context.Background(), JobQueued{})
	fire(context.Background(), JobProcessing{})
	fire(context.Background(), JobProcessed{})
	fire(context.Background(), JobReleased{})
	fire(context.Background(), JobFailed{})
	fire(context.Background(), BatchEvent{EventName: EventBatchUpdated})
	if len(names) != 6 {
		t.Fatalf("expected 6 events, got %v", names)
	}
	UseEventSink(nil)
}

func TestQueueInfrastructureEventSink(t *testing.T) {
	var captured Event
	UseEventSink(func(_ context.Context, ev Event) {
		captured = ev
	})
	t.Cleanup(func() { UseEventSink(nil) })

	now := time.Now()
	fire(context.Background(), InfrastructureEvent{
		EventName:  EventConnectionConnected,
		Connection: "rabbitmq",
		Driver:     "rabbitmq",
		Queue:      "default",
		Exchange:   "prismgo.queue",
		Attempt:    2,
		Error:      "temporary",
		Timestamp:  now,
	})

	infra, ok := captured.(InfrastructureEvent)
	if !ok {
		t.Fatalf("captured event = %T, want InfrastructureEvent", captured)
	}
	if infra.Name() != EventConnectionConnected {
		t.Fatalf("event name = %q, want %q", infra.Name(), EventConnectionConnected)
	}
	if infra.Connection != "rabbitmq" || infra.Driver != "rabbitmq" || infra.Queue != "default" || infra.Exchange != "prismgo.queue" {
		t.Fatalf("payload = %#v, want connection/driver/queue/exchange set", infra)
	}
	if infra.Attempt != 2 || infra.Error != "temporary" || !infra.Timestamp.Equal(now) {
		t.Fatalf("payload = %#v, want attempt/error/timestamp set", infra)
	}
}

func TestSyncConnectionDirectOperations(t *testing.T) {
	conn := NewSyncConnection()
	env := &payload.Envelope{ID: "1", Name: testJobName(), Queue: "default"}
	body, err := conn.codec.Marshal(env)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := conn.Push(context.Background(), "default", queuecontract.Payload(body)); err != nil {
		t.Fatalf("push: %v", err)
	}
	if size, _ := conn.Size(context.Background(), "default"); size != 1 {
		t.Fatalf("size = %d", size)
	}
	reserved, err := conn.Pop(context.Background(), []string{"default"})
	popped := reservedEnvelope(reserved)
	if err != nil || popped == nil || popped.Attempts != 1 {
		t.Fatalf("pop = %#v %v", popped, err)
	}
	if _, err := conn.Pop(context.Background(), []string{"default"}); !errors.Is(err, ErrEmpty) {
		t.Fatalf("expected empty, got %v", err)
	}
	if err := reserved.Release(context.Background(), 0); err != nil {
		t.Fatalf("release: %v", err)
	}
	if err := conn.Clear(context.Background(), "default"); err != nil {
		t.Fatalf("clear: %v", err)
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func TestRedisQueueAndStateStoreDirectOperations(t *testing.T) {
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	defer srv.Close()

	client := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	conn := redisqueue.NewRedisQueueFromClient(client, redisqueue.RedisOptions{Prefix: "", RetryAfter: time.Second, BlockFor: time.Second, FailedTTL: time.Second, Codec: encodingpkg.JSON()})
	failedStore := redisqueue.NewRedisFailedStoreFromClient(client, redisqueue.RedisOptions{Prefix: "", FailedTTL: time.Second, Codec: encodingpkg.JSON()})
	batchStore := redisqueue.NewRedisBatchStoreFromClient(client, redisqueue.RedisOptions{Prefix: "", FailedTTL: time.Second, Codec: encodingpkg.JSON()})
	defer func() {
		if err := conn.Close(); err != nil {
			t.Errorf("close queue connection: %v", err)
		}
	}()
	if conn.ReadyKey("a") != "queue:queues:a" {
		t.Fatalf("unexpected default prefix key: %s", conn.ReadyKey("a"))
	}
	env := &payload.Envelope{ID: "block", Name: testJobName(), Queue: "default"}
	body, err := conn.Codec().Marshal(env)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	go func() {
		time.Sleep(10 * time.Millisecond)
		_ = conn.Later(context.Background(), "default", queuecontract.Payload(body), 0)
	}()
	reserved, err := conn.Pop(context.Background(), []string{"default"})
	popped := reservedEnvelope(reserved)
	if err != nil || popped == nil || popped.ID != "block" {
		t.Fatalf("blocking pop = %#v %v", popped, err)
	}
	if err := reserved.Delete(context.Background()); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if err := conn.Clear(context.Background(), "default"); err != nil {
		t.Fatalf("clear: %v", err)
	}
	if err := failedStore.Flush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if _, err := failedStore.Find(context.Background(), "missing"); !errors.Is(err, ErrEmpty) {
		t.Fatalf("expected missing failed, got %v", err)
	}
	status := payload.BatchStatus{ID: "batch-1", Total: 1, Pending: 1}
	if err := batchStore.CreateBatch(context.Background(), status); err != nil {
		t.Fatalf("create batch: %v", err)
	}
	status.Pending = 0
	if err := batchStore.UpdateBatch(context.Background(), status); err != nil {
		t.Fatalf("update batch: %v", err)
	}
	got, err := batchStore.GetBatch(context.Background(), "batch-1")
	if err != nil || got.Pending != 0 {
		t.Fatalf("get batch = %#v %v", got, err)
	}
}

func TestMiddlewareLocksRateLimitAndTimeout(t *testing.T) {
	resetTestLog()
	manager, _ := newRedisManager(t)
	manager.UseMiddleware(WithoutOverlapping("same", time.Second))
	if _, err := NewDispatcher(manager).Dispatch(context.Background(), &testJob{Key: "lock"}); err != nil {
		t.Fatalf("dispatch lock: %v", err)
	}
	if err := NewWorker(manager).Work(context.Background(), WorkerOptions{Once: true}); err != nil {
		t.Fatalf("work lock: %v", err)
	}

	limited := RateLimit("rate", 1, time.Second)
	err := limited.Handle(context.Background(), &testJob{Key: "r1"}, func(context.Context) error { return nil })
	if err != nil {
		t.Fatalf("first rate limit: %v", err)
	}
	limiter := ratelimit.New(queueCacheRepositoryFromContext(context.Background(), nil, ""))
	if limited, err := limiter.TooManyAttempts(context.Background(), "rate", 1); err != nil || !limited {
		t.Fatalf("queue rate limit should use prismgo/ratelimit, limited=%v err=%v", limited, err)
	}
	t.Cleanup(func() { _ = limiter.Clear(context.Background(), "rate") })
	err = limited.Handle(context.Background(), &testJob{Key: "r2"}, func(context.Context) error { return nil })
	if _, ok := ReleaseDelay(err); !ok {
		t.Fatalf("expected rate limit release, got %v", err)
	}

	if _, err := NewDispatcher(manager).Dispatch(context.Background(), &testJob{Key: "slow", SleepMS: 30}, Timeout(5*time.Millisecond), Tries(1)); err != nil {
		t.Fatalf("dispatch slow: %v", err)
	}
	if err := NewWorker(manager).Work(context.Background(), WorkerOptions{Once: true, Timeout: 5 * time.Millisecond, Tries: 1}); err != nil {
		t.Fatalf("work slow: %v", err)
	}
	items := mustFailedItems(t, manager.Failed())
	if len(items) == 0 {
		t.Fatal("expected timeout failure recorded")
	}
}

func TestOptionsProvidersAndHelperBranches(t *testing.T) {
	resetTestLog()
	manager := newSyncManager()
	if _, err := NewDispatcher(manager).Dispatch(context.Background(), &retryJob{Key: "retryable"}); err != nil {
		t.Fatalf("retryable dispatch: %v", err)
	}
	if _, err := manager.Queue("missing"); err == nil {
		t.Fatal("expected missing connection error")
	}
	if got := seconds(-time.Second); got != 0 {
		t.Fatalf("seconds negative = %d", got)
	}
	if values := durations([]int{0, 2}); len(values) != 1 || values[0] != 2*time.Second {
		t.Fatalf("durations = %v", values)
	}
	if got := firstNonEmpty("", "x"); got != "x" {
		t.Fatalf("firstNonEmpty = %q", got)
	}
	if got := firstNonEmpty("", ""); got != "" {
		t.Fatalf("firstNonEmpty empty = %q", got)
	}
	if got := parsePositiveInt("bad", 7); got != 7 {
		t.Fatalf("parsePositiveInt = %d", got)
	}
	if got := parsePositiveInt("3", 7); got != 3 {
		t.Fatalf("parsePositiveInt good = %d", got)
	}
}

func TestRabbitMQConfigParsingBranches(t *testing.T) {
	spec := map[string]any{
		"url":                        "amqp://guest:secret@127.0.0.1:5672/",
		"scheme":                     "amqps",
		"host":                       "rabbit.local",
		"port":                       5671,
		"username":                   "guest",
		"password":                   "secret",
		"vhost":                      "tenant",
		"exchange":                   "jobs",
		"exchange_type":              "topic",
		"declare":                    "true",
		"exchange_durable":           false,
		"queue_durable":              "false",
		"queue_max_priority":         "9",
		"message_persistent":         "false",
		"auto_delete":                true,
		"exclusive":                  "true",
		"no_wait":                    false,
		"confirm":                    "true",
		"delay_mode":                 "ttl_dlx",
		"delay_buckets":              "1, 5s, bad",
		"prefetch":                   "3",
		"heartbeat":                  "2s",
		"publish_timeout":            4,
		"publish_channels":           "2",
		"reconnect_min_delay":        "100ms",
		"reconnect_max_delay":        "2s",
		"restart_queue":              "restart",
		"restart_enabled":            "false",
		"restart_poll_interval":      "250ms",
		"topology_cache_ttl":         "30s",
		"topology_cache_max_entries": "20",
	}
	opts := rabbitMQOptionsFromMap(spec)
	if opts.Exchange != "jobs" || opts.ExchangeType != "topic" || opts.Prefetch != 3 || opts.PublishChannels != 2 {
		t.Fatalf("rabbit options = %#v", opts)
	}
	if opts.QueueMaxPriority != 9 || opts.DelayMode != "ttl_dlx" || len(opts.DelayBuckets) != 1 {
		t.Fatalf("rabbit numeric options = %#v", opts)
	}
	if opts.RestartEnabled.Or(true) || opts.RestartPollInterval != 250*time.Millisecond || opts.TopologyCacheTTL != 30*time.Second {
		t.Fatalf("rabbit restart/cache options = %#v", opts)
	}
	defaulted := rabbitMQOptionsFromMap(map[string]any{"url": "amqp://guest:guest@example.test:5672/%2F"})
	if defaulted.Confirm.IsSet() || defaulted.Declare.IsSet() || defaulted.RestartEnabled.IsSet() {
		t.Fatalf("missing rabbit bool keys = %#v, want unset bools for runtime defaults", defaulted)
	}
	disabledConfirm := rabbitMQOptionsFromMap(map[string]any{
		"url":     "amqp://guest:guest@example.test:5672/%2F",
		"confirm": false,
	})
	if !disabledConfirm.Confirm.IsSet() || disabledConfirm.Confirm.Or(true) {
		t.Fatalf("explicit confirm=false = %#v, want explicit false retained", disabledConfirm)
	}
}

func TestAdditionalErrorAndBranchCoverage(t *testing.T) {
	resetTestLog()
	bindQueueManagerForTest(t, newSyncManager())
	if Resolve() == nil {
		t.Fatal("expected configured manager")
	}
	var nilRegistry *Registry
	nilRegistry.registerFactory("", nil)
	if nilRegistry.Has("x") {
		t.Fatal("nil registry should not have jobs")
	}
	if _, err := nilRegistry.Unmarshal("x", nil); !errors.Is(err, ErrJobNotRegistered) {
		t.Fatalf("nil registry unmarshal err = %v", err)
	}
	registry := NewRegistry()
	registry.registerFactory("", nil)
	registry.registerFactory("nil.job", func() Job { return nil })
	if _, err := registry.Unmarshal("nil.job", nil); err == nil {
		t.Fatal("expected nil factory product error")
	}

	manager := newSyncManager()
	if manager.Registry() == nil {
		t.Fatal("expected registry")
	}
	if _, err := NewDispatcher(nil).Dispatch(context.Background(), &testJob{}); err == nil {
		t.Fatal("expected nil dispatcher error")
	}
	noBatch := newRuntimeBackedManagerForTest("sync", "default", map[string]queuecontract.Queue{"sync": bareQueue{}}, NewMemoryFailedStore(), nil, newTestRegistry())
	noBatch.runtime.batch = nil
	if _, err := noBatch.BatchStatus(context.Background(), "missing"); !errors.Is(err, ErrEmpty) {
		t.Fatalf("expected empty batch status, got %v", err)
	}
	if err := noBatch.CancelBatch(context.Background(), "missing"); !errors.Is(err, ErrEmpty) {
		t.Fatalf("expected empty cancel, got %v", err)
	}
	if err := noBatch.createBatch(context.Background(), payload.BatchStatus{ID: "x"}); err == nil {
		t.Fatal("expected missing batch store error")
	}
	useTestCache(t, "memory")
	if _, err := NewDispatcher(noBatch).Dispatch(context.Background(), &testJob{Key: "u"}, Unique("x", time.Second)); err != nil {
		t.Fatalf("unique should use cache without connection support: %v", err)
	}
	if _, err := NewDispatcher(noBatch).Dispatch(context.Background(), &testJob{Key: "d"}, Debounce("x", time.Second)); err != nil {
		t.Fatalf("debounce should use cache without connection support: %v", err)
	}

	panicManager := newSyncManager()
	if _, err := NewDispatcher(panicManager).Dispatch(context.Background(), panicJob{}); err == nil {
		t.Fatal("expected panic job error")
	}
}

func TestWorkerBackoffSleepAndBatchCancelBranches(t *testing.T) {
	manager, srv := newRedisManager(t)
	status, err := manager.Batch(&testJob{Key: "cancelled"}).Options(Delay(10 * time.Millisecond)).Dispatch(context.Background())
	if err != nil {
		t.Fatalf("dispatch batch: %v", err)
	}
	if err := manager.CancelBatch(context.Background(), status.ID); err != nil {
		t.Fatalf("cancel batch: %v", err)
	}
	time.Sleep(20 * time.Millisecond)
	if err := NewWorker(manager).Work(context.Background(), WorkerOptions{Once: true}); err != nil {
		t.Fatalf("work cancelled batch: %v", err)
	}
	if _, err := manager.BatchStatus(context.Background(), "missing"); !errors.Is(err, ErrEmpty) {
		t.Fatalf("missing batch status err = %v", err)
	}

	env := &payload.Envelope{Attempts: 3, BackoffSec: []int{1, 2}}
	if got := nextBackoff(env, WorkerOptions{}); got != 2*time.Second {
		t.Fatalf("nextBackoff tail = %v", got)
	}
	if shouldRetry(&payload.Envelope{Attempts: 1}, WorkerOptions{Tries: 0}) != true {
		t.Fatal("tries <= 0 should allow retry")
	}
	if envTimeout(&payload.Envelope{TimeoutSec: 2}, time.Second) != 2*time.Second {
		t.Fatal("expected env timeout")
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()
	if err := NewWorker(manager).Work(ctx, WorkerOptions{Sleep: time.Second}); err != nil {
		t.Fatalf("sleep worker: %v", err)
	}
	srv.Close()
}

func TestLaravelStyleFailureControls(t *testing.T) {
	resetTestLog()
	manager, _ := newRedisManager(t)
	ctx := context.Background()

	if _, err := NewDispatcher(manager).Dispatch(ctx, &testJob{Key: "expired", FailTill: 2}, Tries(5), RetryUntil(time.Now().Add(-time.Second))); err != nil {
		t.Fatalf("dispatch retry until: %v", err)
	}
	if err := NewWorker(manager).Work(ctx, WorkerOptions{Once: true, Tries: 5}); err != nil {
		t.Fatalf("work retry until: %v", err)
	}

	if _, err := NewDispatcher(manager).Dispatch(ctx, &testJob{Key: "max-ex", FailTill: 2}, Tries(5), MaxExceptions(1)); err != nil {
		t.Fatalf("dispatch max exceptions: %v", err)
	}
	if err := NewWorker(manager).Work(ctx, WorkerOptions{Once: true, Tries: 5}); err != nil {
		t.Fatalf("work max exceptions: %v", err)
	}

	if _, err := NewDispatcher(manager).Dispatch(ctx, &testJob{Key: "timeout-fail", SleepMS: 30}, Tries(5), Timeout(time.Millisecond), FailOnTimeout()); err != nil {
		t.Fatalf("dispatch fail on timeout: %v", err)
	}
	if err := NewWorker(manager).Work(ctx, WorkerOptions{Once: true, Tries: 5, Timeout: time.Millisecond}); err != nil {
		t.Fatalf("work fail on timeout: %v", err)
	}

	if _, err := NewDispatcher(manager).Dispatch(ctx, &failedHookJob{Key: "hook"}, Tries(1)); err != nil {
		t.Fatalf("dispatch failed hook: %v", err)
	}
	if err := NewWorker(manager).Work(ctx, WorkerOptions{Once: true, Tries: 1}); err != nil {
		t.Fatalf("work failed hook: %v", err)
	}

	if _, err := NewDispatcher(manager).Dispatch(ctx, &failNowJob{Key: "now"}, Tries(5)); err != nil {
		t.Fatalf("dispatch fail now: %v", err)
	}
	if err := NewWorker(manager).Work(ctx, WorkerOptions{Once: true, Tries: 5}); err != nil {
		t.Fatalf("work fail now: %v", err)
	}

	page, err := manager.Failed().Page(ctx, state.PageRequest{Page: 1, PageSize: 10})
	if err != nil {
		t.Fatalf("failed page: %v", err)
	}
	items := page.Items
	if len(items) != 5 {
		t.Fatalf("failed count = %d, want 5: %#v", len(items), items)
	}
	testLog.Lock()
	defer testLog.Unlock()
	if !containsString(testLog.items, "failed:hook:hook fail hook") {
		t.Fatalf("failed hook log = %v", testLog.items)
	}
}

func TestEncryptedPayloadJob(t *testing.T) {
	resetTestLog()
	ctx := context.Background()
	if _, err := NewDispatcher(newSyncManager()).Dispatch(ctx, &encryptedTestJob{Key: "missing-cipher"}); err == nil {
		t.Fatal("expected missing cipher error")
	}

	manager, _ := newRedisManager(t)
	manager.runtime.payloadEncrypter = testQueueEncrypter(t)
	if _, err := NewDispatcher(manager).Dispatch(ctx, &encryptedTestJob{Key: "encrypted"}); err != nil {
		t.Fatalf("dispatch encrypted provider: %v", err)
	}
	conn := mustQueueEnvelope(t, manager, "redis")
	reserved, err := conn.Pop(ctx, []string{"default"})
	env := reservedEnvelope(reserved)
	if err != nil {
		t.Fatalf("pop encrypted job: %v", err)
	}
	if env == nil {
		t.Fatal("expected encrypted envelope")
	}
	if !env.Encrypted {
		t.Fatal("expected encrypted envelope")
	}
	if bytes.Contains(env.Payload, []byte("encrypted")) {
		t.Fatalf("payload should not contain plain job data: %s", env.Payload)
	}
	if err := newJobRunner(manager, "redis").Process(ctx, env); err != nil {
		t.Fatalf("process encrypted job: %v", err)
	}
	if err := reserved.Delete(ctx); err != nil {
		t.Fatalf("delete encrypted job: %v", err)
	}

	if _, err := NewDispatcher(manager).Dispatch(ctx, &testJob{Key: "encrypted-option"}, Encrypt()); err != nil {
		t.Fatalf("dispatch encrypted option: %v", err)
	}
	if err := NewWorker(manager).Work(ctx, WorkerOptions{Once: true}); err != nil {
		t.Fatalf("work encrypted option: %v", err)
	}
	testLog.Lock()
	defer testLog.Unlock()
	if !containsString(testLog.items, "encrypted") || !containsString(testLog.items, "encrypted-option") {
		t.Fatalf("encrypted jobs were not handled: %v", testLog.items)
	}
}

func TestEncryptionHelpersAndManagerAccessors(t *testing.T) {
	encrypter := testQueueEncrypter(t)
	token, err := encrypter.Encrypt(context.Background(), []byte(`{"key":"value"}`))
	if err != nil {
		t.Fatalf("encrypt payload: %v", err)
	}
	if !bytes.HasPrefix(token, []byte("prismgo:v1:")) {
		t.Fatalf("encrypted payload missing prismgo envelope: %q", token)
	}
	plain, err := encrypter.Decrypt(context.Background(), token)
	if err != nil || string(plain) != `{"key":"value"}` {
		t.Fatalf("decrypt payload = %s, %v", plain, err)
	}
	short := base64.StdEncoding.EncodeToString([]byte("short"))
	if _, err := encrypter.Decrypt(context.Background(), []byte(short)); err == nil {
		t.Fatal("expected legacy encrypted payload to be rejected")
	}
	if _, err := encryptedToken([]byte("{bad")); err == nil {
		t.Fatal("expected encrypted token parse error")
	}
	if raw, err := encryptedRawMessage("token"); err != nil || !bytes.Contains(raw, []byte("token")) {
		t.Fatalf("encrypted raw message = %s, %v", raw, err)
	}

	manager := newSyncManager()
	manager.UseMiddleware(MiddlewareFunc(func(ctx context.Context, _ Job, next Next) error { return next(ctx) }))
	if manager.Failed() == nil {
		t.Fatal("expected failed store")
	}
	if got := parsePositiveInt("7", 1); got != 7 {
		t.Fatalf("parse positive int = %d", got)
	}
	if got := parsePositiveInt("-1", 3); got != 3 {
		t.Fatalf("parse negative int fallback = %d", got)
	}
}

func TestRedisErrorAndFallbackBranches(t *testing.T) {
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	client := redis.NewClient(&redis.Options{Addr: srv.Addr(), MaxRetries: -1})
	t.Cleanup(func() { _ = client.Close() })
	conn := redisqueue.NewRedisQueueFromClient(client, redisqueue.RedisOptions{Prefix: "edge", Codec: encodingpkg.JSON()})
	failedStore := redisqueue.NewRedisFailedStoreFromClient(client, redisqueue.RedisOptions{Prefix: "edge", Codec: encodingpkg.JSON()})
	if err := client.RPush(context.Background(), conn.ReadyKey("default"), "{bad json").Err(); err != nil {
		t.Fatalf("push bad json: %v", err)
	}
	if _, err := conn.Pop(context.Background(), []string{"default"}); err == nil {
		t.Fatal("expected bad json pop error")
	}
	if err := failedStore.Record(context.Background(), payload.FailedJob{ID: "f1", JobID: "j1", JobName: testJobName()}); err != nil {
		t.Fatalf("record failed: %v", err)
	}
	if all := mustFailedItems(t, failedStore); len(all) != 1 {
		t.Fatalf("failed page = %v", all)
	}
	if got := redisqueue.KeyQueueName("edge", "other"); got != "other" {
		t.Fatalf("keyQueueName = %q", got)
	}
	srv.Close()
	if _, err := conn.Size(context.Background(), "default"); err == nil {
		t.Fatal("expected size error after close")
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("close after redis closed: %v", err)
	}
	var nilConn *redisqueue.RedisQueue
	if err := nilConn.Close(); err != nil {
		t.Fatalf("nil close: %v", err)
	}
}

func TestMemoryBatchStoreMarkBatchJobIgnoresDuplicateCompletion(t *testing.T) {
	store := NewMemoryBatchStore()
	status := payload.BatchStatus{ID: "batch-dup", Total: 1, Pending: 1}
	if err := store.CreateBatch(context.Background(), status); err != nil {
		t.Fatalf("create batch: %v", err)
	}
	first, err := store.MarkBatchJob(context.Background(), status.ID, false)
	if err != nil {
		t.Fatalf("first mark: %v", err)
	}
	second, err := store.MarkBatchJob(context.Background(), status.ID, true)
	if err != nil {
		t.Fatalf("second mark: %v", err)
	}
	if first.Processed != 1 || first.Failed != 1 || first.Pending != 0 || first.FinishedAt.IsZero() {
		t.Fatalf("first status = %#v", first)
	}
	if second.Processed != first.Processed || second.Failed != first.Failed || second.Pending != first.Pending {
		t.Fatalf("duplicate mark changed status: first=%#v second=%#v", first, second)
	}
}

func TestStateNormalizePageClampsOversizedPageSize(t *testing.T) {
	page := state.NormalizePage(state.PageRequest{Page: 2, PageSize: state.MaxPageSize + 500})
	if page.Page != 2 {
		t.Fatalf("page = %d, want 2", page.Page)
	}
	if page.PageSize != state.MaxPageSize {
		t.Fatalf("page size = %d, want clamp to %d", page.PageSize, state.MaxPageSize)
	}
}

func TestRemainingBranches(t *testing.T) {
	registry := useQueueTestContainer(t)
	bindQueueConfigInRegistry(t, registry, configpkg.New())
	cacheManager, err := cache.NewManager(cache.Config{
		Default: "memory",
		Stores:  map[string]cache.StoreConfig{"memory": {Driver: "memory", CleanupInterval: time.Millisecond}},
		Lock:    cache.LockConfig{RetrySleep: time.Millisecond},
	})
	if err != nil {
		t.Fatalf("new cache manager: %v", err)
	}
	if err := registry.Instance("cache.manager", cacheManager); err != nil {
		t.Fatalf("bind cache manager: %v", err)
	}
	t.Cleanup(func() { _ = cacheManager.Close() })
	if err := (ServiceProvider{}).Register(providerTestApp{registry: registry}); err != nil {
		t.Fatalf("register service provider: %v", err)
	}
	if resolved := Resolve(); resolved == nil {
		t.Fatal("resolve registered app manager returned nil")
	}
	fire(context.Background(), nil)
	if _, err := NewManager(Config{Default: "sync"}, nil); err != nil {
		t.Fatalf("new manager with nil registry: %v", err)
	}
	jobRegistry := NewRegistry()
	RegisterTypeTo[*testJob](jobRegistry)
	typeName, _ := JobTypeName(&testJob{})
	if !jobRegistry.Has(typeName) {
		t.Fatalf("expected registered type %s", typeName)
	}
	badClose := newRuntimeBackedManagerForTest("bad", "default", map[string]queuecontract.Queue{"bad": errorCloseQueue{}}, NewMemoryFailedStore(), nil, NewRegistry())
	if err := badClose.Close(); err == nil {
		t.Fatal("expected close error")
	}
	if err := releaseUnique(context.Background(), &payload.Envelope{UniqueKey: "x"}); err != nil {
		t.Fatalf("release unique bare: %v", err)
	}
	if _, ok := (&Manager{defaultConnection: "missing"}).batchStore(); ok {
		t.Fatal("expected missing batch store")
	}
	missingRetry := newRuntimeBackedManagerForTest("missing", "default", nil, NewMemoryFailedStore(), nil, nil)
	_ = missingRetry.runtime.failed.Record(context.Background(), payload.FailedJob{ID: "missing-retry", Connection: "missing", Envelope: payload.Envelope{Queue: "default"}})
	if err := NewDispatcher(missingRetry).RetryFailed(context.Background(), "missing-retry"); err == nil {
		t.Fatal("expected retry failed missing connection")
	}
	manager := newSyncManager()
	if _, err := manager.Chain().Dispatch(context.Background()); err != nil {
		t.Fatalf("empty chain: %v", err)
	}
	if _, err := manager.Chain(&testJob{Key: "ok"}, nil).Dispatch(context.Background()); err == nil {
		t.Fatal("expected chain nil job error")
	}
	if _, err := manager.Batch(nil).Dispatch(context.Background()); err == nil {
		t.Fatal("expected batch nil job error")
	}
	if err := manager.MarkBatchJob(context.Background(), "missing", true); err != nil {
		t.Fatalf("mark missing batch: %v", err)
	}
	batchStore, ok := manager.batchStore()
	if !ok {
		t.Fatal("expected manager batch store")
	}
	if _, err := batchStore.GetBatch(context.Background(), "missing"); !errors.Is(err, ErrEmpty) {
		t.Fatalf("missing sync batch = %v", err)
	}
	if err := (&syncReservedJob{}).Delete(context.Background()); err != nil {
		t.Fatalf("sync delete: %v", err)
	}

	if err := RateLimit("disabled", 0, 0).Handle(context.Background(), &testJob{}, func(context.Context) error { return nil }); err != nil {
		t.Fatalf("disabled rate limiter: %v", err)
	}
	if err := SkipIf(nil).Handle(context.Background(), &testJob{}, func(context.Context) error { return nil }); err != nil {
		t.Fatalf("nil skip predicate: %v", err)
	}
	if (ReleaseError{}).Error() == "" {
		t.Fatal("expected default release error")
	}
	useTestCache(t, "memory")
	busy := cache.Default().Lock(overlapCacheKey("busy"), time.Second)
	if ok, err := busy.Get(context.Background()); err != nil || !ok {
		t.Fatalf("pre-acquire overlap lock: %v %v", ok, err)
	}
	t.Cleanup(func() { _ = busy.Release(context.Background()) })
	err = WithoutOverlapping("busy", time.Second).Handle(context.Background(), &testJob{}, func(context.Context) error { return nil })
	if _, ok := ReleaseDelay(err); !ok {
		t.Fatalf("expected overlap release, got %v", err)
	}
	err = WithoutOverlapping("busy").DontRelease().ExpireAfter(time.Second).Shared().Handle(context.Background(), &testJob{}, func(context.Context) error { return nil })
	if !errors.Is(err, ErrSkipped) {
		t.Fatalf("expected overlap skip, got %v", err)
	}
	throttleKey := "queue-throttle-test"
	throttleRepo := cacheManager.Default()
	t.Cleanup(func() { _ = ratelimit.New(throttleRepo).Clear(context.Background(), throttleKey) })
	throttle := ThrottlesExceptions(1, time.Second).By(throttleKey).Backoff(time.Millisecond)
	err = throttle.Handle(context.Background(), &testJob{}, func(context.Context) error { return errors.New("boom") })
	if _, ok := ReleaseDelay(err); !ok {
		t.Fatalf("expected throttled exception release, got %v", err)
	}
	err = throttle.Handle(context.Background(), &testJob{}, func(context.Context) error { return nil })
	if _, ok := ReleaseDelay(err); !ok {
		t.Fatalf("expected active throttle release, got %v", err)
	}
	if err := WithoutOverlapping("err", time.Second).Via(cache.Store("missing")).Handle(context.Background(), &testJob{}, func(context.Context) error { return nil }); err == nil {
		t.Fatal("expected overlap lock error")
	}

	srv, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	defer srv.Close()
	useRedisLifecycleManager(t, srv.Addr())
	conn, err := redisqueue.NewRedisQueue(redisqueue.RedisOptions{Prefix: "p"})
	if err != nil {
		t.Fatalf("new redis queue: %v", err)
	}
	_ = conn.Close()
	defaultConn, err := redisqueue.NewRedisQueue(redisqueue.RedisOptions{})
	if err != nil {
		t.Fatalf("new default redis queue: %v", err)
	}
	if defaultConn.Client().Options().Addr == "" {
		t.Fatal("expected default redis addr")
	}
	_ = defaultConn.Close()
	client := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	redisConn := redisqueue.NewRedisQueueFromClient(client, redisqueue.RedisOptions{Prefix: "edge2", Codec: encodingpkg.JSON(), BlockFor: time.Second})
	batchStore = redisqueue.NewRedisBatchStoreFromClient(client, redisqueue.RedisOptions{Prefix: "edge2", Codec: encodingpkg.JSON()})
	if err := client.LPush(context.Background(), redisConn.ReadyKey("default")+":notify", "1").Err(); err != nil {
		t.Fatalf("seed redis notify token: %v", err)
	}
	if _, err := redisConn.Pop(context.Background(), []string{"default"}); !errors.Is(err, ErrEmpty) {
		t.Fatalf("expected blocking empty, got %v", err)
	}
	if _, err := batchStore.GetBatch(context.Background(), "missing"); !errors.Is(err, ErrEmpty) {
		t.Fatalf("missing redis batch = %v", err)
	}
	if _, err := redisConn.Pop(context.Background(), nil); !errors.Is(err, ErrEmpty) {
		t.Fatalf("expected nil queue empty, got %v", err)
	}
}

func TestMoreStateBranches(t *testing.T) {
	resetTestLog()
	useTestCache(t, "memory")
	manager := newSyncManager()
	if _, err := NewDispatcher(manager).Dispatch(context.Background(), &uniqueTestJob{Key: "provider"}); err != nil {
		t.Fatalf("provider options dispatch: %v", err)
	}
	if _, err := manager.runtime.registry.Marshal(badMarshalJob{Ch: make(chan int)}); err == nil {
		t.Fatal("expected marshal error")
	}
	if _, err := manager.runtime.registry.Unmarshal(testJobName(), []byte("{bad")); err == nil {
		t.Fatal("expected unmarshal error")
	}
	store := NewMemoryFailedStore()
	if err := store.Record(context.Background(), payload.FailedJob{JobID: "default-id"}); err != nil {
		t.Fatalf("record default failed: %v", err)
	}
	if _, err := store.Find(context.Background(), "missing"); !errors.Is(err, ErrEmpty) {
		t.Fatalf("missing memory failed = %v", err)
	}

	redisManager, _ := newRedisManagerWithTiming(t, 10*time.Millisecond, 0)
	conn := mustQueueEnvelope(t, redisManager, "redis")
	env := &payload.Envelope{ID: "reserved", Name: testJobName(), Queue: "default", Payload: []byte(`{"key":"reserved"}`)}
	body, err := conn.Codec().Marshal(env)
	if err != nil {
		t.Fatalf("marshal reserved: %v", err)
	}
	if err := conn.Push(context.Background(), "default", queuecontract.Payload(body)); err != nil {
		t.Fatalf("push reserved: %v", err)
	}
	reserved, err := conn.Pop(context.Background(), []string{"default"})
	popped := reservedEnvelope(reserved)
	if err != nil {
		t.Fatalf("pop reserved: %v", err)
	}
	if popped.Attempts != 1 {
		t.Fatalf("attempts = %d", popped.Attempts)
	}
	time.Sleep(20 * time.Millisecond)
	againReserved, err := conn.Pop(context.Background(), []string{"default"})
	again := reservedEnvelope(againReserved)
	if err != nil || again.Attempts != 2 {
		t.Fatalf("reserved retry pop = %#v %v", again, err)
	}
	if err := againReserved.Release(context.Background(), 0); err != nil {
		t.Fatalf("release reserved: %v", err)
	}
}

func TestRedisDueMigrationIsAtomicAcrossConcurrentWorkers(t *testing.T) {
	manager, _ := newRedisManager(t)
	conn := mustQueueEnvelope(t, manager, "redis")
	ctx := context.Background()
	env := &payload.Envelope{ID: "delayed-once", Name: testJobName(), Queue: "default", Payload: []byte(`{"key":"delayed-once"}`)}
	body, err := conn.Codec().Marshal(env)
	if err != nil {
		t.Fatalf("marshal delayed: %v", err)
	}
	if err := conn.Client().ZAdd(ctx, conn.DelayedKey("default"), redis.Z{Score: float64(time.Now().Add(-time.Millisecond).UnixMilli()), Member: body}).Err(); err != nil {
		t.Fatalf("seed delayed: %v", err)
	}

	start := make(chan struct{})
	var wg sync.WaitGroup
	popped := make(chan *payload.Envelope, 2)
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			reserved, err := conn.Pop(ctx, []string{"default"})
			if errors.Is(err, ErrEmpty) {
				return
			}
			if err != nil {
				errs <- err
				return
			}
			popped <- reservedEnvelope(reserved)
		}()
	}
	close(start)
	wg.Wait()
	close(popped)
	close(errs)
	for err := range errs {
		t.Fatalf("pop error: %v", err)
	}
	if len(popped) != 1 {
		t.Fatalf("concurrent migration popped %d jobs, want 1", len(popped))
	}
}

func TestRedisReservedRawBodyIsRetriedWhenWorkerDiesBeforeEnvelopeUpdate(t *testing.T) {
	manager, _ := newRedisManager(t)
	conn := mustQueueEnvelope(t, manager, "redis")
	ctx := context.Background()
	env := &payload.Envelope{ID: "raw-reserved", Name: testJobName(), Queue: "default", Payload: []byte(`{"key":"raw-reserved"}`)}
	body, err := conn.Codec().Marshal(env)
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	if err := conn.Client().ZAdd(ctx, conn.ReservedKey("default"), redis.Z{
		Score:  float64(time.Now().Add(-time.Millisecond).UnixMilli()),
		Member: body,
	}).Err(); err != nil {
		t.Fatalf("seed raw reserved body: %v", err)
	}

	reserved, err := conn.Pop(ctx, []string{"default"})
	got := reservedEnvelope(reserved)
	if err != nil {
		t.Fatalf("pop migrated raw reserved body: %v", err)
	}
	if got.ID != env.ID || got.Attempts != 1 {
		t.Fatalf("popped envelope = %+v", got)
	}
}

func TestWorkerDoesNotDeleteWhenFailedStoreRecordFails(t *testing.T) {
	registry := newTestRegistry()
	conn := &trackingFailureConnection{}
	manager := newRuntimeBackedManagerForTest("err", "default", map[string]queuecontract.Queue{"err": conn}, errorFailedStore{}, nil, registry)
	err := NewWorker(manager).Work(context.Background(), WorkerOptions{Once: true, Tries: 1})
	if err == nil {
		t.Fatal("expected failed store error")
	}
	if conn.deleted.Load() {
		t.Fatal("job was deleted before failed record succeeded")
	}
}

func TestWorkerReturnsDeleteErrorAfterFailedStoreRecordSucceeds(t *testing.T) {
	registry := newTestRegistry()
	conn := &trackingFailureConnection{deleteErr: errors.New("delete failed")}
	manager := newRuntimeBackedManagerForTest("err", "default", map[string]queuecontract.Queue{"err": conn}, NewMemoryFailedStore(), nil, registry)
	err := NewWorker(manager).Work(context.Background(), WorkerOptions{Once: true, Tries: 1})
	if err == nil || !strings.Contains(err.Error(), "delete failed") {
		t.Fatalf("expected delete error, got %v", err)
	}
	if !conn.deleted.Load() {
		t.Fatal("expected delete attempt after failed record")
	}
	if all := mustFailedItems(t, manager.Failed()); len(all) != 1 {
		t.Fatalf("failed records = %v", all)
	}
}

func TestBatchMarkJobIsAtomicUnderConcurrency(t *testing.T) {
	manager := newSyncManager()
	ctx := context.Background()
	status := payload.BatchStatus{ID: "concurrent-batch", Total: 100, Pending: 100, CreatedAt: time.Now()}
	if err := manager.createBatch(ctx, status); err != nil {
		t.Fatalf("create batch: %v", err)
	}

	var wg sync.WaitGroup
	for i := 0; i < status.Total; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := manager.MarkBatchJob(ctx, status.ID, i%10 != 0); err != nil {
				t.Errorf("mark batch: %v", err)
			}
		}()
	}
	wg.Wait()
	got, err := manager.BatchStatus(ctx, status.ID)
	if err != nil {
		t.Fatalf("batch status: %v", err)
	}
	if got.Pending != 0 || got.Processed != status.Total || got.Failed != 10 || got.FinishedAt.IsZero() {
		t.Fatalf("batch status = %+v", got)
	}
}

func TestRedisBatchMarkJobIsAtomicAcrossConnections(t *testing.T) {
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	defer srv.Close()
	ctx := context.Background()
	clientA := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	clientB := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	connA := redisqueue.NewRedisBatchStoreFromClient(clientA, redisqueue.RedisOptions{Prefix: "redis_batch_atomic"})
	connB := redisqueue.NewRedisBatchStoreFromClient(clientB, redisqueue.RedisOptions{Prefix: "redis_batch_atomic"})
	defer func() {
		_ = clientA.Close()
		_ = clientB.Close()
	}()
	status := payload.BatchStatus{ID: "redis-concurrent-batch", Total: 80, Pending: 80, CreatedAt: time.Now()}
	if err := connA.CreateBatch(ctx, status); err != nil {
		t.Fatalf("create batch: %v", err)
	}

	var wg sync.WaitGroup
	for i := 0; i < status.Total; i++ {
		i := i
		conn := connA
		if i%2 == 1 {
			conn = connB
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := conn.MarkBatchJob(ctx, status.ID, i%8 != 0); err != nil {
				t.Errorf("mark redis batch: %v", err)
			}
		}()
	}
	wg.Wait()
	got, err := connA.GetBatch(ctx, status.ID)
	if err != nil {
		t.Fatalf("get batch: %v", err)
	}
	if got.Pending != 0 || got.Processed != status.Total || got.Failed != 10 || got.FinishedAt.IsZero() {
		t.Fatalf("redis batch status = %+v", got)
	}
}

func TestRedisBatchMarkJobIgnoresDuplicateCompletion(t *testing.T) {
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	defer srv.Close()
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	defer func() { _ = client.Close() }()
	store := redisqueue.NewRedisBatchStoreFromClient(client, redisqueue.RedisOptions{Prefix: "redis_batch_dup"})
	if err := store.CreateBatch(ctx, payload.BatchStatus{ID: "dup", Total: 1, Pending: 1}); err != nil {
		t.Fatalf("create batch: %v", err)
	}
	first, err := store.MarkBatchJob(ctx, "dup", false)
	if err != nil {
		t.Fatalf("first mark: %v", err)
	}
	second, err := store.MarkBatchJob(ctx, "dup", true)
	if err != nil {
		t.Fatalf("second mark: %v", err)
	}
	if first.Processed != 1 || first.Failed != 1 || second.Processed != 1 || second.Failed != 1 {
		t.Fatalf("duplicate redis completion changed status: first=%+v second=%+v", first, second)
	}
}

func TestManagerClosePreventsQueueReuse(t *testing.T) {
	manager := newSyncManager()
	if err := manager.Close(); err != nil {
		t.Fatalf("close manager: %v", err)
	}
	if _, err := manager.Queue("sync"); !errors.Is(err, ErrManagerClosed) {
		t.Fatalf("queue after close err = %v, want ErrManagerClosed", err)
	}
}

func TestManagerCloseWinsOverInFlightQueueBuild(t *testing.T) {
	manager := newSyncManager()
	queueConn := &closeTrackingQueue{contractOnlyQueue: &contractOnlyQueue{}}
	connector := &blockingConnector{
		queue:   queueConn,
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	Extend("blocking", connector)
	manager.connectionSpecs["blocking"] = ConnectionConfig{Driver: "blocking", Queue: "default"}

	result := make(chan error, 1)
	go func() {
		_, err := manager.Queue("blocking")
		result <- err
	}()

	<-connector.started
	if err := manager.Close(); err != nil {
		t.Fatalf("close manager: %v", err)
	}
	close(connector.release)

	if err := <-result; !errors.Is(err, ErrManagerClosed) {
		t.Fatalf("queue build after close err = %v, want ErrManagerClosed", err)
	}
	if got := connector.calls.Load(); got != 1 {
		t.Fatalf("connector calls = %d, want 1", got)
	}
	if got := queueConn.closeCalls.Load(); got != 1 {
		t.Fatalf("built queue close calls = %d, want 1", got)
	}
}

func TestManagerClosePropagatesToConcurrentQueueWaiters(t *testing.T) {
	manager := newSyncManager()
	queueConn := &closeTrackingQueue{contractOnlyQueue: &contractOnlyQueue{}}
	connector := &blockingConnector{
		queue:   queueConn,
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	Extend("blocking", connector)
	manager.connectionSpecs["blocking"] = ConnectionConfig{Driver: "blocking", Queue: "default"}

	first := make(chan error, 1)
	second := make(chan error, 1)
	go func() {
		_, err := manager.Queue("blocking")
		first <- err
	}()
	<-connector.started
	go func() {
		_, err := manager.Queue("blocking")
		second <- err
	}()

	if err := manager.Close(); err != nil {
		t.Fatalf("close manager: %v", err)
	}
	close(connector.release)

	if err := <-first; !errors.Is(err, ErrManagerClosed) {
		t.Fatalf("first waiter err = %v, want ErrManagerClosed", err)
	}
	if err := <-second; !errors.Is(err, ErrManagerClosed) {
		t.Fatalf("second waiter err = %v, want ErrManagerClosed", err)
	}
	if got := connector.calls.Load(); got != 1 {
		t.Fatalf("connector calls = %d, want 1", got)
	}
	if got := queueConn.closeCalls.Load(); got != 1 {
		t.Fatalf("built queue close calls = %d, want 1", got)
	}
}

func TestWorkerTimeoutGraceReleasesUnresponsiveJob(t *testing.T) {
	t.Cleanup(func() { timeoutJobControl.Store(nil) })
	registry := NewRegistry()
	RegisterTypeTo[*blockingTimeoutJob](registry)
	control := &timeoutJobControlState{started: make(chan struct{}), finish: make(chan struct{})}
	conn := &trackingReleaseConnection{
		control:  control,
		released: make(chan struct{}, 1),
	}
	body, err := registry.Marshal(&blockingTimeoutJob{})
	if err != nil {
		t.Fatalf("marshal timeout job: %v", err)
	}
	name, _ := JobTypeName(&blockingTimeoutJob{})
	conn.env = &payload.Envelope{ID: "timeout-wait", Name: name, Queue: "default", Payload: body, Attempts: 1}
	manager := newRuntimeBackedManagerForTest("err", "default", map[string]queuecontract.Queue{"err": conn}, NewMemoryFailedStore(), nil, registry)
	done := make(chan error, 1)
	go func() {
		done <- NewWorker(manager).Work(context.Background(), WorkerOptions{Once: true, Tries: 2, Timeout: 20 * time.Millisecond, TimeoutGrace: 10 * time.Millisecond})
	}()
	<-control.started
	// 需求背景：任务不响应 context 时，旧实现会一直等 Handle 返回；新的语义要求 timeout+grace
	// 后释放 envelope，让 worker 不再被单个坏任务永久占住。
	select {
	case <-conn.released:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected release after timeout grace")
	}
	if err := <-done; err != nil {
		t.Fatalf("worker returned error: %v", err)
	}
	close(control.finish)
}

func TestWorkerTimeoutGraceFailOnTimeoutDeletesAndRecordsFailedJob(t *testing.T) {
	t.Cleanup(func() { timeoutJobControl.Store(nil) })
	appRegistry := useQueueTestContainer(t)
	if err := appRegistry.Instance("exception.handler", exception.New(exception.WithLogging(false))); err != nil {
		t.Fatalf("bind exception handler: %v", err)
	}
	registry := NewRegistry()
	RegisterTypeTo[*blockingTimeoutJob](registry)
	control := &timeoutJobControlState{started: make(chan struct{}), finish: make(chan struct{})}
	conn := &trackingReleaseConnection{
		control:  control,
		released: make(chan struct{}, 1),
		deleted:  make(chan struct{}, 1),
	}
	body, err := registry.Marshal(&blockingTimeoutJob{})
	if err != nil {
		t.Fatalf("marshal timeout job: %v", err)
	}
	name, _ := JobTypeName(&blockingTimeoutJob{})
	conn.env = &payload.Envelope{ID: "timeout-fail", Name: name, Queue: "default", Payload: body, Attempts: 1, FailOnTimeout: true}
	failed := NewMemoryFailedStore()
	manager := newRuntimeBackedManagerForTest("err", "default", map[string]queuecontract.Queue{"err": conn}, failed, nil, registry)
	failedEvents := make(chan JobFailed, 1)
	UseEventSink(func(_ context.Context, ev Event) {
		if failed, ok := ev.(JobFailed); ok {
			failedEvents <- failed
		}
	})
	t.Cleanup(func() { UseEventSink(nil) })

	done := make(chan error, 1)
	go func() {
		done <- NewWorker(manager).Work(context.Background(), WorkerOptions{Once: true, Tries: 5, Timeout: 20 * time.Millisecond, TimeoutGrace: 10 * time.Millisecond})
	}()
	<-control.started

	select {
	case <-conn.deleted:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected delete after fail-on-timeout")
	}
	if err := <-done; err != nil {
		t.Fatalf("worker returned error: %v", err)
	}
	close(control.finish)
	items := mustFailedItems(t, failed)
	if len(items) != 1 || items[0].JobID != "timeout-fail" {
		t.Fatalf("failed items = %#v", items)
	}
	select {
	case ev := <-failedEvents:
		if ev.JobID != items[0].JobID || ev.JobName != items[0].JobName || ev.Queue != items[0].Queue {
			t.Fatalf("job_failed event = %#v, failed item = %#v", ev, items[0])
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected queue.JobFailed event")
	}
	select {
	case <-conn.released:
		t.Fatal("fail-on-timeout should not release for retry")
	default:
	}
}

func TestWorkerTimeoutGraceUsesJobErrorWhenReturnedDuringGrace(t *testing.T) {
	registry := NewRegistry()
	RegisterTypeTo[*graceErrorTimeoutJob](registry)
	conn := &trackingReleaseConnection{released: make(chan struct{}, 1)}
	body, err := registry.Marshal(&graceErrorTimeoutJob{Message: "grace error", DelayMS: 5})
	if err != nil {
		t.Fatalf("marshal grace job: %v", err)
	}
	name, _ := JobTypeName(&graceErrorTimeoutJob{})
	conn.env = &payload.Envelope{ID: "timeout-grace-error", Name: name, Queue: "default", Payload: body, Attempts: 1}
	manager := newRuntimeBackedManagerForTest("err", "default", map[string]queuecontract.Queue{"err": conn}, NewMemoryFailedStore(), nil, registry)
	releasedErr := make(chan string, 1)
	UseEventSink(func(_ context.Context, ev Event) {
		if released, ok := ev.(JobReleased); ok {
			releasedErr <- released.Err
		}
	})
	t.Cleanup(func() { UseEventSink(nil) })

	if err := NewWorker(manager).Work(context.Background(), WorkerOptions{Once: true, Tries: 2, Timeout: 20 * time.Millisecond, TimeoutGrace: 100 * time.Millisecond}); err != nil {
		t.Fatalf("worker returned error: %v", err)
	}
	select {
	case got := <-releasedErr:
		if got != "grace error" {
			t.Fatalf("release error = %q, want grace error", got)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected release event")
	}
}

func TestJobRunnerSkipsSuccessSideEffectsAfterTimeoutGrace(t *testing.T) {
	t.Cleanup(func() { timeoutJobControl.Store(nil) })
	registry := NewRegistry()
	RegisterTypeTo[*lateSuccessTimeoutJob](registry)
	RegisterTypeTo[*testJob](registry)
	control := &timeoutJobControlState{started: make(chan struct{}), finish: make(chan struct{})}
	conn := &trackingLateSuccessConnection{
		SyncConnection: NewSyncConnection(),
		control:        control,
		released:       make(chan struct{}, 1),
	}
	body, err := registry.Marshal(&lateSuccessTimeoutJob{})
	if err != nil {
		t.Fatalf("marshal late success job: %v", err)
	}
	nextPayload, err := registry.Marshal(&testJob{Key: "chain-after-timeout"})
	if err != nil {
		t.Fatalf("marshal chain job: %v", err)
	}
	name, _ := JobTypeName(&lateSuccessTimeoutJob{})
	nextName, _ := JobTypeName(&testJob{})
	conn.env = &payload.Envelope{
		ID:       "timeout-late-success",
		Name:     name,
		Queue:    "default",
		Payload:  body,
		Attempts: 1,
		BatchID:  "late-batch",
		Chain:    []payload.PendingJob{{Name: nextName, Payload: nextPayload}},
	}
	batchStore := NewMemoryBatchStore()
	if err := batchStore.CreateBatch(context.Background(), payload.BatchStatus{ID: "late-batch", Total: 1, Pending: 1, CreatedAt: time.Now()}); err != nil {
		t.Fatalf("create batch: %v", err)
	}
	manager := newRuntimeBackedManagerForTest("err", "default", map[string]queuecontract.Queue{"err": conn}, NewMemoryFailedStore(), batchStore, registry)
	var processed atomic.Int32
	var queued atomic.Int32
	UseEventSink(func(_ context.Context, ev Event) {
		if _, ok := ev.(JobProcessed); ok {
			processed.Add(1)
		}
		if _, ok := ev.(JobQueued); ok {
			queued.Add(1)
		}
	})
	t.Cleanup(func() { UseEventSink(nil) })

	done := make(chan error, 1)
	go func() {
		done <- NewWorker(manager).Work(context.Background(), WorkerOptions{Once: true, Tries: 2, Timeout: 20 * time.Millisecond, TimeoutGrace: 10 * time.Millisecond})
	}()
	<-control.started
	<-conn.released
	if err := <-done; err != nil {
		t.Fatalf("worker returned error: %v", err)
	}

	// 逻辑说明：worker 已经按超时释放 envelope 后，再放行迟到成功的 Handle。
	// 如果 JobRunner 没有检查 ctx.Err()，这里会误更新 batch、派发 chain 并发出成功事件。
	close(control.finish)
	time.Sleep(50 * time.Millisecond)
	status, err := batchStore.GetBatch(context.Background(), "late-batch")
	if err != nil {
		t.Fatalf("get batch: %v", err)
	}
	if status.Pending != 1 || status.Processed != 0 || status.Failed != 0 {
		t.Fatalf("batch was updated after timeout: %#v", status)
	}
	if conn.pushed.Load() != 0 {
		t.Fatal("chain was dispatched after timeout")
	}
	if processed.Load() != 0 {
		t.Fatal("JobProcessed was fired after timeout")
	}
	if queued.Load() != 0 {
		t.Fatal("JobQueued was fired after timeout")
	}
}

func TestJobRunnerProcessNilEnvelopeReturnsExplicitError(t *testing.T) {
	registry := newTestRegistry()
	manager := newRuntimeBackedManagerForTest("err", "default", map[string]queuecontract.Queue{"err": bareQueue{}}, NewMemoryFailedStore(), nil, registry)
	jobRunner := newJobRunner(manager, "err")
	var processing atomic.Int32
	UseEventSink(func(_ context.Context, ev Event) {
		if _, ok := ev.(JobProcessing); ok {
			processing.Add(1)
		}
	})
	t.Cleanup(func() { UseEventSink(nil) })

	err := jobRunner.Process(context.Background(), nil)
	if err == nil || err.Error() != "queue: envelope is nil" {
		t.Fatalf("Process(nil) error = %v, want queue: envelope is nil", err)
	}
	if processing.Load() != 0 {
		t.Fatal("JobProcessing was fired for nil envelope")
	}
}

func TestJobRunnerDispatchNextChainFiresQueuedEventAfterSuccessfulPush(t *testing.T) {
	registry := newTestRegistry()
	conn := &chainQueuedConnection{}
	manager := newRuntimeBackedManagerForTest("err", "default", map[string]queuecontract.Queue{"err": conn}, NewMemoryFailedStore(), nil, registry)
	jobRunner := newQueueJobRunner(manager, manager.runtimeOrDefault(), conn, "err")
	body, err := registry.Marshal(&testJob{Key: "first"})
	if err != nil {
		t.Fatalf("marshal first job: %v", err)
	}
	nextPayload, err := registry.Marshal(&testJob{Key: "next"})
	if err != nil {
		t.Fatalf("marshal next job: %v", err)
	}
	var queued []JobQueued
	UseEventSink(func(_ context.Context, ev Event) {
		if event, ok := ev.(JobQueued); ok {
			queued = append(queued, event)
		}
	})
	t.Cleanup(func() { UseEventSink(nil) })

	cases := []struct {
		name      string
		nextQueue string
		wantQueue string
	}{
		{name: "fallback parent queue", nextQueue: "", wantQueue: "parent"},
		{name: "override next queue", nextQueue: "chain-next", wantQueue: "chain-next"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			queued = nil
			conn.lastQueue = ""
			conn.lastBody = nil
			err := jobRunner.Process(context.Background(), &payload.Envelope{
				ID:       "first-id",
				Name:     testJobName(),
				Queue:    "parent",
				Payload:  body,
				Attempts: 1,
				Tags:     []string{"tag-a", "tag-b"},
				Silenced: true,
				Chain: []payload.PendingJob{{
					Name:    testJobName(),
					Queue:   tc.nextQueue,
					Payload: nextPayload,
				}},
			})
			if err != nil {
				t.Fatalf("Process error: %v", err)
			}
			if conn.lastQueue != tc.wantQueue {
				t.Fatalf("pushed queue = %q, want %q", conn.lastQueue, tc.wantQueue)
			}
			if len(queued) != 1 {
				t.Fatalf("queued events = %d, want 1", len(queued))
			}
			if queued[0].Queue != tc.wantQueue {
				t.Fatalf("queued event queue = %q, want %q", queued[0].Queue, tc.wantQueue)
			}
			if queued[0].Connection != "err" || queued[0].JobName != testJobName() {
				t.Fatalf("queued event = %#v", queued[0])
			}
			if fmt.Sprint(queued[0].Tags) != fmt.Sprint([]string{"tag-a", "tag-b"}) || !queued[0].Silenced {
				t.Fatalf("queued event metadata = %#v", queued[0])
			}
		})
	}
}

func TestJobRunnerDispatchNextChainDoesNotFireQueuedEventWhenPushFails(t *testing.T) {
	registry := newTestRegistry()
	conn := &chainQueuedConnection{pushErr: errors.New("push failed")}
	manager := newRuntimeBackedManagerForTest("err", "default", map[string]queuecontract.Queue{"err": conn}, NewMemoryFailedStore(), nil, registry)
	jobRunner := newQueueJobRunner(manager, manager.runtimeOrDefault(), conn, "err")
	body, err := registry.Marshal(&testJob{Key: "first"})
	if err != nil {
		t.Fatalf("marshal first job: %v", err)
	}
	nextPayload, err := registry.Marshal(&testJob{Key: "next"})
	if err != nil {
		t.Fatalf("marshal next job: %v", err)
	}
	var queued atomic.Int32
	UseEventSink(func(_ context.Context, ev Event) {
		if _, ok := ev.(JobQueued); ok {
			queued.Add(1)
		}
	})
	t.Cleanup(func() { UseEventSink(nil) })

	err = jobRunner.Process(context.Background(), &payload.Envelope{
		ID:       "first-id",
		Name:     testJobName(),
		Queue:    "parent",
		Payload:  body,
		Attempts: 1,
		Chain: []payload.PendingJob{{
			Name:    testJobName(),
			Payload: nextPayload,
		}},
	})
	if err == nil || err.Error() != "push failed" {
		t.Fatalf("Process error = %v, want push failed", err)
	}
	if queued.Load() != 0 {
		t.Fatal("JobQueued was fired after failed chain push")
	}
}

func TestJobRunnerAndWorkerErrorBranches(t *testing.T) {
	useTestCache(t, "memory")
	registry := newTestRegistry()
	manager := newRuntimeBackedManagerForTest("err", "default", map[string]queuecontract.Queue{"err": bareQueue{}}, NewMemoryFailedStore(), errorBatchStore{}, registry)
	jobRunner := newJobRunner(manager, "err")
	err := jobRunner.Process(context.Background(), &payload.Envelope{ID: "p1", Name: testJobName(), Queue: "default", Payload: []byte(`{"key":"batch"}`), BatchID: "batch"})
	if err == nil {
		t.Fatal("expected batch update error")
	}
	err = jobRunner.Process(context.Background(), &payload.Envelope{ID: "p2", Name: "missing", Queue: "default"})
	if err == nil {
		t.Fatal("expected missing job error")
	}
	debounceJobRunner := newJobRunner(manager, "err")
	if err := debounceJobRunner.Process(context.Background(), &payload.Envelope{ID: "p3", Name: testJobName(), Queue: "default", DebounceKey: "x", DebounceVia: "missing"}); err == nil {
		t.Fatal("expected debounce error")
	}
	if got := firstString("", "fallback"); got != "fallback" {
		t.Fatalf("firstString = %q", got)
	}

	popManager := newRuntimeBackedManagerForTest("err", "default", map[string]queuecontract.Queue{"err": errorPopQueue{}}, NewMemoryFailedStore(), nil, registry)
	if err := NewWorker(popManager).Work(context.Background(), WorkerOptions{Once: true}); err == nil {
		t.Fatal("expected pop error")
	}
	releaseManager := newRuntimeBackedManagerForTest("err", "default", map[string]queuecontract.Queue{"err": errorReleaseQueue{}}, NewMemoryFailedStore(), nil, registry)
	if err := NewWorker(releaseManager).Work(context.Background(), WorkerOptions{Once: true, Tries: 2}); err == nil {
		t.Fatal("expected release error")
	}
	failedManager := newRuntimeBackedManagerForTest("err", "default", map[string]queuecontract.Queue{"err": failingJobQueue{}}, errorFailedStore{}, nil, registry)
	if err := NewWorker(failedManager).Work(context.Background(), WorkerOptions{Once: true, Tries: 1}); err == nil {
		t.Fatal("expected failed store error")
	}
	if stale, err := staleDebounce(context.Background(), &payload.Envelope{DebounceKey: "not-written-job-runner-branch"}); stale || err != nil {
		t.Fatalf("bare stale debounce = %v %v", stale, err)
	}
}

func TestWorkerDoesNotDeleteReservedJobWhenStaleDebounceErrors(t *testing.T) {
	// 需求背景：该用例只验证指定的 debounce store 解析失败时，worker 不应删除已保留任务。
	// 测试容器需要显式绑定 cache manager，否则会变成未装配容器导致的 panic，而不是业务错误路径。
	useTestCache(t, "memory")

	registry := newTestRegistry()
	reservedDeleted := atomic.Bool{}
	reserved := &testReservedJob{
		env: &payload.Envelope{ID: "debounce-error", Name: testJobName(), Queue: "default", DebounceKey: "x", DebounceVia: "missing"},
		deleteFn: func(context.Context) error {
			reservedDeleted.Store(true)
			return nil
		},
	}
	queueConn := debounceErrorQueue{reserved: reserved}
	manager := newRuntimeBackedManagerForTest("err", "default", map[string]queuecontract.Queue{"err": queueConn}, NewMemoryFailedStore(), nil, registry)
	if err := NewWorker(manager).Work(context.Background(), WorkerOptions{Connection: "err", Once: true}); err == nil {
		t.Fatal("expected debounce cache error")
	}
	if reservedDeleted.Load() {
		t.Fatal("reserved job should not be deleted when staleDebounce returns error")
	}
}

func TestRedisFailedStorePageReturnsCleanedTotalAndEntryTTL(t *testing.T) {
	ctx := context.Background()
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	client := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	defer func() { _ = client.Close() }()
	store := redisqueue.NewRedisFailedStoreFromClient(client, redisqueue.RedisOptions{Prefix: "failed_ttl", FailedTTL: time.Second, Codec: encodingpkg.JSON()})
	if err := store.Record(ctx, payload.FailedJob{ID: "expired-1", JobID: "job-1", FailedAt: time.Now()}); err != nil {
		t.Fatalf("record failed: %v", err)
	}
	if ttl := srv.TTL(store.FailedHashKey() + ":entry:expired-1"); ttl <= 0 {
		t.Fatalf("entry ttl = %s, want positive", ttl)
	}
	if err := client.ZAdd(ctx, store.FailedIndexKey(), redis.Z{Score: 2, Member: "missing"}).Err(); err != nil {
		t.Fatalf("seed missing index: %v", err)
	}
	page, err := store.Page(ctx, state.PageRequest{Page: 1, PageSize: 10})
	if err != nil {
		t.Fatalf("page failed: %v", err)
	}
	if page.Total != len(page.Items) {
		t.Fatalf("page total = %d items = %d, want cleaned total", page.Total, len(page.Items))
	}
}

func TestRedisStoreErrorBranches(t *testing.T) {
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	client := redis.NewClient(&redis.Options{Addr: srv.Addr(), MaxRetries: -1})
	t.Cleanup(func() { _ = client.Close() })
	failedStore := redisqueue.NewRedisFailedStoreFromClient(client, redisqueue.RedisOptions{Prefix: "redis_errors", Codec: encodingpkg.JSON()})
	batchStore := redisqueue.NewRedisBatchStoreFromClient(client, redisqueue.RedisOptions{Prefix: "redis_errors", Codec: encodingpkg.JSON()})
	queueConn := redisqueue.NewRedisQueueFromClient(client, redisqueue.RedisOptions{Prefix: "redis_errors", Codec: encodingpkg.JSON()})
	if err := client.HSet(context.Background(), failedStore.FailedHashKey(), "bad", "{bad").Err(); err != nil {
		t.Fatalf("hset failed: %v", err)
	}
	if _, err := failedStore.Find(context.Background(), "bad"); err == nil {
		t.Fatal("expected bad failed json error")
	}
	if err := client.Set(context.Background(), batchStore.BatchKey("bad"), "{bad", 0).Err(); err != nil {
		t.Fatalf("set batch: %v", err)
	}
	if _, err := batchStore.GetBatch(context.Background(), "bad"); !errors.Is(err, ErrEmpty) {
		t.Fatalf("expected old string batch to be ignored, got %v", err)
	}
	if err := client.ZAdd(context.Background(), failedStore.FailedIndexKey(), redis.Z{Score: 1, Member: "missing"}, redis.Z{Score: 2, Member: "bad"}).Err(); err != nil {
		t.Fatalf("push index: %v", err)
	}
	if all := mustFailedItems(t, failedStore); len(all) != 0 {
		t.Fatalf("page should skip bad/missing, got %v", all)
	}
	srv.Close()
	if _, err := failedStore.Page(context.Background(), state.PageRequest{Page: 1, PageSize: 10}); err == nil {
		t.Fatal("expected page redis error")
	}
	if err := failedStore.Record(context.Background(), payload.FailedJob{ID: "x"}); err == nil {
		t.Fatal("expected record error")
	}
	if err := batchStore.CreateBatch(context.Background(), payload.BatchStatus{ID: "x"}); err == nil {
		t.Fatal("expected create batch error")
	}
	if err := batchStore.UpdateBatch(context.Background(), payload.BatchStatus{ID: "x"}); err == nil {
		t.Fatal("expected update batch error")
	}
	if _, err := batchStore.GetBatch(context.Background(), "x"); err == nil {
		t.Fatal("expected get batch redis error")
	}
	_ = queueConn
}

type bareQueue struct{}

func (bareQueue) Push(context.Context, string, queuecontract.Payload) error { return nil }
func (bareQueue) Later(context.Context, string, queuecontract.Payload, time.Duration) error {
	return nil
}
func (bareQueue) Bulk(_ context.Context, _ string, bodies []queuecontract.Payload) (queuecontract.BulkResult, error) {
	return queuecontract.BulkResult{Accepted: len(bodies)}, nil
}
func (bareQueue) Pop(context.Context, []string, ...queuecontract.PopWaitMode) (queuecontract.ReservedJob, error) {
	return nil, ErrEmpty
}
func (bareQueue) Clear(context.Context, string) error         { return nil }
func (bareQueue) Size(context.Context, string) (int64, error) { return 0, nil }
func (bareQueue) Close() error                                { return nil }

type testReservedJob struct {
	env        *payload.Envelope
	deleteErr  error
	releaseErr error
	deleteFn   func(context.Context) error
	releaseFn  func(context.Context, time.Duration) error
}

type chainQueuedConnection struct {
	bareQueue
	pushErr   error
	lastQueue string
	lastBody  queuecontract.Payload
}

func (c *chainQueuedConnection) Push(_ context.Context, queue string, body queuecontract.Payload) error {
	if c.pushErr != nil {
		return c.pushErr
	}
	c.lastQueue = queue
	c.lastBody = append(queuecontract.Payload(nil), body...)
	return nil
}

func (j *testReservedJob) ID() string {
	if j == nil || j.env == nil {
		return ""
	}
	return j.env.ID
}

func (j *testReservedJob) Name() string {
	if j == nil || j.env == nil {
		return ""
	}
	return j.env.Name
}

func (j *testReservedJob) Payload() queuecontract.Payload {
	body, _ := encodingpkg.JSON().Marshal(j.env)
	return queuecontract.Payload(body)
}

func (j *testReservedJob) Attempts() int {
	if j == nil || j.env == nil {
		return 0
	}
	return j.env.Attempts
}

func (j *testReservedJob) Delete(ctx context.Context) error {
	if j != nil && j.deleteFn != nil {
		return j.deleteFn(ctx)
	}
	if j == nil {
		return nil
	}
	return j.deleteErr
}

func (j *testReservedJob) Release(ctx context.Context, delay time.Duration) error {
	if j != nil && j.releaseFn != nil {
		return j.releaseFn(ctx, delay)
	}
	if j == nil {
		return nil
	}
	return j.releaseErr
}

func (j *testReservedJob) envelope() *payload.Envelope {
	if j == nil {
		return nil
	}
	return cloneEnvelope(j.env)
}

type capturingConnector struct {
	calls  atomic.Int32
	name   string
	config map[string]any
	queue  *contractOnlyQueue
}

func (c *capturingConnector) Connect(_ context.Context, name string, config map[string]any) (queuecontract.Queue, error) {
	c.calls.Add(1)
	c.name = name
	c.config = cloneAnyMap(config)
	return c.queue, nil
}

type errorConnector struct {
	err error
}

func (c errorConnector) Connect(context.Context, string, map[string]any) (queuecontract.Queue, error) {
	return nil, c.err
}

type blockingConnector struct {
	calls   atomic.Int32
	queue   queuecontract.Queue
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (c *blockingConnector) Connect(context.Context, string, map[string]any) (queuecontract.Queue, error) {
	c.calls.Add(1)
	c.once.Do(func() { close(c.started) })
	<-c.release
	return c.queue, nil
}

type flakyConnector struct {
	calls   atomic.Int32
	queue   queuecontract.Queue
	started chan struct{}
	release chan struct{}
	mu      sync.Mutex
	err     error
}

func (c *flakyConnector) Connect(context.Context, string, map[string]any) (queuecontract.Queue, error) {
	c.calls.Add(1)
	c.mu.Lock()
	started := c.started
	release := c.release
	err := c.err
	queue := c.queue
	c.mu.Unlock()
	select {
	case <-started:
	default:
		close(started)
	}
	<-release
	if err != nil {
		return nil, err
	}
	return queue, nil
}

type closeTrackingQueue struct {
	*contractOnlyQueue
	closeCalls atomic.Int32
}

func (q *closeTrackingQueue) Close() error {
	q.closeCalls.Add(1)
	return nil
}

type notifyingConnection struct {
	*SyncConnection
	popped chan struct{}
	once   sync.Once
}

func (c *notifyingConnection) Pop(context.Context, []string, ...queuecontract.PopWaitMode) (queuecontract.ReservedJob, error) {
	c.once.Do(func() { close(c.popped) })
	return nil, ErrEmpty
}

type debounceErrorQueue struct {
	bareQueue
	reserved queuecontract.ReservedJob
}

func (q debounceErrorQueue) Pop(context.Context, []string, ...queuecontract.PopWaitMode) (queuecontract.ReservedJob, error) {
	return q.reserved, nil
}

type errorCloseQueue struct{ bareQueue }

func (errorCloseQueue) Close() error { return errors.New("close failed") }

type errorRestartStore struct{}

func (errorRestartStore) RequestRestart(context.Context, time.Time) error { return nil }
func (errorRestartStore) RestartRequestedAt(context.Context) (time.Time, error) {
	return time.Now(), errors.New("restart read failed")
}

type errorBatchStore struct{}

func (errorBatchStore) GetBatch(context.Context, string) (*payload.BatchStatus, error) {
	return &payload.BatchStatus{ID: "batch", Pending: 1}, nil
}
func (errorBatchStore) UpdateBatch(context.Context, payload.BatchStatus) error {
	return errors.New("batch update failed")
}
func (errorBatchStore) CreateBatch(context.Context, payload.BatchStatus) error { return nil }
func (errorBatchStore) DeleteBatch(context.Context, string) error              { return nil }
func (errorBatchStore) MarkBatchJob(context.Context, string, bool) (payload.BatchStatus, error) {
	return payload.BatchStatus{}, errors.New("batch update failed")
}
func (errorBatchStore) CancelBatch(context.Context, string) (payload.BatchStatus, error) {
	return payload.BatchStatus{}, errors.New("batch update failed")
}

type errorPopQueue struct{ bareQueue }

func (errorPopQueue) Pop(context.Context, []string, ...queuecontract.PopWaitMode) (queuecontract.ReservedJob, error) {
	return nil, errors.New("pop failed")
}

type errorReleaseQueue struct{ bareQueue }

func (errorReleaseQueue) Pop(context.Context, []string, ...queuecontract.PopWaitMode) (queuecontract.ReservedJob, error) {
	return &testReservedJob{env: &payload.Envelope{ID: "r", Name: testJobName(), Queue: "default", Payload: []byte(`{"key":"r","fail_till":3}`), Attempts: 1}, releaseErr: errors.New("release failed")}, nil
}

type failingJobQueue struct{ bareQueue }

func (failingJobQueue) Pop(context.Context, []string, ...queuecontract.PopWaitMode) (queuecontract.ReservedJob, error) {
	return &testReservedJob{env: &payload.Envelope{ID: "f", Name: testJobName(), Queue: "default", Payload: []byte(`{"key":"f","fail_till":3}`), Attempts: 1}}, nil
}

type trackingFailureConnection struct {
	bareQueue
	deleted   atomic.Bool
	deleteErr error
}

func (c *trackingFailureConnection) Pop(context.Context, []string, ...queuecontract.PopWaitMode) (queuecontract.ReservedJob, error) {
	return &testReservedJob{
		env: &payload.Envelope{ID: "f", Name: testJobName(), Queue: "default", Payload: []byte(`{"key":"f","fail_till":3}`), Attempts: 1},
		deleteFn: func(context.Context) error {
			c.deleted.Store(true)
			return c.deleteErr
		},
	}, nil
}

func (c *trackingFailureConnection) Delete(context.Context, *payload.Envelope) error {
	c.deleted.Store(true)
	return c.deleteErr
}

type blockingTimeoutJob struct{}

// timeoutJobControlState 用于测试中精确控制 Handle 的开始和结束。
//
// 需求背景：本组用例要复现“任务已经超时但 Handle 不返回”的 worker 卡死场景，
// 因此不能用简单 sleep；通过 started/finish 两个 channel 可以让测试稳定等待
// worker 进入超时处理，再决定是否放行后台 job。
type timeoutJobControlState struct {
	started   chan struct{}
	finish    chan struct{}
	startOnce sync.Once
}

var timeoutJobControl atomic.Pointer[timeoutJobControlState]

// Handle 模拟忽略 context 并一直阻塞的任务。
//
// 逻辑说明：只有测试显式关闭 finish 后才返回 ctx.Err()，用于验证 worker 不会在超时后
// 永久等待这个不合作任务。
func (blockingTimeoutJob) Handle(ctx context.Context) error {
	control := timeoutJobControl.Load()
	if control == nil {
		return nil
	}
	control.startOnce.Do(func() { close(control.started) })
	<-control.finish
	return ctx.Err()
}

// graceErrorTimeoutJob 模拟任务在 timeout 后的 grace 窗口内返回业务错误。
//
// 设计思路：worker 应保留这个真实错误，让 retry 事件和失败判定能看到原始原因，
// 不能把所有已响应 context 的任务都改写为 DeadlineExceeded。
type graceErrorTimeoutJob struct {
	Message string `json:"message"`
	DelayMS int    `json:"delay_ms,omitempty"`
}

// Handle 先等待 context 超时，再延迟短时间返回指定错误。
func (j *graceErrorTimeoutJob) Handle(ctx context.Context) error {
	<-ctx.Done()
	if j.DelayMS > 0 {
		time.Sleep(time.Duration(j.DelayMS) * time.Millisecond)
	}
	return errors.New(j.Message)
}

// lateSuccessTimeoutJob 模拟超过 grace 后才迟到成功的任务。
//
// 需求背景：worker 已经按超时释放 envelope 后，后台 Handle 后续返回 nil 不能再触发
// batch 成功、chain 派发或 JobProcessed 事件。
type lateSuccessTimeoutJob struct{}

// Handle 在测试放行后返回 nil，用于验证 JobRunner 的 ctx.Err() 后置保护。
func (lateSuccessTimeoutJob) Handle(context.Context) error {
	control := timeoutJobControl.Load()
	if control == nil {
		return nil
	}
	control.startOnce.Do(func() { close(control.started) })
	<-control.finish
	return nil
}

// trackingReleaseConnection 记录 worker 对 reserved envelope 的 Release/Delete 动作。
//
// 参数用途：env 是 Pop 返回的测试 envelope；control 绑定到阻塞型 job；
// released/deleted 用于断言 worker 在 timeout grace 后执行重试或失败确认。
type trackingReleaseConnection struct {
	bareQueue
	env      *payload.Envelope
	control  *timeoutJobControlState
	released chan struct{}
	deleted  chan struct{}
}

// Pop 返回预置 envelope，并把控制器挂到测试 job 可读取的全局指针上。
func (c *trackingReleaseConnection) Pop(context.Context, []string, ...queuecontract.PopWaitMode) (queuecontract.ReservedJob, error) {
	if c.control != nil {
		timeoutJobControl.Store(c.control)
	}
	return &testReservedJob{
		env: cloneEnvelope(c.env),
		releaseFn: func(context.Context, time.Duration) error {
			if c.released != nil {
				c.released <- struct{}{}
			}
			return nil
		},
		deleteFn: func(context.Context) error {
			if c.deleted != nil {
				c.deleted <- struct{}{}
			}
			return nil
		},
	}, nil
}

// trackingLateSuccessConnection 复用 SyncConnection 的 batch store，同时记录 chain Push。
//
// 逻辑说明：迟到成功如果错误地继续执行 JobRunner 成功后置逻辑，会调用 Push 派发 chain；
// pushed 计数用于断言该副作用没有发生。
type trackingLateSuccessConnection struct {
	*SyncConnection
	env      *payload.Envelope
	control  *timeoutJobControlState
	released chan struct{}
	pushed   atomic.Int32
}

// Pop 返回带 batch 和 chain 的 envelope，并安装迟到成功任务控制器。
func (c *trackingLateSuccessConnection) Pop(context.Context, []string, ...queuecontract.PopWaitMode) (queuecontract.ReservedJob, error) {
	timeoutJobControl.Store(c.control)
	return &testReservedJob{
		env: cloneEnvelope(c.env),
		releaseFn: func(context.Context, time.Duration) error {
			c.released <- struct{}{}
			return nil
		},
	}, nil
}

// Push 记录 chain 派发尝试，并保留 SyncConnection 的正常入队行为供断言扩展使用。
func (c *trackingLateSuccessConnection) Push(ctx context.Context, queue string, body queuecontract.Payload) error {
	c.pushed.Add(1)
	return c.SyncConnection.Push(ctx, queue, body)
}

func mustFailedItems(t *testing.T, store FailedStore) []payload.FailedJob {
	t.Helper()
	page, err := store.Page(context.Background(), state.PageRequest{Page: 1, PageSize: 100})
	if err != nil {
		t.Fatalf("failed page: %v", err)
	}
	return page.Items
}

type errorFailedStore struct{}

func (errorFailedStore) Record(context.Context, payload.FailedJob) error {
	return errors.New("record failed")
}
func (errorFailedStore) Page(context.Context, state.PageRequest) (state.PageEnvelope[payload.FailedJob], error) {
	return state.PageEnvelope[payload.FailedJob]{}, nil
}
func (errorFailedStore) Find(context.Context, string) (*payload.FailedJob, error) {
	return nil, ErrEmpty
}
func (errorFailedStore) Forget(context.Context, string) error { return nil }
func (errorFailedStore) Flush(context.Context) error          { return nil }
