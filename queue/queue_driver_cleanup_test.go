package queue

import (
	"context"
	"errors"
	redisqueue "github.com/prismgo/framework/queue/redis"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
	"unsafe"

	miniredis "github.com/alicebob/miniredis/v2"
	configpkg "github.com/prismgo/framework/config"
	queuecontract "github.com/prismgo/framework/contracts/queue"
	encodingpkg "github.com/prismgo/framework/encoding"
	"github.com/prismgo/framework/queue/payload"
	rabbitmqdriver "github.com/prismgo/framework/queue/rabbitmq"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"
)

func TestBuildStateStoresRejectUnknownDrivers(t *testing.T) {
	codec := encodingpkg.JSON()

	// 独立 state repository 只能接受显式支持的 driver，避免未知配置静默回退到 memory。
	if _, err := buildFailedStore(Config{Failed: StateStoreConfig{Driver: "unknown"}}, codec); err == nil || !strings.Contains(err.Error(), "unknown failed store driver") {
		t.Fatalf("failed store error = %v", err)
	}
	if _, err := buildBatchStore(Config{Batching: StateStoreConfig{Driver: "unknown"}}, codec); err == nil || !strings.Contains(err.Error(), "unknown batch store driver") {
		t.Fatalf("batch store error = %v", err)
	}
}

func TestBuildRedisStateStoresUseConfiguredPrefixes(t *testing.T) {
	ctx := context.Background()
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	t.Cleanup(srv.Close)
	useRedisLifecycleManager(t, srv.Addr())

	// failed/batch 是独立 state repository，前缀必须读取各自配置，不能继承 transport prefix。
	codec := encodingpkg.JSON()
	failedStore, err := buildFailedStore(Config{Failed: StateStoreConfig{
		Driver: "redis",
		Store:  "default",
		Prefix: "queue_failed_state",
		TTL:    time.Minute,
	}}, codec)
	if err != nil {
		t.Fatalf("build failed store: %v", err)
	}
	batchStore, err := buildBatchStore(Config{Batching: StateStoreConfig{
		Driver: "redis",
		Store:  "default",
		Prefix: "queue_batch_state",
		TTL:    time.Minute,
	}}, codec)
	if err != nil {
		t.Fatalf("build batch store: %v", err)
	}

	if err := failedStore.Record(ctx, payload.FailedJob{ID: "failed-1", JobID: "job-1", Queue: "critical", FailedAt: time.Now()}); err != nil {
		t.Fatalf("record failed job: %v", err)
	}
	if err := batchStore.CreateBatch(ctx, payload.BatchStatus{ID: "batch-1", Name: "imports", Total: 1, Pending: 1}); err != nil {
		t.Fatalf("create batch: %v", err)
	}
	if !srv.Exists("queue_failed_state:failed:entry:failed-1") || !srv.Exists("queue_failed_state:failed:index") {
		t.Fatalf("failed store did not use configured prefix, keys = %v", srv.Keys())
	}
	if !srv.Exists("queue_batch_state:batches:batch-1") {
		t.Fatalf("batch store did not use configured prefix, keys = %v", srv.Keys())
	}
}

func TestQueueConfigUsesCurrentDriverConfigNames(t *testing.T) {
	// 需求背景：driver 配置命名已收口为连接级配置路径；测试直接校验运行时配置解析结果，
	// 避免读取 README/文档导致包测试依赖仓库文档目录布局。
	t.Cleanup(func() {
		resetQueueEncodingConfigRegistryForTest()
	})
	configpkg.Add("queue", func() map[string]any {
		return map[string]any{
			"default": "redis",
			"restart": map[string]any{
				"cache": "cache-store",
				"key":   "queue:restart:test",
			},
			"connections": map[string]any{
				"sync": map[string]any{
					"driver": "sync",
					"queue":  "sync-current",
				},
				"redis": map[string]any{
					"driver":      "redis",
					"queue":       "redis-current",
					"prefix":      "redis-prefix",
					"connection":  "redis-connection",
					"retry_after": 45,
					"block_for":   3,
				},
			},
		}
	})
	cfg, err := configpkg.NewFromFile(filepath.Join(t.TempDir(), ".env"))
	if err != nil {
		t.Fatalf("reload config: %v", err)
	}

	built := buildConfigFromRepository(cfg)
	if built.Default != "redis" {
		t.Fatalf("default connection = %q, want redis", built.Default)
	}
	if built.Restart.Cache != "cache-store" {
		t.Fatalf("restart cache = %q, want cache-store", built.Restart.Cache)
	}
	if built.Connections["sync"].Queue != "sync-current" {
		t.Fatalf("sync queue = %q, want sync-current", built.Connections["sync"].Queue)
	}
	redisConfig := built.Connections["redis"]
	if redisConfig.Queue != "redis-current" || redisConfig.Prefix != "redis-prefix" {
		t.Fatalf("redis queue config = queue %q prefix %q, want redis-current redis-prefix", redisConfig.Queue, redisConfig.Prefix)
	}
	if redisConfig.Options["connection"] != "redis-connection" {
		t.Fatalf("redis connection option = %v, want redis-connection", redisConfig.Options["connection"])
	}
	if redisConfig.RetryAfter != 45*time.Second || redisConfig.BlockFor != 3*time.Second {
		t.Fatalf("redis timing = retry_after %s block_for %s, want 45s 3s", redisConfig.RetryAfter, redisConfig.BlockFor)
	}
}

func TestRedisQueueBulkReservedAccessorsAndRelease(t *testing.T) {
	ctx := context.Background()
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	t.Cleanup(srv.Close)
	client := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	codec := encodingpkg.JSON()
	redisQueue := redisqueue.NewRedisQueueFromClient(client, redisqueue.RedisOptions{
		Name:       "redis_cleanup_test",
		Prefix:     "queue_transport_state",
		RetryAfter: time.Minute,
		Codec:      codec,
	})
	// Redis transport 只接收已编码 envelope；ReservedJob 负责暴露 metadata 和 release/delete。
	bodyA := mustMarshalEnvelopeForTest(t, codec, payload.Envelope{ID: "job-a", Name: testJobName(), Queue: "default", Payload: []byte(`{"key":"a"}`)})
	bodyB := mustMarshalEnvelopeForTest(t, codec, payload.Envelope{ID: "job-b", Name: testJobName(), Queue: "default", Payload: []byte(`{"key":"b"}`)})

	if result, err := redisQueue.Bulk(ctx, "default", []queuecontract.Payload{bodyA, bodyB}); err != nil || result.Accepted != 2 {
		t.Fatalf("bulk push: %v", err)
	}
	if size, err := redisQueue.Size(ctx, "default"); err != nil || size != 2 {
		t.Fatalf("size after bulk = %d, %v; want 2, nil", size, err)
	}

	reserved, err := redisQueue.Pop(ctx, []string{"default"})
	if err != nil {
		t.Fatalf("pop reserved job: %v", err)
	}
	if reserved.ID() != "job-a" || reserved.Name() != testJobName() || reserved.Attempts() != 1 {
		t.Fatalf("reserved metadata = id:%q name:%q attempts:%d", reserved.ID(), reserved.Name(), reserved.Attempts())
	}
	payloadCopy := reserved.Payload()
	payloadCopy[0] = ' '
	if string(reserved.Payload()[0]) == " " {
		t.Fatal("reserved payload should be returned as a defensive copy")
	}
	if err := reserved.Release(ctx, 0); err != nil {
		t.Fatalf("release reserved job: %v", err)
	}
	if size, err := redisQueue.Size(ctx, "default"); err != nil || size != 2 {
		t.Fatalf("size after release = %d, %v; want 2, nil", size, err)
	}

	second, err := redisQueue.Pop(ctx, []string{"default"})
	if err != nil {
		t.Fatalf("pop second job: %v", err)
	}
	if second.ID() != "job-b" {
		t.Fatalf("second reserved id = %q, want job-b", second.ID())
	}
	if err := second.Delete(ctx); err != nil {
		t.Fatalf("delete second job: %v", err)
	}
}

func TestRedisReservedReleasePreservesExceptionsCounter(t *testing.T) {
	ctx := context.Background()
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	defer srv.Close()
	client := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	defer func() { _ = client.Close() }()
	codec := encodingpkg.JSON()
	redisQueue := redisqueue.NewRedisQueueFromClient(client, redisqueue.RedisOptions{Prefix: "queue_release_exceptions", RetryAfter: time.Minute, Codec: codec})
	body := mustMarshalEnvelopeForTest(t, codec, payload.Envelope{ID: "job-ex", Name: testJobName(), Queue: "default", Payload: []byte(`{"key":"ex"}`)})
	if err := redisQueue.Push(ctx, "default", body); err != nil {
		t.Fatalf("push: %v", err)
	}
	reserved, err := redisQueue.Pop(ctx, []string{"default"})
	if err != nil {
		t.Fatalf("pop: %v", err)
	}
	typed, ok := reserved.(*redisqueue.RedisReservedJob)
	if !ok {
		t.Fatalf("reserved type = %T", reserved)
	}
	typedEnv := typedPayloadEnvelopeForTest(t, typed.Payload())
	typedEnv.Exceptions = 2
	setRedisReservedEnvelopeForTest(t, typed, typedEnv)
	if err := typed.Release(ctx, 0); err != nil {
		t.Fatalf("release: %v", err)
	}
	requeued, err := redisQueue.Pop(ctx, []string{"default"})
	if err != nil {
		t.Fatalf("pop requeued: %v", err)
	}
	requeuedEnv := reservedEnvelope(requeued)
	if requeuedEnv == nil || requeuedEnv.Exceptions != 2 {
		t.Fatalf("requeued exceptions = %+v, want 2", requeuedEnv)
	}
}

func TestRedisQueueBulkUsesBatchWriteAndNotifySemantics(t *testing.T) {
	ctx := context.Background()
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	client := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	redisQueue := redisqueue.NewRedisQueueFromClient(client, redisqueue.RedisOptions{Name: "redis_bulk_laravel_config", Prefix: "laravel_config_bulk", Codec: encodingpkg.JSON()})
	if result, err := redisQueue.Bulk(ctx, "default", nil); err != nil || result.Accepted != 0 {
		t.Fatalf("empty bulk: %v", err)
	}
	if srv.Exists(redisQueue.ReadyKey("default")) {
		t.Fatal("empty bulk should not create ready list")
	}

	bodies := []queuecontract.Payload{[]byte("one"), []byte("two"), []byte("three")}
	if result, err := redisQueue.Bulk(ctx, "default", bodies); err != nil || result.Accepted != len(bodies) {
		t.Fatalf("bulk write: %v", err)
	}
	if size, err := redisQueue.Size(ctx, "default"); err != nil || size != int64(len(bodies)) {
		t.Fatalf("size after bulk = %d, %v; want %d, nil", size, err, len(bodies))
	}
	notify, err := srv.List(redisQueue.ReadyKey("default") + ":notify")
	if err != nil {
		t.Fatalf("read notify list: %v", err)
	}
	if len(notify) != len(bodies) {
		t.Fatalf("notify len = %d, want %d", len(notify), len(bodies))
	}

	srv.Close()
	if result, err := redisQueue.Bulk(ctx, "default", []queuecontract.Payload{[]byte("after-close")}); err == nil || result.Accepted != 0 {
		t.Fatal("expected bulk write error after redis closes")
	}
}

func TestRedisQueuePopConsumesNotifyTokenAfterPush(t *testing.T) {
	ctx := context.Background()
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	t.Cleanup(srv.Close)
	client := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	codec := encodingpkg.JSON()
	redisQueue := redisqueue.NewRedisQueueFromClient(client, redisqueue.RedisOptions{Name: "redis_notify_push", Prefix: "issue_notify_push", Codec: codec})
	body := mustMarshalEnvelopeForTest(t, codec, payload.Envelope{ID: "notify-push", Name: testJobName(), Queue: "default", Payload: []byte(`{"key":"notify-push"}`)})
	if err := redisQueue.Push(ctx, "default", body); err != nil {
		t.Fatalf("push: %v", err)
	}

	// 需求背景：Laravel Redis queue 把 :notify 作为阻塞 worker 的唤醒信号；
	// 成功 reserve ready job 时必须同步消费一个 token，否则长期运行会留下无界 notify 列表。
	reserved, err := redisQueue.Pop(ctx, []string{"default"})
	if err != nil {
		t.Fatalf("pop: %v", err)
	}
	if reserved.ID() != "notify-push" {
		t.Fatalf("reserved id = %q, want notify-push", reserved.ID())
	}
	if notify, err := client.LLen(ctx, redisQueue.ReadyKey("default")+":notify").Result(); err != nil || notify != 0 {
		t.Fatalf("notify len after pop = %d, %v; want 0, nil", notify, err)
	}
}

func TestRedisQueuePopConsumesNotifyTokensAfterBulk(t *testing.T) {
	ctx := context.Background()
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	t.Cleanup(srv.Close)
	client := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	codec := encodingpkg.JSON()
	redisQueue := redisqueue.NewRedisQueueFromClient(client, redisqueue.RedisOptions{Name: "redis_notify_bulk", Prefix: "issue_notify_bulk", Codec: codec})
	bodies := []queuecontract.Payload{
		mustMarshalEnvelopeForTest(t, codec, payload.Envelope{ID: "notify-bulk-a", Name: testJobName(), Queue: "default", Payload: []byte(`{"key":"a"}`)}),
		mustMarshalEnvelopeForTest(t, codec, payload.Envelope{ID: "notify-bulk-b", Name: testJobName(), Queue: "default", Payload: []byte(`{"key":"b"}`)}),
		mustMarshalEnvelopeForTest(t, codec, payload.Envelope{ID: "notify-bulk-c", Name: testJobName(), Queue: "default", Payload: []byte(`{"key":"c"}`)}),
	}
	if result, err := redisQueue.Bulk(ctx, "default", bodies); err != nil || result.Accepted != len(bodies) {
		t.Fatalf("bulk: %v", err)
	}

	// 逻辑说明：Bulk 为每条 ready job 写一个 notify token；连续 Pop 后 token 数必须归零，
	// 这样 block_for worker 不会被历史 token 空唤醒，也不会让 Redis list 持续增长。
	for range bodies {
		if _, err := redisQueue.Pop(ctx, []string{"default"}); err != nil {
			t.Fatalf("pop bulk body: %v", err)
		}
	}
	if notify, err := client.LLen(ctx, redisQueue.ReadyKey("default")+":notify").Result(); err != nil || notify != 0 {
		t.Fatalf("notify len after bulk pops = %d, %v; want 0, nil", notify, err)
	}
}

func TestRedisQueueBlockingPopWaitsOnNotifyAndReservesPushedJob(t *testing.T) {
	ctx := context.Background()
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	t.Cleanup(srv.Close)
	client := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	recorder := newRedisCommandRecorder()
	client.AddHook(recorder)

	codec := encodingpkg.JSON()
	redisQueue := redisqueue.NewRedisQueueFromClient(client, redisqueue.RedisOptions{
		Name:       "redis_notify_block",
		Prefix:     "issue_notify_block",
		RetryAfter: time.Minute,
		BlockFor:   500 * time.Millisecond,
		Codec:      codec,
	})
	popped := make(chan queuecontract.ReservedJob, 1)
	errs := make(chan error, 1)
	go func() {
		reserved, err := redisQueue.Pop(ctx, []string{"default"})
		if err != nil {
			errs <- err
			return
		}
		popped <- reserved
	}()

	// 需求背景：阻塞语义必须由 Redis BLPOP :notify 提供；先观察到 BLPOP 再 Push，
	// 才能证明测试不是被短轮询实现碰巧通过。
	recorder.waitForRedisCommand(t, "blpop", 300*time.Millisecond)
	body := mustMarshalEnvelopeForTest(t, codec, payload.Envelope{ID: "notify-block", Name: testJobName(), Queue: "default", Payload: []byte(`{"key":"block"}`)})
	if err := redisQueue.Push(ctx, "default", body); err != nil {
		t.Fatalf("push while blocking: %v", err)
	}

	select {
	case err := <-errs:
		t.Fatalf("blocking pop returned error: %v", err)
	case reserved := <-popped:
		if reserved.ID() != "notify-block" {
			t.Fatalf("reserved id = %q, want notify-block", reserved.ID())
		}
	case <-time.After(time.Second):
		t.Fatal("blocking pop did not wake from notify token")
	}
}

func TestRedisQueueSecondaryQueueHitSuppressesNextPrimaryBlock(t *testing.T) {
	ctx := context.Background()
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	t.Cleanup(srv.Close)
	client := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	recorder := newRedisCommandRecorder()
	client.AddHook(recorder)

	codec := encodingpkg.JSON()
	redisQueue := redisqueue.NewRedisQueueFromClient(client, redisqueue.RedisOptions{
		Name:       "redis_secondary_block",
		Prefix:     "issue_secondary_block",
		RetryAfter: time.Minute,
		BlockFor:   30 * time.Millisecond,
		Codec:      codec,
	})
	for _, id := range []string{"secondary-1", "secondary-2"} {
		body := mustMarshalEnvelopeForTest(t, codec, payload.Envelope{ID: id, Name: testJobName(), Queue: "low", Payload: []byte(`{"key":"secondary"}`)})
		if err := redisQueue.Push(ctx, "low", body); err != nil {
			t.Fatalf("push low %s: %v", id, err)
		}
	}

	first, err := redisQueue.Pop(ctx, []string{"default", "low"}, queuecontract.PopNoWait)
	if err != nil {
		t.Fatalf("secondary first pop: %v", err)
	}
	if first.ID() != "secondary-1" {
		t.Fatalf("first secondary id = %q", first.ID())
	}
	blpopBefore := recorder.commandCount("blpop")
	start := time.Now()
	_, err = redisQueue.Pop(ctx, []string{"default"}, queuecontract.PopNoWait)
	if !errors.Is(err, ErrEmpty) {
		t.Fatalf("primary second pop err = %v, want empty", err)
	}
	if elapsed := time.Since(start); elapsed >= 20*time.Millisecond {
		t.Fatalf("primary second pop took %s, want no block_for wait", elapsed)
	}
	if got := recorder.commandCount("blpop"); got != blpopBefore {
		t.Fatalf("blpop count after secondary hit = %d, want %d", got, blpopBefore)
	}
	second, err := redisQueue.Pop(ctx, []string{"default", "low"}, queuecontract.PopNoWait)
	if err != nil {
		t.Fatalf("secondary second pop: %v", err)
	}
	if second.ID() != "secondary-2" {
		t.Fatalf("second secondary id = %q", second.ID())
	}
}

func TestRedisQueuePopSessionScopesSecondaryQueueState(t *testing.T) {
	ctx := context.Background()
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	t.Cleanup(srv.Close)
	client := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	recorder := newRedisCommandRecorder()
	client.AddHook(recorder)

	codec := encodingpkg.JSON()
	redisQueue := redisqueue.NewRedisQueueFromClient(client, redisqueue.RedisOptions{
		Name:       "redis_secondary_session",
		Prefix:     "issue_secondary_session",
		RetryAfter: time.Minute,
		BlockFor:   30 * time.Millisecond,
		Codec:      codec,
	})
	body := mustMarshalEnvelopeForTest(t, codec, payload.Envelope{ID: "secondary-session", Name: testJobName(), Queue: "low", Payload: []byte(`{"key":"secondary"}`)})
	if err := redisQueue.Push(ctx, "low", body); err != nil {
		t.Fatalf("push low: %v", err)
	}

	sessionA := redisQueue.NewPopSession()
	sessionB := redisQueue.NewPopSession()
	if _, err := sessionA.Pop(ctx, []string{"default"}, queuecontract.PopNoWait); !errors.Is(err, ErrEmpty) {
		t.Fatalf("session A primary pop err = %v, want empty", err)
	}
	if _, err := sessionA.Pop(ctx, []string{"low"}, queuecontract.PopNoWait); err != nil {
		t.Fatalf("session A secondary pop: %v", err)
	}

	blpopBefore := recorder.commandCount("blpop")
	if _, err := sessionB.Pop(ctx, []string{"default"}, queuecontract.PopWaitAvailable); !errors.Is(err, ErrEmpty) {
		t.Fatalf("session B primary pop err = %v, want empty", err)
	}
	if got := recorder.commandCount("blpop"); got != blpopBefore+1 {
		t.Fatalf("session B blpop count = %d, want %d", got, blpopBefore+1)
	}
}

func TestRedisQueueStaleNotifyUsesSingleLaravelStyleWait(t *testing.T) {
	ctx := context.Background()
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	t.Cleanup(srv.Close)
	client := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	recorder := newRedisCommandRecorder()
	client.AddHook(recorder)

	redisQueue := redisqueue.NewRedisQueueFromClient(client, redisqueue.RedisOptions{
		Name:       "redis_stale_notify",
		Prefix:     "issue_stale_notify",
		RetryAfter: time.Minute,
		BlockFor:   60 * time.Millisecond,
		Codec:      encodingpkg.JSON(),
	})
	if err := client.LPush(ctx, redisQueue.ReadyKey("default")+":notify", "1").Err(); err != nil {
		t.Fatalf("seed stale notify: %v", err)
	}

	start := time.Now()
	if _, err := redisQueue.Pop(ctx, []string{"default"}, queuecontract.PopWaitAvailable); !errors.Is(err, ErrEmpty) {
		t.Fatalf("stale notify pop err = %v, want empty", err)
	}
	if elapsed := time.Since(start); elapsed >= 30*time.Millisecond {
		t.Fatalf("stale notify pop took %s, want single notify wake without second block", elapsed)
	}
	if got := recorder.commandCount("blpop"); got != 1 {
		t.Fatalf("blpop count = %d, want 1", got)
	}
}

func TestRedisQueueDueDelayedMigrationNotifiesBlockedWorker(t *testing.T) {
	ctx := context.Background()
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	t.Cleanup(srv.Close)
	client := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	recorder := newRedisCommandRecorder()
	client.AddHook(recorder)

	codec := encodingpkg.JSON()
	blockingQueue := redisqueue.NewRedisQueueFromClient(client, redisqueue.RedisOptions{
		Name:       "redis_notify_delayed_block",
		Prefix:     "issue_notify_delayed",
		RetryAfter: time.Minute,
		BlockFor:   750 * time.Millisecond,
		Codec:      codec,
	})
	migratingQueue := redisqueue.NewRedisQueueFromClient(client, redisqueue.RedisOptions{
		Name:       "redis_notify_delayed_migrate",
		Prefix:     "issue_notify_delayed",
		RetryAfter: time.Minute,
		BlockFor:   0,
		Codec:      codec,
	})
	popped := make(chan queuecontract.ReservedJob, 1)
	errs := make(chan error, 1)
	go func() {
		reserved, err := blockingQueue.Pop(ctx, []string{"default"})
		if err != nil {
			errs <- err
			return
		}
		popped <- reserved
	}()

	// 逻辑说明：先确认阻塞 worker 已进入 BLPOP，再放入到期 delayed job 并由另一个
	// worker 触发迁移；这样测试覆盖的是迁移脚本写 notify 唤醒已有阻塞者。
	recorder.waitForRedisCommand(t, "blpop", 300*time.Millisecond)
	for _, id := range []string{"notify-delayed-a", "notify-delayed-b"} {
		body := mustMarshalEnvelopeForTest(t, codec, payload.Envelope{ID: id, Name: testJobName(), Queue: "default", Payload: []byte(`{"key":"delayed"}`)})
		if err := client.ZAdd(ctx, blockingQueue.DelayedKey("default"), redis.Z{Score: float64(time.Now().Add(-time.Millisecond).UnixMilli()), Member: string(body)}).Err(); err != nil {
			t.Fatalf("seed delayed %s: %v", id, err)
		}
	}
	// 设计思路：另一个 worker 的 Pop 会迁移两条到期 delayed job 并消费其中一条；
	// 迁移脚本必须为剩余 ready job 写 notify，正在 BLPOP :notify 的 worker 才能被唤醒。
	if _, err := migratingQueue.Pop(ctx, []string{"default"}); err != nil {
		t.Fatalf("migrating pop: %v", err)
	}

	select {
	case err := <-errs:
		t.Fatalf("blocked delayed pop returned error: %v", err)
	case reserved := <-popped:
		if reserved.ID() != "notify-delayed-a" && reserved.ID() != "notify-delayed-b" {
			t.Fatalf("reserved delayed id = %q", reserved.ID())
		}
	case <-time.After(time.Second):
		t.Fatal("blocking pop did not wake after delayed migration")
	}
}

func TestRedisQueueDueReservedMigrationNotifiesBlockedWorker(t *testing.T) {
	ctx := context.Background()
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	t.Cleanup(srv.Close)
	client := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	recorder := newRedisCommandRecorder()
	client.AddHook(recorder)

	codec := encodingpkg.JSON()
	blockingQueue := redisqueue.NewRedisQueueFromClient(client, redisqueue.RedisOptions{
		Name:       "redis_notify_reserved_block",
		Prefix:     "issue_notify_reserved",
		RetryAfter: time.Minute,
		BlockFor:   750 * time.Millisecond,
		Codec:      codec,
	})
	migratingQueue := redisqueue.NewRedisQueueFromClient(client, redisqueue.RedisOptions{
		Name:       "redis_notify_reserved_migrate",
		Prefix:     "issue_notify_reserved",
		RetryAfter: time.Minute,
		BlockFor:   0,
		Codec:      codec,
	})
	popped := make(chan queuecontract.ReservedJob, 1)
	errs := make(chan error, 1)
	go func() {
		reserved, err := blockingQueue.Pop(ctx, []string{"default"})
		if err != nil {
			errs <- err
			return
		}
		popped <- reserved
	}()

	// 逻辑说明：先确认阻塞 worker 已进入 BLPOP，再放入过期 reserved job 并由另一个
	// worker 触发迁移；这样能证明 retry_after 迁移也会写 notify 唤醒阻塞者。
	recorder.waitForRedisCommand(t, "blpop", 300*time.Millisecond)
	for _, id := range []string{"notify-reserved-a", "notify-reserved-b"} {
		body := mustMarshalEnvelopeForTest(t, codec, payload.Envelope{ID: id, Name: testJobName(), Queue: "default", Payload: []byte(`{"key":"reserved"}`)})
		if err := client.ZAdd(ctx, blockingQueue.ReservedKey("default"), redis.Z{Score: float64(time.Now().Add(-time.Millisecond).UnixMilli()), Member: string(body)}).Err(); err != nil {
			t.Fatalf("seed reserved %s: %v", id, err)
		}
	}
	// 逻辑说明：reserved retry_after 到期走同一迁移脚本；它与 delayed 一样需要补 notify，
	// 否则已有阻塞 worker 只能等 block_for 超时后下一轮才有机会看到 ready job。
	if _, err := migratingQueue.Pop(ctx, []string{"default"}); err != nil {
		t.Fatalf("migrating pop: %v", err)
	}

	select {
	case err := <-errs:
		t.Fatalf("blocked reserved pop returned error: %v", err)
	case reserved := <-popped:
		if reserved.ID() != "notify-reserved-a" && reserved.ID() != "notify-reserved-b" {
			t.Fatalf("reserved retry id = %q", reserved.ID())
		}
	case <-time.After(time.Second):
		t.Fatal("blocking pop did not wake after reserved migration")
	}
}

func TestManagerQueueReturnsRabbitMQDriverContractQueue(t *testing.T) {
	// 需求背景：Laravel 13 的 QueueManager 只通过 connector 建立 queue contract；
	// PrismGo 的 rabbitmq connector 不应再回到父包 wrapper 或旧 Connection 兼容面。
	dialer := func(string, amqp.Config) (rabbitmqdriver.AMQPConnection, error) {
		return &rabbitMQContractManagerTestConnection{}, nil
	}

	manager, err := NewManager(Config{
		Default: "sync",
		Connections: map[string]ConnectionConfig{
			"sync":     {Driver: "sync", Queue: "default"},
			"rabbitmq": {Driver: "rabbitmq", Queue: "jobs", Options: map[string]any{"declare": true, "dialer": rabbitmqdriver.Dialer(dialer)}},
		},
	}, NewRegistry())
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	t.Cleanup(func() { _ = manager.Close() })

	queueConn, err := manager.Queue("rabbitmq")
	if err != nil {
		t.Fatalf("resolve rabbitmq queue: %v", err)
	}
	if _, ok := queueConn.(*rabbitmqdriver.RabbitMQQueue); !ok {
		t.Fatalf("rabbitmq queue type = %T, want *rabbitmq.RabbitMQQueue", queueConn)
	}
	var _ queuecontract.Queue = queueConn
	if _, ok := queueConn.(queuecontract.ConsumerIntentLeaser); !ok {
		t.Fatalf("rabbitmq queue type = %T, want ConsumerIntentLeaser on contract path", queueConn)
	}
}

type rabbitMQContractManagerTestConnection struct {
	closed bool
}

func (c *rabbitMQContractManagerTestConnection) Channel() (rabbitmqdriver.AMQPChannel, error) {
	return nil, errors.New("manager contract test should not open AMQP channels")
}

func (c *rabbitMQContractManagerTestConnection) NotifyClose(receiver chan *amqp.Error) chan *amqp.Error {
	return receiver
}

func (c *rabbitMQContractManagerTestConnection) Close() error {
	c.closed = true
	return nil
}

func (c *rabbitMQContractManagerTestConnection) IsClosed() bool {
	return c.closed
}

func TestConfigCastHelpersCoverSupportedShapes(t *testing.T) {
	fallback := 9 * time.Second
	// RabbitMQ 配置支持秒数、Go duration 字符串和测试直接传入的强类型 duration。
	durationCases := []struct {
		name  string
		value any
		want  time.Duration
	}{
		{name: "duration", value: 1500 * time.Millisecond, want: 1500 * time.Millisecond},
		{name: "int seconds", value: 2, want: 2 * time.Second},
		{name: "int64 seconds", value: int64(3), want: 3 * time.Second},
		{name: "float seconds", value: 1.5, want: 1500 * time.Millisecond},
		{name: "string duration", value: "250ms", want: 250 * time.Millisecond},
		{name: "string seconds", value: "4", want: 4 * time.Second},
		{name: "empty", value: "", want: fallback},
		{name: "negative", value: -1, want: fallback},
	}
	for _, tc := range durationCases {
		t.Run(tc.name, func(t *testing.T) {
			if got := castDurationValue(tc.value, fallback); got != tc.want {
				t.Fatalf("duration = %v, want %v", got, tc.want)
			}
		})
	}

	if got := castDurationBuckets([]time.Duration{time.Second, 2 * time.Second}, nil); len(got) != 2 || got[1] != 2*time.Second {
		t.Fatalf("duration bucket slice = %v", got)
	}
	if got := castDurationBuckets([]int{1, 0, 3}, nil); len(got) != 2 || got[1] != 3*time.Second {
		t.Fatalf("int bucket slice = %v", got)
	}
	if got := castDurationBuckets([]any{"2", int64(4), -1}, nil); len(got) != 2 || got[1] != 4*time.Second {
		t.Fatalf("any bucket slice = %v", got)
	}
	if got := castDurationBuckets("5, bad, 7", nil); len(got) != 2 || got[1] != 7*time.Second {
		t.Fatalf("string bucket slice = %v", got)
	}
	if got := castDurationBuckets(-1, []time.Duration{fallback}); len(got) != 1 || got[0] != fallback {
		t.Fatalf("fallback bucket slice = %v", got)
	}

	boolCases := []struct {
		value    any
		fallback bool
		want     bool
	}{
		{value: true, fallback: false, want: true},
		{value: "yes", fallback: false, want: true},
		{value: "off", fallback: true, want: false},
		{value: 1, fallback: false, want: true},
		{value: int64(0), fallback: true, want: false},
		{value: 2.0, fallback: false, want: true},
		{value: "unknown", fallback: true, want: true},
	}
	for _, tc := range boolCases {
		if got := castBool(tc.value, tc.fallback); got != tc.want {
			t.Fatalf("castBool(%v) = %v, want %v", tc.value, got, tc.want)
		}
	}
}

func typedPayloadEnvelopeForTest(t *testing.T, body queuecontract.Payload) *payload.Envelope {
	t.Helper()
	var env payload.Envelope
	if err := encodingpkg.JSON().Unmarshal(body, &env); err != nil {
		t.Fatalf("unmarshal envelope payload: %v", err)
	}
	return &env
}

func setRedisReservedEnvelopeForTest(t *testing.T, reserved *redisqueue.RedisReservedJob, env *payload.Envelope) {
	t.Helper()
	value := reflect.ValueOf(reserved).Elem().FieldByName("env")
	reflect.NewAt(value.Type(), unsafe.Pointer(value.UnsafeAddr())).Elem().Set(reflect.ValueOf(env))
}

func mustMarshalEnvelopeForTest(t *testing.T, codec interface {
	Marshal(any) ([]byte, error)
}, env payload.Envelope) queuecontract.Payload {
	t.Helper()
	body, err := codec.Marshal(&env)
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	return queuecontract.Payload(body)
}

type redisCommandRecorder struct {
	mu       sync.Mutex
	commands []string
	seen     chan string
}

// newRedisCommandRecorder 创建测试专用的 Redis 命令观察器。
//
// 设计思路：go-redis Hook 是客户端公开扩展点，测试通过它记录真实发出的命令名，
// 避免用 sleep 猜测 goroutine 是否已经进入阻塞等待。
func newRedisCommandRecorder() *redisCommandRecorder {
	return &redisCommandRecorder{
		seen: make(chan string, 64),
	}
}

func (r *redisCommandRecorder) DialHook(next redis.DialHook) redis.DialHook {
	return next
}

func (r *redisCommandRecorder) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		// 逻辑说明：命令执行前记录名称；BLPOP 会阻塞在 next 中，提前记录才能作为
		// “客户端已实际发送阻塞命令”的同步点。
		r.record(cmd.Name())
		return next(ctx, cmd)
	}
}

func (r *redisCommandRecorder) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		for _, cmd := range cmds {
			r.record(cmd.Name())
		}
		return next(ctx, cmds)
	}
}

func (r *redisCommandRecorder) record(command string) {
	name := strings.ToLower(command)
	r.mu.Lock()
	r.commands = append(r.commands, name)
	r.mu.Unlock()

	select {
	case r.seen <- name:
	default:
	}
}

// waitForRedisCommand 等待指定 Redis 命令出现。
//
// 参数说明：command 是不区分大小写的命令名；timeout 是测试最多等待时间。超时失败时
// 输出已观察到的命令序列，方便判断 Pop 是否退化成短轮询或卡在其他 Redis 操作。
func (r *redisCommandRecorder) waitForRedisCommand(t *testing.T, command string, timeout time.Duration) {
	t.Helper()
	want := strings.ToLower(command)
	if r.hasCommand(want) {
		return
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case got := <-r.seen:
			if got == want {
				return
			}
		case <-timer.C:
			t.Fatalf("未观察到 Redis 命令 %q，已观察命令：%v", want, r.snapshot())
		}
	}
}

func (r *redisCommandRecorder) hasCommand(command string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, got := range r.commands {
		if got == command {
			return true
		}
	}
	return false
}

func (r *redisCommandRecorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.commands...)
}

func (r *redisCommandRecorder) commandCount(command string) int {
	want := strings.ToLower(command)
	count := 0
	for _, got := range r.snapshot() {
		if got == want {
			count++
		}
	}
	return count
}

func TestRestartNanoSupportedValues(t *testing.T) {
	// cache store 可能按不同底层编码返回数值类型，restart 解析必须保持显式且可拒绝非法值。
	values := []any{int64(10), int(11), float64(12), "13"}
	for _, value := range values {
		if got, ok := restartNano(value); !ok || got <= 0 {
			t.Fatalf("restartNano(%v) = %d, %v", value, got, ok)
		}
	}
	if got, ok := restartNano("bad"); ok || got != 0 {
		t.Fatalf("restartNano bad = %d, %v", got, ok)
	}
	if got, ok := restartNano(errors.New("bad")); ok || got != 0 {
		t.Fatalf("restartNano unsupported = %d, %v", got, ok)
	}
}
