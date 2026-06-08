package cache

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"
)

func TestCacheFacadeAliasesManyRememberAndTypes(t *testing.T) {
	ctx := context.Background()
	m := newTestManager(t)
	bindCacheManagerForTest(t, m)

	if err := Set(ctx, "string", "hello", time.Minute); err != nil {
		t.Fatalf("set string: %v", err)
	}
	if err := PutMany(ctx, map[string]int{"one": 1, "two": 2}, time.Minute); err != nil {
		t.Fatalf("put many typed map: %v", err)
	}
	values, err := Many[int](ctx, []string{"one", "two", "missing"}, Value(9))
	if err != nil {
		t.Fatalf("many int: %v", err)
	}
	if values["one"] != 1 || values["two"] != 2 || values["missing"] != 9 {
		t.Fatalf("many values = %#v", values)
	}
	raw, err := Default().Many(ctx, []string{"one", "missing"})
	if err != nil {
		t.Fatalf("repo many: %v", err)
	}
	if raw["one"].(float64) != 1 || raw["missing"] != nil {
		t.Fatalf("repo many raw = %#v", raw)
	}

	s, err := String(ctx, "string")
	if err != nil || s != "hello" {
		t.Fatalf("string = %q err=%v", s, err)
	}
	i, err := Integer(ctx, "one")
	if err != nil || i != 1 {
		t.Fatalf("integer = %d err=%v", i, err)
	}
	if err := Put(ctx, "float", 1.5, time.Minute); err != nil {
		t.Fatalf("put float: %v", err)
	}
	f, err := Float(ctx, "float")
	if err != nil || f != 1.5 {
		t.Fatalf("float = %f err=%v", f, err)
	}
	if err := Put(ctx, "bool", true, time.Minute); err != nil {
		t.Fatalf("put bool: %v", err)
	}
	b, err := Boolean(ctx, "bool")
	if err != nil || !b {
		t.Fatalf("boolean = %v err=%v", b, err)
	}
	missing, err := Missing(ctx, "absent")
	if err != nil || !missing {
		t.Fatalf("missing = %v err=%v", missing, err)
	}

	remembered, err := RememberForever(ctx, "forever-loader", func(context.Context) (string, error) {
		return "loaded", nil
	})
	if err != nil || remembered != "loaded" {
		t.Fatalf("remember forever = %q err=%v", remembered, err)
	}
	seared, err := Sear(ctx, "forever-loader", func(context.Context) (string, error) {
		return "new", nil
	})
	if err != nil || seared != "loaded" {
		t.Fatalf("sear cached = %q err=%v", seared, err)
	}
	if err := DeleteMultiple(ctx, []string{"one", "two"}); err != nil {
		t.Fatalf("delete multiple: %v", err)
	}
	if ok, _ := Has(ctx, "one"); ok {
		t.Fatal("delete multiple should remove one")
	}
}

func TestMemoRepositoryCachesAndInvalidatesLocalReads(t *testing.T) {
	ctx := context.Background()
	m := newTestManager(t)
	repo := m.defaultRepository()
	memo := repo.Memo()

	if err := repo.Put(ctx, "memo", "first", time.Minute); err != nil {
		t.Fatalf("put memo: %v", err)
	}
	first, err := memo.Get(ctx, "memo")
	if err != nil || first != "first" {
		t.Fatalf("memo first = %v err=%v", first, err)
	}
	if err := repo.Put(ctx, "memo", "second", time.Minute); err != nil {
		t.Fatalf("put memo second: %v", err)
	}
	cached, err := memo.Get(ctx, "memo")
	if err != nil || cached != "first" {
		t.Fatalf("memo should keep local first value, got %v err=%v", cached, err)
	}
	if err := memo.Put(ctx, "memo", "third", time.Minute); err != nil {
		t.Fatalf("memo put third: %v", err)
	}
	third, err := memo.Get(ctx, "memo")
	if err != nil || third != "third" {
		t.Fatalf("memo after local invalidation = %v err=%v", third, err)
	}
}

func TestTaggedCacheMemoryRedisAndFileUnsupported(t *testing.T) {
	ctx := context.Background()
	m := newTestManager(t)
	repo := m.defaultRepository()
	if !repo.SupportsTags() {
		t.Fatal("memory repo should support tags")
	}
	tagged := repo.Tags("tenant:1", "settings")
	if err := tagged.Put(ctx, "theme", "dark", time.Minute); err != nil {
		t.Fatalf("memory tagged put: %v", err)
	}
	got, err := tagged.Get(ctx, "theme")
	if err != nil || got != "dark" {
		t.Fatalf("memory tagged get = %v err=%v", got, err)
	}
	otherTags := repo.Tags("tenant:1")
	if _, err := otherTags.Get(ctx, "theme"); !errors.Is(err, ErrCacheMiss) {
		t.Fatalf("different tag set should miss, err=%v", err)
	}
	if err := repo.Tags("settings").Flush(ctx); err != nil {
		t.Fatalf("memory tagged flush: %v", err)
	}
	if _, err := tagged.Get(ctx, "theme"); !errors.Is(err, ErrCacheMiss) {
		t.Fatalf("flushed tagged key err=%v, want miss", err)
	}

	srv, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	t.Cleanup(srv.Close)
	useRedisLifecycleManager(t, srv.Addr())
	redisManager, err := NewManager(Config{
		Default: "redis",
		Prefix:  "tagtest",
		Stores: map[string]StoreConfig{
			"redis": {Driver: "redis", Redis: RedisConfig{Connection: "default"}},
		},
	})
	if err != nil {
		t.Fatalf("new redis manager: %v", err)
	}
	t.Cleanup(func() { _ = redisManager.Close() })
	redisTagged := redisManager.defaultRepository().Tags("reports")
	if err := redisTagged.Put(ctx, "daily", map[string]int{"count": 3}, time.Minute); err != nil {
		t.Fatalf("redis tagged put: %v", err)
	}
	daily, err := redisTagged.Get(ctx, "daily")
	if err != nil {
		t.Fatalf("redis tagged get: %v", err)
	}
	count, ok := daily.(map[string]any)["count"].(int64)
	if !ok || count != 3 {
		t.Fatalf("redis tagged daily = %#v", daily)
	}
	if err := redisManager.defaultRepository().Tags("reports").Flush(ctx); err != nil {
		t.Fatalf("redis tagged flush: %v", err)
	}
	if _, err := redisTagged.Get(ctx, "daily"); !errors.Is(err, ErrCacheMiss) {
		t.Fatalf("redis tagged flushed err=%v, want miss", err)
	}

	fileRepo := newFileTestManager(t).Default()
	if fileRepo.SupportsTags() {
		t.Fatal("file repo should not support tags")
	}
	if err := fileRepo.Tags("x").Put(ctx, "k", "v", time.Minute); !errors.Is(err, ErrTagsUnsupported) {
		t.Fatalf("file tagged put err=%v, want ErrTagsUnsupported", err)
	}
}

func TestLockOwnerFlushAndWithoutOverlapping(t *testing.T) {
	ctx := context.Background()
	m := newTestManager(t)
	repo := m.defaultRepository()

	lock := repo.LockWithOwner("owned", time.Second, "owner-token")
	if owner := lock.Owner(); owner != "owner-token" {
		t.Fatalf("owner before get = %q", owner)
	}
	ok, err := lock.Get(ctx)
	if err != nil || !ok {
		t.Fatalf("lock with owner get: ok=%v err=%v", ok, err)
	}
	if owner := lock.Owner(); owner != "owner-token" {
		t.Fatalf("owner after get = %q", owner)
	}
	if err := repo.RestoreLock("owned", "owner-token").Release(ctx); err != nil {
		t.Fatalf("restore owner release: %v", err)
	}

	held := repo.Lock("flush-me", time.Second)
	if ok, err = held.Get(ctx); err != nil || !ok {
		t.Fatalf("lock before flush: ok=%v err=%v", ok, err)
	}
	if !repo.SupportsFlushingLocks() {
		t.Fatal("memory repo should support lock flushing")
	}
	if err := repo.FlushLocks(ctx); err != nil {
		t.Fatalf("flush locks: %v", err)
	}
	if ok, err = repo.Lock("flush-me", time.Second).Get(ctx); err != nil || !ok {
		t.Fatalf("lock after flush: ok=%v err=%v", ok, err)
	}

	running := make(chan struct{})
	release := make(chan struct{})
	var mu sync.Mutex
	ran := 0
	go func() {
		_, _ = repo.WithoutOverlapping(ctx, "job", func(context.Context) error {
			mu.Lock()
			ran++
			mu.Unlock()
			close(running)
			<-release
			return nil
		}, WithOverlapLock(time.Second), WithOverlapWait(time.Second), WithOverlapSleep(time.Millisecond))
	}()
	<-running
	ok, err = repo.WithoutOverlapping(ctx, "job", func(context.Context) error {
		mu.Lock()
		ran++
		mu.Unlock()
		return nil
	}, WithOverlapWait(5*time.Millisecond), WithOverlapSleep(time.Millisecond))
	if !errors.Is(err, ErrLockTimeout) || ok {
		t.Fatalf("overlap should timeout: ok=%v err=%v", ok, err)
	}
	close(release)
	time.Sleep(20 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	if ran != 1 {
		t.Fatalf("overlapping job ran count = %d, want 1", ran)
	}
}

func TestFailoverStoreAndCacheEvents(t *testing.T) {
	ctx := context.Background()
	var eventsMu sync.Mutex
	var events []CacheEvent
	UseEventSink(func(_ context.Context, ev CacheEvent) {
		eventsMu.Lock()
		events = append(events, ev)
		eventsMu.Unlock()
	})
	t.Cleanup(func() { UseEventSink(nil) })

	m, err := NewManager(Config{
		Default: "failover",
		Prefix:  "fail",
		Stores: map[string]StoreConfig{
			"failover": {Driver: "failover", Stores: []string{"missing", "memory"}},
			"memory":   {Driver: "memory"},
		},
	})
	if err != nil {
		t.Fatalf("new failover manager: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })
	bindCacheManagerForTest(t, m)

	if err := Put(ctx, "key", "value", time.Minute); err != nil {
		t.Fatalf("failover put: %v", err)
	}
	value, err := Get[string](ctx, "key")
	if err != nil || value != "value" {
		t.Fatalf("failover get = %q err=%v", value, err)
	}
	if _, err := Get[string](ctx, "missing"); !errors.Is(err, ErrCacheMiss) {
		t.Fatalf("failover miss err=%v, want ErrCacheMiss", err)
	}

	eventsMu.Lock()
	defer eventsMu.Unlock()
	var sawFailover, sawHit, sawMiss bool
	for _, ev := range events {
		switch ev.Name() {
		case EventCacheFailedOver:
			sawFailover = true
		case EventCacheHit:
			if ev.Key == "key" {
				sawHit = true
			}
		case EventCacheMissed:
			if ev.Key == "missing" {
				sawMiss = true
			}
		}
	}
	if !sawFailover || !sawHit || !sawMiss {
		t.Fatalf("events missing failover=%v hit=%v miss=%v all=%#v", sawFailover, sawHit, sawMiss, events)
	}
}
