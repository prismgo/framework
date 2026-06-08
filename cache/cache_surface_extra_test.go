package cache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"
	libstore "github.com/eko/gocache/lib/v4/store"
	configpkg "github.com/prismgo/framework/config"
)

type errorStore struct{}

func (s errorStore) Get(ctx context.Context, key any) (any, error) {
	_ = ctx
	_ = key
	return nil, errors.New("store get failed")
}

func (s errorStore) GetWithTTL(ctx context.Context, key any) (any, time.Duration, error) {
	_ = ctx
	_ = key
	return nil, 0, errors.New("store ttl failed")
}

func (s errorStore) Set(ctx context.Context, key any, value any, options ...libstore.Option) error {
	_ = ctx
	_ = key
	_ = value
	_ = options
	return errors.New("store set failed")
}

func (s errorStore) Delete(ctx context.Context, key any) error {
	_ = ctx
	_ = key
	return errors.New("store delete failed")
}

func (s errorStore) Invalidate(ctx context.Context, options ...libstore.InvalidateOption) error {
	_ = ctx
	_ = options
	return errors.New("store invalidate failed")
}

func (s errorStore) Clear(ctx context.Context) error {
	_ = ctx
	return errors.New("store clear failed")
}

func (s errorStore) GetType() string {
	return "error"
}

type failingBulkStore struct {
	*memoryStore
}

func (s failingBulkStore) GetMany(ctx context.Context, keys []string) (map[string][]byte, error) {
	_ = ctx
	_ = keys
	return nil, errors.New("bulk get failed")
}

func (s failingBulkStore) PutMany(ctx context.Context, values map[string][]byte, ttl time.Duration) error {
	_ = ctx
	_ = values
	_ = ttl
	return errors.New("bulk put failed")
}

func (s failingBulkStore) ForgetMany(ctx context.Context, keys []string) error {
	_ = ctx
	_ = keys
	return errors.New("bulk forget failed")
}

type failingTagStore struct {
	*memoryStore
}

func (s failingTagStore) GetTagged(ctx context.Context, prefix string, tags []string, key string) ([]byte, error) {
	_ = ctx
	_ = prefix
	_ = tags
	_ = key
	return nil, errors.New("tag get failed")
}

func (s failingTagStore) PutTagged(ctx context.Context, prefix string, tags []string, key string, value []byte, ttl time.Duration) error {
	_ = ctx
	_ = prefix
	_ = tags
	_ = key
	_ = value
	_ = ttl
	return errors.New("tag put failed")
}

func (s failingTagStore) ForgetTagged(ctx context.Context, prefix string, tags []string, key string) error {
	_ = ctx
	_ = prefix
	_ = tags
	_ = key
	return errors.New("tag forget failed")
}

func (s failingTagStore) FlushTags(ctx context.Context, prefix string, tags []string) error {
	_ = ctx
	_ = prefix
	_ = tags
	return errors.New("tag flush failed")
}

func TestCacheFacadeFromAliases(t *testing.T) {
	ctx := context.Background()
	m, err := NewManager(Config{
		Default: "memory",
		Prefix:  "aliases",
		Stores: map[string]StoreConfig{
			"memory": {Driver: "memory", CleanupInterval: time.Millisecond},
			"other":  {Driver: "memory", CleanupInterval: time.Millisecond},
		},
		Lock: LockConfig{RetrySleep: time.Millisecond},
	})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })
	bindCacheManagerForTest(t, m)

	if err := PutFrom(ctx, "other", "name", "alice", time.Minute); err != nil {
		t.Fatalf("put from: %v", err)
	}
	if err := SetFrom(ctx, "other", "set", "bob", time.Minute); err != nil {
		t.Fatalf("set from: %v", err)
	}
	if ok, err := HasFrom(ctx, "other", "name"); err != nil || !ok {
		t.Fatalf("has from: ok=%v err=%v", ok, err)
	}
	if missing, err := MissingFrom(ctx, "other", "missing"); err != nil || !missing {
		t.Fatalf("missing from: missing=%v err=%v", missing, err)
	}
	if err := ForeverFrom(ctx, "other", "forever", "kept"); err != nil {
		t.Fatalf("forever from: %v", err)
	}
	if ok, err := AddFrom(ctx, "other", "add", "first", time.Minute); err != nil || !ok {
		t.Fatalf("add from: ok=%v err=%v", ok, err)
	}
	if err := PutManyFrom(ctx, "other", map[string]any{"i": 1, "f": 1.25, "b": true}, time.Minute); err != nil {
		t.Fatalf("put many from: %v", err)
	}
	if err := SetMultiple(ctx, map[string]string{"local": "value"}, time.Minute); err != nil {
		t.Fatalf("set multiple: %v", err)
	}
	if err := SetMultipleFrom(ctx, "other", map[string]string{"remote": "value"}, time.Minute); err != nil {
		t.Fatalf("set multiple from: %v", err)
	}
	if got, err := StringFrom(ctx, "other", "name"); err != nil || got != "alice" {
		t.Fatalf("string from = %q err=%v", got, err)
	}
	if got, err := IntegerFrom(ctx, "other", "i"); err != nil || got != 1 {
		t.Fatalf("integer from = %d err=%v", got, err)
	}
	if got, err := FloatFrom(ctx, "other", "f"); err != nil || got != 1.25 {
		t.Fatalf("float from = %f err=%v", got, err)
	}
	if got, err := BooleanFrom(ctx, "other", "b"); err != nil || !got {
		t.Fatalf("boolean from = %v err=%v", got, err)
	}
	if got, err := GetMultiple[string](ctx, []string{"local"}); err != nil || got["local"] != "value" {
		t.Fatalf("get multiple = %#v err=%v", got, err)
	}
	if got, err := GetMultipleFrom[string](ctx, "other", []string{"remote"}); err != nil || got["remote"] != "value" {
		t.Fatalf("get multiple from = %#v err=%v", got, err)
	}
	if got, err := RememberForeverFrom(ctx, "other", "remember-forever", func(context.Context) (string, error) {
		return "loaded", nil
	}); err != nil || got != "loaded" {
		t.Fatalf("remember forever from = %q err=%v", got, err)
	}
	if got, err := SearFrom(ctx, "other", "sear", func(context.Context) (string, error) {
		return "seared", nil
	}); err != nil || got != "seared" {
		t.Fatalf("sear from = %q err=%v", got, err)
	}
	if ok, err := TouchFrom(ctx, "other", "name", time.Minute); err != nil || !ok {
		t.Fatalf("touch from: ok=%v err=%v", ok, err)
	}
	if got, err := IncrementFrom(ctx, "other", "counter", 2); err != nil || got != 2 {
		t.Fatalf("increment from = %d err=%v", got, err)
	}
	if got, err := DecrementFrom(ctx, "other", "counter"); err != nil || got != 1 {
		t.Fatalf("decrement from = %d err=%v", got, err)
	}
	if err := ForgetFrom(ctx, "other", "set"); err != nil {
		t.Fatalf("forget from: %v", err)
	}
	if err := Delete(ctx, "local"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if err := ForgetManyFrom(ctx, "other", []string{"remote"}); err != nil {
		t.Fatalf("forget many from: %v", err)
	}
	if err := DeleteMultipleFrom(ctx, "other", []string{"name"}); err != nil {
		t.Fatalf("delete multiple from: %v", err)
	}
	if err := Clear(ctx); err != nil {
		t.Fatalf("clear: %v", err)
	}
	if err := ClearFrom(ctx, "other"); err != nil {
		t.Fatalf("clear from: %v", err)
	}

	lock := LockWithOwner("owned", time.Second, "facade-owner")
	if ok, err := lock.Get(ctx); err != nil || !ok {
		t.Fatalf("lock with owner: ok=%v err=%v", ok, err)
	}
	if err := RestoreLock("owned", "facade-owner").Release(ctx); err != nil {
		t.Fatalf("restore facade owner: %v", err)
	}
	lock = LockWithOwnerFrom("other", "owned-from", time.Second, "owner-from")
	if ok, err := lock.Get(ctx); err != nil || !ok {
		t.Fatalf("lock with owner from: ok=%v err=%v", ok, err)
	}
	if err := RestoreLockFrom("other", "owned-from", "owner-from").Release(ctx); err != nil {
		t.Fatalf("restore lock from: %v", err)
	}
	if ok, err := LockFrom("other", "from-lock", time.Second).Get(ctx); err != nil || !ok {
		t.Fatalf("lock from: ok=%v err=%v", ok, err)
	}
	if err := FlushLocks(ctx); err != nil {
		t.Fatalf("flush locks: %v", err)
	}
	if err := FlushLocksFrom(ctx, "other"); err != nil {
		t.Fatalf("flush locks from: %v", err)
	}
	if ok, err := FunnelFrom("other", "alias").Then(ctx, func(context.Context) error { return nil }); err != nil || !ok {
		t.Fatalf("funnel from: ok=%v err=%v", ok, err)
	}
	if err := Tags("facade").Put(ctx, "tagged", "ok", time.Minute); err != nil {
		t.Fatalf("facade tags put: %v", err)
	}
	if err := TagsFrom("other", "facade").Put(ctx, "tagged", "ok", time.Minute); err != nil {
		t.Fatalf("facade tags from put: %v", err)
	}
	if err := Memo().Put(ctx, "memo", "ok", time.Minute); err != nil {
		t.Fatalf("memo facade put: %v", err)
	}
	if err := MemoFrom("other").Put(ctx, "memo", "ok", time.Minute); err != nil {
		t.Fatalf("memo facade from put: %v", err)
	}
	if ok, err := WithoutOverlapping(ctx, "facade", func(context.Context) error { return nil }, WithOverlapWait(time.Millisecond)); err != nil || !ok {
		t.Fatalf("without overlapping: ok=%v err=%v", ok, err)
	}
	if ok, err := WithoutOverlappingFrom(ctx, "other", "facade", func(context.Context) error { return nil }, WithOverlapWait(time.Millisecond)); err != nil || !ok {
		t.Fatalf("without overlapping from: ok=%v err=%v", ok, err)
	}
}

func TestRepositoryAliasesMemoAndTaggedBranches(t *testing.T) {
	ctx := context.Background()
	m := newTestManager(t)
	repo := m.defaultRepository()

	if err := repo.Set(ctx, "set", "value", time.Minute); err != nil {
		t.Fatalf("repo set: %v", err)
	}
	if got, err := repo.GetMultiple(ctx, []string{"set"}); err != nil || got["set"] != "value" {
		t.Fatalf("repo get multiple = %#v err=%v", got, err)
	}
	if err := repo.SetMultiple(ctx, map[string]string{"a": "A", "b": "B"}, time.Minute); err != nil {
		t.Fatalf("repo set multiple: %v", err)
	}
	if got, err := repo.RememberForever(ctx, "remember-forever", func(context.Context) (any, error) { return "forever", nil }); err != nil || got != "forever" {
		t.Fatalf("repo remember forever = %v err=%v", got, err)
	}
	if got, err := repo.Sear(ctx, "sear", func(context.Context) (any, error) { return "sear", nil }); err != nil || got != "sear" {
		t.Fatalf("repo sear = %v err=%v", got, err)
	}
	if err := repo.Delete(ctx, "a"); err != nil {
		t.Fatalf("repo delete: %v", err)
	}
	if err := repo.DeleteMultiple(ctx, []string{"b"}); err != nil {
		t.Fatalf("repo delete multiple: %v", err)
	}
	if err := repo.PutMany(ctx, []string{"bad"}, time.Minute); err == nil {
		t.Fatal("expected PutMany non-map error")
	}

	memo := repo.Memo()
	if memo.Repository() != repo {
		t.Fatal("memo should expose wrapped repository")
	}
	if err := memo.Set(ctx, "memo-set", "value", time.Minute); err != nil {
		t.Fatalf("memo set: %v", err)
	}
	if ok, err := memo.Has(ctx, "memo-set"); err != nil || !ok {
		t.Fatalf("memo has: ok=%v err=%v", ok, err)
	}
	if missing, err := memo.Missing(ctx, "memo-missing"); err != nil || !missing {
		t.Fatalf("memo missing: missing=%v err=%v", missing, err)
	}
	if got, err := memo.Remember(ctx, "memo-remember", time.Minute, func(context.Context) (any, error) { return "remembered", nil }); err != nil || got != "remembered" {
		t.Fatalf("memo remember = %v err=%v", got, err)
	}
	if got, err := memo.RememberForever(ctx, "memo-forever", func(context.Context) (any, error) { return "forever", nil }); err != nil || got != "forever" {
		t.Fatalf("memo remember forever = %v err=%v", got, err)
	}
	if got, err := memo.Sear(ctx, "memo-sear", func(context.Context) (any, error) { return "seared", nil }); err != nil || got != "seared" {
		t.Fatalf("memo sear = %v err=%v", got, err)
	}
	if err := memo.Forever(ctx, "memo-count", int64(1)); err != nil {
		t.Fatalf("memo forever: %v", err)
	}
	if got, err := memo.Increment(ctx, "memo-count", 2); err != nil || got != 3 {
		t.Fatalf("memo increment = %d err=%v", got, err)
	}
	if got, err := memo.Decrement(ctx, "memo-count"); err != nil || got != 2 {
		t.Fatalf("memo decrement = %d err=%v", got, err)
	}
	if err := memo.Forget(ctx, "memo-set"); err != nil {
		t.Fatalf("memo forget: %v", err)
	}
	if err := memo.Delete(ctx, "memo-count"); err != nil {
		t.Fatalf("memo delete: %v", err)
	}
	if err := memo.Clear(ctx); err != nil {
		t.Fatalf("memo clear: %v", err)
	}
	if err := memo.Flush(ctx); err != nil {
		t.Fatalf("memo flush: %v", err)
	}

	tagged := repo.Tags(" z ", "z", "a")
	if got := tagged.Tags(); len(got) != 2 || got[0] != "a" || got[1] != "z" {
		t.Fatalf("normalized tags = %#v", got)
	}
	if err := tagged.Set(ctx, "set", "value", time.Minute); err != nil {
		t.Fatalf("tagged set: %v", err)
	}
	if ok, err := tagged.Has(ctx, "set"); err != nil || !ok {
		t.Fatalf("tagged has: ok=%v err=%v", ok, err)
	}
	if missing, err := tagged.Missing(ctx, "missing"); err != nil || !missing {
		t.Fatalf("tagged missing: missing=%v err=%v", missing, err)
	}
	if err := tagged.Forever(ctx, "forever", "kept"); err != nil {
		t.Fatalf("tagged forever: %v", err)
	}
	if got, err := tagged.Remember(ctx, "remember", time.Minute, func(context.Context) (any, error) { return "remembered", nil }); err != nil || got != "remembered" {
		t.Fatalf("tagged remember = %v err=%v", got, err)
	}
	if got, err := tagged.RememberForever(ctx, "remember-forever", func(context.Context) (any, error) { return "forever", nil }); err != nil || got != "forever" {
		t.Fatalf("tagged remember forever = %v err=%v", got, err)
	}
	if got, err := tagged.Sear(ctx, "sear", func(context.Context) (any, error) { return "seared", nil }); err != nil || got != "seared" {
		t.Fatalf("tagged sear = %v err=%v", got, err)
	}
	if err := tagged.Forget(ctx, "set"); err != nil {
		t.Fatalf("tagged forget: %v", err)
	}
	if err := tagged.Delete(ctx, "forever"); err != nil {
		t.Fatalf("tagged delete: %v", err)
	}
	if err := tagged.Clear(ctx); err != nil {
		t.Fatalf("tagged clear: %v", err)
	}
	if err := repo.Tags().Put(ctx, "empty", "value", time.Minute); !errors.Is(err, ErrTagsUnsupported) {
		t.Fatalf("empty tags err=%v, want ErrTagsUnsupported", err)
	}
}

func TestFailoverStoreFullSurface(t *testing.T) {
	ctx := context.Background()
	m, err := NewManager(Config{
		Default: "failover",
		Prefix:  "fo",
		Stores: map[string]StoreConfig{
			"failover": {Driver: "failover", Stores: []string{"missing", "memory"}},
			"memory":   {Driver: "memory", CleanupInterval: time.Millisecond},
		},
		Lock: LockConfig{RetrySleep: time.Millisecond},
	})
	if err != nil {
		t.Fatalf("new failover manager: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })
	repo := m.defaultRepository()

	if repo.store.GetType() != "failover" {
		t.Fatalf("failover type = %s", repo.store.GetType())
	}
	if err := repo.Put(ctx, "key", "value", time.Minute); err != nil {
		t.Fatalf("failover put: %v", err)
	}
	if value, ttl, err := repo.store.GetWithTTL(ctx, "key"); err != nil || value == nil || ttl != 0 {
		t.Fatalf("failover get with ttl value=%v ttl=%v err=%v", value, ttl, err)
	}
	if ok, err := repo.Touch(ctx, "key", time.Minute); err != nil || !ok {
		t.Fatalf("failover touch: ok=%v err=%v", ok, err)
	}
	if ok, err := repo.Add(ctx, "add", "first", time.Minute); err != nil || !ok {
		t.Fatalf("failover add: ok=%v err=%v", ok, err)
	}
	if got, err := repo.Increment(ctx, "counter", 3); err != nil || got != 3 {
		t.Fatalf("failover increment = %d err=%v", got, err)
	}
	if err := repo.Put(ctx, "pull", "once", time.Minute); err != nil {
		t.Fatalf("failover pull put: %v", err)
	}
	if got, err := repo.Pull(ctx, "pull"); err != nil || got != "once" {
		t.Fatalf("failover pull = %v err=%v", got, err)
	}
	if err := repo.PutMany(ctx, map[string]string{"m1": "one", "m2": "two"}, time.Minute); err != nil {
		t.Fatalf("failover put many: %v", err)
	}
	if got, err := repo.Many(ctx, []string{"m1", "m2"}); err != nil || got["m1"] != "one" || got["m2"] != "two" {
		t.Fatalf("failover many = %#v err=%v", got, err)
	}
	if err := repo.ForgetMany(ctx, []string{"m1"}); err != nil {
		t.Fatalf("failover forget many: %v", err)
	}
	if err := repo.Forget(ctx, "m2"); err != nil {
		t.Fatalf("failover forget: %v", err)
	}
	if err := repo.Tags("tag").Put(ctx, "tagged", "value", time.Minute); err != nil {
		t.Fatalf("failover tagged put: %v", err)
	}
	if got, err := repo.Tags("tag").Get(ctx, "tagged"); err != nil || got != "value" {
		t.Fatalf("failover tagged get = %v err=%v", got, err)
	}
	if err := repo.Tags("tag").Forget(ctx, "tagged"); err != nil {
		t.Fatalf("failover tagged forget: %v", err)
	}
	if err := repo.Tags("tag").Put(ctx, "tagged", "value", time.Minute); err != nil {
		t.Fatalf("failover tagged put second: %v", err)
	}
	if err := repo.Tags("tag").Flush(ctx); err != nil {
		t.Fatalf("failover tagged flush: %v", err)
	}
	lock := repo.Lock("job", time.Second)
	if ok, err := lock.Get(ctx); err != nil || !ok {
		t.Fatalf("failover lock get: ok=%v err=%v", ok, err)
	}
	if err := repo.RestoreLock("job", lock.Owner()).Release(ctx); err != nil {
		t.Fatalf("failover lock release: %v", err)
	}
	if ok, err := repo.Lock("force", time.Second).Get(ctx); err != nil || !ok {
		t.Fatalf("failover force lock get: ok=%v err=%v", ok, err)
	}
	if err := repo.Lock("force", time.Second).ForceRelease(ctx); err != nil {
		t.Fatalf("failover force release: %v", err)
	}
	if err := repo.FlushLocks(ctx); err != nil {
		t.Fatalf("failover flush locks: %v", err)
	}
	if err := repo.Flush(ctx); err != nil {
		t.Fatalf("failover flush: %v", err)
	}
	if err := repo.store.Invalidate(ctx); err != nil {
		t.Fatalf("failover invalidate: %v", err)
	}
	if _, err := repo.store.Get(ctx, 123); err == nil {
		t.Fatal("expected failover non-string get error")
	}
	if err := repo.store.Delete(ctx, 123); err == nil {
		t.Fatal("expected failover non-string delete error")
	}
}

func TestRedisAndFileBulkTagsAndLockFlush(t *testing.T) {
	ctx := context.Background()
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	t.Cleanup(srv.Close)
	useRedisLifecycleManager(t, srv.Addr())
	redisManager, err := NewManager(Config{
		Default: "redis",
		Prefix:  "bulk",
		Stores: map[string]StoreConfig{
			"redis": {Driver: "redis", Redis: RedisConfig{Connection: "default"}},
		},
	})
	if err != nil {
		t.Fatalf("new redis manager: %v", err)
	}
	t.Cleanup(func() { _ = redisManager.Close() })
	redisRepo := redisManager.defaultRepository()
	if err := redisRepo.PutMany(ctx, map[string]string{"a": "A", "b": "B"}, time.Minute); err != nil {
		t.Fatalf("redis put many: %v", err)
	}
	if got, err := redisRepo.Many(ctx, []string{"a", "b"}); err != nil || got["a"] != "A" || got["b"] != "B" {
		t.Fatalf("redis many = %#v err=%v", got, err)
	}
	if err := redisRepo.ForgetMany(ctx, []string{"a"}); err != nil {
		t.Fatalf("redis forget many: %v", err)
	}
	if err := redisRepo.Tags("tag").Put(ctx, "key", "value", time.Minute); err != nil {
		t.Fatalf("redis tagged put: %v", err)
	}
	if err := redisRepo.Tags("tag").Forget(ctx, "key"); err != nil {
		t.Fatalf("redis tagged forget: %v", err)
	}
	if ok, err := redisRepo.Lock("flush", time.Second).Get(ctx); err != nil || !ok {
		t.Fatalf("redis lock get before flush: ok=%v err=%v", ok, err)
	}
	if err := redisRepo.FlushLocks(ctx); err != nil {
		t.Fatalf("redis flush locks: %v", err)
	}

	fileRepo := newFileTestManager(t).Default()
	if err := fileRepo.PutMany(ctx, map[string]string{"a": "A", "b": "B"}, time.Minute); err != nil {
		t.Fatalf("file put many: %v", err)
	}
	if got, err := fileRepo.Many(ctx, []string{"a", "b"}); err != nil || got["a"] != "A" || got["b"] != "B" {
		t.Fatalf("file many = %#v err=%v", got, err)
	}
	if err := fileRepo.ForgetMany(ctx, []string{"a"}); err != nil {
		t.Fatalf("file forget many: %v", err)
	}
	if ok, err := fileRepo.Lock("flush", time.Second).Get(ctx); err != nil || !ok {
		t.Fatalf("file lock get before flush: ok=%v err=%v", ok, err)
	}
	if err := fileRepo.FlushLocks(ctx); err != nil {
		t.Fatalf("file flush locks: %v", err)
	}
}

func TestConfigParsingAndEventDisableBranches(t *testing.T) {
	falseValue := false
	trueValue := true
	spec := cloneStoreConfig(StoreConfig{Stores: []string{"a", "b"}, Events: &falseValue, Options: map[string]any{"x": "y"}})
	*spec.Events = true
	spec.Stores[0] = "changed"
	spec.Options["x"] = "changed"
	if *cloneStoreConfig(StoreConfig{Events: &trueValue}).Events != true {
		t.Fatal("expected cloned true events")
	}
	if storeEvents(StoreConfig{}) != true || storeEvents(StoreConfig{Events: &falseValue}) != false {
		t.Fatal("unexpected storeEvents result")
	}
	if castBoolPtr(nil) != nil || *castBoolPtr(true) != true || *castBoolPtr("off") != false {
		t.Fatal("unexpected castBoolPtr result")
	}
	if got := castStringSlice([]any{"a", "", "b"}); len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("slice from []any = %#v", got)
	}
	if got := castStringSlice("a,b"); len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("slice from string = %#v", got)
	}
	if got := castStringSlice(123); got != nil {
		t.Fatalf("slice from invalid = %#v", got)
	}

	configpkg.Add("cache", func() map[string]any {
		root := filepath.Join(t.TempDir(), "cache")
		return map[string]any{
			"default": "failover",
			"prefix":  "cfg",
			"stores": map[string]any{
				"failover": map[string]any{"driver": "failover", "stores": "memory,file", "events": "false"},
				"memory":   map[string]any{"driver": "memory", "events": "true"},
				"file":     map[string]any{"driver": "file", "path": filepath.Join(root, "data")},
			},
		}
	})
	cfg := configpkg.New()
	if err := cfg.ReloadFromFile(filepath.Join(t.TempDir(), ".env")); err != nil {
		t.Fatalf("reload config: %v", err)
	}
	bindConfigForCacheTest(t, cfg)
	_, manager, err := NewManagerFromConfig()
	if err != nil {
		t.Fatalf("new application manager with failover config: %v", err)
	}
	t.Cleanup(func() { _ = manager.Close() })
	if manager.specs["failover"].Stores[0] != "memory" || *manager.specs["failover"].Events != false {
		t.Fatalf("unexpected failover spec: %#v", manager.specs["failover"])
	}
}

func TestFailoverEmptyAndErrorBranches(t *testing.T) {
	ctx := context.Background()
	m, err := NewManager(Config{
		Default: "failover",
		Stores: map[string]StoreConfig{
			"failover": {Driver: "failover"},
		},
	})
	if err != nil {
		t.Fatalf("new empty failover manager: %v", err)
	}
	repo := m.defaultRepository()
	store := repo.store.(*failoverStore)
	if _, err := store.Get(ctx, "x"); !errors.Is(err, ErrStoreNotFound) {
		t.Fatalf("empty failover get err=%v", err)
	}
	if _, _, err := store.GetWithTTL(ctx, "x"); !errors.Is(err, ErrStoreNotFound) {
		t.Fatalf("empty failover get ttl err=%v", err)
	}
	if err := store.Set(ctx, "x", []byte(`"x"`)); !errors.Is(err, ErrStoreNotFound) {
		t.Fatalf("empty failover set err=%v", err)
	}
	if err := store.Delete(ctx, "x"); !errors.Is(err, ErrStoreNotFound) {
		t.Fatalf("empty failover delete err=%v", err)
	}
	if err := store.Clear(ctx); !errors.Is(err, ErrStoreNotFound) {
		t.Fatalf("empty failover clear err=%v", err)
	}
	if ok, err := store.Touch(ctx, "x", time.Minute); !errors.Is(err, ErrStoreNotFound) || ok {
		t.Fatalf("empty failover touch ok=%v err=%v", ok, err)
	}
	if ok, err := store.Add(ctx, "x", []byte(`"x"`), time.Minute); !errors.Is(err, ErrStoreNotFound) || ok {
		t.Fatalf("empty failover add ok=%v err=%v", ok, err)
	}
	if _, err := store.Increment(ctx, "x", 1); !errors.Is(err, ErrStoreNotFound) {
		t.Fatalf("empty failover increment err=%v", err)
	}
	if _, err := store.Pull(ctx, "x"); !errors.Is(err, ErrStoreNotFound) {
		t.Fatalf("empty failover pull err=%v", err)
	}
	if _, err := store.GetMany(ctx, []string{"x"}); !errors.Is(err, ErrStoreNotFound) {
		t.Fatalf("empty failover get many err=%v", err)
	}
	if err := store.PutMany(ctx, map[string][]byte{"x": []byte(`"x"`)}, time.Minute); !errors.Is(err, ErrStoreNotFound) {
		t.Fatalf("empty failover put many err=%v", err)
	}
	if err := store.ForgetMany(ctx, []string{"x"}); !errors.Is(err, ErrStoreNotFound) {
		t.Fatalf("empty failover forget many err=%v", err)
	}
	if err := store.Flush(ctx, ""); !errors.Is(err, ErrStoreNotFound) {
		t.Fatalf("empty failover flush err=%v", err)
	}
	if _, err := store.GetTagged(ctx, "", []string{"t"}, "x"); !errors.Is(err, ErrStoreNotFound) {
		t.Fatalf("empty failover get tagged err=%v", err)
	}
	if err := store.PutTagged(ctx, "", []string{"t"}, "x", []byte(`"x"`), time.Minute); !errors.Is(err, ErrStoreNotFound) {
		t.Fatalf("empty failover put tagged err=%v", err)
	}
	if err := store.ForgetTagged(ctx, "", []string{"t"}, "x"); !errors.Is(err, ErrStoreNotFound) {
		t.Fatalf("empty failover forget tagged err=%v", err)
	}
	if err := store.FlushTags(ctx, "", []string{"t"}); !errors.Is(err, ErrStoreNotFound) {
		t.Fatalf("empty failover flush tags err=%v", err)
	}
	if ok, err := store.Acquire(ctx, "x", "token", time.Minute); !errors.Is(err, ErrStoreNotFound) || ok {
		t.Fatalf("empty failover acquire ok=%v err=%v", ok, err)
	}
	if ok, err := store.Release(ctx, "x", "token"); !errors.Is(err, ErrStoreNotFound) || ok {
		t.Fatalf("empty failover release ok=%v err=%v", ok, err)
	}
	if err := store.ForceRelease(ctx, "x"); !errors.Is(err, ErrStoreNotFound) {
		t.Fatalf("empty failover force release err=%v", err)
	}
	if err := store.FlushLocks(ctx, "locks"); !errors.Is(err, ErrStoreNotFound) {
		t.Fatalf("empty failover flush locks err=%v", err)
	}
	store.dispatchFailover(ctx, "memory", ErrCacheMiss)

	Extend("error-store", func(StoreFactoryContext) (StoreDriver, error) {
		return NewStoreDriver(errorStore{}), nil
	})
	errorManager, err := NewManager(Config{
		Default: "failover",
		Stores: map[string]StoreConfig{
			"failover": {Driver: "failover", Stores: []string{"err"}},
			"err":      {Driver: "error-store"},
		},
	})
	if err != nil {
		t.Fatalf("new error failover manager: %v", err)
	}
	errRepo := errorManager.defaultRepository()
	if err := errRepo.Put(ctx, "x", "value", time.Minute); err == nil {
		t.Fatal("expected failover put to return child error")
	}
	if _, err := errRepo.Get(ctx, "x"); err == nil {
		t.Fatal("expected failover get to return child error")
	}
}

func TestRepositoryMemoTaggedAndFlexibleErrorBranches(t *testing.T) {
	ctx := context.Background()
	m := newTestManager(t)
	repo := m.defaultRepository()

	if err := repo.Put(ctx, "bad-encode", func() {}, time.Minute); err == nil {
		t.Fatal("expected repo put encode error")
	}
	if err := repo.PutMany(ctx, map[string]any{"bad": func() {}}, time.Minute); err == nil {
		t.Fatal("expected repo put many encode error")
	}
	if err := repo.Clear(ctx); err != nil {
		t.Fatalf("repo clear: %v", err)
	}
	stub := &stubStore{value: []byte(`{`)}
	stubRepo := newRepository("stub", "", stub, nil, nil, nil, nil, nil, nil, nil, nil, "", m, true)
	if _, err := stubRepo.Get(ctx, "bad-json"); err == nil {
		t.Fatal("expected bad json decode error")
	}
	if err := stubRepo.FlushLocks(ctx); err == nil {
		t.Fatal("expected unsupported flush locks error")
	}
	if err := stubRepo.Tags("x").Put(ctx, "k", "v", time.Minute); !errors.Is(err, ErrTagsUnsupported) {
		t.Fatalf("unsupported tags err=%v", err)
	}

	memo := repo.Memo()
	if _, err := memo.Get(ctx, "missing"); !errors.Is(err, ErrCacheMiss) {
		t.Fatalf("memo missing err=%v", err)
	}
	if _, err := memo.Remember(ctx, "loader-error", time.Minute, func(context.Context) (any, error) {
		return nil, fmt.Errorf("loader failed")
	}); err == nil {
		t.Fatal("expected memo loader error")
	}
	if err := memo.Put(ctx, "bad", func() {}, time.Minute); err == nil {
		t.Fatal("expected memo put encode error")
	}
	failingMemo := newMemoRepository(newErrorRepository("missing", ErrStoreNotFound, m))
	if _, err := failingMemo.Has(ctx, "x"); !errors.Is(err, ErrStoreNotFound) {
		t.Fatalf("failing memo has err=%v", err)
	}
	if err := failingMemo.Flush(ctx); !errors.Is(err, ErrStoreNotFound) {
		t.Fatalf("failing memo flush err=%v", err)
	}

	tagged := repo.Tags("errors")
	if _, err := tagged.Get(ctx, "missing", "fallback"); err != nil {
		t.Fatalf("tagged fallback: %v", err)
	}
	if _, err := tagged.Remember(ctx, "loader-error", time.Minute, func(context.Context) (any, error) {
		return nil, fmt.Errorf("loader failed")
	}); err == nil {
		t.Fatal("expected tagged loader error")
	}
	if err := tagged.Put(ctx, "bad", func() {}, time.Minute); err == nil {
		t.Fatal("expected tagged put encode error")
	}
	failingTagged := newTaggedRepository(newErrorRepository("missing", ErrStoreNotFound, m), []string{"x"})
	if _, err := failingTagged.Get(ctx, "x"); !errors.Is(err, ErrStoreNotFound) {
		t.Fatalf("failing tagged get err=%v", err)
	}

	if err := repo.putEncoded(ctx, "flex-corrupt", []byte(`{`), time.Minute); err != nil {
		t.Fatalf("put corrupt flexible: %v", err)
	}
	value, err := repo.Flexible(ctx, "flex-corrupt", FlexibleWindow{Fresh: time.Second, Stale: time.Millisecond}, func(context.Context) (any, error) {
		return "refreshed", nil
	})
	if err != nil || value != "refreshed" {
		t.Fatalf("flex corrupt refresh value=%v err=%v", value, err)
	}
}

func TestRedisFileAndStoreDirectExtraBranches(t *testing.T) {
	ctx := context.Background()
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	t.Cleanup(srv.Close)
	useRedisLifecycleManager(t, srv.Addr())
	redisManager, err := NewManager(Config{
		Default: "redis",
		Stores: map[string]StoreConfig{
			"redis": {Driver: "redis", Redis: RedisConfig{Connection: "default"}},
		},
	})
	if err != nil {
		t.Fatalf("new redis manager: %v", err)
	}
	t.Cleanup(func() { _ = redisManager.Close() })
	redisRepo := redisManager.defaultRepository()
	redisStore := redisRepo.store.(*redisStore)
	if values, err := redisStore.GetMany(ctx, nil); err != nil || len(values) != 0 {
		t.Fatalf("redis empty get many values=%#v err=%v", values, err)
	}
	if err := redisStore.PutMany(ctx, nil, time.Minute); err != nil {
		t.Fatalf("redis empty put many: %v", err)
	}
	if err := redisStore.ForgetMany(ctx, nil); err != nil {
		t.Fatalf("redis empty forget many: %v", err)
	}
	if err := redisRepo.Put(ctx, "persist", "value", time.Minute); err != nil {
		t.Fatalf("redis persist put: %v", err)
	}
	if ok, err := redisStore.Touch(ctx, redisRepo.key("persist"), 0); err != nil || !ok {
		t.Fatalf("redis persist touch ok=%v err=%v", ok, err)
	}
	if _, err := redisStore.Pull(ctx, redisRepo.key("missing")); !errors.Is(err, ErrCacheMiss) {
		t.Fatalf("redis missing pull err=%v", err)
	}
	if ok, err := redisStore.Release(ctx, redisRepo.lockKey("missing"), "wrong"); err != nil || ok {
		t.Fatalf("redis wrong release ok=%v err=%v", ok, err)
	}
	if err := redisStore.FlushLocks(ctx, ""); err != nil {
		t.Fatalf("redis empty flush locks: %v", err)
	}
	if err := redisStore.FlushTags(ctx, "", nil); err != nil {
		t.Fatalf("redis empty flush tags: %v", err)
	}

	fileStore := newFileStore(FileConfig{Path: filepath.Join(t.TempDir(), "data")}, 0, "", "")
	if values, err := fileStore.GetMany(ctx, nil); err != nil || len(values) != 0 {
		t.Fatalf("file empty get many values=%#v err=%v", values, err)
	}
	if err := fileStore.PutMany(ctx, nil, time.Minute); err != nil {
		t.Fatalf("file empty put many: %v", err)
	}
	if err := fileStore.ForgetMany(ctx, nil); err != nil {
		t.Fatalf("file empty forget many: %v", err)
	}
	if err := fileStore.FlushLocks(ctx, "locks"); err != nil {
		t.Fatalf("file direct flush locks: %v", err)
	}

	memStore := newMemoryStore(0, 0)
	if values, err := memStore.GetMany(ctx, nil); err != nil || len(values) != 0 {
		t.Fatalf("memory empty get many values=%#v err=%v", values, err)
	}
	if err := memStore.PutMany(ctx, nil, time.Minute); err != nil {
		t.Fatalf("memory empty put many: %v", err)
	}
	if err := memStore.ForgetMany(ctx, nil); err != nil {
		t.Fatalf("memory empty forget many: %v", err)
	}
	if err := memStore.ForgetTagged(ctx, "", []string{"missing"}, "key"); err != nil {
		t.Fatalf("memory forget tagged missing: %v", err)
	}
}

func TestRepositoryBulkTagLockAndRememberErrorBranches(t *testing.T) {
	ctx := context.Background()
	m := newTestManager(t)

	plain := &stubStore{value: []byte(`"plain"`)}
	plainRepo := newRepository("plain", "p", plain, nil, nil, nil, nil, nil, nil, nil, nil, "locks", m, true)
	values, err := plainRepo.Many(ctx, []string{"a", "b"})
	if err != nil || values["a"] != "plain" || values["b"] != "plain" {
		t.Fatalf("plain many = %#v err=%v", values, err)
	}
	if err := plainRepo.PutMany(ctx, map[string]string{"a": "A", "b": "B"}, time.Minute); err != nil {
		t.Fatalf("plain put many fallback: %v", err)
	}
	if err := plainRepo.ForgetMany(ctx, []string{"a", "b"}); err != nil {
		t.Fatalf("plain forget many fallback: %v", err)
	}
	plain.missing = true
	values, err = plainRepo.Many(ctx, []string{"missing"})
	if err != nil || values["missing"] != nil {
		t.Fatalf("plain missing many = %#v err=%v", values, err)
	}

	bulkStore := failingBulkStore{memoryStore: newMemoryStore(0, 0)}
	bulkRepo := newRepository("bulk", "", bulkStore, bulkStore, bulkStore, bulkStore, nil, bulkStore, bulkStore, bulkStore, nil, "locks", m, true)
	if _, err := bulkRepo.Many(ctx, []string{"x"}); err == nil {
		t.Fatal("expected bulk get error")
	}
	if err := bulkRepo.PutMany(ctx, map[string]string{"x": "y"}, time.Minute); err == nil {
		t.Fatal("expected bulk put error")
	}
	if err := bulkRepo.ForgetMany(ctx, []string{"x"}); err == nil {
		t.Fatal("expected bulk forget error")
	}

	tagStore := failingTagStore{memoryStore: newMemoryStore(0, 0)}
	tagRepo := newRepository("tags", "", tagStore, tagStore, tagStore, tagStore, nil, tagStore, tagStore, tagStore, nil, "locks", m, true)
	tagged := tagRepo.Tags("x")
	if _, err := tagged.Get(ctx, "key"); err == nil {
		t.Fatal("expected tagged get error")
	}
	if err := tagged.Put(ctx, "key", "value", time.Minute); err == nil {
		t.Fatal("expected tagged put error")
	}
	if err := tagged.Forget(ctx, "key"); err == nil {
		t.Fatal("expected tagged forget error")
	}
	if err := tagged.Flush(ctx); err == nil {
		t.Fatal("expected tagged flush error")
	}

	errRepo := newErrorRepository("missing", ErrStoreNotFound, m)
	if err := errRepo.RestoreLock("x", "owner").Release(ctx); !errors.Is(err, ErrStoreNotFound) {
		t.Fatalf("error restored release err=%v", err)
	}
	if _, err := rememberTyped(ctx, errRepo, "x", time.Minute, func(context.Context) (string, error) {
		return "x", nil
	}); !errors.Is(err, ErrStoreNotFound) {
		t.Fatalf("remember error repo err=%v", err)
	}
	putFailRepo := newRepository("put-fail", "", errorStore{}, nil, nil, nil, nil, nil, nil, nil, nil, "", m, true)
	if _, err := rememberTyped(ctx, putFailRepo, "x", time.Minute, func(context.Context) (string, error) {
		return "x", nil
	}); err == nil {
		t.Fatal("expected remember put failure")
	}
}

func TestFailoverUnsupportedCapabilityBranches(t *testing.T) {
	ctx := context.Background()
	Extend("plain-store", func(StoreFactoryContext) (StoreDriver, error) {
		return NewStoreDriver(&stubStore{value: []byte(`"value"`)}), nil
	})
	m, err := NewManager(Config{
		Default: "failover",
		Stores: map[string]StoreConfig{
			"failover": {Driver: "failover", Stores: []string{"plain"}},
			"plain":    {Driver: "plain-store"},
		},
	})
	if err != nil {
		t.Fatalf("new plain failover manager: %v", err)
	}
	repo := m.defaultRepository()
	if ok, err := repo.Add(ctx, "x", "y", time.Minute); err == nil || ok {
		t.Fatalf("expected failover add unsupported, ok=%v err=%v", ok, err)
	}
	if _, err := repo.Increment(ctx, "x"); err == nil {
		t.Fatal("expected failover increment unsupported")
	}
	if _, err := repo.Tags("x").Get(ctx, "key"); !errors.Is(err, ErrTagsUnsupported) {
		t.Fatalf("expected failover tags unsupported, err=%v", err)
	}
	if err := repo.Tags("x").Put(ctx, "key", "value", time.Minute); !errors.Is(err, ErrTagsUnsupported) {
		t.Fatalf("expected failover tag put unsupported, err=%v", err)
	}
	if err := repo.Tags("x").Forget(ctx, "key"); !errors.Is(err, ErrTagsUnsupported) {
		t.Fatalf("expected failover tag forget unsupported, err=%v", err)
	}
	if err := repo.Tags("x").Flush(ctx); !errors.Is(err, ErrTagsUnsupported) {
		t.Fatalf("expected failover tag flush unsupported, err=%v", err)
	}
	if ok, err := repo.Lock("x", time.Second).Get(ctx); err == nil || ok {
		t.Fatalf("expected failover lock unsupported, ok=%v err=%v", ok, err)
	}
	if err := repo.RestoreLock("x", "owner").Release(ctx); err == nil {
		t.Fatal("expected failover lock release unsupported")
	}
	if err := repo.Lock("x", time.Second).ForceRelease(ctx); err == nil {
		t.Fatal("expected failover force release unsupported")
	}
	if err := repo.FlushLocks(ctx); err == nil {
		t.Fatal("expected failover flush locks unsupported")
	}
}

func TestCacheAdditionalEdgeBranches(t *testing.T) {
	ctx := context.Background()
	m := newTestManager(t)
	repo := m.defaultRepository()
	bindCacheManagerForTest(t, m)

	var nilRepo *Repository
	nilRepo.dispatch(ctx, EventCacheHit, CacheEvent{})
	if data, err := rawBytes("plain"); err != nil || string(data) != "plain" {
		t.Fatalf("raw string bytes = %q err=%v", data, err)
	}
	if _, err := rawBytes(func() {}); err == nil {
		t.Fatal("expected rawBytes encode error")
	}
	if _, err := decodeCounter(func() {}); err == nil {
		t.Fatal("expected decodeCounter raw bytes error")
	}
	if _, _, err := encodeMany(map[int]string{1: "bad"}); err == nil {
		t.Fatal("expected encodeMany key error")
	}
	opts := libstore.ApplyOptionsWithDefault(&libstore.Options{}, expirationOptions(-time.Second)...)
	if opts.Expiration != 0 {
		t.Fatalf("negative expiration = %v, want 0", opts.Expiration)
	}

	if _, err := repo.Add(ctx, "bad-add", func() {}, time.Minute); err == nil {
		t.Fatal("expected add encode error")
	}
	plain := &stubStore{value: []byte(`"plain"`)}
	plainRepo := newRepository("plain-edge", "", plain, nil, nil, nil, nil, nil, nil, nil, nil, "locks", m, true)
	if _, err := plainRepo.Add(ctx, "x", "y", time.Minute); err == nil {
		t.Fatal("expected unsupported add error")
	}
	if _, err := plainRepo.Touch(ctx, "x", time.Minute); err == nil {
		t.Fatal("expected unsupported touch error")
	}
	if ok, err := plainRepo.Has(ctx, "x"); err != nil || !ok {
		t.Fatalf("plain has ok=%v err=%v", ok, err)
	}
	flushFailRepo := newRepository("flush-fail", "", errorStore{}, nil, nil, nil, nil, nil, nil, nil, nil, "", m, true)
	if err := flushFailRepo.Forget(ctx, "x"); err == nil {
		t.Fatal("expected forget error")
	}
	if err := flushFailRepo.Flush(ctx); err == nil {
		t.Fatal("expected flush error")
	}

	if err := repo.putEncoded(ctx, "bad-get", []byte(`{`), time.Minute); err != nil {
		t.Fatalf("put bad get: %v", err)
	}
	if _, err := GetFrom[map[string]any](ctx, "", "bad-get"); err == nil {
		t.Fatal("expected GetFrom decode error")
	}
	if _, err := ManyFrom[string](ctx, "", []string{"missing"}, Lazy(func(context.Context) (string, error) {
		return "", errors.New("fallback failed")
	})); err == nil {
		t.Fatal("expected ManyFrom fallback error")
	}
	if err := repo.putEncoded(ctx, "bad-pull", []byte(`{`), time.Minute); err != nil {
		t.Fatalf("put bad pull: %v", err)
	}
	if _, err := PullFrom[map[string]any](ctx, "", "bad-pull"); err == nil {
		t.Fatal("expected PullFrom decode error")
	}

	memo := repo.Memo()
	if err := repo.Put(ctx, "memo-hit", "cached", time.Minute); err != nil {
		t.Fatalf("memo seed: %v", err)
	}
	if got, err := memo.Remember(ctx, "memo-hit", time.Minute, func(context.Context) (any, error) {
		return "loaded", nil
	}); err != nil || got != "cached" {
		t.Fatalf("memo cached remember = %v err=%v", got, err)
	}
	if err := repo.putEncoded(ctx, "memo-bad-json", []byte(`{`), time.Minute); err != nil {
		t.Fatalf("memo bad json seed: %v", err)
	}
	if _, err := memo.Get(ctx, "memo-bad-json"); err == nil {
		t.Fatal("expected memo decode error")
	}
	forgetFailMemo := newMemoRepository(flushFailRepo)
	if err := forgetFailMemo.Forget(ctx, "x"); err == nil {
		t.Fatal("expected memo forget error")
	}

	tagged := repo.Tags("edge")
	if err := tagged.Put(ctx, "hit", "cached", time.Minute); err != nil {
		t.Fatalf("tagged seed: %v", err)
	}
	if got, err := tagged.Remember(ctx, "hit", time.Minute, func(context.Context) (any, error) {
		return "loaded", nil
	}); err != nil || got != "cached" {
		t.Fatalf("tagged cached remember = %v err=%v", got, err)
	}
	if err := repo.tags.PutTagged(ctx, repo.prefix, []string{"bad-json"}, "x", []byte(`{`), time.Minute); err != nil {
		t.Fatalf("tagged bad json seed: %v", err)
	}
	if _, err := repo.Tags("bad-json").Get(ctx, "x"); err == nil {
		t.Fatal("expected tagged decode error")
	}
	tagFailStore := failingTagStore{memoryStore: newMemoryStore(0, 0)}
	tagFailRepo := newRepository("tag-has-fail", "", tagFailStore, tagFailStore, tagFailStore, tagFailStore, nil, tagFailStore, tagFailStore, tagFailStore, nil, "locks", m, true)
	if ok, err := tagFailRepo.Tags("x").Has(ctx, "key"); err == nil || ok {
		t.Fatalf("expected failing tagged has error, ok=%v err=%v", ok, err)
	}

	held := repo.Lock("funnel:edge-block", time.Second)
	if ok, err := held.Get(ctx); err != nil || !ok {
		t.Fatalf("hold funnel lock ok=%v err=%v", ok, err)
	}
	if ok, err := repo.Funnel("edge-block").SleepFor(time.Millisecond).Then(ctx, nil); !errors.Is(err, ErrLockTimeout) || ok {
		t.Fatalf("expected funnel timeout, ok=%v err=%v", ok, err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if ok, err := repo.Funnel("edge-block").BlockFor(50*time.Millisecond).SleepFor(time.Millisecond).Then(canceled, func(context.Context) error {
		return nil
	}); !errors.Is(err, context.Canceled) || ok {
		t.Fatalf("expected canceled funnel wait, ok=%v err=%v", ok, err)
	}
	if err := held.Release(ctx); err != nil {
		t.Fatalf("release funnel lock: %v", err)
	}
	if ok, err := repo.Funnel("nil-success").Then(ctx, nil); err != nil || !ok {
		t.Fatalf("nil success funnel ok=%v err=%v", ok, err)
	}
	if ok, err := repo.Funnel("success-error").Then(ctx, func(context.Context) error {
		return errors.New("success failed")
	}); err == nil || !ok {
		t.Fatalf("expected success callback error, ok=%v err=%v", ok, err)
	}
}

func TestFlexibleAndFileStoreAdditionalBranches(t *testing.T) {
	ctx := context.Background()
	m := newTestManager(t)
	repo := m.defaultRepository()

	if _, err := flexibleTyped(ctx, newErrorRepository("missing", ErrStoreNotFound, m), "x", FlexibleWindow{}, func(context.Context) (string, error) {
		return "x", nil
	}); !errors.Is(err, ErrStoreNotFound) {
		t.Fatalf("flexible error repo err=%v", err)
	}
	payload, err := encode("old")
	if err != nil {
		t.Fatalf("encode payload: %v", err)
	}
	badValue, err := encode(flexibleEntry{Value: json.RawMessage(`{}`), WrittenAt: time.Now().UnixNano()})
	if err != nil {
		t.Fatalf("encode bad flexible entry: %v", err)
	}
	if err := repo.putEncoded(ctx, "flex-bad-value", badValue, time.Minute); err != nil {
		t.Fatalf("put bad flexible value: %v", err)
	}
	if _, err := flexibleTyped(ctx, repo, "flex-bad-value", FlexibleWindow{Fresh: time.Minute, Stale: time.Minute}, func(context.Context) (string, error) {
		return "new", nil
	}); err == nil {
		t.Fatal("expected flexible value decode error")
	}
	repo.startRefresh("already-refreshing")
	scheduleRefreshTyped(ctx, repo, "already-refreshing", FlexibleWindow{Fresh: time.Millisecond, Stale: time.Second}, func(context.Context) (string, error) {
		t.Fatal("duplicate refresh should not run loader")
		return "", nil
	})
	repo.finishRefresh("already-refreshing")

	staleEntry, err := encode(flexibleEntry{Value: payload, WrittenAt: time.Now().Add(-50 * time.Millisecond).UnixNano()})
	if err != nil {
		t.Fatalf("encode stale flexible entry: %v", err)
	}
	if err := repo.putEncoded(ctx, "flex-go-refresh", staleEntry, time.Minute); err != nil {
		t.Fatalf("put stale flexible entry: %v", err)
	}
	refreshed := make(chan struct{}, 1)
	got, err := flexibleTyped(ctx, repo, "flex-go-refresh", FlexibleWindow{Fresh: time.Millisecond, Stale: time.Second}, func(context.Context) (string, error) {
		refreshed <- struct{}{}
		return "new", nil
	})
	if err != nil || got != "old" {
		t.Fatalf("stale flexible got=%q err=%v", got, err)
	}
	select {
	case <-refreshed:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for flexible goroutine refresh")
	}

	root := t.TempDir()
	store := newFileStore(FileConfig{Path: filepath.Join(root, "data"), LockPath: filepath.Join(root, "locks")}, 0, "", "")
	if _, _, err := store.GetWithTTL(ctx, 123); err == nil {
		t.Fatal("expected file ttl non-string error")
	}
	badCachePath := store.cachePath("bad-json")
	if err := os.MkdirAll(filepath.Dir(badCachePath), 0o755); err != nil {
		t.Fatalf("mkdir bad cache: %v", err)
	}
	if err := os.WriteFile(badCachePath, []byte(`{`), 0o600); err != nil {
		t.Fatalf("write bad cache: %v", err)
	}
	if _, err := store.Get(ctx, "bad-json"); err == nil {
		t.Fatal("expected bad cache json error")
	}
	if ok, err := store.Add(ctx, "bad-json", []byte(`"x"`), time.Minute); err == nil || ok {
		t.Fatalf("expected file add bad json error, ok=%v err=%v", ok, err)
	}
	if ok, err := store.Touch(ctx, "bad-json", time.Minute); err == nil || ok {
		t.Fatalf("expected file touch bad json error, ok=%v err=%v", ok, err)
	}
	if _, err := store.Increment(ctx, "bad-json", 1); err == nil {
		t.Fatal("expected file increment bad json error")
	}

	badLockPath := store.lockPath("bad-lock")
	if err := os.MkdirAll(filepath.Dir(badLockPath), 0o755); err != nil {
		t.Fatalf("mkdir bad lock: %v", err)
	}
	if err := os.WriteFile(badLockPath, []byte(`{`), 0o600); err != nil {
		t.Fatalf("write bad lock: %v", err)
	}
	if ok, err := store.Release(ctx, "bad-lock", "token"); err == nil || ok {
		t.Fatalf("expected bad lock release error, ok=%v err=%v", ok, err)
	}
	if err := os.WriteFile(badLockPath, []byte(`{`), 0o600); err != nil {
		t.Fatalf("rewrite bad lock: %v", err)
	}
	if ok, err := store.Acquire(ctx, "bad-lock", "token", time.Minute); err != nil || !ok {
		t.Fatalf("expected acquire to recover bad lock, ok=%v err=%v", ok, err)
	}
}
