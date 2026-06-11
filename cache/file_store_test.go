package cache

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func newFileTestManager(t *testing.T) *Manager {
	t.Helper()
	root := t.TempDir()
	m, err := NewManager(Config{
		Default: "file",
		Prefix:  "test",
		Stores: map[string]StoreConfig{
			"file": {
				Driver: "file",
				Prefix: "file",
				File: FileConfig{
					Path:     filepath.Join(root, "data"),
					LockPath: filepath.Join(root, "locks"),
				},
			},
		},
		Lock: LockConfig{RetrySleep: time.Millisecond},
	})
	if err != nil {
		t.Fatalf("new file manager: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })
	return m
}

func TestFileDriverBasicOperations(t *testing.T) {
	ctx := context.Background()
	m := newFileTestManager(t)
	bindCacheManagerForTest(t, m)
	repo := m.defaultRepository()

	if err := repo.Put(ctx, "profile", map[string]string{"name": "alice"}, time.Second); err != nil {
		t.Fatalf("file put: %v", err)
	}
	profile, err := GetFrom[map[string]string](ctx, "file", "profile")
	if err != nil {
		t.Fatalf("file get typed: %v", err)
	}
	if profile["name"] != "alice" {
		t.Fatalf("file profile = %#v", profile)
	}

	if err := repo.Forever(ctx, "forever", "kept"); err != nil {
		t.Fatalf("file forever: %v", err)
	}
	ok, err := repo.Has(ctx, "forever")
	if err != nil || !ok {
		t.Fatalf("file has forever: ok=%v err=%v", ok, err)
	}

	ok, err = repo.Add(ctx, "dedupe", "first", 300*time.Millisecond)
	if err != nil || !ok {
		t.Fatalf("file add first: ok=%v err=%v", ok, err)
	}
	ok, err = repo.Add(ctx, "dedupe", "second", time.Second)
	if err != nil || ok {
		t.Fatalf("file add second: ok=%v err=%v", ok, err)
	}
	time.Sleep(350 * time.Millisecond)
	ok, err = repo.Add(ctx, "dedupe", "third", time.Second)
	if err != nil || !ok {
		t.Fatalf("file add after expiration: ok=%v err=%v", ok, err)
	}
	value, err := repo.Get(ctx, "dedupe")
	if err != nil || value != "third" {
		t.Fatalf("file add value = %v err=%v", value, err)
	}

	if err := repo.Put(ctx, "touch", "value", 300*time.Millisecond); err != nil {
		t.Fatalf("file touch put: %v", err)
	}
	time.Sleep(50 * time.Millisecond)
	ok, err = repo.Touch(ctx, "touch", 600*time.Millisecond)
	if err != nil || !ok {
		t.Fatalf("file touch: ok=%v err=%v", ok, err)
	}
	time.Sleep(350 * time.Millisecond)
	touched, err := GetFrom[string](ctx, "file", "touch")
	if err != nil || touched != "value" {
		t.Fatalf("file touched value = %q err=%v", touched, err)
	}

	if err := repo.Put(ctx, "once", "token", time.Minute); err != nil {
		t.Fatalf("file pull put: %v", err)
	}
	pulled, err := PullFrom[string](ctx, "file", "once")
	if err != nil || pulled != "token" {
		t.Fatalf("file pull = %q err=%v", pulled, err)
	}
	if _, err := PullFrom[string](ctx, "file", "once"); !errors.Is(err, ErrCacheMiss) {
		t.Fatalf("file second pull err = %v, want ErrCacheMiss", err)
	}

	count, err := repo.Increment(ctx, "views", 5)
	if err != nil || count != 5 {
		t.Fatalf("file increment = %d err=%v", count, err)
	}
	count, err = repo.Decrement(ctx, "views", 2)
	if err != nil || count != 3 {
		t.Fatalf("file decrement = %d err=%v", count, err)
	}
	added, err := repo.Add(ctx, "seeded-counter", 1, time.Minute)
	if err != nil || !added {
		t.Fatalf("file add seeded counter = %v err=%v", added, err)
	}
	count, err = repo.Increment(ctx, "seeded-counter")
	if err != nil || count != 2 {
		t.Fatalf("file increment seeded counter = %d err=%v", count, err)
	}
	if err := repo.Put(ctx, "bad-counter", "not-number", time.Minute); err != nil {
		t.Fatalf("file bad counter put: %v", err)
	}
	if _, err := repo.Increment(ctx, "bad-counter"); !errors.Is(err, ErrInvalidCounter) {
		t.Fatalf("file invalid counter err = %v, want ErrInvalidCounter", err)
	}

	if err := repo.Flush(ctx); err != nil {
		t.Fatalf("file flush: %v", err)
	}
	if _, err := repo.Get(ctx, "profile"); !errors.Is(err, ErrCacheMiss) {
		t.Fatalf("file get after flush err = %v, want ErrCacheMiss", err)
	}
}

func TestFileStoreFlushIsScopedByRepositoryPrefix(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	m, err := NewManager(Config{
		Default: "tenant-a",
		Prefix:  "app",
		Stores: map[string]StoreConfig{
			"tenant-a": {
				Driver: "file",
				Prefix: "a",
				File: FileConfig{
					Path:     filepath.Join(root, "data"),
					LockPath: filepath.Join(root, "locks"),
				},
			},
			"tenant-b": {
				Driver: "file",
				Prefix: "b",
				File: FileConfig{
					Path:     filepath.Join(root, "data"),
					LockPath: filepath.Join(root, "locks"),
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("new file manager: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })

	repoA := m.storeRepository("tenant-a")
	repoB := m.storeRepository("tenant-b")
	if err := repoA.Put(ctx, "profile", "alice", time.Minute); err != nil {
		t.Fatalf("put repo a: %v", err)
	}
	if err := repoB.Put(ctx, "profile", "bob", time.Minute); err != nil {
		t.Fatalf("put repo b: %v", err)
	}
	if err := repoA.Flush(ctx); err != nil {
		t.Fatalf("flush repo a: %v", err)
	}
	if _, err := repoA.Get(ctx, "profile"); !errors.Is(err, ErrCacheMiss) {
		t.Fatalf("repo a after flush err=%v, want miss", err)
	}
	got, err := repoB.Get(ctx, "profile")
	if err != nil || got != "bob" {
		t.Fatalf("repo b after repo a flush = %v err=%v, want bob", got, err)
	}
}

func TestFileStoreFlushLocksIsScopedByLockPrefix(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	m, err := NewManager(Config{
		Default: "tenant-a",
		Prefix:  "app",
		Stores: map[string]StoreConfig{
			"tenant-a": {Driver: "file", Prefix: "a", File: FileConfig{Path: filepath.Join(root, "data"), LockPath: filepath.Join(root, "locks")}},
			"tenant-b": {Driver: "file", Prefix: "b", File: FileConfig{Path: filepath.Join(root, "data"), LockPath: filepath.Join(root, "locks")}},
		},
		Lock: LockConfig{RetrySleep: time.Millisecond},
	})
	if err != nil {
		t.Fatalf("new file manager: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })

	repoA := m.storeRepository("tenant-a")
	repoB := m.storeRepository("tenant-b")
	lockA := repoA.Lock("job", time.Minute)
	if ok, err := lockA.Get(ctx); err != nil || !ok {
		t.Fatalf("lock a get: ok=%v err=%v", ok, err)
	}
	lockB := repoB.Lock("job", time.Minute)
	if ok, err := lockB.Get(ctx); err != nil || !ok {
		t.Fatalf("lock b get: ok=%v err=%v", ok, err)
	}
	if err := repoA.FlushLocks(ctx); err != nil {
		t.Fatalf("flush locks repo a: %v", err)
	}
	if ok, err := repoA.Lock("job", time.Minute).Get(ctx); err != nil || !ok {
		t.Fatalf("repo a lock after flush: ok=%v err=%v", ok, err)
	}
	if ok, err := repoB.Lock("job", time.Minute).Get(ctx); err != nil || ok {
		t.Fatalf("repo b lock should remain held: ok=%v err=%v", ok, err)
	}
}

func TestFileLockExpiredOwnerReleaseCannotDeleteNewOwner(t *testing.T) {
	ctx := context.Background()
	m := newFileTestManager(t)
	repo := m.defaultRepository()

	oldLock := repo.Lock("lease", 20*time.Millisecond)
	if ok, err := oldLock.Get(ctx); err != nil || !ok {
		t.Fatalf("old lock get: ok=%v err=%v", ok, err)
	}
	time.Sleep(30 * time.Millisecond)
	newLock := repo.Lock("lease", time.Minute)
	if ok, err := newLock.Get(ctx); err != nil || !ok {
		t.Fatalf("new lock get after expiry: ok=%v err=%v", ok, err)
	}
	if err := oldLock.Release(ctx); !errors.Is(err, ErrLockNotHeld) {
		t.Fatalf("old release err=%v, want ErrLockNotHeld", err)
	}
	if err := newLock.Release(ctx); err != nil {
		t.Fatalf("new lock release after old release attempt: %v", err)
	}
}

func TestFileStoreResolvesPathsFromApplicationStorage(t *testing.T) {
	base := t.TempDir()
	c := useCacheTestContainer(t)
	if err := c.Instance("path.storage", filepath.Join(base, "storage")); err != nil {
		t.Fatalf("bind storage path: %v", err)
	}

	defaultStore := newFileStore(FileConfig{}, 0, "", "")
	if want := filepath.Join(base, "storage", "framework", "cache", "data"); defaultStore.root != want {
		t.Fatalf("default root = %q, want %q", defaultStore.root, want)
	}
	if want := filepath.Join(base, "storage", "framework", "cache", "locks"); defaultStore.lockRoot != want {
		t.Fatalf("default lockRoot = %q, want %q", defaultStore.lockRoot, want)
	}

	relativeStore := newFileStore(FileConfig{Path: "storage/custom/cache"}, 0, "", "")
	if want := filepath.Join(base, "storage", "custom", "cache"); relativeStore.root != want {
		t.Fatalf("relative root = %q, want %q", relativeStore.root, want)
	}
	if want := filepath.Join(base, "storage", "custom", "locks"); relativeStore.lockRoot != want {
		t.Fatalf("relative lockRoot = %q, want %q", relativeStore.lockRoot, want)
	}

	absoluteRoot := filepath.Join(t.TempDir(), "cache")
	absoluteLockRoot := filepath.Join(t.TempDir(), "locks")
	absoluteStore := newFileStore(FileConfig{Path: absoluteRoot, LockPath: absoluteLockRoot}, 0, "", "")
	if absoluteStore.root != absoluteRoot || absoluteStore.lockRoot != absoluteLockRoot {
		t.Fatalf("absolute paths = %q/%q, want %q/%q", absoluteStore.root, absoluteStore.lockRoot, absoluteRoot, absoluteLockRoot)
	}
}

func TestFileDriverLockOwnerRestoreForceReleaseAndFunnel(t *testing.T) {
	ctx := context.Background()
	m := newFileTestManager(t)
	repo := m.defaultRepository()

	lock := repo.Lock("owner", time.Second)
	ok, err := lock.Get(ctx)
	if err != nil || !ok {
		t.Fatalf("file lock get: ok=%v err=%v", ok, err)
	}
	owner := lock.Owner()
	if owner == "" {
		t.Fatal("file lock owner should not be empty")
	}
	if err := repo.RestoreLock("owner", owner).Release(ctx); err != nil {
		t.Fatalf("file restored release: %v", err)
	}
	ok, err = repo.Lock("owner", time.Second).Get(ctx)
	if err != nil || !ok {
		t.Fatalf("file lock after restored release: ok=%v err=%v", ok, err)
	}

	forced := repo.Lock("force", time.Second)
	if ok, err = forced.Get(ctx); err != nil || !ok {
		t.Fatalf("file force lock get: ok=%v err=%v", ok, err)
	}
	blocked := repo.Lock("force", time.Second)
	if ok, err = blocked.Get(ctx); err != nil || ok {
		t.Fatalf("file blocked lock get: ok=%v err=%v", ok, err)
	}
	if err := forced.ForceRelease(ctx); err != nil {
		t.Fatalf("file force release: %v", err)
	}
	if ok, err = blocked.Get(ctx); err != nil || !ok {
		t.Fatalf("file blocked lock after force release: ok=%v err=%v", ok, err)
	}

	held := repo.Lock("funnel:serial", time.Second)
	if ok, err = held.Get(ctx); err != nil || !ok {
		t.Fatalf("file funnel held lock: ok=%v err=%v", ok, err)
	}
	failed := false
	ok, err = repo.Funnel("serial").SleepFor(time.Millisecond).Then(ctx, func(context.Context) error {
		t.Fatal("funnel success should not run while slot is held")
		return nil
	}, func(context.Context) error {
		failed = true
		return nil
	})
	if err != nil || ok || !failed {
		t.Fatalf("file funnel failure: ok=%v failed=%v err=%v", ok, failed, err)
	}
	if err := held.Release(ctx); err != nil {
		t.Fatalf("file funnel held release: %v", err)
	}

	slot0 := repo.Lock("funnel:multi:0", time.Second)
	if ok, err = slot0.Get(ctx); err != nil || !ok {
		t.Fatalf("file funnel slot0 get: ok=%v err=%v", ok, err)
	}
	ran := false
	ok, err = repo.Funnel("multi").Limit(2).Then(ctx, func(context.Context) error {
		ran = true
		return nil
	})
	if err != nil || !ok || !ran {
		t.Fatalf("file funnel multi: ok=%v ran=%v err=%v", ok, ran, err)
	}
}

func TestFileStoreDirectBranches(t *testing.T) {
	ctx := context.Background()
	store := newFileStore(FileConfig{
		Path:     filepath.Join(t.TempDir(), "data"),
		LockPath: filepath.Join(t.TempDir(), "locks"),
	}, 120*time.Millisecond, "", "")

	if err := store.Set(ctx, "ttl", []byte(`"value"`)); err != nil {
		t.Fatalf("file direct set: %v", err)
	}
	value, ttl, err := store.GetWithTTL(ctx, "ttl")
	if err != nil {
		t.Fatalf("file direct ttl: %v", err)
	}
	if string(value.([]byte)) != `"value"` || ttl <= 0 {
		t.Fatalf("file direct value/ttl = %v %v", value, ttl)
	}
	if got := store.GetType(); got != fileType {
		t.Fatalf("file type = %s, want %s", got, fileType)
	}
	if err := store.Invalidate(ctx); err != nil {
		t.Fatalf("file invalidate: %v", err)
	}
	time.Sleep(150 * time.Millisecond)
	if _, err := store.Get(ctx, "ttl"); !isMiss(err) {
		t.Fatalf("file expired direct get err = %v, want miss", err)
	}
	if err := store.Set(ctx, 123, []byte(`1`)); err == nil {
		t.Fatal("expected non-string file key set error")
	}
	if _, err := store.Get(ctx, 123); err == nil {
		t.Fatal("expected non-string file key get error")
	}
	if err := store.Delete(ctx, 123); err == nil {
		t.Fatal("expected non-string file key delete error")
	}
	if err := store.PutMany(ctx, map[string][]byte{"bulk-a": []byte(`"a"`), "bulk-b": []byte(`"b"`)}, time.Minute); err != nil {
		t.Fatalf("file put many: %v", err)
	}
	many, err := store.GetMany(ctx, []string{"bulk-a", "bulk-b", "bulk-missing"})
	if err != nil {
		t.Fatalf("file get many: %v", err)
	}
	if string(many["bulk-a"]) != `"a"` || string(many["bulk-b"]) != `"b"` {
		t.Fatalf("file get many values = %#v", many)
	}
	if err := store.Set(ctx, "bulk-object", map[string]string{"name": "alice"}, expirationOptions(time.Minute)...); err != nil {
		t.Fatalf("file set object: %v", err)
	}
	objectMany, err := store.GetMany(ctx, []string{"bulk-object"})
	if err != nil {
		t.Fatalf("file get many object: %v", err)
	}
	if string(objectMany["bulk-object"]) != `{"name":"alice"}` {
		t.Fatalf("file get many object bytes = %q", string(objectMany["bulk-object"]))
	}
	if err := store.ForgetMany(ctx, []string{"bulk-a", "bulk-b"}); err != nil {
		t.Fatalf("file forget many: %v", err)
	}
	if err := store.Clear(ctx); err != nil {
		t.Fatalf("file clear: %v", err)
	}
}

func TestFileStoreLockAndExpirationBranches(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	store := newFileStore(FileConfig{Path: filepath.Join(root, "data")}, 0, "", "")

	if err := store.Set(ctx, "forever", []byte(`"value"`)); err != nil {
		t.Fatalf("file forever set: %v", err)
	}
	_, ttl, err := store.GetWithTTL(ctx, "forever")
	if err != nil || ttl != -1 {
		t.Fatalf("file forever ttl = %v err=%v, want -1", ttl, err)
	}
	if ok, err := store.Touch(ctx, "missing", time.Second); err != nil || ok {
		t.Fatalf("file touch missing: ok=%v err=%v", ok, err)
	}
	if err := store.Set(ctx, "expired-pull", []byte(`"old"`), expirationOptions(40*time.Millisecond)...); err != nil {
		t.Fatalf("file expired pull set: %v", err)
	}
	time.Sleep(70 * time.Millisecond)
	if _, err := store.Pull(ctx, "expired-pull"); !isMiss(err) {
		t.Fatalf("file expired pull err = %v, want miss", err)
	}
	if err := store.Delete(ctx, "missing-delete"); err != nil {
		t.Fatalf("file missing delete: %v", err)
	}

	// 使用长 TTL 单独验证有效锁会阻塞第二个 owner，避免 CI 调度延迟越过短 TTL。
	ok, err := store.Acquire(ctx, "valid-lock", "owner", time.Minute)
	if err != nil || !ok {
		t.Fatalf("file valid lock acquire: ok=%v err=%v", ok, err)
	}
	ok, err = store.Acquire(ctx, "valid-lock", "blocked", time.Second)
	if err != nil || ok {
		t.Fatalf("file valid lock second acquire: ok=%v err=%v", ok, err)
	}
	if err := store.ForceRelease(ctx, "valid-lock"); err != nil {
		t.Fatalf("file valid lock cleanup: %v", err)
	}

	// 短 TTL 锁只用于覆盖过期后可重新获取的分支，不再承担“仍有效”的断言。
	ok, err = store.Acquire(ctx, "stale-lock", "old", 40*time.Millisecond)
	if err != nil || !ok {
		t.Fatalf("file stale lock acquire: ok=%v err=%v", ok, err)
	}
	time.Sleep(70 * time.Millisecond)
	ok, err = store.Acquire(ctx, "stale-lock", "new", time.Second)
	if err != nil || !ok {
		t.Fatalf("file stale lock reacquire: ok=%v err=%v", ok, err)
	}
	ok, err = store.Release(ctx, "stale-lock", "wrong")
	if err != nil || ok {
		t.Fatalf("file wrong token release: ok=%v err=%v", ok, err)
	}
	if err := store.ForceRelease(ctx, "stale-lock"); err != nil {
		t.Fatalf("file force release existing: %v", err)
	}
	if err := store.ForceRelease(ctx, "stale-lock"); err != nil {
		t.Fatalf("file force release missing: %v", err)
	}

	token := randomToken()
	if ok, err := store.Acquire(ctx, "__cache_mutation:blocked", token, time.Second); err != nil || !ok {
		t.Fatalf("file mutation lock acquire: ok=%v err=%v", ok, err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := store.Set(canceled, "blocked", []byte(`"value"`)); !errors.Is(err, context.Canceled) {
		t.Fatalf("file canceled mutation err = %v, want context.Canceled", err)
	}
	if _, err := store.Release(ctx, "__cache_mutation:blocked", token); err != nil {
		t.Fatalf("file mutation lock release: %v", err)
	}
}

func TestFileStoreGuardFileBranches(t *testing.T) {
	root := t.TempDir()
	store := newFileStore(FileConfig{
		Path:     filepath.Join(root, "data"),
		LockPath: filepath.Join(root, "locks"),
	}, 0, "", "")
	guardPath := filepath.Join(root, "locks", "guard.lock")

	ok, err := store.acquireGuardFile(guardPath, "first")
	if err != nil || !ok {
		t.Fatalf("first guard acquire: ok=%v err=%v", ok, err)
	}
	ok, err = store.acquireGuardFile(guardPath, "blocked")
	if err != nil || ok {
		t.Fatalf("second guard acquire: ok=%v err=%v, want held", ok, err)
	}
	store.releaseGuardFile(guardPath, "wrong")
	if _, err := os.Stat(guardPath); err != nil {
		t.Fatalf("guard should remain after wrong-token release: %v", err)
	}
	store.releaseGuardFile(guardPath, "first")
	if _, err := os.Stat(guardPath); !os.IsNotExist(err) {
		t.Fatalf("guard should be removed after owner release, stat err=%v", err)
	}

	expired := fileLockEntry{Token: "old", ExpiresAt: time.Now().Add(-time.Second).UnixNano()}
	if err := store.createLockFile(guardPath, expired); err != nil {
		t.Fatalf("create expired guard: %v", err)
	}
	ok, err = store.acquireGuardFile(guardPath, "new")
	if err != nil || ok {
		t.Fatalf("expired guard cleanup acquire = ok:%v err:%v, want cleaned but not acquired", ok, err)
	}
	if _, err := os.Stat(guardPath); !os.IsNotExist(err) {
		t.Fatalf("expired guard should be removed, stat err=%v", err)
	}

	if err := os.MkdirAll(filepath.Dir(guardPath), 0o755); err != nil {
		t.Fatalf("mkdir guard dir: %v", err)
	}
	if err := os.WriteFile(guardPath, []byte("{bad json"), 0o600); err != nil {
		t.Fatalf("write bad guard: %v", err)
	}
	if ok, err = store.acquireGuardFile(guardPath, "bad"); err == nil || ok {
		t.Fatalf("bad guard acquire = ok:%v err:%v, want parse error", ok, err)
	}

	parentFile := filepath.Join(root, "locks", "parent-file")
	if err := os.WriteFile(parentFile, []byte("not a directory"), 0o600); err != nil {
		t.Fatalf("write parent file: %v", err)
	}
	if err := store.createLockFile(filepath.Join(parentFile, "child.lock"), fileLockEntry{Token: "x"}); err == nil {
		t.Fatal("expected createLockFile to fail when parent path is a file")
	}
}
