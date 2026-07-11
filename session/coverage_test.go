package session

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

// TestServiceProviderNameAndBoot 测试 ServiceProvider 的 Name 和 Boot 方法
func TestServiceProviderNameAndBoot(t *testing.T) {
	sp := ServiceProvider{}
	if name := sp.Name(); name != "session" {
		t.Fatalf("Name() = %q, want %q", name, "session")
	}
	if err := sp.Boot(nil); err != nil {
		t.Fatalf("Boot() error = %v", err)
	}
}

// TestFileLockReleaseBranches 测试 fileLock.Release 的各种分支
func TestFileLockReleaseBranches(t *testing.T) {
	t.Run("release with empty token", func(t *testing.T) {
		lock := &fileLock{path: "test.lock", token: "", held: true}
		if err := lock.Release(context.Background()); !errors.Is(err, ErrLockNotHeld) {
			t.Fatalf("Release error = %v, want ErrLockNotHeld", err)
		}
	})

	t.Run("release with non-existent file", func(t *testing.T) {
		lock := &fileLock{path: "/nonexistent/test.lock", token: "abc", held: true}
		if err := lock.Release(context.Background()); !errors.Is(err, ErrLockNotHeld) {
			t.Fatalf("Release error = %v, want ErrLockNotHeld", err)
		}
	})

	t.Run("release with mismatched token", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "test.lock")
		if err := os.WriteFile(path, []byte("different-token"), 0o600); err != nil {
			t.Fatalf("write lock file: %v", err)
		}
		lock := &fileLock{path: path, token: "abc", held: true}
		if err := lock.Release(context.Background()); !errors.Is(err, ErrLockNotHeld) {
			t.Fatalf("Release error = %v, want ErrLockNotHeld", err)
		}
	})
}

// TestTryCreateLockFileBranches 测试 tryCreateLockFile 的错误路径
func TestTryCreateLockFileBranches(t *testing.T) {
	t.Run("create lock in non-existent directory", func(t *testing.T) {
		path := "/nonexistent/dir/test.lock"
		if _, err := tryCreateLockFile(path, "token"); err == nil {
			t.Fatal("expected error for non-existent directory")
		}
	})

	t.Run("create lock with existing file", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "test.lock")
		if err := os.WriteFile(path, []byte("existing"), 0o600); err != nil {
			t.Fatalf("write file: %v", err)
		}
		if _, err := tryCreateLockFile(path, "token"); !errors.Is(err, os.ErrExist) {
			t.Fatalf("error = %v, want os.ErrExist", err)
		}
	})
}

// TestAtomicWriteBranches 测试 atomicWrite 的错误路径
func TestAtomicWriteBranches(t *testing.T) {
	t.Run("write to read-only directory", func(t *testing.T) {
		cfg := testConfig(t)
		driver := newTestFileDriver(t, cfg)
		id := newSessionID()
		payload := Payload{ID: id, Values: map[string]any{"key": "value"}, LastActivity: testNow()}
		expiresAt := time.Now().Add(time.Hour)
		// 删除目录使写入失败
		if err := os.RemoveAll(driver.root); err != nil {
			t.Fatalf("RemoveAll error = %v", err)
		}
		if err := driver.Write(context.Background(), id, payload, &expiresAt); err == nil {
			t.Fatal("expected error for non-existent directory")
		}
	})
}

// TestContextFromBranches 测试 contextFrom 的各种分支
func TestContextFromBranches(t *testing.T) {
	t.Run("nil gin context", func(t *testing.T) {
		ctx := contextFrom(nil)
		if ctx == nil {
			t.Fatal("contextFrom(nil) returned nil")
		}
	})

	t.Run("gin context with nil request", func(t *testing.T) {
		gin.SetMode(gin.TestMode)
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		c.Request = nil
		ctx := contextFrom(c)
		if ctx == nil {
			t.Fatal("contextFrom with nil request returned nil")
		}
	})

	t.Run("gin context with valid request", func(t *testing.T) {
		gin.SetMode(gin.TestMode)
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		c.Request = req
		ctx := contextFrom(c)
		if ctx != req.Context() {
			t.Fatal("contextFrom did not return request context")
		}
	})
}

// TestAppendUniqueBranches 测试 appendUnique 的各种分支
func TestAppendUniqueBranches(t *testing.T) {
	t.Run("append to empty slice", func(t *testing.T) {
		result := appendUnique([]string{}, "item")
		if len(result) != 1 || result[0] != "item" {
			t.Fatalf("result = %v, want [item]", result)
		}
	})

	t.Run("append existing item", func(t *testing.T) {
		result := appendUnique([]string{"item"}, "item")
		if len(result) != 1 {
			t.Fatalf("result = %v, want [item]", result)
		}
	})

	t.Run("append new item", func(t *testing.T) {
		result := appendUnique([]string{"item1"}, "item2")
		if len(result) != 2 || result[0] != "item1" || result[1] != "item2" {
			t.Fatalf("result = %v, want [item1 item2]", result)
		}
	})
}

// TestNewFileLockManagerBranches 测试 newFileLockManager 的错误路径
func TestNewFileLockManagerBranches(t *testing.T) {
	t.Run("create in non-existent parent", func(t *testing.T) {
		// 使用一个文件作为父目录，确保 MkdirAll 在跨平台都会失败
		blocker := filepath.Join(t.TempDir(), "blocker")
		if err := os.WriteFile(blocker, []byte("x"), 0o600); err != nil {
			t.Fatalf("write blocker: %v", err)
		}
		dir := filepath.Join(blocker, "child")
		if _, err := newFileLockManager(dir); err == nil {
			t.Fatal("expected error for non-existent parent directory")
		}
	})
}

// TestRemoveStaleBranches 测试 removeStale 的各种分支
func TestRemoveStaleBranches(t *testing.T) {
	t.Run("remove non-existent file", func(t *testing.T) {
		dir := t.TempDir()
		mgr, err := newFileLockManager(dir)
		if err != nil {
			t.Fatalf("newFileLockManager error = %v", err)
		}
		path := filepath.Join(dir, "nonexistent.lock")
		mgr.removeStale(path, time.Second) // 应该不 panic
	})

	t.Run("remove fresh file", func(t *testing.T) {
		dir := t.TempDir()
		mgr, err := newFileLockManager(dir)
		if err != nil {
			t.Fatalf("newFileLockManager error = %v", err)
		}
		path := filepath.Join(dir, "fresh.lock")
		if err := os.WriteFile(path, []byte("token"), 0o600); err != nil {
			t.Fatalf("write file: %v", err)
		}
		mgr.removeStale(path, time.Hour) // TTL 大于文件年龄
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("file should not be removed: %v", err)
		}
	})

	t.Run("remove stale file", func(t *testing.T) {
		dir := t.TempDir()
		mgr, err := newFileLockManager(dir)
		if err != nil {
			t.Fatalf("newFileLockManager error = %v", err)
		}
		path := filepath.Join(dir, "stale.lock")
		if err := os.WriteFile(path, []byte("token"), 0o600); err != nil {
			t.Fatalf("write file: %v", err)
		}
		// 修改文件时间为过去
		pastTime := time.Now().Add(-2 * time.Hour)
		if err := os.Chtimes(path, pastTime, pastTime); err != nil {
			t.Fatalf("chtimes: %v", err)
		}
		mgr.removeStale(path, time.Hour) // TTL 小于文件年龄
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatal("stale file should be removed")
		}
	})
}

// TestManagerNowBranches 测试 Manager.now 的分支
func TestManagerNowBranches(t *testing.T) {
	t.Run("with custom clock", func(t *testing.T) {
		cfg := testConfig(t)
		manager, err := NewManager(cfg, nil)
		if err != nil {
			t.Fatalf("NewManager error = %v", err)
		}
		customTime := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
		manager.clock = func() time.Time { return customTime }
		if now := manager.now(); !now.Equal(customTime) {
			t.Fatalf("now() = %v, want %v", now, customTime)
		}
	})

	t.Run("without custom clock", func(t *testing.T) {
		cfg := testConfig(t)
		manager, err := NewManager(cfg, nil)
		if err != nil {
			t.Fatalf("NewManager error = %v", err)
		}
		manager.clock = nil
		before := time.Now()
		now := manager.now()
		after := time.Now()
		if now.Before(before) || now.After(after) {
			t.Fatalf("now() = %v, want between %v and %v", now, before, after)
		}
	})
}

// TestSetStoreBranches 测试 SetStore 的各种分支
func TestSetStoreBranches(t *testing.T) {
	t.Run("nil context", func(t *testing.T) {
		SetStore(nil, nil) // 应该不 panic
	})

	t.Run("nil store", func(t *testing.T) {
		gin.SetMode(gin.TestMode)
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		SetStore(c, nil) // 应该不 panic
	})

	t.Run("valid store", func(t *testing.T) {
		gin.SetMode(gin.TestMode)
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		manager, _ := newTestManager(t)
		store := newStore(manager, Payload{}, nil)
		SetStore(c, store)
		if _, exists := c.Get(storeContextKey); !exists {
			t.Fatal("store not set in context")
		}
	})
}

// TestWithLockBranches 测试 withLock 的错误路径
func TestWithLockBranches(t *testing.T) {
	t.Run("lock acquire error", func(t *testing.T) {
		cfg := testConfig(t)
		driver := newReleaseTrackingDriver()
		manager, err := NewManager(cfg, driver)
		if err != nil {
			t.Fatalf("NewManager error = %v", err)
		}
		id := newSessionID()
		// 先获取锁
		lock, err := driver.Lock(context.Background(), id, time.Second, 20*time.Millisecond)
		if err != nil {
			t.Fatalf("seed Lock error = %v", err)
		}
		defer func() {
			if err := lock.Release(context.Background()); err != nil {
				t.Errorf("release seed lock: %v", err)
			}
		}()
		// 尝试再次获取锁，应该失败
		if err := manager.withLock(context.Background(), id, func() error {
			return nil
		}); !errors.Is(err, ErrLockTimeout) {
			t.Fatalf("withLock error = %v, want ErrLockTimeout", err)
		}
	})
}

// TestGCBranches 测试 GC 的各种分支
func TestGCBranches(t *testing.T) {
	t.Run("GC with no expired files", func(t *testing.T) {
		cfg := testConfig(t)
		driver := newTestFileDriver(t, cfg)
		id := newSessionID()
		payload := Payload{ID: id, Values: map[string]any{"key": "value"}, LastActivity: testNow()}
		expiresAt := testNow().Add(time.Hour)
		if err := driver.Write(context.Background(), id, payload, &expiresAt); err != nil {
			t.Fatalf("Write error = %v", err)
		}
		// GC 使用过去的时间点，这样不会删除任何文件
		pastTime := testNow().Add(-time.Hour)
		if err := driver.GC(context.Background(), pastTime); err != nil {
			t.Fatalf("GC error = %v", err)
		}
		if _, err := driver.Read(context.Background(), id); err != nil {
			t.Fatalf("Read after GC error = %v", err)
		}
	})

	t.Run("GC with expired files", func(t *testing.T) {
		cfg := testConfig(t)
		driver := newTestFileDriver(t, cfg)
		id := newSessionID()
		// 先写入一个有效的 session
		payload := Payload{ID: id, Values: map[string]any{"key": "value"}, LastActivity: testNow()}
		expiresAt := time.Now().Add(time.Hour)
		if err := driver.Write(context.Background(), id, payload, &expiresAt); err != nil {
			t.Fatalf("Write error = %v", err)
		}
		// 直接修改文件使其过期
		expiredAt := testNow().Add(-time.Minute)
		expiredPayload := Payload{ID: id, Values: map[string]any{"key": "value"}, LastActivity: testNow(), ExpiresAt: &expiredAt}
		data, err := driver.encode(context.Background(), expiredPayload)
		if err != nil {
			t.Fatalf("encode error = %v", err)
		}
		if err := os.WriteFile(driver.pathForID(id), data, 0o600); err != nil {
			t.Fatalf("write expired file error = %v", err)
		}
		if err := driver.GC(context.Background(), testNow()); err != nil {
			t.Fatalf("GC error = %v", err)
		}
		if _, err := driver.Read(context.Background(), id); !errors.Is(err, ErrSessionNotFound) {
			t.Fatalf("Read error = %v, want ErrSessionNotFound", err)
		}
	})
}

// TestManagerExpiresAtBranches 测试 Manager.expiresAt 的分支
func TestManagerExpiresAtBranches(t *testing.T) {
	t.Run("with positive lifetime", func(t *testing.T) {
		cfg := testConfig(t)
		manager, err := NewManager(cfg, nil)
		if err != nil {
			t.Fatalf("NewManager error = %v", err)
		}
		manager.clock = testNow
		expected := testNow().Add(cfg.Lifetime)
		got := manager.expiresAt()
		if got == nil || !got.Equal(expected) {
			t.Fatalf("expiresAt() = %v, want %v", got, expected)
		}
	})

	t.Run("with zero lifetime returns nil", func(t *testing.T) {
		// normalizeConfig 会把 Lifetime=0 重置为默认值，所以直接构造 Manager 绕过归一化。
		manager := &Manager{cfg: Config{Lifetime: 0}, clock: testNow}
		if got := manager.expiresAt(); got != nil {
			t.Fatalf("expiresAt() = %v, want nil", got)
		}
	})
}

// TestNewSessionIDBranches 测试 newSessionID 的分支
func TestNewSessionIDBranches(t *testing.T) {
	// 测试多次生成确保唯一性
	ids := make(map[string]bool)
	for i := 0; i < 100; i++ {
		id := newSessionID()
		if ids[id] {
			t.Fatalf("duplicate session ID: %s", id)
		}
		ids[id] = true
	}
}

// TestRedisDriverLockBranches 测试 RedisDriver.Lock 的分支
func TestRedisDriverLockBranches(t *testing.T) {
	// 这个测试需要 Redis，暂时跳过
	t.Skip("requires Redis")
}

// TestStoreOwnsRequestLockBranches 测试 ownsRequestLock 的分支
func TestStoreOwnsRequestLockBranches(t *testing.T) {
	t.Run("with matching lock", func(t *testing.T) {
		manager, _ := newTestManager(t)
		store := newStore(manager, Payload{}, nil)
		lock := &mockLock{}
		id := newSessionID()
		store.attachRequestLock(id, lock)
		if !store.ownsRequestLock(id) {
			t.Fatal("ownsRequestLock should return true")
		}
	})

	t.Run("with different lock", func(t *testing.T) {
		manager, _ := newTestManager(t)
		store := newStore(manager, Payload{}, nil)
		lock1 := &mockLock{}
		lock2 := &mockLock{}
		id1 := newSessionID()
		id2 := newSessionID()
		store.attachRequestLock(id1, lock1)
		if store.ownsRequestLock(id2) {
			t.Fatal("ownsRequestLock should return false for different lock")
		}
		_ = lock2
	})

	t.Run("with no lock", func(t *testing.T) {
		manager, _ := newTestManager(t)
		store := newStore(manager, Payload{}, nil)
		if store.ownsRequestLock(newSessionID()) {
			t.Fatal("ownsRequestLock should return false when no lock attached")
		}
	})
}

// TestStoreReleaseRequestLockBranches 测试 releaseRequestLock 的分支
func TestStoreReleaseRequestLockBranches(t *testing.T) {
	t.Run("release with lock", func(t *testing.T) {
		manager, _ := newTestManager(t)
		store := newStore(manager, Payload{}, nil)
		lock := &mockLock{}
		id := newSessionID()
		store.attachRequestLock(id, lock)
		if err := store.releaseRequestLock(context.Background()); err != nil {
			t.Fatalf("releaseRequestLock error = %v", err)
		}
		if lock.released != 1 {
			t.Fatalf("lock released %d times, want 1", lock.released)
		}
	})

	t.Run("release without lock", func(t *testing.T) {
		manager, _ := newTestManager(t)
		store := newStore(manager, Payload{}, nil)
		if err := store.releaseRequestLock(context.Background()); err != nil {
			t.Fatalf("releaseRequestLock error = %v", err)
		}
	})
}

// TestStoreGetBranches 测试 Store.Get 的分支
func TestStoreGetBranches(t *testing.T) {
	t.Run("get existing key", func(t *testing.T) {
		manager, _ := newTestManager(t)
		store := newStore(manager, Payload{Values: map[string]any{"key": "value"}}, nil)
		if got := store.Get("key"); got != "value" {
			t.Fatalf("Get() = %v, want %v", got, "value")
		}
	})

	t.Run("get non-existing key", func(t *testing.T) {
		manager, _ := newTestManager(t)
		store := newStore(manager, Payload{Values: map[string]any{}}, nil)
		if got := store.Get("key"); got != nil {
			t.Fatalf("Get() = %v, want nil", got)
		}
	})
}

// TestNewFileDriverBranches 测试 NewFileDriver 的错误路径
func TestNewFileDriverBranches(t *testing.T) {
	t.Run("with non-directory path", func(t *testing.T) {
		// 使用一个文件作为目录路径，确保在跨平台都会失败
		blocker := filepath.Join(t.TempDir(), "blocker")
		if err := os.WriteFile(blocker, []byte("x"), 0o600); err != nil {
			t.Fatalf("write blocker: %v", err)
		}
		cfg := testConfig(t)
		cfg.Files = blocker
		if _, err := NewFileDriver(cfg); err == nil {
			t.Fatal("expected error for non-directory path")
		}
	})
}

// TestFacadeGetBranches 测试 facade Get 的分支
func TestFacadeGetBranches(t *testing.T) {
	t.Run("get from nil context", func(t *testing.T) {
		if got := Get(nil, "key", "default"); got != "default" {
			t.Fatalf("Get() = %v, want %v", got, "default")
		}
	})

	t.Run("get from context without store", func(t *testing.T) {
		gin.SetMode(gin.TestMode)
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		c.Request = httptest.NewRequest(http.MethodGet, "/", nil)
		if got := Get(c, "key", "default"); got != "default" {
			t.Fatalf("Get() = %v, want %v", got, "default")
		}
	})
}

// TestFacadePullBranches 测试 facade Pull 的分支
func TestFacadePullBranches(t *testing.T) {
	t.Run("pull from nil context", func(t *testing.T) {
		if got := Pull(nil, "key", "default"); got != "default" {
			t.Fatalf("Pull() = %v, want %v", got, "default")
		}
	})

	t.Run("pull from context without store", func(t *testing.T) {
		gin.SetMode(gin.TestMode)
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		c.Request = httptest.NewRequest(http.MethodGet, "/", nil)
		if got := Pull(c, "key", "default"); got != "default" {
			t.Fatalf("Pull() = %v, want %v", got, "default")
		}
	})
}

// TestFileDriverDestroyBranches 测试 FileDriver.Destroy 的分支
func TestFileDriverDestroyBranches(t *testing.T) {
	t.Run("destroy non-existent file", func(t *testing.T) {
		cfg := testConfig(t)
		driver := newTestFileDriver(t, cfg)
		id := newSessionID()
		if err := driver.Destroy(context.Background(), id); err != nil {
			t.Fatalf("Destroy error = %v", err)
		}
	})

	t.Run("destroy existing file", func(t *testing.T) {
		cfg := testConfig(t)
		driver := newTestFileDriver(t, cfg)
		id := newSessionID()
		payload := Payload{ID: id, Values: map[string]any{"key": "value"}, LastActivity: testNow()}
		expiresAt := time.Now().Add(time.Hour)
		if err := driver.Write(context.Background(), id, payload, &expiresAt); err != nil {
			t.Fatalf("Write error = %v", err)
		}
		if err := driver.Destroy(context.Background(), id); err != nil {
			t.Fatalf("Destroy error = %v", err)
		}
		if _, err := driver.Read(context.Background(), id); !errors.Is(err, ErrSessionNotFound) {
			t.Fatalf("Read error = %v, want ErrSessionNotFound", err)
		}
	})
}

// TestFileDriverEncodeBranches 测试 FileDriver.encode 的分支
func TestFileDriverEncodeBranches(t *testing.T) {
	t.Run("encode with encryption", func(t *testing.T) {
		cfg := testConfig(t)
		cfg.Encrypt = true
		cfg.Encryptor = &mockEncryptor{}
		driver, err := NewFileDriver(cfg)
		if err != nil {
			t.Fatalf("NewFileDriver error = %v", err)
		}
		payload := Payload{ID: "test", Values: map[string]any{"key": "value"}, LastActivity: testNow()}
		if _, err := driver.encode(context.Background(), payload); err != nil {
			t.Fatalf("encode error = %v", err)
		}
	})
}

// TestFileDriverAcquireBranches 测试 fileLockManager.acquire 的分支
func TestFileDriverAcquireBranches(t *testing.T) {
	t.Run("acquire with zero TTL", func(t *testing.T) {
		dir := t.TempDir()
		mgr, err := newFileLockManager(dir)
		if err != nil {
			t.Fatalf("newFileLockManager error = %v", err)
		}
		id := newSessionID()
		if _, err := mgr.acquire(context.Background(), id, 0, time.Second); err != nil {
			t.Fatalf("acquire error = %v", err)
		}
	})

	t.Run("acquire with zero wait", func(t *testing.T) {
		dir := t.TempDir()
		mgr, err := newFileLockManager(dir)
		if err != nil {
			t.Fatalf("newFileLockManager error = %v", err)
		}
		id := newSessionID()
		if _, err := mgr.acquire(context.Background(), id, time.Second, 0); err != nil {
			t.Fatalf("acquire error = %v", err)
		}
	})
}

// TestRedisDriverEncodeBranches 测试 RedisDriver.encode 的分支
func TestRedisDriverEncodeBranches(t *testing.T) {
	t.Skip("requires Redis")
}

// TestResolveDriverBranches 测试 ResolveDriver 的分支
func TestResolveDriverBranches(t *testing.T) {
	t.Run("resolve file driver", func(t *testing.T) {
		cfg := testConfig(t)
		cfg.Driver = "file"
		driver, err := ResolveDriver("file", cfg)
		if err != nil {
			t.Fatalf("ResolveDriver error = %v", err)
		}
		if _, ok := driver.(*FileDriver); !ok {
			t.Fatalf("driver = %T, want *FileDriver", driver)
		}
	})

	t.Run("resolve unknown driver", func(t *testing.T) {
		cfg := testConfig(t)
		cfg.Driver = "unknown"
		if _, err := ResolveDriver("unknown", cfg); err == nil {
			t.Fatal("expected error for unknown driver")
		}
	})
}

// TestConfigFromFacadeStrictBranches 测试 ConfigFromFacadeStrict 的分支
func TestConfigFromFacadeStrictBranches(t *testing.T) {
	// 这个测试需要设置 config facade，暂时跳过
	t.Skip("requires config facade setup")
}

// TestResolveEncryptionForConfigBranches 测试 resolveEncryptionForConfig 的分支
func TestResolveEncryptionForConfigBranches(t *testing.T) {
	// 这个测试需要设置 encryption facade，暂时跳过
	t.Skip("requires encryption facade setup")
}

// TestNewManagerFromConfigBranches 测试 NewManagerFromConfig 的分支
func TestNewManagerFromConfigBranches(t *testing.T) {
	// 这个测试需要设置 config facade，暂时跳过
	t.Skip("requires config facade setup")
}

// TestStoreFromBranches 测试 StoreFrom 的分支
func TestStoreFromBranches(t *testing.T) {
	t.Run("store from nil context", func(t *testing.T) {
		if store, ok := StoreFrom(nil); store != nil || ok {
			t.Fatal("StoreFrom(nil) should return nil, false")
		}
	})

	t.Run("store from context without store", func(t *testing.T) {
		gin.SetMode(gin.TestMode)
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		c.Request = httptest.NewRequest(http.MethodGet, "/", nil)
		if store, ok := StoreFrom(c); store != nil || ok {
			t.Fatal("StoreFrom without store should return nil, false")
		}
	})
}

// TestRegenerateBranches 测试 Regenerate 的分支
func TestRegenerateBranches(t *testing.T) {
	t.Run("regenerate without manager", func(t *testing.T) {
		store := &Store{}
		if err := store.Regenerate(context.Background()); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("Regenerate error = %v, want ErrInvalidConfig", err)
		}
	})
}

// TestRedisDriverClientBranches 测试 redisDriverClient 的分支
func TestRedisDriverClientBranches(t *testing.T) {
	t.Skip("requires Redis")
}

// TestRedisDriverReadBranches 测试 RedisDriver.Read 的分支
func TestRedisDriverReadBranches(t *testing.T) {
	t.Skip("requires Redis")
}

// TestRedisLockReleaseBranches 测试 RedisLock.Release 的分支
func TestRedisLockReleaseBranches(t *testing.T) {
	t.Skip("requires Redis")
}

// TestSleepLockPollBranches 测试 sleepLockPoll 的分支
func TestSleepLockPollBranches(t *testing.T) {
	t.Run("poll with past deadline", func(t *testing.T) {
		deadline := time.Now().Add(-time.Second)
		if err := sleepLockPoll(context.Background(), deadline); err != nil {
			t.Fatalf("sleepLockPoll error = %v", err)
		}
	})

	t.Run("poll with cancelled context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		deadline := time.Now().Add(time.Second)
		if err := sleepLockPoll(ctx, deadline); !errors.Is(err, context.Canceled) {
			t.Fatalf("sleepLockPoll error = %v, want context.Canceled", err)
		}
	})
}

// TestRedisDriverWriteBranches 测试 RedisDriver.Write 的分支
func TestRedisDriverWriteBranches(t *testing.T) {
	t.Skip("requires Redis")
}

// TestFileDriverDecodeBranches 测试 FileDriver.decode 的分支
func TestFileDriverDecodeBranches(t *testing.T) {
	t.Run("decode with encryption", func(t *testing.T) {
		cfg := testConfig(t)
		cfg.Encrypt = true
		cfg.Encryptor = &mockEncryptor{}
		driver, err := NewFileDriver(cfg)
		if err != nil {
			t.Fatalf("NewFileDriver error = %v", err)
		}
		payload := Payload{ID: "test", Values: map[string]any{"key": "value"}, LastActivity: testNow()}
		data, err := driver.encode(context.Background(), payload)
		if err != nil {
			t.Fatalf("encode error = %v", err)
		}
		decoded, err := driver.decode(context.Background(), data)
		if err != nil {
			t.Fatalf("decode error = %v", err)
		}
		if decoded.ID != payload.ID {
			t.Fatalf("decoded.ID = %v, want %v", decoded.ID, payload.ID)
		}
	})
}

// TestRedisDriverDecodeBranches 测试 RedisDriver.decode 的分支
func TestRedisDriverDecodeBranches(t *testing.T) {
	t.Skip("requires Redis")
}

// TestNumericValueBranches 测试 numericValue 的分支
func TestNumericValueBranches(t *testing.T) {
	t.Run("numeric value from int", func(t *testing.T) {
		if got, err := numericValue(42); err != nil || got != 42 {
			t.Fatalf("numericValue(42) = %v, %v, want 42, nil", got, err)
		}
	})

	t.Run("numeric value from int64", func(t *testing.T) {
		if got, err := numericValue(int64(42)); err != nil || got != 42 {
			t.Fatalf("numericValue(int64(42)) = %v, %v, want 42, nil", got, err)
		}
	})

	t.Run("numeric value from uint64", func(t *testing.T) {
		if got, err := numericValue(uint64(42)); err != nil || got != 42 {
			t.Fatalf("numericValue(uint64(42)) = %v, %v, want 42, nil", got, err)
		}
	})

	t.Run("numeric value from float64", func(t *testing.T) {
		if _, err := numericValue(42.5); !errors.Is(err, ErrInvalidValueType) {
			t.Fatalf("numericValue(42.5) error = %v, want ErrInvalidValueType", err)
		}
	})

	t.Run("numeric value from string", func(t *testing.T) {
		if _, err := numericValue("42"); !errors.Is(err, ErrInvalidValueType) {
			t.Fatalf("numericValue(\"42\") error = %v, want ErrInvalidValueType", err)
		}
	})
}

// TestFileDriverReadBranches 测试 FileDriver.Read 的分支
func TestFileDriverReadBranches(t *testing.T) {
	t.Run("read with invalid ID", func(t *testing.T) {
		cfg := testConfig(t)
		driver := newTestFileDriver(t, cfg)
		if _, err := driver.Read(context.Background(), "invalid id"); !errors.Is(err, ErrInvalidSessionID) {
			t.Fatalf("Read error = %v, want ErrInvalidSessionID", err)
		}
	})

	t.Run("read non-existent file", func(t *testing.T) {
		cfg := testConfig(t)
		driver := newTestFileDriver(t, cfg)
		id := newSessionID()
		if _, err := driver.Read(context.Background(), id); !errors.Is(err, ErrSessionNotFound) {
			t.Fatalf("Read error = %v, want ErrSessionNotFound", err)
		}
	})

	t.Run("read expired session", func(t *testing.T) {
		cfg := testConfig(t)
		driver := newTestFileDriver(t, cfg)
		id := newSessionID()
		// 使用实际当前时间的过去时间，因为 Read 内部使用 time.Now() 检查过期
		expiredAt := time.Now().Add(-time.Minute)
		payload := Payload{ID: id, Values: map[string]any{"key": "value"}, LastActivity: time.Now(), ExpiresAt: &expiredAt}
		// Write 现在拒绝过去时间，直接写入文件绕过验证
		data, err := driver.encode(context.Background(), payload)
		if err != nil {
			t.Fatalf("encode error = %v", err)
		}
		if err := os.WriteFile(driver.pathForID(id), data, 0o600); err != nil {
			t.Fatalf("write file error = %v", err)
		}
		if _, err := driver.Read(context.Background(), id); !errors.Is(err, ErrSessionExpired) {
			t.Fatalf("Read error = %v, want ErrSessionExpired", err)
		}
	})
}

// mockLock 用于测试
type mockLock struct {
	released int
}

func (l *mockLock) Release(context.Context) error {
	l.released++
	return nil
}

// mockEncryptor 用于测试
type mockEncryptor struct{}

func (e *mockEncryptor) Encrypt(_ context.Context, data []byte) ([]byte, error) {
	return append([]byte("encrypted:"), data...), nil
}

func (e *mockEncryptor) Decrypt(_ context.Context, data []byte) ([]byte, error) {
	if len(data) < 10 {
		return nil, errors.New("invalid data")
	}
	return data[10:], nil
}
