package session

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/prismgo/framework/container"
)

func TestFileDriverPersistsPayloadAndRecovers(t *testing.T) {
	driver := newTestFileDriver(t, testConfig(t))
	id := newSessionID()
	payload := Payload{
		ID:           id,
		Values:       map[string]any{"name": "alice"},
		OldFlash:     []string{"old"},
		NewFlash:     []string{"new"},
		CreatedAt:    testNow(),
		LastActivity: testNow(),
	}
	expiresAt := testNow().Add(time.Hour)

	if err := driver.Write(context.Background(), id, payload, &expiresAt); err != nil {
		t.Fatalf("Write error = %v", err)
	}
	restored, err := driver.Read(context.Background(), id)
	if err != nil {
		t.Fatalf("Read error = %v", err)
	}
	if restored.ID != id || restored.Values["name"] != "alice" {
		t.Fatalf("restored = %#v", restored)
	}
	if len(restored.OldFlash) != 1 || restored.OldFlash[0] != "old" ||
		len(restored.NewFlash) != 1 || restored.NewFlash[0] != "new" {
		t.Fatalf("flash metadata = %#v %#v", restored.OldFlash, restored.NewFlash)
	}

	if err := driver.Destroy(context.Background(), id); err != nil {
		t.Fatalf("Destroy error = %v", err)
	}
	if _, err := driver.Read(context.Background(), id); !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("Read destroyed error = %v", err)
	}
}

func TestFileDriverExpiresGCAndMalformedRecovery(t *testing.T) {
	cfg := testConfig(t)
	driver := newTestFileDriver(t, cfg)

	// 模拟已存在的过期 session 文件（绕过 Write 方法，直接写入文件）
	// 这符合 Laravel 的语义：Write 总是写入未来时间，过期检查在 Read 时进行
	expiredID := newSessionID()
	expiredAt := time.Now().Add(-time.Minute)
	expiredPayload := Payload{
		ID:           expiredID,
		Values:       map[string]any{"secret": "hidden"},
		CreatedAt:    testNow().Add(-time.Hour),
		LastActivity: testNow().Add(-time.Hour),
		ExpiresAt:    &expiredAt,
	}
	expiredData, err := driver.encode(context.Background(), expiredPayload)
	if err != nil {
		t.Fatalf("encode expired payload error = %v", err)
	}
	if err := os.WriteFile(driver.pathForID(expiredID), expiredData, 0o600); err != nil {
		t.Fatalf("write expired fixture error = %v", err)
	}

	// 验证 Read 能检测到过期并返回 ErrSessionExpired
	if _, err := driver.Read(context.Background(), expiredID); !errors.Is(err, ErrSessionExpired) {
		t.Fatalf("Read expired error = %v", err)
	}

	// 测试损坏文件的恢复
	malformedID := newSessionID()
	if err := os.WriteFile(driver.pathForID(malformedID), []byte("{secret payload"), 0o600); err != nil {
		t.Fatalf("write malformed fixture error = %v", err)
	}
	if _, err := driver.Read(context.Background(), malformedID); !errors.Is(err, ErrPayloadDeserialize) {
		t.Fatalf("Read malformed error = %v", err)
	}

	// 验证 GC 能清理过期文件
	if err := driver.GC(context.Background(), testNow()); err != nil {
		t.Fatalf("GC error = %v", err)
	}
	if _, err := os.Stat(driver.pathForID(expiredID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expired file stat error = %v", err)
	}
}

func TestFileDriverRejectsInvalidIDAndBadDirectory(t *testing.T) {
	cfg := testConfig(t)
	driver := newTestFileDriver(t, cfg)
	if _, err := driver.Read(context.Background(), "bad id"); !errors.Is(err, ErrInvalidSessionID) {
		t.Fatalf("invalid read error = %v", err)
	}
	if err := driver.Write(context.Background(), "bad id", Payload{}, nil); !errors.Is(err, ErrInvalidSessionID) {
		t.Fatalf("invalid write error = %v", err)
	}

	filePath := filepath.Join(t.TempDir(), "not-directory")
	if err := os.WriteFile(filePath, []byte("x"), 0o600); err != nil {
		t.Fatalf("fixture write error = %v", err)
	}
	cfg.Files = filePath
	if _, err := NewFileDriver(cfg); err == nil {
		t.Fatalf("NewFileDriver should reject file path")
	}
}

func TestFileDriverResolvesFilesFromApplicationStorage(t *testing.T) {
	base := t.TempDir()
	c := container.NewContainer()
	if err := c.Instance("path.storage", filepath.Join(base, "storage")); err != nil {
		t.Fatalf("bind storage path: %v", err)
	}
	container.SetProvider(func() *container.Container { return c })
	t.Cleanup(func() { container.SetProvider(nil) })

	relativeCfg := DefaultConfig()
	relativeCfg.Files = "storage/framework/sessions"
	relativeDriver := newTestFileDriver(t, relativeCfg)
	if want := filepath.Join(base, "storage", "framework", "sessions"); relativeDriver.root != want {
		t.Fatalf("relative root = %q, want %q", relativeDriver.root, want)
	}

	absoluteRoot := filepath.Join(t.TempDir(), "sessions")
	absoluteCfg := DefaultConfig()
	absoluteCfg.Files = absoluteRoot
	absoluteDriver := newTestFileDriver(t, absoluteCfg)
	if absoluteDriver.root != absoluteRoot {
		t.Fatalf("absolute root = %q, want %q", absoluteDriver.root, absoluteRoot)
	}
}

func TestFileDriverMalformedAndBranchCoverage(t *testing.T) {
	driver := newTestFileDriver(t, testConfig(t))
	id := newSessionID()
	raw, err := driver.encode(context.Background(), Payload{ID: newSessionID(), Values: map[string]any{"a": "b"}})
	if err != nil {
		t.Fatalf("Marshal fixture error = %v", err)
	}
	if err := os.WriteFile(driver.pathForID(id), raw, 0o600); err != nil {
		t.Fatalf("write mismatch fixture error = %v", err)
	}
	if _, err := driver.Read(context.Background(), id); !errors.Is(err, ErrPayloadMalformed) {
		t.Fatalf("mismatch read error = %v", err)
	}
	if err := driver.Destroy(context.Background(), "bad id"); !errors.Is(err, ErrInvalidSessionID) {
		t.Fatalf("invalid destroy error = %v", err)
	}
	if _, err := driver.Lock(context.Background(), "bad id", time.Second, time.Second); !errors.Is(err, ErrInvalidSessionID) {
		t.Fatalf("invalid lock error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(driver.root, "not-a-session"), []byte("skip"), 0o600); err != nil {
		t.Fatalf("write skipped fixture error = %v", err)
	}
	if err := driver.GC(context.Background(), time.Now()); err != nil {
		t.Fatalf("GC skipped fixture error = %v", err)
	}
}

func newTestFileDriver(t *testing.T, cfg Config) *FileDriver {
	t.Helper()
	driver, err := NewFileDriver(cfg)
	if err != nil {
		t.Fatalf("NewFileDriver error = %v", err)
	}
	return driver
}
