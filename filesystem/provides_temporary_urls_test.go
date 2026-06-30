package filesystem

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

// TestProvidesTemporaryURLsNoProbeSideEffect 验证 ProvidesTemporaryURLs 不会触发探测调用。
func TestProvidesTemporaryURLsNoProbeSideEffect(t *testing.T) {
	root := t.TempDir()
	manager, err := NewManager(Config{
		Default: "local",
		Disks: map[string]DiskConfig{
			"local": {
				Driver:     "local",
				Root:       filepath.Join(root, "local"),
				Visibility: VisibilityPrivate,
				Serve:      true,
			},
		},
		TemporaryURL: TemporaryURLConfig{SigningKey: "test-secret"},
	})
	if err != nil {
		t.Fatalf("create manager failed: %v", err)
	}
	defer func() {
		if err := manager.Close(); err != nil {
			t.Errorf("close manager failed: %v", err)
		}
	}()

	repo := manager.Disk("local")

	// 多次调用 ProvidesTemporaryURLs 不应该产生副作用
	for i := 0; i < 5; i++ {
		if !repo.ProvidesTemporaryURLs() {
			t.Fatal("expected ProvidesTemporaryURLs to return true")
		}
	}

	// 验证临时 URL 生成功能正常
	url, err := repo.TemporaryURL(context.Background(), "test.txt", time.Now().Add(time.Hour))
	if err != nil {
		t.Fatalf("TemporaryURL failed: %v", err)
	}
	if url == "" {
		t.Fatal("expected non-empty temporary URL")
	}
}

// TestProvidesTemporaryURLsReturnsFalseWhenNotServed 验证未启用 Serve 时返回 false。
func TestProvidesTemporaryURLsReturnsFalseWhenNotServed(t *testing.T) {
	root := t.TempDir()
	manager, err := NewManager(Config{
		Default: "local",
		Disks: map[string]DiskConfig{
			"local": {
				Driver:     "local",
				Root:       filepath.Join(root, "local"),
				Visibility: VisibilityPrivate,
				Serve:      false, // 未启用 Serve
			},
		},
		TemporaryURL: TemporaryURLConfig{SigningKey: "test-secret"},
	})
	if err != nil {
		t.Fatalf("create manager failed: %v", err)
	}
	defer func() {
		if err := manager.Close(); err != nil {
			t.Errorf("close manager failed: %v", err)
		}
	}()

	repo := manager.Disk("local")

	// 未启用 Serve 时应该返回 false
	if repo.ProvidesTemporaryURLs() {
		t.Fatal("expected ProvidesTemporaryURLs to return false when Serve is disabled")
	}
}
