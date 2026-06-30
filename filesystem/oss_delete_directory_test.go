package filesystem

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestOSSDeleteDirectoryReturnsMarkerDeleteError 验证 DeleteDirectory 在删除目录标记对象失败时返回错误。
func TestOSSDeleteDirectoryReturnsMarkerDeleteError(t *testing.T) {
	serverState := newFakeOSSServer()
	serverState.failDeleteMarker = true

	server := httptest.NewServer(serverState)
	defer server.Close()

	driver, err := newOSSDriver(OSSConfig{
		Bucket:    "test-bucket",
		Endpoint:  server.URL,
		AccessKey: "key",
		SecretKey: "secret",
	})
	if err != nil {
		t.Fatalf("create driver failed: %v", err)
	}

	ctx := context.Background()

	// 创建目录标记
	if err := driver.MakeDirectory(ctx, "testdir"); err != nil {
		t.Fatalf("MakeDirectory failed: %v", err)
	}

	// 写入一个文件
	if err := driver.Write(ctx, "testdir/file.txt", strings.NewReader("content"), PutOptions{
		Visibility: VisibilityPrivate,
	}); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// DeleteDirectory 应该返回错误，因为目录标记删除失败
	err = driver.DeleteDirectory(ctx, "testdir")
	if err == nil {
		t.Fatal("expected DeleteDirectory to return error when marker delete fails")
	}
	// OSS SDK 会将 403 响应包装为通用错误消息
	if !strings.Contains(err.Error(), "403") {
		t.Errorf("expected error to contain '403', got: %v", err)
	}
}

// TestOSSDeleteDirectoryIgnoresMissingMarker 验证 DeleteDirectory 在目录标记不存在时不报错。
func TestOSSDeleteDirectoryIgnoresMissingMarker(t *testing.T) {
	serverState := newFakeOSSServer()
	server := httptest.NewServer(serverState)
	defer server.Close()

	driver, err := newOSSDriver(OSSConfig{
		Bucket:    "test-bucket",
		Endpoint:  server.URL,
		AccessKey: "key",
		SecretKey: "secret",
	})
	if err != nil {
		t.Fatalf("create driver failed: %v", err)
	}

	ctx := context.Background()

	// 写入文件但不创建目录标记
	if err := driver.Write(ctx, "testdir/file.txt", strings.NewReader("content"), PutOptions{
		Visibility: VisibilityPrivate,
	}); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// DeleteDirectory 应该成功，即使目录标记不存在
	if err := driver.DeleteDirectory(ctx, "testdir"); err != nil {
		t.Fatalf("DeleteDirectory should succeed when marker doesn't exist: %v", err)
	}

	// 验证文件已被删除
	exists, err := driver.Exists(ctx, "testdir/file.txt")
	if err != nil {
		t.Fatalf("Exists failed: %v", err)
	}
	if exists {
		t.Error("file should be deleted")
	}
}
