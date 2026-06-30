package filesystem

import (
	"context"
	"net/http/httptest"
	"testing"
)

// TestOSSDirectoryExistsWithMarker verifies DirectoryExists returns true when directory marker exists.
func TestOSSDirectoryExistsWithMarker(t *testing.T) {
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
	defer func() {
		if err := driver.Close(); err != nil {
			t.Errorf("close driver failed: %v", err)
		}
	}()

	ctx := context.Background()

	// Create directory marker
	if err := driver.MakeDirectory(ctx, "testdir"); err != nil {
		t.Fatalf("MakeDirectory failed: %v", err)
	}

	// DirectoryExists should return true
	exists, err := driver.DirectoryExists(ctx, "testdir")
	if err != nil {
		t.Fatalf("DirectoryExists failed: %v", err)
	}
	if !exists {
		t.Error("expected directory to exist")
	}
}

// TestOSSDirectoryExistsWithFiles verifies DirectoryExists returns true when directory has files.
func TestOSSDirectoryExistsWithFiles(t *testing.T) {
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
	defer func() {
		if err := driver.Close(); err != nil {
			t.Errorf("close driver failed: %v", err)
		}
	}()

	ctx := context.Background()

	// Write a file without creating directory marker
	if err := driver.Write(ctx, "testdir/file.txt", nil, PutOptions{
		Visibility: VisibilityPrivate,
	}); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// DirectoryExists should return true because files exist
	exists, err := driver.DirectoryExists(ctx, "testdir")
	if err != nil {
		t.Fatalf("DirectoryExists failed: %v", err)
	}
	if !exists {
		t.Error("expected directory to exist")
	}
}

// TestOSSDirectoryExistsNotFound verifies DirectoryExists returns false for non-existent directory.
func TestOSSDirectoryExistsNotFound(t *testing.T) {
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
	defer func() {
		if err := driver.Close(); err != nil {
			t.Errorf("close driver failed: %v", err)
		}
	}()

	ctx := context.Background()

	// DirectoryExists should return false for non-existent directory
	exists, err := driver.DirectoryExists(ctx, "nonexistent")
	if err != nil {
		t.Fatalf("DirectoryExists failed: %v", err)
	}
	if exists {
		t.Error("expected directory to not exist")
	}
}

// TestOSSProvidesTemporaryURLs verifies OSS driver supports temporary URLs.
func TestOSSProvidesTemporaryURLs(t *testing.T) {
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
	defer func() {
		if err := driver.Close(); err != nil {
			t.Errorf("close driver failed: %v", err)
		}
	}()

	if !driver.ProvidesTemporaryURLs() {
		t.Error("expected OSS driver to support temporary URLs")
	}
}

// TestOSSProvidesTemporaryUploadURLs verifies OSS driver supports temporary upload URLs.
func TestOSSProvidesTemporaryUploadURLs(t *testing.T) {
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
	defer func() {
		if err := driver.Close(); err != nil {
			t.Errorf("close driver failed: %v", err)
		}
	}()

	if !driver.ProvidesTemporaryUploadURLs() {
		t.Error("expected OSS driver to support temporary upload URLs")
	}
}
