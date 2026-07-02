package session

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestFileDriverWriteWithNilExpiresAtReturnsError 验证当 expiresAt 为 nil 时，
// FileDriver.Write 应该返回 ErrInvalidExpiresAt 错误。
func TestFileDriverWriteWithNilExpiresAtReturnsError(t *testing.T) {
	cfg := testConfig(t)
	cfg.Lifetime = 2 * time.Hour

	driver, err := NewFileDriver(cfg)
	if err != nil {
		t.Fatalf("NewFileDriver error = %v", err)
	}

	id := newSessionID()
	payload := Payload{
		ID:           id,
		Values:       map[string]any{"user": "alice"},
		CreatedAt:    testNow(),
		LastActivity: testNow(),
	}

	// 使用 nil 作为 expiresAt 应该返回错误
	err = driver.Write(context.Background(), id, payload, nil)
	if !errors.Is(err, ErrInvalidExpiresAt) {
		t.Fatalf("Write error = %v, want ErrInvalidExpiresAt", err)
	}
}

// TestFileDriverWriteWithPastExpiresAtReturnsError 验证当 expiresAt 是过去时间时，
// FileDriver.Write 应该返回 ErrInvalidExpiresAt 错误。
func TestFileDriverWriteWithPastExpiresAtReturnsError(t *testing.T) {
	cfg := testConfig(t)
	cfg.Lifetime = 2 * time.Hour

	driver, err := NewFileDriver(cfg)
	if err != nil {
		t.Fatalf("NewFileDriver error = %v", err)
	}

	id := newSessionID()
	payload := Payload{
		ID:           id,
		Values:       map[string]any{"user": "bob"},
		CreatedAt:    testNow(),
		LastActivity: testNow(),
	}

	// 使用过去的时间作为 expiresAt 应该返回错误
	pastTime := time.Now().Add(-1 * time.Hour)
	err = driver.Write(context.Background(), id, payload, &pastTime)
	if !errors.Is(err, ErrInvalidExpiresAt) {
		t.Fatalf("Write error = %v, want ErrInvalidExpiresAt", err)
	}
}

// TestFileDriverWriteWithFutureExpiresAtUsesProvidedTime 验证当 expiresAt 是未来时间时，
// FileDriver.Write 应该使用提供的 expiresAt。
func TestFileDriverWriteWithFutureExpiresAtUsesProvidedTime(t *testing.T) {
	cfg := testConfig(t)
	cfg.Lifetime = 2 * time.Hour

	driver, err := NewFileDriver(cfg)
	if err != nil {
		t.Fatalf("NewFileDriver error = %v", err)
	}

	id := newSessionID()
	payload := Payload{
		ID:           id,
		Values:       map[string]any{"user": "charlie"},
		CreatedAt:    testNow(),
		LastActivity: testNow(),
	}

	// 使用未来 30 分钟的时间作为 expiresAt
	futureTime := time.Now().Add(30 * time.Minute)

	if err := driver.Write(context.Background(), id, payload, &futureTime); err != nil {
		t.Fatalf("Write error = %v", err)
	}

	// 验证 session 可以被读取
	restored, err := driver.Read(context.Background(), id)
	if err != nil {
		t.Fatalf("Read error = %v", err)
	}

	if restored.ID != id {
		t.Errorf("restored.ID = %q, want %q", restored.ID, id)
	}

	// 验证 ExpiresAt 应该接近提供的 futureTime（30分钟）
	if restored.ExpiresAt == nil {
		t.Fatalf("restored.ExpiresAt is nil, want non-nil")
	}
	diff := futureTime.Sub(*restored.ExpiresAt)
	if diff < -time.Second || diff > time.Second {
		t.Errorf("restored.ExpiresAt diff from provided = %v, want within 1 second", diff)
	}
}
