package session

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// TestRedisDriverWriteWithNilExpiresAtReturnsError 验证当 expiresAt 为 nil 时，
// RedisDriver.Write 应该返回 ErrInvalidExpiresAt 错误。
func TestRedisDriverWriteWithNilExpiresAtReturnsError(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	cfg := testConfig(t)
	cfg.Lifetime = 2 * time.Hour
	cfg.Redis.Prefix = "test_session"

	driver, err := NewRedisDriverFromClient(client, cfg)
	if err != nil {
		t.Fatalf("NewRedisDriverFromClient error = %v", err)
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

// TestRedisDriverWriteWithPastExpiresAtReturnsError 验证当 expiresAt 是过去时间时，
// RedisDriver.Write 应该返回 ErrInvalidExpiresAt 错误。
func TestRedisDriverWriteWithPastExpiresAtReturnsError(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	cfg := testConfig(t)
	cfg.Lifetime = 2 * time.Hour
	cfg.Redis.Prefix = "test_session"

	driver, err := NewRedisDriverFromClient(client, cfg)
	if err != nil {
		t.Fatalf("NewRedisDriverFromClient error = %v", err)
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

// TestRedisDriverWriteWithFutureExpiresAtUsesProvidedTTL 验证当 expiresAt 是未来时间时，
// RedisDriver.Write 应该使用提供的 expiresAt 计算 TTL。
func TestRedisDriverWriteWithFutureExpiresAtUsesProvidedTTL(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	cfg := testConfig(t)
	cfg.Lifetime = 2 * time.Hour
	cfg.Redis.Prefix = "test_session"

	driver, err := NewRedisDriverFromClient(client, cfg)
	if err != nil {
		t.Fatalf("NewRedisDriverFromClient error = %v", err)
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

	// 验证 Redis 中的 TTL 应该接近 30 分钟
	key := "test_session:sessions:" + id
	ttl := server.TTL(key)
	if ttl < 29*time.Minute || ttl > 31*time.Minute {
		t.Errorf("Redis TTL = %v, want approximately 30 minutes", ttl)
	}
}
