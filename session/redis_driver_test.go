package session

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	configpkg "github.com/prismgo/framework/config"
	"github.com/redis/go-redis/v9"
)

func TestRedisDriverPersistsPayloadThroughInjectedClient(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	cfg := testConfig(t)
	cfg.Redis.Prefix = " test_session: "
	driver, err := NewRedisDriverFromClient(client, cfg)
	if err != nil {
		t.Fatalf("NewRedisDriverFromClient error = %v", err)
	}

	id := newSessionID()
	payload := Payload{
		ID:           id,
		Values:       map[string]any{"name": "alice"},
		OldFlash:     []string{"old"},
		NewFlash:     []string{"new"},
		CreatedAt:    testNow(),
		LastActivity: testNow(),
	}
	expiresAt := time.Now().Add(time.Hour)

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
	if !server.Exists("test_session:sessions:" + id) {
		t.Fatalf("redis payload key was not written with normalized prefix")
	}
}

func TestRedisDriverRefreshesTTLOnWriteWithoutTouchingTTLOnRead(t *testing.T) {
	server, driver := newTestRedisDriver(t, testConfig(t))
	id := newSessionID()
	payload := Payload{ID: id, Values: map[string]any{"name": "alice"}}
	key := "prismgo_session:sessions:" + id

	firstExpires := time.Now().Add(3 * time.Second)
	if err := driver.Write(context.Background(), id, payload, &firstExpires); err != nil {
		t.Fatalf("first Write error = %v", err)
	}
	server.FastForward(2 * time.Second)
	ttlAfterReadBefore, err := readSessionAndTTL(server, driver, id, key)
	if err != nil {
		t.Fatalf("Read error = %v", err)
	}
	if ttlAfterReadBefore > 1500*time.Millisecond {
		t.Fatalf("read refreshed TTL unexpectedly: %v", ttlAfterReadBefore)
	}

	secondExpires := time.Now().Add(6 * time.Second)
	if err := driver.Write(context.Background(), id, payload, &secondExpires); err != nil {
		t.Fatalf("second Write error = %v", err)
	}
	if ttl := server.TTL(key); ttl < 4*time.Second {
		t.Fatalf("TTL was not refreshed on write: %v", ttl)
	}
}

func TestRedisDriverReadErrorsAreRecoverableAndDestroyIsIdempotent(t *testing.T) {
	server, driver := newTestRedisDriver(t, testConfig(t))
	id := newSessionID()

	if _, err := driver.Read(context.Background(), id); !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("missing read error = %v", err)
	}
	server.Set("prismgo_session:sessions:"+id, "{bad payload")
	if _, err := driver.Read(context.Background(), id); !errors.Is(err, ErrPayloadDeserialize) {
		t.Fatalf("malformed read error = %v", err)
	}
	if err := driver.Destroy(context.Background(), id); err != nil {
		t.Fatalf("Destroy malformed key error = %v", err)
	}
	if err := driver.Destroy(context.Background(), id); err != nil {
		t.Fatalf("Destroy missing key error = %v", err)
	}
	if err := driver.GC(context.Background(), time.Now()); err != nil {
		t.Fatalf("GC error = %v", err)
	}
}

func TestRedisDriverRejectsMismatchedExpiredAndInvalidPayloads(t *testing.T) {
	server, driver := newTestRedisDriver(t, testConfig(t))
	id := newSessionID()
	otherID := newSessionID()
	expiredAt := time.Now().Add(-time.Minute)

	if err := driver.Write(context.Background(), id, Payload{ID: otherID}, nil); !errors.Is(err, ErrInvalidSessionID) {
		t.Fatalf("mismatched write error = %v", err)
	}
	if _, err := driver.Read(context.Background(), "bad id"); !errors.Is(err, ErrInvalidSessionID) {
		t.Fatalf("invalid read error = %v", err)
	}
	raw, err := driver.encode(context.Background(), Payload{ID: id, Values: map[string]any{}, ExpiresAt: &expiredAt})
	if err != nil {
		t.Fatalf("encode expired fixture error = %v", err)
	}
	server.Set("prismgo_session:sessions:"+id, string(raw))
	if _, err := driver.Read(context.Background(), id); !errors.Is(err, ErrSessionExpired) {
		t.Fatalf("expired read error = %v", err)
	}
}

func TestRedisDriverEncryptionCloseAndErrorBranches(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	encryptor := prefixEncryptor{}
	cfg := testConfig(t)
	cfg.Encrypt = true
	cfg.Encryptor = encryptor

	driver, err := NewRedisDriverFromClient(client, cfg)
	if err != nil {
		t.Fatalf("NewRedisDriverFromClient encrypted error = %v", err)
	}
	id := newSessionID()
	expiresAt := time.Now().Add(time.Hour)
	if err := driver.Write(context.Background(), id, Payload{ID: id, Values: map[string]any{"secret": "value"}}, &expiresAt); err != nil {
		t.Fatalf("encrypted Write error = %v", err)
	}
	if _, err := driver.Read(context.Background(), id); err != nil {
		t.Fatalf("encrypted Read error = %v", err)
	}
	if err := driver.Close(); err != nil {
		t.Fatalf("injected Close error = %v", err)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("client Close error = %v", err)
	}
	if err := driver.Destroy(context.Background(), id); err == nil {
		t.Fatalf("Destroy on closed client should return error")
	}
	if _, err := NewRedisDriverFromClient(nil, cfg); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("nil client constructor error = %v", err)
	}
}

func TestRedisDriverMalformedEncryptedAndLockBranches(t *testing.T) {
	server, driver := newTestRedisDriver(t, Config{Encrypt: true, Encryptor: prefixEncryptor{}})
	id := newSessionID()
	server.Set("prismgo_session:sessions:"+id, "not encrypted")
	if _, err := driver.Read(context.Background(), id); !errors.Is(err, ErrDecryptionFailed) {
		t.Fatalf("encrypted malformed read error = %v", err)
	}
	if err := driver.Destroy(context.Background(), "bad id"); !errors.Is(err, ErrInvalidSessionID) {
		t.Fatalf("invalid destroy error = %v", err)
	}
	if _, err := driver.Lock(context.Background(), "bad id", time.Second, time.Second); !errors.Is(err, ErrInvalidSessionID) {
		t.Fatalf("invalid lock error = %v", err)
	}
	if err := (&redisLock{}).Release(context.Background()); !errors.Is(err, ErrLockNotHeld) {
		t.Fatalf("empty release error = %v", err)
	}
	past := time.Now().Add(-time.Second)
	if ttl := redisTTL(&past); ttl <= 0 {
		t.Fatalf("past redisTTL should remain positive, got %v", ttl)
	}
	if ttl := redisTTL(nil); ttl != 0 {
		t.Fatalf("nil redisTTL = %v", ttl)
	}
}

func TestRedisDriverLockWaitsTimesOutAndUsesTokenCheckedRelease(t *testing.T) {
	server, driver := newTestRedisDriver(t, testConfig(t))
	id := newSessionID()
	lock, err := driver.Lock(context.Background(), id, time.Second, 10*time.Millisecond)
	if err != nil {
		t.Fatalf("first Lock error = %v", err)
	}

	if _, err := driver.Lock(context.Background(), id, time.Second, 25*time.Millisecond); !errors.Is(err, ErrLockTimeout) {
		t.Fatalf("second Lock error = %v", err)
	}
	wrong := &redisLock{client: driver.client, key: "prismgo_session:locks:" + id, token: "wrong", held: true}
	if err := wrong.Release(context.Background()); !errors.Is(err, ErrLockNotHeld) {
		t.Fatalf("wrong token release error = %v", err)
	}
	if !server.Exists("prismgo_session:locks:" + id) {
		t.Fatalf("wrong token release deleted lock")
	}
	if err := lock.Release(context.Background()); err != nil {
		t.Fatalf("release lock error = %v", err)
	}
	if _, err := driver.Lock(context.Background(), id, time.Second, 10*time.Millisecond); err != nil {
		t.Fatalf("lock after release error = %v", err)
	}
}

func TestRedisDriverRegistrationAndConfigIsolationFromCache(t *testing.T) {
	registerSessionRedisConfigForTest(t)
	t.Setenv("SESSION_DRIVER", "redis")
	t.Setenv("SESSION_PREFIX", " : app_sessions: ")
	t.Setenv("CACHE_REDIS_CONNECTION", "cache")
	t.Setenv("CACHE_REDIS_PREFIX", "cache_prefix")
	t.Setenv("REDIS_MAIN_DB", "2")
	t.Setenv("REDIS_CACHE_DB", "7")

	repo, err := configpkg.NewFromFile(t.TempDir() + "/missing.env")
	if err != nil {
		t.Fatalf("NewFromFile error = %v", err)
	}
	cfg := ConfigFromRepository(repo)
	if cfg.Driver != "redis" || cfg.Redis.Connection != "default" {
		t.Fatalf("driver/connection = %#v", cfg.Redis)
	}
	if cfg.Redis.Prefix != "app_sessions" {
		t.Fatalf("prefix = %#v", cfg.Redis)
	}
	server := miniredis.RunT(t)
	useRedisLifecycleManager(t, server.Addr())
	if _, err := ResolveDriver("redis", cfg); err != nil {
		t.Fatalf("ResolveDriver redis error = %v", err)
	}
	if DefaultConfig().Driver != DefaultDriver {
		t.Fatalf("default driver changed")
	}
}

func newTestRedisDriver(t *testing.T, cfg Config) (*miniredis.Miniredis, *RedisDriver) {
	t.Helper()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	driver, err := NewRedisDriverFromClient(client, cfg)
	if err != nil {
		t.Fatalf("NewRedisDriverFromClient error = %v", err)
	}
	return server, driver
}

func readSessionAndTTL(server *miniredis.Miniredis, driver *RedisDriver, id string, key string) (time.Duration, error) {
	if _, err := driver.Read(context.Background(), id); err != nil {
		return 0, err
	}
	return server.TTL(key), nil
}

func registerSessionRedisConfigForTest(t *testing.T) {
	t.Helper()
	configpkg.Add("session", func() map[string]any {
		return map[string]any{
			"driver":     configpkg.Env("SESSION_DRIVER", "file"),
			"connection": configpkg.Env("SESSION_CONNECTION", "default"),
			"prefix":     configpkg.Env("SESSION_PREFIX", "prismgo_session"),
			"lifetime":   configpkg.Env("SESSION_LIFETIME", 120),
		}
	})
	configpkg.Add("redis", func() map[string]any {
		return map[string]any{
			"default": map[string]any{"host": "127.0.0.1", "port": "6379", "database": configpkg.Env("REDIS_MAIN_DB", 1)},
			"cache":   map[string]any{"host": "127.0.0.1", "port": "6379", "database": configpkg.Env("REDIS_CACHE_DB", 0)},
		}
	})
	configpkg.Add("cache", func() map[string]any {
		return map[string]any{
			"default": configpkg.Env("CACHE_STORE", "memory"),
			"prefix":  configpkg.Env("CACHE_REDIS_PREFIX", "cache"),
			"stores":  map[string]any{"redis": map[string]any{"connection": configpkg.Env("CACHE_REDIS_CONNECTION", "cache")}},
		}
	})
}
