package session

import (
	"context"
	"strings"
	"testing"

	miniredis "github.com/alicebob/miniredis/v2"
	"github.com/prismgo/framework/container"
	prismredis "github.com/prismgo/framework/redis"
)

func TestRedisDriverCloseLeavesSharedClientOpen(t *testing.T) {
	ctx := context.Background()
	server := miniredis.RunT(t)
	redisManager := useRedisLifecycleManager(t, server.Addr())

	driver, err := NewRedisDriver(Config{
		Redis: RedisConfig{
			Connection: "default",
			Prefix:     "session_lifecycle",
		},
	})
	if err != nil {
		t.Fatalf("NewRedisDriver error = %v", err)
	}
	shared, err := prismredis.Client("default")
	if err != nil {
		t.Fatalf("redis client: %v", err)
	}

	if err := driver.Close(); err != nil {
		t.Fatalf("driver close: %v", err)
	}
	if err := shared.Ping(ctx).Err(); err != nil {
		t.Fatalf("shared redis client should survive session close: %v", err)
	}
	if err := redisManager.Close(ctx); err != nil {
		t.Fatalf("redis manager close: %v", err)
	}
	if err := shared.Ping(ctx).Err(); err == nil {
		t.Fatal("shared redis client should close with redis manager")
	}
}

func TestRedisDriverRequiresSharedRedisFacade(t *testing.T) {
	_, err := NewRedisDriver(Config{
		Redis: RedisConfig{},
	})
	if err == nil {
		t.Fatal("expected redis facade resolution error")
	}
	if !strings.Contains(err.Error(), `container "redis"`) {
		t.Fatalf("error = %v", err)
	}
}

func useRedisLifecycleManager(t *testing.T, addr string) *prismredis.Manager {
	t.Helper()
	registry := container.NewContainer()
	container.SetProvider(func() *container.Container { return registry })
	t.Cleanup(func() { container.SetProvider(nil) })
	manager, err := prismredis.NewManager(prismredis.Config{
		DefaultName: "default",
		Connections: map[string]prismredis.ConnectionConfig{
			"default": {Name: "default", Addr: addr},
		},
	})
	if err != nil {
		t.Fatalf("redis NewManager error = %v", err)
	}
	if err := registry.Instance("redis", manager); err != nil {
		t.Fatalf("bind redis manager: %v", err)
	}
	return manager
}
