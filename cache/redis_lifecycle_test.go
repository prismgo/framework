package cache

import (
	"context"
	"strings"
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"
	"github.com/prismgo/framework/container"
	prismredis "github.com/prismgo/framework/redis"
)

func TestRedisStoreCloseLeavesSharedClientOpen(t *testing.T) {
	ctx := context.Background()
	server := miniredis.RunT(t)
	redisManager := useRedisLifecycleManager(t, server.Addr())

	cacheManager, err := NewManager(Config{
		Default: "redis",
		Stores: map[string]StoreConfig{
			"redis": {
				Driver: "redis",
				Redis:  RedisConfig{Connection: "default"},
			},
		},
	})
	if err != nil {
		t.Fatalf("NewManager error = %v", err)
	}
	repo := cacheManager.defaultRepository()
	if err := repo.Put(ctx, "lifecycle", "ok", time.Minute); err != nil {
		t.Fatalf("cache put: %v", err)
	}
	shared, err := prismredis.Client("default")
	if err != nil {
		t.Fatalf("redis client: %v", err)
	}

	if err := cacheManager.Close(); err != nil {
		t.Fatalf("cache close: %v", err)
	}
	if err := shared.Ping(ctx).Err(); err != nil {
		t.Fatalf("shared redis client should survive cache close: %v", err)
	}
	if err := redisManager.Close(ctx); err != nil {
		t.Fatalf("redis manager close: %v", err)
	}
	if err := shared.Ping(ctx).Err(); err == nil {
		t.Fatal("shared redis client should close with redis manager")
	}
}

func TestRedisStoreRequiresSharedRedisFacade(t *testing.T) {
	m, err := NewManager(Config{
		Default: "redis",
		Stores: map[string]StoreConfig{
			"redis": {
				Driver: "redis",
				Redis:  RedisConfig{},
			},
		},
	})
	if err != nil {
		t.Fatalf("NewManager error = %v", err)
	}

	err = m.defaultRepository().Put(context.Background(), "strict", "value", time.Minute)
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
	if err := registry.Instance("redis", manager, prismredis.ManagerCloseOption()); err != nil {
		t.Fatalf("bind redis manager: %v", err)
	}
	return manager
}
