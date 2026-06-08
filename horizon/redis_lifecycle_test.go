package horizon

import (
	"context"
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"
	rediscontract "github.com/prismgo/framework/contracts/redis"
	prismredis "github.com/prismgo/framework/redis"
	goredis "github.com/redis/go-redis/v9"
)

func TestStoreFactoryRedisUsesSharedManagerClient(t *testing.T) {
	ctx := context.Background()
	server := miniredis.RunT(t)
	redisManager := useRedisLifecycleManager(t, server.Addr())

	store, err := (&DefaultStoreFactory{}).ResolveStore(ctx, Config{
		Store:        "redis",
		Connection:   "default",
		Prefix:       "horizon_lifecycle",
		HeartbeatTTL: time.Minute,
	})
	if err != nil {
		t.Fatalf("ResolveStore error = %v", err)
	}
	if _, ok := store.(*RedisStore); !ok {
		t.Fatalf("store type = %T, want *RedisStore", store)
	}
	shared, err := prismredis.Client("default")
	if err != nil {
		t.Fatalf("redis client: %v", err)
	}
	if err := shared.Ping(ctx).Err(); err != nil {
		t.Fatalf("shared redis client should be active after store resolve: %v", err)
	}
	if err := redisManager.Close(ctx); err != nil {
		t.Fatalf("redis manager close: %v", err)
	}
	if err := shared.Ping(ctx).Err(); err == nil {
		t.Fatal("shared redis client should close with redis manager")
	}
}

func TestNewRedisStoreUsesSharedManagerClient(t *testing.T) {
	server := miniredis.RunT(t)
	useRedisLifecycleManager(t, server.Addr())

	store, err := NewRedisStore(RedisOptions{Connection: "default"}, StoreOptions{Prefix: "horizon_direct"})
	if err != nil {
		t.Fatalf("NewRedisStore error = %v", err)
	}
	if store.client == nil {
		t.Fatal("expected shared redis client")
	}
	if got := store.client.Options().Addr; got != server.Addr() {
		t.Fatalf("redis addr = %q, want %q", got, server.Addr())
	}
}

func TestNewRedisStoreReturnsRedisResolutionError(t *testing.T) {
	useHorizonTestContainer(t)

	store, err := NewRedisStore(RedisOptions{Connection: "missing"}, StoreOptions{Prefix: "horizon_direct"})
	if err == nil {
		t.Fatalf("NewRedisStore error = nil, store = %#v", store)
	}
	if store != nil {
		t.Fatalf("store = %#v, want nil on resolution error", store)
	}
}

func TestNewRedisStoreReturnsTypeMismatchError(t *testing.T) {
	registry := useHorizonTestContainer(t)
	if err := registry.Instance("redis", redisClusterFactory{}); err != nil {
		t.Fatalf("register redis factory: %v", err)
	}

	store, err := NewRedisStore(RedisOptions{Connection: "cluster"}, StoreOptions{Prefix: "horizon_direct"})
	if err == nil {
		t.Fatalf("NewRedisStore error = nil, store = %#v", store)
	}
	if store != nil {
		t.Fatalf("store = %#v, want nil on type mismatch", store)
	}
}

func useRedisLifecycleManager(t *testing.T, addr string) *prismredis.Manager {
	t.Helper()
	registry := useHorizonTestContainer(t)
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

type redisClusterFactory struct{}

func (redisClusterFactory) Connection(name ...string) (rediscontract.Connection, error) {
	return redisClusterConnection{name: "cluster"}, nil
}

func (redisClusterFactory) DefaultConnection() (rediscontract.Connection, error) {
	return redisClusterConnection{name: "cluster"}, nil
}

func (redisClusterFactory) Connections() map[string]rediscontract.Connection {
	return map[string]rediscontract.Connection{"cluster": redisClusterConnection{name: "cluster"}}
}

func (redisClusterFactory) Purge(name ...string) error { return nil }

func (redisClusterFactory) EnableEvents() {}

func (redisClusterFactory) DisableEvents() {}

func (redisClusterFactory) Close(context.Context) error { return nil }

type redisClusterConnection struct {
	name string
}

func (c redisClusterConnection) Name() string { return c.name }

func (redisClusterConnection) Client() goredis.UniversalClient {
	return goredis.NewClusterClient(&goredis.ClusterOptions{Addrs: []string{"127.0.0.1:6379"}})
}

func (redisClusterConnection) Listen(func(context.Context, rediscontract.CommandExecuted)) {}

func (redisClusterConnection) ListenForFailures(func(context.Context, rediscontract.CommandFailed)) {}
