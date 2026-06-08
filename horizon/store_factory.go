package horizon

import (
	"context"
	"fmt"
	"strings"
	"sync"

	redisfacade "github.com/prismgo/framework/redis"
	"github.com/redis/go-redis/v9"
)

// DefaultStoreFactory 根据 Horizon Config 创建运行时 Store。
//
// 设计思路：Store 构造集中在 manager 依赖边界后面，命令层只调用 ResolveStore，避免命令包直接读取全局配置
// 或创建 Redis client。
type DefaultStoreFactory struct {
	// mu 保护 memory store 复用表，避免并发命令重复创建不同实例。
	mu sync.Mutex
	// memory 按 prefix 缓存本地开发/测试用 Store；不用于生产跨进程状态。
	memory map[string]*MemoryStore
}

// ResolveStore 创建或复用 Horizon Store；Redis 不可用时不回退到 memory。
//
// 参数说明：ctx 用于 Redis Ping；cfg 是已解析的 Horizon Config。未知 store 返回配置错误，避免静默 no-op。
func (f *DefaultStoreFactory) ResolveStore(ctx context.Context, cfg Config) (Store, error) {
	store := strings.TrimSpace(strings.ToLower(cfg.Store))
	if store == "" {
		store = "redis"
	}
	switch store {
	case "memory":
		return f.memoryStore(cfg), nil
	case "redis":
		client, err := horizonRedisClient(cfg.Connection)
		if err != nil {
			return nil, err
		}
		redisStore := NewRedisStoreFromClient(client, StoreOptions{
			Prefix:       cfg.Prefix,
			HeartbeatTTL: cfg.HeartbeatTTL,
			Encoding:     cfg.Encoding,
		})
		if err := redisStore.client.Ping(ctx).Err(); err != nil {
			return nil, err
		}
		return redisStore, nil
	default:
		return nil, fmt.Errorf("horizon: unknown store %q", cfg.Store)
	}
}

// horizonRedisClient 解析 Horizon Store 使用的 Redis client。
//
// 设计思路：Horizon 生产 Redis Store 只复用 prismgo/redis 的共享连接，client 生命周期由
// prismgo/redis.Manager.Close(ctx) 统一释放；解析失败时直接返回错误，不自行创建游离 client。
func horizonRedisClient(connection string) (*redis.Client, error) {
	client, err := redisfacade.Client(connection)
	if err == nil {
		typed, ok := client.(*redis.Client)
		if !ok {
			return nil, fmt.Errorf("horizon: redis connection %q is %T, want *redis.Client", connection, client)
		}
		return typed, nil
	}
	return nil, err
}

// memoryStore 按 prefix 复用 memory store。
//
// 需求背景：memory store 仅适合本地/测试，同一进程内复用可以让连续命令看到相同控制状态。
func (f *DefaultStoreFactory) memoryStore(cfg Config) Store {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.memory == nil {
		f.memory = make(map[string]*MemoryStore)
	}
	key := strings.Trim(strings.TrimSpace(cfg.Prefix), ":")
	if key == "" {
		key = "prismgo_horizon"
	}
	store, ok := f.memory[key]
	if !ok {
		store = NewMemoryStore(StoreOptions{Prefix: key, HeartbeatTTL: cfg.HeartbeatTTL, Encoding: cfg.Encoding})
		f.memory[key] = store
	}
	return store
}

var defaultStoreFactory = &DefaultStoreFactory{
	memory: make(map[string]*MemoryStore),
}

// defaultManager 为应用注册的运行时命令创建默认 Horizon manager。
func defaultManager() (*Manager, error) {
	manager := Resolve()
	if manager == nil {
		return nil, ErrStoreNotConfigured
	}
	return manager, nil
}
