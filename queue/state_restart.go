package queue

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/prismgo/framework/cache"
	queuecontract "github.com/prismgo/framework/contracts/queue"
)

// MemoryRestartStore 是 queue:restart 的进程内 state repository。
//
// 设计说明：restart 信号不属于 transport driver。Redis/RabbitMQ 可以作为底层存储实现，
// 但 Worker 只依赖这个独立 store 契约；默认 memory store 覆盖 sync 和测试场景。
type MemoryRestartStore struct {
	value atomicTime
}

func NewMemoryRestartStore() *MemoryRestartStore {
	return &MemoryRestartStore{}
}

func (s *MemoryRestartStore) RequestRestart(_ context.Context, at time.Time) error {
	if s == nil {
		return nil
	}
	s.value.Store(at)
	return nil
}

func (s *MemoryRestartStore) RestartRequestedAt(context.Context) (time.Time, error) {
	if s == nil {
		return time.Time{}, nil
	}
	return s.value.Load(), nil
}

// CacheRestartStore 使用 cache repository 保存跨进程 queue:restart 信号。
//
// 需求背景：restart 信号是 queue runtime state，不属于 Redis/RabbitMQ transport。多进程
// worker 需要共享时间戳时，应通过独立 cache store 写入，而不是回到具体 driver 的 restart queue。
type CacheRestartStore struct {
	store string
	key   string
}

// NewCacheRestartStore 创建 cache-backed restart store。
//
// 参数 store 是 cache store 名称，空值表示 cache 默认 store；key 是 restart 时间戳 key，
// 空值使用 Prismgo 队列默认 key。
func NewCacheRestartStore(store, key string) *CacheRestartStore {
	if key == "" {
		key = "prismgo:queue:restart"
	}
	return &CacheRestartStore{store: store, key: key}
}

func (s *CacheRestartStore) RequestRestart(ctx context.Context, at time.Time) error {
	if s == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return cache.Store(s.store).Forever(ctx, s.key, at.UnixNano())
}

func (s *CacheRestartStore) RestartRequestedAt(ctx context.Context) (time.Time, error) {
	if s == nil {
		return time.Time{}, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	value, err := cache.Store(s.store).Get(ctx, s.key, int64(0))
	if err != nil {
		if errors.Is(err, cache.ErrCacheMiss) {
			return time.Time{}, nil
		}
		return time.Time{}, err
	}
	nano, ok := restartNano(value)
	if !ok || nano <= 0 {
		return time.Time{}, nil
	}
	return time.Unix(0, nano), nil
}

func restartStoreFromConfig(cfg RestartConfig) queuecontract.RestartStore {
	if cfg.Cache == "" {
		return NewMemoryRestartStore()
	}
	return NewCacheRestartStore(cfg.Cache, cfg.Key)
}

func restartNano(value any) (int64, bool) {
	switch typed := value.(type) {
	case int64:
		return typed, true
	case int:
		return int64(typed), true
	case float64:
		return int64(typed), true
	case string:
		var parsed int64
		if _, err := fmt.Sscan(typed, &parsed); err != nil {
			return 0, false
		}
		return parsed, true
	default:
		return 0, false
	}
}

type atomicTime struct {
	mu sync.RWMutex
	t  time.Time
}

func (a *atomicTime) Store(t time.Time) {
	a.mu.Lock()
	a.t = t
	a.mu.Unlock()
}

func (a *atomicTime) Load() time.Time {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.t
}
