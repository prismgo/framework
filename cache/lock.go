package cache

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"strings"
	"sync"
	"time"

	cachecontract "github.com/prismgo/framework/contracts/cache"
	"github.com/redis/go-redis/v9"
)

// DistributedLock 表示一个带 TTL 的缓存锁。
//
// 每次成功获取锁都会生成独立 token，Release 时会校验 token，避免误删其他调用方持有的锁。
type DistributedLock struct {
	provider   LockProvider
	key        string
	ttl        time.Duration
	retrySleep time.Duration

	mu    sync.Mutex
	token string
	held  bool
}

// newLock 创建锁对象，并补齐默认 TTL 与重试间隔。
func newLock(provider LockProvider, key string, ttl, retrySleep time.Duration) *DistributedLock {
	if ttl <= 0 {
		ttl = time.Second
	}
	if retrySleep <= 0 {
		retrySleep = 50 * time.Millisecond
	}
	return &DistributedLock{provider: provider, key: key, ttl: ttl, retrySleep: retrySleep}
}

// newLockWithOwner 创建使用调用方指定 owner token 的锁对象。
func newLockWithOwner(provider LockProvider, key string, ttl, retrySleep time.Duration, owner string) *DistributedLock {
	lock := newLock(provider, key, ttl, retrySleep)
	lock.token = owner
	return lock
}

// newRestoredLock 使用已有 owner token 恢复一个可释放的锁实例。
func newRestoredLock(provider LockProvider, key, owner string, ttl, retrySleep time.Duration) *DistributedLock {
	lock := newLock(provider, key, ttl, retrySleep)
	lock.token = owner
	lock.held = owner != ""
	return lock
}

// Get 尝试立即获取锁。
//
// 不传回调时，调用方需要在成功后显式调用 Release；传入回调时会在回调结束后自动释放。
func (l *DistributedLock) Get(ctx context.Context, fn ...func(context.Context) error) (bool, error) {
	l.mu.Lock()
	token := l.token
	if token == "" || l.held {
		token = randomToken()
	}
	l.mu.Unlock()
	ok, err := l.provider.Acquire(ctx, l.key, token, l.ttl)
	if err != nil || !ok {
		return ok, err
	}

	l.mu.Lock()
	l.token = token
	l.held = true
	l.mu.Unlock()

	if len(fn) == 0 || fn[0] == nil {
		return true, nil
	}
	defer func() { _ = l.Release(context.Background()) }()
	if err := fn[0](ctx); err != nil {
		return true, err
	}
	return true, nil
}

// Owner 返回当前锁实例持有的 owner token，可用于跨进程恢复锁后释放。
func (l *DistributedLock) Owner() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.token
}

// BetweenBlockedAttemptsSleepFor 设置 Block 等待锁时两次尝试之间的等待时间。
func (l *DistributedLock) BetweenBlockedAttemptsSleepFor(d time.Duration) cachecontract.Lock {
	if d > 0 {
		l.mu.Lock()
		l.retrySleep = d
		l.mu.Unlock()
	}
	return l
}

// Block 在 wait 时间内轮询等待锁，并在获取成功后执行回调。
func (l *DistributedLock) Block(ctx context.Context, wait time.Duration, fn func(context.Context) error) (bool, error) {
	deadline := time.Now().Add(wait)
	for {
		ok, err := l.Get(ctx)
		if err != nil || ok {
			if ok && fn != nil {
				defer func() { _ = l.Release(context.Background()) }()
				if callErr := fn(ctx); callErr != nil {
					return true, callErr
				}
			}
			return ok, err
		}
		l.mu.Lock()
		retrySleep := l.retrySleep
		l.mu.Unlock()
		if wait <= 0 || !time.Now().Add(retrySleep).Before(deadline) {
			return false, ErrLockTimeout
		}
		timer := time.NewTimer(retrySleep)
		select {
		case <-ctx.Done():
			timer.Stop()
			return false, ctx.Err()
		case <-timer.C:
		}
	}
}

// ForceRelease 不校验 owner token，直接移除底层锁。
func (l *DistributedLock) ForceRelease(ctx context.Context) error {
	if err := l.provider.ForceRelease(ctx, l.key); err != nil {
		return err
	}
	l.mu.Lock()
	l.held = false
	l.token = ""
	l.mu.Unlock()
	return nil
}

// Release 释放当前 DistributedLock 实例持有的锁。
//
// 释放时会校验 token，确保只释放本实例成功获取的锁。
func (l *DistributedLock) Release(ctx context.Context) error {
	l.mu.Lock()
	if !l.held || l.token == "" {
		l.mu.Unlock()
		return ErrLockNotHeld
	}
	token := l.token
	l.mu.Unlock()

	ok, err := l.provider.Release(ctx, l.key, token)
	if err != nil {
		return err
	}
	if !ok {
		return ErrLockNotHeld
	}

	l.mu.Lock()
	if l.token == token {
		l.held = false
		l.token = ""
	}
	l.mu.Unlock()
	return nil
}

// memoryLockEntry 保存 memory 锁的 token 和过期时间。
type memoryLockEntry struct {
	token     string
	expiresAt time.Time
}

// memoryLockProvider 为 memory store 提供进程内锁能力。
type memoryLockProvider struct {
	mu    sync.Mutex
	locks map[string]memoryLockEntry
}

// newMemoryLockProvider 创建进程内锁仓库。
func newMemoryLockProvider() *memoryLockProvider {
	return &memoryLockProvider{locks: map[string]memoryLockEntry{}}
}

// Acquire 让 memoryStore 满足 LockProvider 接口。
func (s *memoryStore) Acquire(ctx context.Context, key, token string, ttl time.Duration) (bool, error) {
	return s.locks.Acquire(ctx, key, token, ttl)
}

// Release 让 memoryStore 满足 LockProvider 接口。
func (s *memoryStore) Release(ctx context.Context, key, token string) (bool, error) {
	return s.locks.Release(ctx, key, token)
}

// ForceRelease 让 memoryStore 满足 LockProvider 接口。
func (s *memoryStore) ForceRelease(ctx context.Context, key string) error {
	return s.locks.ForceRelease(ctx, key)
}

// Acquire 在进程内以 key 为粒度尝试获取锁。
func (p *memoryLockProvider) Acquire(ctx context.Context, key, token string, ttl time.Duration) (bool, error) {
	_ = ctx
	now := time.Now()
	p.mu.Lock()
	defer p.mu.Unlock()
	if entry, ok := p.locks[key]; ok && now.Before(entry.expiresAt) {
		return false, nil
	}
	p.locks[key] = memoryLockEntry{token: token, expiresAt: now.Add(ttl)}
	return true, nil
}

// Release 按 token 校验并释放进程内锁。
func (p *memoryLockProvider) Release(ctx context.Context, key, token string) (bool, error) {
	_ = ctx
	p.mu.Lock()
	defer p.mu.Unlock()
	entry, ok := p.locks[key]
	if !ok || entry.token != token {
		return false, nil
	}
	delete(p.locks, key)
	return true, nil
}

// ForceRelease 不校验 token，直接移除进程内锁。
func (p *memoryLockProvider) ForceRelease(ctx context.Context, key string) error {
	_ = ctx
	p.mu.Lock()
	delete(p.locks, key)
	p.mu.Unlock()
	return nil
}

// FlushLocks 清理指定锁前缀下的全部进程内锁。
func (p *memoryLockProvider) FlushLocks(ctx context.Context, lockPrefix string) error {
	_ = ctx
	lockPrefix = strings.Trim(strings.TrimSpace(lockPrefix), ":")
	p.mu.Lock()
	defer p.mu.Unlock()
	if lockPrefix == "" {
		p.locks = map[string]memoryLockEntry{}
		return nil
	}
	prefix := lockPrefix + ":"
	for key := range p.locks {
		if key == lockPrefix || strings.HasPrefix(key, prefix) {
			delete(p.locks, key)
		}
	}
	return nil
}

// FlushLocks 让 memoryStore 满足 LockFlushStore 接口。
func (s *memoryStore) FlushLocks(ctx context.Context, lockPrefix string) error {
	return s.locks.FlushLocks(ctx, lockPrefix)
}

// Acquire 通过 Redis SET NX 获取分布式锁。
func (s *redisStore) Acquire(ctx context.Context, key, token string, ttl time.Duration) (bool, error) {
	return s.client.SetNX(ctx, key, token, ttl).Result()
}

// Release 通过 Lua 脚本原子校验 token 并删除 Redis 锁。
func (s *redisStore) Release(ctx context.Context, key, token string) (bool, error) {
	result, err := s.client.Eval(ctx, `
if redis.call("get", KEYS[1]) == ARGV[1] then
	return redis.call("del", KEYS[1])
end
return 0
`, []string{key}, token).Int64()
	if err != nil && !errors.Is(err, redis.Nil) {
		return false, err
	}
	return result == 1, nil
}

// ForceRelease 不校验 token，直接删除 Redis 锁 key。
func (s *redisStore) ForceRelease(ctx context.Context, key string) error {
	return s.client.Del(ctx, key).Err()
}

// FlushLocks 按锁前缀删除 Redis 中的锁 key。
func (s *redisStore) FlushLocks(ctx context.Context, lockPrefix string) error {
	lockPrefix = strings.Trim(strings.TrimSpace(lockPrefix), ":")
	if lockPrefix == "" {
		return nil
	}
	iter := s.client.Scan(ctx, 0, lockPrefix+":*", 100).Iterator()
	keys := make([]string, 0, 100)
	for iter.Next(ctx) {
		keys = append(keys, iter.Val())
		if len(keys) == cap(keys) {
			if err := s.client.Del(ctx, keys...).Err(); err != nil {
				return err
			}
			keys = keys[:0]
		}
	}
	if err := iter.Err(); err != nil {
		return err
	}
	if len(keys) > 0 {
		return s.client.Del(ctx, keys...).Err()
	}
	return nil
}

// errorLockProvider 用于把 store 构造错误延迟暴露到锁调用。
type errorLockProvider struct {
	err error
}

// Acquire 始终返回预先保存的错误。
func (p errorLockProvider) Acquire(ctx context.Context, key, token string, ttl time.Duration) (bool, error) {
	_ = ctx
	_ = key
	_ = token
	_ = ttl
	return false, p.err
}

// Release 始终返回预先保存的错误。
func (p errorLockProvider) Release(ctx context.Context, key, token string) (bool, error) {
	_ = ctx
	_ = key
	_ = token
	return false, p.err
}

// ForceRelease 始终返回预先保存的错误。
func (p errorLockProvider) ForceRelease(ctx context.Context, key string) error {
	_ = ctx
	_ = key
	return p.err
}

// randomToken 生成锁拥有者 token。
func randomToken() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return time.Now().Format(time.RFC3339Nano)
	}
	return hex.EncodeToString(b[:])
}
