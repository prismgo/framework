package cache

import (
	"context"
	"errors"
	"fmt"
	"time"

	libstore "github.com/eko/gocache/lib/v4/store"
)

// failoverStore 按配置顺序把操作转发到第一个可用的子 store。
//
// 缓存未命中属于正常业务结果，不会触发 failover；只有底层 store 操作错误时
// 才会继续尝试后备 store，并派发 cache.failed_over 事件。
type failoverStore struct {
	manager *Manager
	name    string
	stores  []string
}

func newFailoverStore(manager *Manager, name string, stores []string) *failoverStore {
	return &failoverStore{manager: manager, name: name, stores: append([]string(nil), stores...)}
}

func (s *failoverStore) Get(ctx context.Context, key any) (any, error) {
	k, err := failoverStringKey(key)
	if err != nil {
		return nil, err
	}
	var last error
	for _, repo := range s.repositories() {
		value, err := repo.getTyped(ctx, k)
		if err == nil || isMiss(err) {
			return value, err
		}
		s.dispatchFailover(ctx, repo.Name(), err)
		last = err
	}
	if last != nil {
		return nil, last
	}
	return nil, ErrStoreNotFound
}

func (s *failoverStore) GetWithTTL(ctx context.Context, key any) (any, time.Duration, error) {
	value, err := s.Get(ctx, key)
	if err != nil {
		return nil, 0, err
	}
	return value, 0, nil
}

func (s *failoverStore) Set(ctx context.Context, key any, value any, options ...libstore.Option) error {
	k, err := failoverStringKey(key)
	if err != nil {
		return err
	}
	data, err := rawBytes(value)
	if err != nil {
		return err
	}
	opts := libstore.ApplyOptionsWithDefault(&libstore.Options{}, options...)
	var last error
	for _, repo := range s.repositories() {
		if err := repo.putEncoded(ctx, k, data, opts.Expiration); err == nil {
			return nil
		} else {
			s.dispatchFailover(ctx, repo.Name(), err)
			last = err
		}
	}
	if last != nil {
		return last
	}
	return ErrStoreNotFound
}

func (s *failoverStore) Delete(ctx context.Context, key any) error {
	k, err := failoverStringKey(key)
	if err != nil {
		return err
	}
	var last error
	for _, repo := range s.repositories() {
		if err := repo.Forget(ctx, k); err == nil {
			return nil
		} else {
			s.dispatchFailover(ctx, repo.Name(), err)
			last = err
		}
	}
	if last != nil {
		return last
	}
	return ErrStoreNotFound
}

func (s *failoverStore) Invalidate(ctx context.Context, options ...libstore.InvalidateOption) error {
	_ = ctx
	_ = options
	return nil
}

func (s *failoverStore) Clear(ctx context.Context) error {
	return s.Flush(ctx, "")
}

func (s *failoverStore) GetType() string {
	return "failover"
}

func (s *failoverStore) Touch(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	var last error
	for _, repo := range s.repositories() {
		ok, err := repo.Touch(ctx, key, ttl)
		if err == nil {
			return ok, nil
		}
		s.dispatchFailover(ctx, repo.Name(), err)
		last = err
	}
	if last != nil {
		return false, last
	}
	return false, ErrStoreNotFound
}

func (s *failoverStore) Add(ctx context.Context, key string, value []byte, ttl time.Duration) (bool, error) {
	var last error
	for _, repo := range s.repositories() {
		if repo.atomic == nil {
			last = fmt.Errorf("cache: store %q does not support atomic add", repo.Name())
			continue
		}
		ok, err := repo.atomic.Add(ctx, repo.key(key), value, ttl)
		if err == nil {
			return ok, nil
		}
		s.dispatchFailover(ctx, repo.Name(), err)
		last = err
	}
	if last != nil {
		return false, last
	}
	return false, ErrStoreNotFound
}

func (s *failoverStore) Increment(ctx context.Context, key string, delta int64) (int64, error) {
	var last error
	for _, repo := range s.repositories() {
		count, err := repo.Increment(ctx, key, delta)
		if err == nil {
			return count, nil
		}
		s.dispatchFailover(ctx, repo.Name(), err)
		last = err
	}
	if last != nil {
		return 0, last
	}
	return 0, ErrStoreNotFound
}

func (s *failoverStore) Pull(ctx context.Context, key string) ([]byte, error) {
	var last error
	for _, repo := range s.repositories() {
		data, err := repo.pullEncoded(ctx, key)
		if err == nil || isMiss(err) {
			return data, err
		}
		s.dispatchFailover(ctx, repo.Name(), err)
		last = err
	}
	if last != nil {
		return nil, last
	}
	return nil, ErrStoreNotFound
}

func (s *failoverStore) GetMany(ctx context.Context, keys []string) (map[string][]byte, error) {
	var last error
	for _, repo := range s.repositories() {
		values, err := repo.manyEncoded(ctx, keys)
		if err == nil {
			return values, nil
		}
		s.dispatchFailover(ctx, repo.Name(), err)
		last = err
	}
	if last != nil {
		return nil, last
	}
	return nil, ErrStoreNotFound
}

func (s *failoverStore) PutMany(ctx context.Context, values map[string][]byte, ttl time.Duration) error {
	var last error
	for _, repo := range s.repositories() {
		if err := repo.putManyEncoded(ctx, values, ttl); err == nil {
			return nil
		} else {
			s.dispatchFailover(ctx, repo.Name(), err)
			last = err
		}
	}
	if last != nil {
		return last
	}
	return ErrStoreNotFound
}

func (s *failoverStore) ForgetMany(ctx context.Context, keys []string) error {
	var last error
	for _, repo := range s.repositories() {
		if err := repo.ForgetMany(ctx, keys); err == nil {
			return nil
		} else {
			s.dispatchFailover(ctx, repo.Name(), err)
			last = err
		}
	}
	if last != nil {
		return last
	}
	return ErrStoreNotFound
}

func (s *failoverStore) Flush(ctx context.Context, prefix string) error {
	_ = prefix
	var last error
	for _, repo := range s.repositories() {
		if err := repo.Flush(ctx); err == nil {
			return nil
		} else {
			s.dispatchFailover(ctx, repo.Name(), err)
			last = err
		}
	}
	if last != nil {
		return last
	}
	return ErrStoreNotFound
}

func (s *failoverStore) GetTagged(ctx context.Context, prefix string, tags []string, key string) ([]byte, error) {
	_ = prefix
	var last error
	for _, repo := range s.repositories() {
		if !repo.SupportsTags() {
			last = ErrTagsUnsupported
			continue
		}
		data, err := repo.tags.GetTagged(ctx, repo.prefix, tags, key)
		if err == nil || isMiss(err) {
			return data, err
		}
		s.dispatchFailover(ctx, repo.Name(), err)
		last = err
	}
	if last != nil {
		return nil, last
	}
	return nil, ErrStoreNotFound
}

func (s *failoverStore) PutTagged(ctx context.Context, prefix string, tags []string, key string, value []byte, ttl time.Duration) error {
	_ = prefix
	var last error
	for _, repo := range s.repositories() {
		if !repo.SupportsTags() {
			last = ErrTagsUnsupported
			continue
		}
		if err := repo.tags.PutTagged(ctx, repo.prefix, tags, key, value, ttl); err == nil {
			return nil
		} else {
			s.dispatchFailover(ctx, repo.Name(), err)
			last = err
		}
	}
	if last != nil {
		return last
	}
	return ErrStoreNotFound
}

func (s *failoverStore) ForgetTagged(ctx context.Context, prefix string, tags []string, key string) error {
	_ = prefix
	var last error
	for _, repo := range s.repositories() {
		if !repo.SupportsTags() {
			last = ErrTagsUnsupported
			continue
		}
		if err := repo.tags.ForgetTagged(ctx, repo.prefix, tags, key); err == nil {
			return nil
		} else {
			s.dispatchFailover(ctx, repo.Name(), err)
			last = err
		}
	}
	if last != nil {
		return last
	}
	return ErrStoreNotFound
}

func (s *failoverStore) FlushTags(ctx context.Context, prefix string, tags []string) error {
	_ = prefix
	var last error
	for _, repo := range s.repositories() {
		if !repo.SupportsTags() {
			last = ErrTagsUnsupported
			continue
		}
		if err := repo.tags.FlushTags(ctx, repo.prefix, tags); err == nil {
			return nil
		} else {
			s.dispatchFailover(ctx, repo.Name(), err)
			last = err
		}
	}
	if last != nil {
		return last
	}
	return ErrStoreNotFound
}

func (s *failoverStore) Acquire(ctx context.Context, key, token string, ttl time.Duration) (bool, error) {
	var last error
	for _, repo := range s.repositories() {
		if repo.locker == nil {
			last = fmt.Errorf("cache: store %q does not support locks", repo.Name())
			continue
		}
		ok, err := repo.locker.Acquire(ctx, repo.lockKey(key), token, ttl)
		if err == nil {
			return ok, nil
		}
		s.dispatchFailover(ctx, repo.Name(), err)
		last = err
	}
	if last != nil {
		return false, last
	}
	return false, ErrStoreNotFound
}

func (s *failoverStore) Release(ctx context.Context, key, token string) (bool, error) {
	var last error
	for _, repo := range s.repositories() {
		if repo.locker == nil {
			last = fmt.Errorf("cache: store %q does not support locks", repo.Name())
			continue
		}
		ok, err := repo.locker.Release(ctx, repo.lockKey(key), token)
		if err == nil {
			return ok, nil
		}
		s.dispatchFailover(ctx, repo.Name(), err)
		last = err
	}
	if last != nil {
		return false, last
	}
	return false, ErrStoreNotFound
}

func (s *failoverStore) ForceRelease(ctx context.Context, key string) error {
	var last error
	for _, repo := range s.repositories() {
		if repo.locker == nil {
			last = fmt.Errorf("cache: store %q does not support locks", repo.Name())
			continue
		}
		if err := repo.locker.ForceRelease(ctx, repo.lockKey(key)); err == nil {
			return nil
		} else {
			s.dispatchFailover(ctx, repo.Name(), err)
			last = err
		}
	}
	if last != nil {
		return last
	}
	return ErrStoreNotFound
}

func (s *failoverStore) FlushLocks(ctx context.Context, lockPrefix string) error {
	_ = lockPrefix
	var last error
	for _, repo := range s.repositories() {
		if !repo.SupportsFlushingLocks() {
			last = fmt.Errorf("cache: store %q does not support flushing locks", repo.Name())
			continue
		}
		if err := repo.FlushLocks(ctx); err == nil {
			return nil
		} else {
			s.dispatchFailover(ctx, repo.Name(), err)
			last = err
		}
	}
	if last != nil {
		return last
	}
	return ErrStoreNotFound
}

func (s *failoverStore) repositories() []*Repository {
	if s.manager == nil {
		return nil
	}
	names := s.stores
	if len(names) == 0 && s.manager.defaultName != "" && s.manager.defaultName != s.name {
		names = []string{s.manager.defaultName}
	}
	out := make([]*Repository, 0, len(names))
	for _, name := range names {
		if name == "" || name == s.name {
			continue
		}
		out = append(out, s.manager.storeRepository(name))
	}
	return out
}

func (s *failoverStore) dispatchFailover(ctx context.Context, from string, err error) {
	if errors.Is(err, ErrCacheMiss) {
		return
	}
	dispatchCacheEvent(ctx, EventCacheFailedOver, CacheEvent{
		Store: s.name,
		From:  from,
		Error: err,
	})
}

func failoverStringKey(key any) (string, error) {
	k, ok := key.(string)
	if !ok {
		return "", errors.New("cache: failover key must be string")
	}
	return k, nil
}
