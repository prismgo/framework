package cache

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"time"
)

// TaggedRepository 是带标签的缓存操作入口。
//
// tagged cache 适合按一组业务标签批量失效缓存，例如清理某个租户或某类字典的
// 所有缓存项。当前内置 memory 与 redis store 支持标签，file store 不支持。
type TaggedRepository struct {
	repo *Repository
	tags []string
	err  error
}

func newTaggedRepository(repo *Repository, tags []string) *TaggedRepository {
	normalized := normalizeTags(tags)
	tr := &TaggedRepository{repo: repo, tags: normalized}
	if repo == nil || repo.err != nil {
		if repo != nil {
			tr.err = repo.err
		}
		return tr
	}
	if len(normalized) == 0 {
		tr.err = fmt.Errorf("%w: no tags supplied", ErrTagsUnsupported)
		return tr
	}
	if repo.tags == nil {
		tr.err = ErrTagsUnsupported
	}
	return tr
}

// Tags 返回当前 TaggedRepository 绑定的规范化标签。
func (t *TaggedRepository) Tags() []string {
	return append([]string(nil), t.tags...)
}

// Put 将 value 写入 tagged cache。
func (t *TaggedRepository) Put(ctx context.Context, key string, value any, ttl time.Duration) error {
	if t.err != nil {
		return t.err
	}
	data, err := t.repo.encode(value)
	if err != nil {
		t.repo.dispatch(ctx, EventCacheWriteFailed, CacheEvent{Key: key, Tags: t.Tags(), Error: err})
		return err
	}
	t.repo.dispatch(ctx, EventCacheWriting, CacheEvent{Key: key, Tags: t.Tags()})
	if err := t.repo.tags.PutTagged(ctx, t.repo.prefix, t.tags, key, data, ttl); err != nil {
		t.repo.dispatch(ctx, EventCacheWriteFailed, CacheEvent{Key: key, Tags: t.Tags(), Error: err})
		return err
	}
	t.repo.dispatch(ctx, EventCacheWritten, CacheEvent{Key: key, Tags: t.Tags()})
	return nil
}

// Set 是 Put 的别名。
func (t *TaggedRepository) Set(ctx context.Context, key string, value any, ttl time.Duration) error {
	return t.Put(ctx, key, value, ttl)
}

// Forever 永久写入 tagged cache。
func (t *TaggedRepository) Forever(ctx context.Context, key string, value any) error {
	return t.Put(ctx, key, value, 0)
}

// Get 读取 tagged cache；未命中时可返回 fallback。
func (t *TaggedRepository) Get(ctx context.Context, key string, fallback ...any) (any, error) {
	data, err := t.getEncoded(ctx, key)
	if err == nil {
		var out any
		if err := t.repo.decode(data, &out); err != nil {
			return nil, err
		}
		return out, nil
	}
	if !isMiss(err) {
		return nil, err
	}
	return resolveAnyDefault(ctx, fallback)
}

// Has 判断 tagged cache 中是否存在指定 key。
func (t *TaggedRepository) Has(ctx context.Context, key string) (bool, error) {
	_, err := t.getEncoded(ctx, key)
	if err == nil {
		return true, nil
	}
	if isMiss(err) {
		return false, nil
	}
	return false, err
}

// Missing 判断 tagged cache 中是否不存在指定 key。
func (t *TaggedRepository) Missing(ctx context.Context, key string) (bool, error) {
	ok, err := t.Has(ctx, key)
	return !ok, err
}

// Remember 读取 tagged cache；未命中时执行 loader 并写入。
func (t *TaggedRepository) Remember(ctx context.Context, key string, ttl time.Duration, loader func(context.Context) (any, error)) (any, error) {
	value, err := t.Get(ctx, key)
	if err == nil {
		return value, nil
	}
	if !isMiss(err) {
		return nil, err
	}
	value, err = loader(ctx)
	if err != nil {
		return nil, err
	}
	if err := t.Put(ctx, key, value, ttl); err != nil {
		return nil, err
	}
	return value, nil
}

// RememberForever 读取 tagged cache；未命中时执行 loader 并永久写入。
func (t *TaggedRepository) RememberForever(ctx context.Context, key string, loader func(context.Context) (any, error)) (any, error) {
	return t.Remember(ctx, key, 0, loader)
}

// Sear 是 RememberForever 的别名。
func (t *TaggedRepository) Sear(ctx context.Context, key string, loader func(context.Context) (any, error)) (any, error) {
	return t.RememberForever(ctx, key, loader)
}

// Forget 删除 tagged cache 中的指定 key。
func (t *TaggedRepository) Forget(ctx context.Context, key string) error {
	if t.err != nil {
		return t.err
	}
	t.repo.dispatch(ctx, EventCacheForgetting, CacheEvent{Key: key, Tags: t.Tags()})
	if err := t.repo.tags.ForgetTagged(ctx, t.repo.prefix, t.tags, key); err != nil {
		t.repo.dispatch(ctx, EventCacheForgetFailed, CacheEvent{Key: key, Tags: t.Tags(), Error: err})
		return err
	}
	t.repo.dispatch(ctx, EventCacheForgotten, CacheEvent{Key: key, Tags: t.Tags()})
	return nil
}

// Delete 是 Forget 的别名。
func (t *TaggedRepository) Delete(ctx context.Context, key string) error {
	return t.Forget(ctx, key)
}

// Flush 清理当前标签集合关联的缓存项。
func (t *TaggedRepository) Flush(ctx context.Context) error {
	if t.err != nil {
		return t.err
	}
	t.repo.dispatch(ctx, EventCacheFlushing, CacheEvent{Tags: t.Tags()})
	if err := t.repo.tags.FlushTags(ctx, t.repo.prefix, t.tags); err != nil {
		t.repo.dispatch(ctx, EventCacheFlushFailed, CacheEvent{Tags: t.Tags(), Error: err})
		return err
	}
	t.repo.dispatch(ctx, EventCacheFlushed, CacheEvent{Tags: t.Tags()})
	return nil
}

// Clear 是 Flush 的别名。
func (t *TaggedRepository) Clear(ctx context.Context) error {
	return t.Flush(ctx)
}

func (t *TaggedRepository) getEncoded(ctx context.Context, key string) ([]byte, error) {
	if t.err != nil {
		return nil, t.err
	}
	t.repo.dispatch(ctx, EventCacheRetrieving, CacheEvent{Key: key, Tags: t.Tags()})
	data, err := t.repo.tags.GetTagged(ctx, t.repo.prefix, t.tags, key)
	if err != nil {
		if isMiss(err) {
			t.repo.dispatch(ctx, EventCacheMissed, CacheEvent{Key: key, Tags: t.Tags()})
		} else {
			t.repo.dispatch(ctx, EventCacheMissed, CacheEvent{Key: key, Tags: t.Tags(), Error: err})
		}
		return nil, err
	}
	t.repo.dispatch(ctx, EventCacheHit, CacheEvent{Key: key, Tags: t.Tags()})
	return data, nil
}

func normalizeTags(tags []string) []string {
	seen := make(map[string]struct{}, len(tags))
	out := make([]string, 0, len(tags))
	for _, tag := range tags {
		tag = strings.Trim(strings.TrimSpace(tag), ":")
		if tag == "" {
			continue
		}
		if _, ok := seen[tag]; ok {
			continue
		}
		seen[tag] = struct{}{}
		out = append(out, tag)
	}
	sort.Strings(out)
	return out
}

func taggedDataKey(prefix string, tags []string, key string) string {
	key = strings.Trim(strings.TrimSpace(key), ":")
	parts := []string{strings.Trim(strings.TrimSpace(prefix), ":"), "tagged", tagSetHash(tags), key}
	return joinPrefix(parts...)
}

func tagIndexKey(prefix string, tag string) string {
	parts := []string{strings.Trim(strings.TrimSpace(prefix), ":"), "tag", tagHash(tag), "keys"}
	return joinPrefix(parts...)
}

func tagSetHash(tags []string) string {
	return hashString(strings.Join(tags, "\x00"))
}

func tagHash(tag string) string {
	return hashString(tag)
}

func hashString(value string) string {
	sum := sha1.Sum([]byte(value))
	return hex.EncodeToString(sum[:])
}
