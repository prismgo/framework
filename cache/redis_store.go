package cache

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"time"

	redisstore "github.com/eko/gocache/store/redis/v4"
	"github.com/redis/go-redis/v9"
)

const redisPersistIfExistsScript = `
if redis.call("exists", KEYS[1]) == 0 then
	return 0
end
redis.call("persist", KEYS[1])
return 1
`

// redisStore 包装 eko/gocache 的 RedisStore，并补充 Touch、原子操作和 Close 能力。
type redisStore struct {
	*redisstore.RedisStore
	client *redis.Client
}

// Touch 通过 Redis EXPIRE/PERSIST 更新已有 key 的 TTL。
func (s *redisStore) Touch(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	if ttl <= 0 {
		result, err := s.client.Eval(ctx, redisPersistIfExistsScript, []string{key}).Int()
		return result == 1, err
	}
	return s.client.Expire(ctx, key, ttl).Result()
}

// Add 通过 Redis SET NX 实现原子只写不存在 key。
func (s *redisStore) Add(ctx context.Context, key string, value []byte, ttl time.Duration) (bool, error) {
	if ttl < 0 {
		ttl = 0
	}
	return s.client.SetNX(ctx, key, value, ttl).Result()
}

// Increment 通过 Redis INCRBY 实现原子整数计数器。
func (s *redisStore) Increment(ctx context.Context, key string, delta int64) (int64, error) {
	count, err := s.client.IncrBy(ctx, key, delta).Result()
	if err != nil {
		return 0, normalizeCounterError(err)
	}
	return count, nil
}

// Pull 通过 Lua 脚本原子读取并删除指定 key。
func (s *redisStore) Pull(ctx context.Context, key string) ([]byte, error) {
	result, err := s.client.Eval(ctx, `
local value = redis.call("get", KEYS[1])
if value then
	redis.call("del", KEYS[1])
end
return value
`, []string{key}).Result()
	if errors.Is(err, redis.Nil) || result == nil {
		return nil, ErrCacheMiss
	}
	if err != nil {
		return nil, err
	}
	switch value := result.(type) {
	case string:
		return []byte(value), nil
	case []byte:
		return value, nil
	default:
		return rawBytes(value)
	}
}

// GetMany 批量读取 Redis 缓存，未命中的 key 不会出现在返回值中。
func (s *redisStore) GetMany(ctx context.Context, keys []string) (map[string][]byte, error) {
	out := make(map[string][]byte, len(keys))
	if len(keys) == 0 {
		return out, nil
	}
	values, err := s.client.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, err
	}
	for i, value := range values {
		if value == nil {
			continue
		}
		data, err := rawBytes(value)
		if err != nil {
			return nil, err
		}
		out[keys[i]] = data
	}
	return out, nil
}

// PutMany 批量写入 Redis 缓存。
func (s *redisStore) PutMany(ctx context.Context, values map[string][]byte, ttl time.Duration) error {
	if len(values) == 0 {
		return nil
	}
	pipe := s.client.Pipeline()
	for key, value := range values {
		pipe.Set(ctx, key, value, ttl)
	}
	_, err := pipe.Exec(ctx)
	return err
}

// ForgetMany 批量删除 Redis 缓存。
func (s *redisStore) ForgetMany(ctx context.Context, keys []string) error {
	if len(keys) == 0 {
		return nil
	}
	return s.client.Del(ctx, keys...).Err()
}

// Close intentionally leaves Redis clients open.
//
// Redis client lifecycle is owned by prismgo/redis.Manager.Close(ctx) for
// shared connections, or by the caller for legacy/direct clients.
func (s *redisStore) Close() error {
	return nil
}

// Flush 按 Repository 前缀删除 Redis key；前缀为空时清空当前 Redis DB。
func (s *redisStore) Flush(ctx context.Context, prefix string) error {
	prefix = strings.Trim(strings.TrimSpace(prefix), ":")
	if prefix == "" {
		return s.client.FlushDB(ctx).Err()
	}
	iter := s.client.Scan(ctx, 0, prefix+":*", 100).Iterator()
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

// GetTagged 读取 Redis tagged cache 对应的数据 key。
func (s *redisStore) GetTagged(ctx context.Context, prefix string, tags []string, key string) ([]byte, error) {
	data, err := s.client.Get(ctx, taggedDataKey(prefix, tags, key)).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, ErrCacheMiss
	}
	return data, err
}

// PutTagged 写入 Redis tagged cache 并维护标签索引。
func (s *redisStore) PutTagged(ctx context.Context, prefix string, tags []string, key string, value []byte, ttl time.Duration) error {
	dataKey := taggedDataKey(prefix, tags, key)
	score := redisTagScore(ttl)
	pipe := s.client.Pipeline()
	pipe.Set(ctx, dataKey, value, ttl)
	for _, tag := range tags {
		indexKey := tagIndexKey(prefix, tag)
		pipe.ZRemRangeByScore(ctx, indexKey, "1", redisNowMilli())
		pipe.ZAdd(ctx, indexKey, redis.Z{Score: score, Member: dataKey})
	}
	_, err := pipe.Exec(ctx)
	return err
}

// ForgetTagged 删除 Redis tagged cache 中的指定 key。
func (s *redisStore) ForgetTagged(ctx context.Context, prefix string, tags []string, key string) error {
	dataKey := taggedDataKey(prefix, tags, key)
	pipe := s.client.Pipeline()
	pipe.Del(ctx, dataKey)
	for _, tag := range tags {
		pipe.ZRem(ctx, tagIndexKey(prefix, tag), dataKey)
	}
	_, err := pipe.Exec(ctx)
	return err
}

// FlushTags 清理任一指定标签关联的全部 Redis 缓存项。
func (s *redisStore) FlushTags(ctx context.Context, prefix string, tags []string) error {
	if len(tags) == 0 {
		return nil
	}
	keys := make(map[string]struct{})
	indexKeys := make([]string, 0, len(tags))
	for _, tag := range tags {
		indexKey := tagIndexKey(prefix, tag)
		if err := s.pruneTagIndex(ctx, indexKey); err != nil {
			return err
		}
		indexKeys = append(indexKeys, indexKey)
		members, err := s.client.ZRange(ctx, indexKey, 0, -1).Result()
		if err != nil {
			return err
		}
		for _, key := range members {
			keys[key] = struct{}{}
		}
	}
	pipe := s.client.Pipeline()
	if len(keys) > 0 {
		dataKeys := make([]string, 0, len(keys))
		for key := range keys {
			dataKeys = append(dataKeys, key)
		}
		pipe.Del(ctx, dataKeys...)
	}
	pipe.Del(ctx, indexKeys...)
	_, err := pipe.Exec(ctx)
	return err
}

// redisTagScore 返回 tag 索引成员的过期分数；0 表示永久 key，不能被过期清理删除。
func redisTagScore(ttl time.Duration) float64 {
	if ttl <= 0 {
		return 0
	}
	return float64(time.Now().Add(ttl).UnixMilli())
}

// pruneTagIndex 清理 tag 索引中过期的数据 key 成员。
func (s *redisStore) pruneTagIndex(ctx context.Context, indexKey string) error {
	return s.client.ZRemRangeByScore(ctx, indexKey, "1", redisNowMilli()).Err()
}

func redisNowMilli() string {
	return strconv.FormatInt(time.Now().UnixMilli(), 10)
}

// normalizeCounterError 把 Redis 非整数计数错误归一为本包错误。
func normalizeCounterError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, redis.Nil) {
		return err
	}
	if strings.Contains(strings.ToLower(err.Error()), "not an integer") {
		return ErrInvalidCounter
	}
	return err
}
