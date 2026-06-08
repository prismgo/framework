package redis

import (
	"context"
	"errors"
	"time"

	encodingcontract "github.com/prismgo/framework/contracts/encoding"
	"github.com/prismgo/framework/queue/payload"
	"github.com/prismgo/framework/queue/state"
	"github.com/redis/go-redis/v9"
)

// RedisFailedStore 使用 Redis hash+zset 保存最终失败任务。
type RedisFailedStore struct {
	client    *redis.Client
	prefix    string
	failedTTL time.Duration
	codec     encodingcontract.Codec
}

// NewRedisFailedStoreFromClient 创建 Redis failed job repository。
func NewRedisFailedStoreFromClient(client *redis.Client, options RedisOptions) *RedisFailedStore {
	return &RedisFailedStore{
		client:    client,
		prefix:    cleanPrefix(options.Prefix, "queue"),
		failedTTL: options.FailedTTL,
		codec:     redisCodec(options.Codec),
	}
}

func (c *RedisFailedStore) Record(ctx context.Context, failed payload.FailedJob) error {
	if failed.ID == "" {
		failed.ID = failed.JobID
	}
	if failed.FailedAt.IsZero() {
		failed.FailedAt = time.Now()
	}
	body, err := c.codec.Marshal(failed)
	if err != nil {
		return err
	}
	pipe := c.client.TxPipeline()
	entryKey := c.failedEntryKey(failed.ID)
	pipe.Set(ctx, entryKey, body, c.failedTTL)
	pipe.ZAdd(ctx, c.failedIndexKey(), redis.Z{Score: float64(failed.FailedAt.UnixNano()), Member: failed.ID})
	if c.failedTTL > 0 {
		pipe.Expire(ctx, c.failedIndexKey(), c.failedTTL)
	}
	_, err = pipe.Exec(ctx)
	return err
}

// Page 只读取当前页的 failed index 范围，再按 id 批量取实体。
//
// 设计原因：失败任务可能长期累积，命令和 Dashboard 读取时不能再 LRange 0 -1 后逐条 Find。
// Redis zset 的 score 固定为 FailedAt.UnixNano，因此分页顺序稳定为失败时间升序；
// hash 中缺失或损坏的实体会被顺手从 index 清掉，避免坏记录永久污染后续分页。
func (c *RedisFailedStore) Page(ctx context.Context, page state.PageRequest) (state.PageEnvelope[payload.FailedJob], error) {
	page = normalizeQueuePage(page)
	start := int64((page.Page - 1) * page.PageSize)
	stop := start + int64(page.PageSize) - 1
	pipe := c.client.TxPipeline()
	totalCmd := pipe.ZCard(ctx, c.failedIndexKey())
	idsCmd := pipe.ZRange(ctx, c.failedIndexKey(), start, stop)
	if _, err := pipe.Exec(ctx); err != nil {
		return state.PageEnvelope[payload.FailedJob]{}, err
	}
	ids, err := idsCmd.Result()
	if err != nil {
		return state.PageEnvelope[payload.FailedJob]{}, err
	}
	result := make([]payload.FailedJob, 0, len(ids))
	invalidIDs := make([]string, 0)
	if len(ids) > 0 {
		for _, id := range ids {
			body, err := c.client.Get(ctx, c.failedEntryKey(id)).Bytes()
			if errors.Is(err, redis.Nil) {
				invalidIDs = append(invalidIDs, id)
				continue
			}
			if err != nil {
				return state.PageEnvelope[payload.FailedJob]{}, err
			}
			var failed payload.FailedJob
			if err := c.codec.Unmarshal(body, &failed); err != nil {
				invalidIDs = append(invalidIDs, id)
				_ = c.client.Del(ctx, c.failedEntryKey(id)).Err()
				continue
			}
			result = append(result, failed)
		}
	}
	if len(invalidIDs) > 0 {
		_ = c.client.ZRem(ctx, c.failedIndexKey(), stringSliceToAny(invalidIDs)...).Err()
	}
	total := int(totalCmd.Val()) - len(invalidIDs)
	if total < len(result) {
		total = len(result)
	}
	return state.PageEnvelope[payload.FailedJob]{
		Items:    result,
		Total:    total,
		Page:     page.Page,
		PageSize: page.PageSize,
	}, nil
}

func (c *RedisFailedStore) Find(ctx context.Context, id string) (*payload.FailedJob, error) {
	body, err := c.client.Get(ctx, c.failedEntryKey(id)).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, ErrEmpty
	}
	if err != nil {
		return nil, err
	}
	var failed payload.FailedJob
	if err := c.codec.Unmarshal(body, &failed); err != nil {
		return nil, err
	}
	return &failed, nil
}

func (c *RedisFailedStore) Forget(ctx context.Context, id string) error {
	pipe := c.client.TxPipeline()
	pipe.Del(ctx, c.failedEntryKey(id))
	pipe.ZRem(ctx, c.failedIndexKey(), id)
	_, err := pipe.Exec(ctx)
	return err
}

func (c *RedisFailedStore) Flush(ctx context.Context) error {
	ids, err := c.client.ZRange(ctx, c.failedIndexKey(), 0, -1).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return err
	}
	keys := make([]string, 0, len(ids)+1)
	for _, id := range ids {
		keys = append(keys, c.failedEntryKey(id))
	}
	keys = append(keys, c.failedIndexKey())
	return c.client.Del(ctx, keys...).Err()
}

func stringSliceToAny(values []string) []any {
	result := make([]any, 0, len(values))
	for _, value := range values {
		result = append(result, value)
	}
	return result
}
