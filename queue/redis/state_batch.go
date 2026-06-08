package redis

import (
	"context"
	"strconv"
	"strings"
	"time"

	encodingcontract "github.com/prismgo/framework/contracts/encoding"
	"github.com/prismgo/framework/queue/payload"
	"github.com/redis/go-redis/v9"
)

// redisMarkBatchJobScript 在 Redis 内一次性完成单个批次任务的进度更新并返回最新 hash 快照。
//
// 设计原因：批次状态改为 hash 后，pending、processed、failed 和 finished_at 必须在同一个
// Redis 原子步骤中移动；如果回到 Go 侧先读再写，并发 worker 仍会互相覆盖计数。
// 语义边界：脚本只负责“一个任务完成”的计数变更；取消状态、批次名称和总数仍由
// CreateBatch/UpdateBatch 维护，避免把所有 batch 业务规则塞进 Lua。
const redisMarkBatchJobScript = `
if redis.call('exists', KEYS[1]) == 0 then
	return {}
end
local total = tonumber(redis.call('hget', KEYS[1], 'total') or '0')
local processed = tonumber(redis.call('hget', KEYS[1], 'processed') or '0')
if total > 0 and processed >= total then
	return redis.call('hgetall', KEYS[1])
end
local pending = tonumber(redis.call('hget', KEYS[1], 'pending') or '0')
if pending > 0 then
	pending = redis.call('hincrby', KEYS[1], 'pending', -1)
end
redis.call('hincrby', KEYS[1], 'processed', 1)
if ARGV[1] == '0' then
	redis.call('hincrby', KEYS[1], 'failed', 1)
end
local finished = redis.call('hget', KEYS[1], 'finished_at')
if pending <= 0 and (not finished or finished == '' or finished == '0') then
	redis.call('hset', KEYS[1], 'finished_at', ARGV[2])
end
return redis.call('hgetall', KEYS[1])
`

const redisCancelBatchScript = `
if redis.call('exists', KEYS[1]) == 0 then
	return {}
end
local cancelled = redis.call('hget', KEYS[1], 'cancelled')
if cancelled == '1' or cancelled == 'true' then
	return redis.call('hgetall', KEYS[1])
end
redis.call('hset', KEYS[1], 'cancelled', '1')
local cancelledAt = redis.call('hget', KEYS[1], 'cancelled_at')
if not cancelledAt or cancelledAt == '' or cancelledAt == '0' then
	redis.call('hset', KEYS[1], 'cancelled_at', ARGV[1])
end
return redis.call('hgetall', KEYS[1])
`

// RedisBatchStore 使用 Redis hash 保存 batch metadata。
type RedisBatchStore struct {
	client    *redis.Client
	prefix    string
	failedTTL time.Duration
	codec     encodingcontract.Codec
}

// NewRedisBatchStoreFromClient 创建 Redis batch repository。
func NewRedisBatchStoreFromClient(client *redis.Client, options RedisOptions) *RedisBatchStore {
	return &RedisBatchStore{
		client:    client,
		prefix:    cleanPrefix(options.Prefix, "queue"),
		failedTTL: options.FailedTTL,
		codec:     redisCodec(options.Codec),
	}
}

func (c *RedisBatchStore) CreateBatch(ctx context.Context, status payload.BatchStatus) error {
	if status.CreatedAt.IsZero() {
		status.CreatedAt = time.Now()
	}
	pipe := c.client.TxPipeline()
	pipe.HSet(ctx, c.batchKey(status.ID), batchHash(status))
	if c.failedTTL > 0 {
		pipe.Expire(ctx, c.batchKey(status.ID), c.failedTTL)
	}
	_, err := pipe.Exec(ctx)
	return err
}

func (c *RedisBatchStore) GetBatch(ctx context.Context, id string) (*payload.BatchStatus, error) {
	values, err := c.client.HGetAll(ctx, c.batchKey(id)).Result()
	if isRedisWrongType(err) {
		return nil, ErrEmpty
	}
	if err != nil {
		return nil, err
	}
	if len(values) == 0 {
		return nil, ErrEmpty
	}
	return batchFromHash(values)
}

func (c *RedisBatchStore) UpdateBatch(ctx context.Context, status payload.BatchStatus) error {
	pipe := c.client.TxPipeline()
	pipe.HSet(ctx, c.batchKey(status.ID), batchHash(status))
	if c.failedTTL > 0 {
		pipe.Expire(ctx, c.batchKey(status.ID), c.failedTTL)
	}
	_, err := pipe.Exec(ctx)
	return err
}

// DeleteBatch 删除 Redis 中的 batch metadata。
//
// 需求背景：Laravel PendingBatch 在批次任务写入队列失败时会删除已创建的 metadata。
// 这里保持同样的 repository 语义；删除缺失 key 视为成功，便于投递失败路径幂等清理。
func (c *RedisBatchStore) DeleteBatch(ctx context.Context, id string) error {
	return c.client.Del(ctx, c.batchKey(id)).Err()
}

// MarkBatchJob 通过 Lua 原子标记 Redis 批次内一个任务完成。
//
// 设计原因：多个 worker 可能同时完成同一批次的不同任务；pending--、processed++、
// failed++ 与 finished_at 设置必须作为一个 Redis 原子操作提交，不能依赖进程内锁。
// Lua 脚本直接返回更新后的 hash 快照，避免额外 GetBatch 调用产生的时间窗口内状态漂移。
func (c *RedisBatchStore) MarkBatchJob(ctx context.Context, id string, success bool) (payload.BatchStatus, error) {
	successValue := "0"
	if success {
		successValue = "1"
	}
	return c.evalBatchStatusScript(ctx, redisMarkBatchJobScript, id, successValue, strconv.FormatInt(time.Now().UnixNano(), 10))
}

func (c *RedisBatchStore) CancelBatch(ctx context.Context, id string) (payload.BatchStatus, error) {
	return c.evalBatchStatusScript(ctx, redisCancelBatchScript, id, strconv.FormatInt(time.Now().UnixNano(), 10))
}

func (c *RedisBatchStore) evalBatchStatusScript(ctx context.Context, script string, id string, args ...string) (payload.BatchStatus, error) {
	result, err := c.client.Eval(ctx, script, []string{c.batchKey(id)}, args).StringSlice()
	if err != nil {
		return payload.BatchStatus{}, err
	}
	if len(result) == 0 {
		return payload.BatchStatus{}, ErrEmpty
	}
	values := make(map[string]string, len(result)/2)
	for i := 0; i+1 < len(result); i += 2 {
		values[result[i]] = result[i+1]
	}
	status, err := batchFromHash(values)
	if err != nil {
		return payload.BatchStatus{}, err
	}
	return *status, nil
}

func batchHash(status payload.BatchStatus) map[string]any {
	return map[string]any{
		"id":           status.ID,
		"name":         status.Name,
		"total":        status.Total,
		"pending":      status.Pending,
		"processed":    status.Processed,
		"failed":       status.Failed,
		"cancelled":    boolString(status.Cancelled),
		"created_at":   timeNanoString(status.CreatedAt),
		"finished_at":  timeNanoString(status.FinishedAt),
		"cancelled_at": timeNanoString(status.CancelledAt),
	}
}

func batchFromHash(values map[string]string) (*payload.BatchStatus, error) {
	status := &payload.BatchStatus{
		ID:          values["id"],
		Name:        values["name"],
		Total:       parseHashInt(values["total"]),
		Pending:     parseHashInt(values["pending"]),
		Processed:   parseHashInt(values["processed"]),
		Failed:      parseHashInt(values["failed"]),
		Cancelled:   values["cancelled"] == "1" || values["cancelled"] == "true",
		CreatedAt:   parseHashTime(values["created_at"]),
		FinishedAt:  parseHashTime(values["finished_at"]),
		CancelledAt: parseHashTime(values["cancelled_at"]),
	}
	if status.ID == "" {
		return nil, ErrEmpty
	}
	return status, nil
}

func boolString(value bool) string {
	if value {
		return "1"
	}
	return "0"
}

func timeNanoString(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return strconv.FormatInt(value.UnixNano(), 10)
}

func parseHashInt(value string) int {
	n, _ := strconv.Atoi(value)
	return n
}

func parseHashTime(value string) time.Time {
	n, _ := strconv.ParseInt(value, 10, 64)
	if n <= 0 {
		return time.Time{}
	}
	return time.Unix(0, n)
}

func isRedisWrongType(err error) bool {
	return err != nil && strings.Contains(err.Error(), "WRONGTYPE")
}
