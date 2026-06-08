package redis

import "strings"

func (c *RedisQueue) readyKey(queue string) string {
	return c.prefix + ":queues:" + queue
}

// ReadyKey 返回业务队列对应的 Redis ready list key。
//
// 需求背景：Redis key 是持久化格式，迁移到子包后测试仍需要显式断言 key schema 不变。
func (c *RedisQueue) ReadyKey(queue string) string {
	return c.readyKey(queue)
}

func (c *RedisQueue) DelayedKey(queue string) string {
	return c.delayedKey(queue)
}

func (c *RedisQueue) ReservedKey(queue string) string {
	return c.reservedKey(queue)
}

func (c *RedisQueue) delayedKey(queue string) string {
	return c.readyKey(queue) + ":delayed"
}

func (c *RedisQueue) notifyKey(queue string) string {
	return c.readyKey(queue) + ":notify"
}

func (c *RedisQueue) reservedKey(queue string) string {
	return c.readyKey(queue) + ":reserved"
}

func (c *RedisFailedStore) failedHashKey() string {
	return c.prefix + ":failed"
}

func (c *RedisFailedStore) failedEntryKey(id string) string {
	return c.failedHashKey() + ":entry:" + id
}

func (c *RedisFailedStore) FailedHashKey() string {
	return c.failedHashKey()
}

func (c *RedisFailedStore) FailedEntryKey(id string) string {
	return c.failedEntryKey(id)
}

func (c *RedisFailedStore) failedIndexKey() string {
	return c.prefix + ":failed:index"
}

func (c *RedisFailedStore) FailedIndexKey() string {
	return c.failedIndexKey()
}

func (c *RedisBatchStore) batchKey(id string) string {
	return c.prefix + ":batches:" + id
}

func (c *RedisBatchStore) BatchKey(id string) string {
	return c.batchKey(id)
}

func keyQueueName(prefix string, key string) string {
	trimmed := strings.TrimPrefix(key, prefix+":queues:")
	return strings.TrimSuffix(strings.TrimSuffix(trimmed, ":delayed"), ":reserved")
}

func KeyQueueName(prefix string, key string) string {
	return keyQueueName(prefix, key)
}
