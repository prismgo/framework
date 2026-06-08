package redis

// redisMigrateDueScript 把 delayed/reserved 中到期的 body 原子迁移到 ready list 并写入 notify。
//
// 需求背景：多个 worker 同时迁移同一个 sorted set 时，必须只在 ZREM 成功的一方执行 RPUSH，
// 否则同一个任务会被重复放入 ready list。Laravel Redis queue 还会为每个迁回 ready 的任务
// 写入一个 notify token，用来唤醒正在 BLPOP :notify 的阻塞 worker。
const redisMigrateDueScript = `
local items = redis.call('zrangebyscore', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, 100)
local moved = 0
for _, item in ipairs(items) do
	if redis.call('zrem', KEYS[1], item) == 1 then
		redis.call('rpush', KEYS[2], item)
		redis.call('lpush', KEYS[3], '1')
		moved = moved + 1
	end
end
return moved
`

// redisReserveReadyScript 把 ready list 中的一个 body 原子移动到 reserved sorted set 并消费 notify。
//
// 需求背景：如果先 LPOP 再由 Go 代码 ZADD reserved，worker 在两个命令之间崩溃会静默丢任务。
// reserve 成功时同步 LPOP 一个 notify token，避免 push/bulk/migrate 留下只增不减的唤醒列表。
const redisReserveReadyScript = `
local body = redis.call('lpop', KEYS[1])
if not body then
	return nil
end
redis.call('zadd', KEYS[2], ARGV[1], body)
redis.call('lpop', KEYS[3])
return body
`

// redisReplaceReservedScript 用带 Attempts/ReservedAt 的新 body 替换 reserved 中的原始 body。
//
// 设计思路：reserve 阶段先保留 raw body，Go 解码并更新 envelope 后再原子替换；若替换前
// raw body 已不存在，说明该任务已被其他流程处理，调用方应放弃本次消费。
const redisReplaceReservedScript = `
local score = redis.call('zscore', KEYS[1], ARGV[1])
if not score then
	return 0
end
redis.call('zrem', KEYS[1], ARGV[1])
redis.call('zadd', KEYS[1], score, ARGV[2])
return 1
`
