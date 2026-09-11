package queue

import (
	"context"

	queuecontract "github.com/prismgo/framework/contracts/queue"
	redisqueue "github.com/prismgo/framework/queue/redis"
)

// RedisConnector 只负责按 connection 配置创建 Redis transport。
//
// failed/batch/restart state repository 不从这里构造，避免 Redis transport
// connection 重新承载 Laravel 13 已拆开的 queue-adjacent state。
type RedisConnector struct{}

func (RedisConnector) Connect(_ context.Context, name string, config queuecontract.ConnectorConfig) (queuecontract.Queue, error) {
	options := redisOptionsFromSpec(config)
	options.Name = name
	options.Codec = config.Codec
	client, err := redisqueue.ResolveQueueClient(options)
	if err != nil {
		return nil, err
	}
	return redisqueue.NewRedisQueueFromClient(client, options), nil
}

func redisOptionsFromSpec(spec queuecontract.ConnectorConfig) redisqueue.RedisOptions {
	options := redisqueue.RedisOptions{
		Connection: firstNonEmpty(castString(spec.Options["connection"]), "default"),
		Prefix:     firstNonEmpty(castString(spec.Options["prefix"]), spec.Prefix, "prismgo_queue"),
		RetryAfter: spec.RetryAfter,
		BlockFor:   spec.BlockFor,
	}
	return options
}
