package redis

import (
	"fmt"
	"strings"
	"time"

	encodingcontract "github.com/prismgo/framework/contracts/encoding"
	"github.com/prismgo/framework/encoding"
	redisfacade "github.com/prismgo/framework/redis"
	"github.com/redis/go-redis/v9"
)

// RedisOptions 描述 redis 队列连接参数。
type RedisOptions struct {
	// Name 是 queue.connections 中的连接配置 key，用于 Redis 通用事件的 Connection 字段。
	//
	// 需求背景：应用侧通过同一个 queue.UseEventSink 观察多个 Redis 连接实例时，
	// Driver 只能表达后端类型 redis，Connection 必须保留 redis_high/redis_low 等配置名。
	Name       string
	Connection string
	Prefix     string
	RetryAfter time.Duration
	BlockFor   time.Duration
	FailedTTL  time.Duration
	// Codec 是 Redis envelope、failed 和 batch metadata 使用的 queue Payload Encoding。
	//
	// 需求背景：Redis transport 与 Redis state repository 都需要使用同一套 queue Payload Encoding；
	// 直接构造时没有 Manager 注入，因此空值按 msgpack。
	Codec encodingcontract.Codec
}

// ResolveQueueClient 解析 Redis facade 中的底层 *redis.Client。
//
// 参数 options.Connection 对应 queue.connections.redis.connection 或 state repository
// 的 store 配置；该函数集中保持 Redis driver 对 Redis facade 的唯一依赖点。
func ResolveQueueClient(options RedisOptions) (*redis.Client, error) {
	connection := strings.TrimSpace(options.Connection)
	client, err := redisfacade.Client(connection)
	if err == nil {
		if typed, ok := client.(*redis.Client); ok {
			return typed, nil
		}
		err = fmt.Errorf("queue: redis connection %q is %T, want *redis.Client", connection, client)
	}
	return nil, err
}

func cleanPrefix(value string, fallback string) string {
	value = strings.Trim(strings.TrimSpace(value), ":")
	if value == "" {
		return fallback
	}
	return value
}

func defaultDuration(value time.Duration, fallback time.Duration) time.Duration {
	if value > 0 {
		return value
	}
	return fallback
}

func retryAfter(value time.Duration, fallback time.Duration) time.Duration {
	if value > 0 {
		return value
	}
	return fallback
}

func redisConnectionName(name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return "redis"
	}
	return name
}

func redisCodec(codec encodingcontract.Codec) encodingcontract.Codec {
	if codec == nil {
		return encoding.Msgpack()
	}
	return codec
}
