package queue

import (
	"context"
	"fmt"
	"strings"
	"time"

	encodingcontract "github.com/prismgo/framework/contracts/encoding"
	queuecontract "github.com/prismgo/framework/contracts/queue"
	rabbitmqdriver "github.com/prismgo/framework/queue/rabbitmq"
)

// RabbitMQConnector 构造 RabbitMQ transport。
//
// RabbitMQ 不是 Laravel core driver，但在 Prismgo 中仍通过同一 Queue contract
// 暴露传输能力；failed/batch/restart state 不从 AMQP 配置派生。
type RabbitMQConnector struct {
	codec encodingcontract.Codec
}

func (c RabbitMQConnector) Connect(_ context.Context, name string, config map[string]any) (queuecontract.Queue, error) {
	spec, err := connectorSpec(name, config)
	if err != nil {
		return nil, err
	}
	if spec.retryAfterConfigured && spec.RetryAfter > 0 {
		return nil, fmt.Errorf("queue: connection %q: %w", name, ErrUnsupportedRetryAfter)
	}
	return rabbitmqdriver.NewRabbitMQQueue(name, rabbitMQOptionsFromMap(spec.Options), c.codec, spec.BlockFor)
}

func rabbitMQOptionsFromMap(spec map[string]any) rabbitmqdriver.Options {
	options := rabbitmqdriver.Options{}
	if dialer, ok := spec["dialer"].(rabbitmqdriver.Dialer); ok {
		options.Dialer = dialer
	}
	options.URL = castString(spec["url"])
	options.Scheme = castString(spec["scheme"])
	options.Host = castString(spec["host"])
	options.Port = castString(spec["port"])
	options.Username = castString(spec["username"])
	options.Password = castString(spec["password"])
	options.VHost = castString(spec["vhost"])
	options.Exchange = castString(spec["exchange"])
	options.ExchangeType = castString(spec["exchange_type"])
	if _, ok := spec["declare"]; ok {
		options.Declare = rabbitmqdriver.Bool(castBool(spec["declare"], false))
	}
	if _, ok := spec["exchange_durable"]; ok {
		options.ExchangeDurable = rabbitmqdriver.Bool(castBool(spec["exchange_durable"], false))
	}
	if _, ok := spec["queue_durable"]; ok {
		options.QueueDurable = rabbitmqdriver.Bool(castBool(spec["queue_durable"], false))
	}
	options.QueueMaxPriority = castInt(spec["queue_max_priority"], 0)
	if _, ok := spec["message_persistent"]; ok {
		options.MessagePersistent = rabbitmqdriver.Bool(castBool(spec["message_persistent"], false))
	}
	if _, ok := spec["auto_delete"]; ok {
		options.AutoDelete = rabbitmqdriver.Bool(castBool(spec["auto_delete"], false))
	}
	if _, ok := spec["exclusive"]; ok {
		options.Exclusive = rabbitmqdriver.Bool(castBool(spec["exclusive"], false))
	}
	if _, ok := spec["no_wait"]; ok {
		options.NoWait = rabbitmqdriver.Bool(castBool(spec["no_wait"], false))
	}
	if _, ok := spec["confirm"]; ok {
		options.Confirm = rabbitmqdriver.Bool(castBool(spec["confirm"], false))
	}
	options.DelayMode = castString(spec["delay_mode"])
	options.DelayBuckets = castDurationBuckets(spec["delay_buckets"], nil)
	options.Prefetch = castInt(spec["prefetch"], 0)
	options.Heartbeat = secondsValue(spec["heartbeat"], 0)
	options.PublishTimeout = secondsValue(spec["publish_timeout"], 0)
	options.PublishChannels = castInt(spec["publish_channels"], 0)
	options.ReconnectMinDelay = castDurationValue(spec["reconnect_min_delay"], 0)
	options.ReconnectMaxDelay = castDurationValue(spec["reconnect_max_delay"], 0)
	options.RestartQueue = castString(spec["restart_queue"])
	if _, ok := spec["restart_enabled"]; ok {
		options.RestartEnabled = rabbitmqdriver.Bool(castBool(spec["restart_enabled"], false))
	}
	options.RestartPollInterval = castDurationValue(spec["restart_poll_interval"], options.RestartPollInterval)
	options.TopologyCacheTTL = castDurationValue(spec["topology_cache_ttl"], 0)
	options.TopologyCacheMaxEntries = castInt(spec["topology_cache_max_entries"], 0)
	return options
}

func castDurationValue(value any, fallback time.Duration) time.Duration {
	switch typed := value.(type) {
	case time.Duration:
		if typed > 0 {
			return typed
		}
	case int:
		if typed > 0 {
			return time.Duration(typed) * time.Second
		}
	case int64:
		if typed > 0 {
			return time.Duration(typed) * time.Second
		}
	case float64:
		if typed > 0 {
			return time.Duration(typed * float64(time.Second))
		}
	case string:
		trimmed := strings.TrimSpace(typed)
		if trimmed == "" {
			return fallback
		}
		if parsed, err := time.ParseDuration(trimmed); err == nil && parsed > 0 {
			return parsed
		}
		if seconds := parsePositiveInt(trimmed, 0); seconds > 0 {
			return time.Duration(seconds) * time.Second
		}
	default:
		if seconds := castInt(value, 0); seconds > 0 {
			return time.Duration(seconds) * time.Second
		}
	}
	return fallback
}

func castDurationBuckets(value any, fallback []time.Duration) []time.Duration {
	var out []time.Duration
	switch typed := value.(type) {
	case []time.Duration:
		out = append([]time.Duration(nil), typed...)
	case []int:
		for _, item := range typed {
			if item > 0 {
				out = append(out, time.Duration(item)*time.Second)
			}
		}
	case []any:
		for _, item := range typed {
			seconds := castInt(item, 0)
			if seconds > 0 {
				out = append(out, time.Duration(seconds)*time.Second)
			}
		}
	case string:
		for _, part := range strings.Split(typed, ",") {
			seconds := parsePositiveInt(part, 0)
			if seconds > 0 {
				out = append(out, time.Duration(seconds)*time.Second)
			}
		}
	default:
		if seconds := castInt(value, 0); seconds > 0 {
			out = append(out, time.Duration(seconds)*time.Second)
		}
	}
	if len(out) == 0 {
		return append([]time.Duration(nil), fallback...)
	}
	return out
}
