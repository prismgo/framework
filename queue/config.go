package queue

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	configpkg "github.com/prismgo/framework/config"
	encodingcontract "github.com/prismgo/framework/contracts/encoding"
	encryptioncontract "github.com/prismgo/framework/contracts/encryption"
	queuecontract "github.com/prismgo/framework/contracts/queue"
	encodingpkg "github.com/prismgo/framework/encoding"
)

// Config 描述队列系统配置。
type Config struct {
	Default string
	// Encoding 是 queue envelope/job payload/failed/batch/chain metadata 的 Payload Encoding 名称。
	//
	// 需求背景：issue 01 只建立配置和严格校验基线，不切换 queue 读写路径。空值表示继承
	// encoding.default，严格装配路径遇到非法名称必须返回错误。
	Encoding    string
	Connections map[string]ConnectionConfig
	FailedTTL   time.Duration
	Failed      StateStoreConfig
	Batching    StateStoreConfig
	Restart     RestartConfig
	// PayloadEncrypter 用于加密 ShouldEncrypt/Encrypt 声明的 Job payload。
	PayloadEncrypter encryptioncontract.Encrypter
}

// StateStoreConfig 描述 failed job provider 或 batch repository 的独立状态存储配置。
type StateStoreConfig struct {
	Driver string
	Store  string
	Prefix string
	TTL    time.Duration
}

// RestartConfig 描述 queue:restart 独立状态源配置。
type RestartConfig struct {
	Cache string
	Key   string
}

// ConnectionConfig 描述单个队列连接。
type ConnectionConfig struct {
	Driver     string
	Queue      string
	Prefix     string
	RetryAfter time.Duration
	BlockFor   time.Duration
	// Options 保存 driver 原始扩展参数，具体解析由 connector/driver 自己完成。
	Options map[string]any

	retryAfterConfigured bool
}

// NewManagerFromConfig 从 config facade 构造 Manager。
//
// 需求背景：service provider 的 lazy factory 需要复用队列配置解析逻辑，
// 但不再暴露旧的 Application 装配命名，避免调用方误以为这是启动注册入口。
func NewManagerFromConfig() (*Manager, error) {
	repo := configpkg.Resolve()
	if repo == nil {
		return nil, fmt.Errorf("queue: config facade not initialized")
	}
	cfg := buildConfigFromRepository(repo)
	codec, err := resolveQueueEncodingCodec(repo)
	if err != nil {
		return nil, fmt.Errorf("queue.encoding: %w", err)
	}
	cfg.Encoding = codec.Name()
	return NewManager(cfg, DefaultRegistry())
}

// BuildConfig 从 Laravel 风格点路径配置读取队列设置。
func BuildConfig() Config {
	return buildConfigFromRepository(configpkg.Resolve())
}

// buildConfigFromRepository 把指定配置仓库转换为队列运行配置。
//
// 设计思路：NewManagerFromConfig 传入严格 Resolve 得到的仓库，BuildConfig 保留旧的
// 进程级默认仓库入口；两个入口共用解析逻辑，避免 provider 路径和便捷路径产生配置差异。
func buildConfigFromRepository(repo *configpkg.Config) Config {
	defaultName := repo.GetString("queue.default", "sync")
	encodingName := resolvedQueueEncodingName(repo)
	failedTTL := time.Duration(repo.GetInt("queue.failed.ttl", 0)) * time.Second
	failed := StateStoreConfig{
		Driver: repo.GetString("queue.failed.driver", "memory"),
		Store:  repo.GetString("queue.failed.store", "default"),
		Prefix: repo.GetString("queue.failed.prefix", "prismgo_queue"),
		TTL:    failedTTL,
	}
	batching := StateStoreConfig{
		Driver: repo.GetString("queue.batching.driver", "memory"),
		Store:  repo.GetString("queue.batching.store", "default"),
		Prefix: repo.GetString("queue.batching.prefix", "prismgo_queue"),
		TTL:    time.Duration(repo.GetInt("queue.batching.ttl", 0)) * time.Second,
	}

	connections := defaultConnectionConfigs(repo, "")
	if parsed := configuredConnectionConfigs(repo.GetStringMap("queue.connections"), ""); len(parsed) > 0 {
		connections = parsed
	}

	return Config{
		Default:     defaultName,
		Encoding:    encodingName,
		Connections: connections,
		FailedTTL:   failedTTL,
		Failed:      failed,
		Batching:    batching,
		Restart: RestartConfig{
			Cache: repo.GetString("queue.restart.cache", ""),
			Key:   repo.GetString("queue.restart.key", "prismgo:queue:restart"),
		},
	}
}

// resolvedQueueEncodingName 让 BuildConfig 便捷入口也呈现继承后的 queue encoding。
//
// 需求背景：QUEUE_ENCODING 为空时应继承 PRISMGO_ENCODING；BuildConfig 没有 error 返回值，
// 因此非法配置仍交给 NewManager/NewManagerFromConfig 的严格装配路径暴露。
func resolvedQueueEncodingName(repo *configpkg.Config) string {
	codec, err := resolveQueueEncodingCodec(repo)
	if err != nil {
		return repo.GetString("queue.encoding", "")
	}
	return codec.Name()
}

// resolveQueueEncodingCodec 解析 queue encoding，并保证 queue 显式配置优先于全局默认。
//
// 参数说明：repo 是 config facade 当前仓库；queue.encoding 非空时直接作为覆盖值解析，
// 为空时才读取 encoding.default。
func resolveQueueEncodingCodec(repo *configpkg.Config) (encodingcontract.Codec, error) {
	queueEncoding := repo.GetString("queue.encoding", "")
	if strings.TrimSpace(queueEncoding) != "" {
		return encodingpkg.Resolve(queueEncoding)
	}
	return encodingpkg.ResolveWithDefault(repo.GetString("encoding.default", encodingpkg.NameMsgpack), "")
}

func buildConnections(cfg Config, codecs ...encodingcontract.Codec) (map[string]ConnectionConfig, queuecontract.Queue, FailedStore, BatchStore, error) {
	// Queue manager 构造连接前先严格校验 Payload Encoding 配置，并把解析后的 codec 注入内置 driver。
	//
	// 设计思路：Redis/RabbitMQ envelope、failed、batch 和 restart 以外的 queue 内部 payload 必须
	// 统一使用同一个 codec；unique/debounce key 和 Redis 计数器仍按各自语义保存，不经过这里。
	var codec encodingcontract.Codec
	if len(codecs) > 0 {
		codec = codecs[0]
	}
	if codec == nil {
		resolved, err := encodingpkg.ResolveWithDefault(encodingpkg.NameMsgpack, cfg.Encoding)
		if err != nil {
			return nil, nil, nil, nil, fmt.Errorf("queue.encoding: %w", err)
		}
		codec = resolved
	}
	connections := cloneConnectionConfigs(cfg.Connections)
	if len(connections) == 0 {
		connections = map[string]ConnectionConfig{"sync": {Driver: "sync"}}
	}

	def := cfg.Default
	if def == "" {
		def = "sync"
	}
	if _, ok := connections[def]; !ok {
		return nil, nil, nil, nil, fmt.Errorf("queue: default connection %q is not configured", def)
	}

	failed, err := buildFailedStore(cfg, codec)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	batch, err := buildBatchStore(cfg, codec)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	return connections, nil, failed, batch, nil
}

// defaultConnectionConfigs 构造未显式声明 queue.connections 时的内置连接清单。
//
// 参数 repo 用于读取默认连接细节；queueName 是连接默认队列名。
func defaultConnectionConfigs(repo *configpkg.Config, queueName string) map[string]ConnectionConfig {
	redisConfig := ConnectionConfig{
		Driver:     "redis",
		Queue:      queueName,
		Prefix:     repo.GetString("queue.connections.redis.prefix", "prismgo_queue"),
		RetryAfter: secondsConfig(repo, "queue.connections.redis.retry_after", 90),
		BlockFor:   secondsConfig(repo, "queue.connections.redis.block_for", 0),
		Options: map[string]any{
			"connection": repo.GetString("queue.connections.redis.connection", "default"),
			"prefix":     repo.GetString("queue.connections.redis.prefix", "prismgo_queue"),
		},
	}
	return map[string]ConnectionConfig{
		"sync": {
			Driver: "sync",
			Queue:  queueName,
		},
		"redis": redisConfig,
		"rabbitmq": {
			Driver:   "rabbitmq",
			Queue:    queueName,
			BlockFor: secondsConfig(repo, "queue.connections.rabbitmq.block_for", 1),
			Options:  map[string]any{},
		},
	}
}

func configuredConnectionConfigs(raw map[string]any, queueName string) map[string]ConnectionConfig {
	if len(raw) == 0 {
		return nil
	}
	connections := make(map[string]ConnectionConfig, len(raw))
	for name, item := range raw {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		spec, ok := item.(map[string]any)
		if !ok {
			continue
		}
		connections[name] = connectionConfigFromMap(spec, queueName)
	}
	return connections
}

func connectionConfigFromMap(spec map[string]any, queueName string) ConnectionConfig {
	prefix := castString(spec["prefix"])
	retryAfter := secondsValue(spec["retry_after"], 0)
	blockFor := secondsValue(spec["block_for"], 0)
	cfg := ConnectionConfig{
		Driver:               castString(spec["driver"]),
		Queue:                firstNonEmpty(castString(spec["queue"]), queueName),
		Prefix:               prefix,
		RetryAfter:           retryAfter,
		BlockFor:             blockFor,
		Options:              cloneAnyMap(spec),
		retryAfterConfigured: mapHasKey(spec, "retry_after"),
	}
	return cfg
}

func secondsConfig(repo *configpkg.Config, path string, fallback int) time.Duration {
	return time.Duration(repo.GetInt(path, fallback)) * time.Second
}

func secondsValue(value any, fallback int) time.Duration {
	return time.Duration(castInt(value, fallback)) * time.Second
}

func castInt(value any, fallback int) int {
	switch typed := value.(type) {
	case int:
		if typed < 0 {
			return fallback
		}
		return typed
	case int64:
		if typed < 0 {
			return fallback
		}
		return int(typed)
	case float64:
		if typed < 0 {
			return fallback
		}
		return int(typed)
	case string:
		return parsePositiveInt(typed, fallback)
	default:
		return parsePositiveInt(castString(value), fallback)
	}
}

// castString 将任意类型转换为字符串。
//
// 支持类型：string、[]byte、fmt.Stringer（如 net.IP、time.Duration 等）。
// 无法识别的类型返回空字符串，避免 panic。
func castString(value any) string {
	if value == nil {
		return ""
	}
	switch typed := value.(type) {
	case string:
		return typed
	case []byte:
		return string(typed)
	case fmt.Stringer:
		return typed.String()
	}
	return ""
}

func castBool(value any, fallback bool) bool {
	switch typed := value.(type) {
	case bool:
		return typed
	case string:
		switch strings.TrimSpace(strings.ToLower(typed)) {
		case "1", "true", "yes", "on":
			return true
		case "0", "false", "no", "off":
			return false
		}
	case int:
		return typed != 0
	case int64:
		return typed != 0
	case float64:
		return typed != 0
	}
	return fallback
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func parsePositiveInt(value string, fallback int) int {
	n, err := strconv.Atoi(strings.TrimSpace(value))
	if err != nil || n < 0 {
		return fallback
	}
	return n
}

func mapHasKey(values map[string]any, key string) bool {
	if len(values) == 0 {
		return false
	}
	_, ok := values[key]
	return ok
}

func cloneConnectionConfigs(input map[string]ConnectionConfig) map[string]ConnectionConfig {
	if len(input) == 0 {
		return nil
	}
	out := make(map[string]ConnectionConfig, len(input))
	for key, value := range input {
		out[key] = cloneConnectionConfig(value)
	}
	return out
}
