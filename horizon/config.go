package horizon

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	configpkg "github.com/prismgo/framework/config"
	encodingpkg "github.com/prismgo/framework/encoding"
)

const (
	// BalanceFalse 表示 supervisor 不执行自动均衡，仅按固定进程数消费队列。
	BalanceFalse = "false"
	// BalanceSimple 表示后续 supervisor 可使用简单均衡策略。
	BalanceSimple = "simple"
	// BalanceAuto 表示后续 supervisor 可使用自动均衡策略。
	BalanceAuto = "auto"

	// AutoScalingStrategyTime 表示按 Laravel Horizon 的 time workload 分配 auto worker。
	AutoScalingStrategyTime = "time"
	// AutoScalingStrategySize 表示按 Laravel Horizon 的 ready job 数量分配 auto worker。
	AutoScalingStrategySize = "size"

	// ObservabilityPresetFull 保持 Horizon 开发期完整观测体验。
	ObservabilityPresetFull = "full"
	// ObservabilityPresetProductionLight 保留核心健康、队列长度和队列级聚合，关闭 per-job 明细。
	ObservabilityPresetProductionLight = "production_light"
	// ObservabilityPresetMinimal 只保留核心健康与队列长度能力。
	ObservabilityPresetMinimal = "minimal"

	// ObservabilityDropOldest 表示 buffer 满时优先丢弃最旧的低价值观测数据。
	ObservabilityDropOldest = "drop_oldest"
	// ObservabilityDropNewest 表示 buffer 满时丢弃当前入队的低价值观测数据。
	ObservabilityDropNewest = "drop_newest"
)

// ObservabilityFeature 表示 Horizon 运行时可独立判断的观测能力名称。
// 需求背景：issue 20 要求后续 collector、snapshot 和 Dashboard 不要散落读取原始配置字段，
// 而是通过统一能力入口判断成本开关。
type ObservabilityFeature string

const (
	// ObservabilityEventMetrics 表示事件派生队列级 counters 与 runtime 聚合，不是 collector 订阅总闸。
	ObservabilityEventMetrics ObservabilityFeature = "event_metrics"
	// ObservabilityWaits 表示 queued_at 等待时间观测与长等待事件能力。
	ObservabilityWaits ObservabilityFeature = "waits"
	// ObservabilityBatchSummaries 表示 BatchEvent 的 Horizon 只读摘要投影。
	ObservabilityBatchSummaries ObservabilityFeature = "batch_summaries"
	// ObservabilityProcessHealth 表示 master/supervisor/worker heartbeat 与控制状态。
	ObservabilityProcessHealth ObservabilityFeature = "process_health"
	// ObservabilityQueueLengths 表示后端队列长度采样与持久化能力。
	ObservabilityQueueLengths ObservabilityFeature = "queue_lengths"
	// ObservabilityHighValueDetail 表示失败、poison 或慢任务安全摘要的高价值诊断通道。
	ObservabilityHighValueDetail ObservabilityFeature = "high_value_detail"
	// ObservabilityFailedDetail 表示 failed job 的 High-value Horizon job detail 诊断通道。
	ObservabilityFailedDetail ObservabilityFeature = "failed_detail"
	// ObservabilityPoisonDetail 表示 poison envelope 的 High-value Horizon job detail 诊断通道。
	ObservabilityPoisonDetail ObservabilityFeature = "poison_detail"
	// ObservabilitySlowJobDetail 表示超过阈值的慢任务 High-value Horizon job detail 诊断通道。
	ObservabilitySlowJobDetail ObservabilityFeature = "slow_job_detail"
	// ObservabilityProcessingSpans 表示用于 runtime 聚合关联的短期 per-job processing span。
	ObservabilityProcessingSpans ObservabilityFeature = "processing_spans"
)

// ConfigReader 是 Horizon 配置解析需要的最小配置读取接口。
// 设计思路：Horizon 的配置解析可以接收显式 ConfigReader，测试和独立工具不需要污染全局
// config facade；生产启动路径再通过 LoadConfig 读取当前应用配置。
type ConfigReader interface {
	GetString(path string, defaultValue ...any) string
	GetStringMap(path string) map[string]any
}

// Config 是 Horizon 的静态配置视图。
// 需求背景：issue 01 只建立配置和命令注册基线，因此该结构只保存可展示、可校验的静态字段，
// 不包含 worker、metrics 或运行期 Store 状态。
type Config struct {
	// Path 是 Horizon Dashboard 与内部只读 API 的挂载前缀，默认 horizon，不包含首尾斜杠。
	Path string
	// Environment 是当前 Horizon 使用的环境名称，来自 horizon.environment/app.env/APP_ENV fallback。
	Environment string
	// Store 是 horizon.use 解析出的 Store 类型字符串；issue 01 只展示该值，不实例化真实 Store。
	Store string
	// Connection 是 Horizon Redis Store 使用的 database.redis.{connection} 配置名，独立于 queue/cache/session。
	Connection string
	// Prefix 是 Horizon 后续写入 Store 时使用的 key 前缀；当前切片只用于配置展示。
	Prefix string
	// Encoding 是 Horizon store record 的 Payload Encoding 名称。
	//
	// 需求背景：issue 01 只建立配置和严格校验基线，不切换 Horizon store records 的读写路径。
	// 空值表示继承 encoding.default，严格配置解析会归一为 msgpack 或 json。
	Encoding string
	// HeartbeatTTL 控制 supervisor/worker heartbeat 被视为存活的时间窗口。
	HeartbeatTTL time.Duration
	// LoopInterval 控制 master/supervisor 长驻循环的轮询与 heartbeat 节奏，测试可显式缩短。
	LoopInterval time.Duration
	// Waits 按 connection:queue 保存长等待阈值秒数，供 waits 观测复用。
	Waits map[string]int
	// Observability 保存大规模任务量下的观测成本能力开关与 per-job 状态上限。
	Observability ObservabilityConfig
	// FastTermination 控制 terminate 时是否允许新 master 先启动，默认 false。
	FastTermination bool
	// MemoryLimit 是 Horizon worker 默认内存限制 MB。
	MemoryLimit int
	// Watch 保存 horizon:listen 应监听的相对路径列表。
	Watch []string
	// Supervisors 保存 defaults 与当前 environment 合并后的 supervisor 配置视图。
	Supervisors map[string]SupervisorConfig
}

// ObservabilityConfig 保存 Horizon 观测成本控制配置。
// 使用方式：运行期通过 Enabled 判断能力是否可用；不要在 snapshot 里直接读取布尔字段。
type ObservabilityConfig struct {
	// Preset 是 full、production_light 或 minimal，显式字段覆盖 preset。
	Preset string
	// EventMetrics 控制事件派生的队列级 counters 和 runtime 聚合。
	EventMetrics bool
	// Waits 控制 queued wait 状态、wait snapshot 和 long wait 事件。
	Waits bool
	// BatchSummaries 控制 BatchEvent 的 Horizon 摘要投影和持久化。
	BatchSummaries bool
	// ProcessHealth 控制 heartbeat、状态和控制标记这类核心健康能力。
	ProcessHealth bool
	// QueueLengths 控制 queue backend 长度采样与保存。
	QueueLengths bool
	// QueuedWaitsMax 限制等待时间 per-job 状态数量；小于等于 0 时不保留 queued wait 状态。
	QueuedWaitsMax int
	// ProcessingSpansMax 限制 processing span 数量；0 表示不保存 processing span。
	ProcessingSpansMax int
	// ProcessingCleanupIntervalSeconds 控制 processing span TTL 清理节流间隔秒数。
	ProcessingCleanupIntervalSeconds int
	// MetricsWindow 控制 event_metrics 的事件时间聚合桶宽度，独立于 flush_interval。
	MetricsWindow time.Duration
	// FlushInterval 控制后续 async flusher 的常规批量落盘间隔。
	FlushInterval time.Duration
	// FlushTimeout 控制优雅退出或周期 flush 的单次 Store 写入等待上限。
	FlushTimeout time.Duration
	// BatchSize 控制一次 flush 最多写入的观测增量或明细数量。
	BatchSize int
	// BatchSummarySize 控制 collector/flusher 一次最多保留或写入的 batch summary 数量。
	BatchSummarySize int
	// BufferSize 控制 collector 与 flusher 之间的有界内存 buffer 条数。
	BufferSize int
	// MaxEventsPerSecond 控制 collector 入口的可选速率上限，0 表示不额外限速。
	MaxEventsPerSecond int
	// DropPolicy 控制 buffer 满或降级时低价值观测数据的丢弃策略。
	DropPolicy string
	// EventMetricsSampleRate 控制 queue event 进入 event_metrics 观测管线的比例，允许显式 0。
	EventMetricsSampleRate float64
	// HighValueDetailSampleRate 是高价值诊断明细的可选采样率；nil 时回落到当前实际 event_metrics 采样率。
	HighValueDetailSampleRate *float64
	// SampleReservoirSize 限制 runtime/P95/P99 估算所需样本池大小。
	SampleReservoirSize int
	// MaxAggregateKeys 限制 connection+queue/job type 等聚合 key 基数。
	MaxAggregateKeys int
	// AggregateKeyTTL 控制低活跃聚合 key 的过期窗口，避免 key 集合长期增长。
	AggregateKeyTTL time.Duration
	// EventMetricsRetention 控制 event_metrics window 聚合数据的保留期。
	EventMetricsRetention time.Duration
	// HighValueDetailRetention 控制 High-value Horizon job detail 诊断摘要的保留期。
	HighValueDetailRetention time.Duration
	// BatchSummaryRetention 控制 batch summary 只读摘要的保留期。
	BatchSummaryRetention time.Duration
	// DiagnosticsRetention 控制 drop/degradation 诊断数据的保留期。
	DiagnosticsRetention time.Duration
	// FailedDetailEnabled 控制 failed job 安全摘要是否进入高价值明细通道。
	FailedDetailEnabled bool
	// PoisonDetailEnabled 控制 poison envelope 安全摘要是否进入高价值明细通道。
	PoisonDetailEnabled bool
	// SlowJobDetailEnabled 控制超过阈值的慢任务摘要是否进入高价值明细通道。
	SlowJobDetailEnabled bool
	// SlowJobThreshold 定义慢任务诊断阈值。
	SlowJobThreshold time.Duration
	// DynamicSamplingEnabled 控制后续 sampling policy 是否可按压力降低实际采样率。
	DynamicSamplingEnabled bool
	// MinSampleRate 控制动态采样降级时的最低采样率，显式 event_metrics_sample_rate=0 时仍保持 0。
	MinSampleRate float64
}

// Enabled 返回指定观测能力的最终可用性。
// 逻辑说明：该方法集中处理默认 full、event_metrics 派生能力和上限为 0 的关闭语义。
func (c ObservabilityConfig) Enabled(feature ObservabilityFeature) bool {
	c = normalizeObservabilityConfig(c)
	switch feature {
	case ObservabilityEventMetrics:
		return c.EventMetrics
	case ObservabilityWaits:
		return c.EventMetrics && c.Waits && c.QueuedWaitsMax > 0
	case ObservabilityBatchSummaries:
		return c.BatchSummaries
	case ObservabilityProcessHealth:
		return c.ProcessHealth
	case ObservabilityQueueLengths:
		return c.QueueLengths
	case ObservabilityHighValueDetail:
		return c.EffectiveHighValueDetailSampleRate(c.EventMetricsSampleRate) > 0 &&
			(c.FailedDetailEnabled || c.PoisonDetailEnabled || c.SlowJobDetailEnabled)
	case ObservabilityFailedDetail:
		return c.FailedDetailEnabled && c.EffectiveHighValueDetailSampleRate(c.EventMetricsSampleRate) > 0
	case ObservabilityPoisonDetail:
		return c.PoisonDetailEnabled && c.EffectiveHighValueDetailSampleRate(c.EventMetricsSampleRate) > 0
	case ObservabilitySlowJobDetail:
		return c.SlowJobDetailEnabled && c.SlowJobThreshold > 0 &&
			c.EffectiveHighValueDetailSampleRate(c.EventMetricsSampleRate) > 0
	case ObservabilityProcessingSpans:
		return c.EventMetrics && c.ProcessingSpansMax > 0
	default:
		return false
	}
}

// EffectiveHighValueDetailSampleRate 返回高价值明细通道当前应使用的采样率。
// 参数说明：currentEventMetricsRate 是动态采样后实际 event_metrics 采样率；显式
// high_value_detail_sample_rate 缺失时使用该值，避免高价值明细默认绕过全局采样。
func (c ObservabilityConfig) EffectiveHighValueDetailSampleRate(currentEventMetricsRate float64) float64 {
	if c.HighValueDetailSampleRate != nil {
		return clampSampleRate(*c.HighValueDetailSampleRate)
	}
	return clampSampleRate(currentEventMetricsRate)
}

// SupervisorConfig 是单个 Horizon supervisor 的规范化配置。
// 使用方式：配置解析完成后，horizon:list 直接读取该结构展示静态配置；后续 issue 的 worker
// 启动逻辑也会以该结构作为 supervisor 构造输入。
// 设计原因：把来自 .env、map 和数组的宽松输入收敛为强类型字段，避免长驻 worker 以错误配置启动。
type SupervisorConfig struct {
	// Name 是 supervisor 配置项名称，通常来自 defaults/environments 下的 key。
	Name string
	// Connection 是 queue connection 名称，后续用于解析队列连接。
	Connection string
	// Queues 是规范化后的队列名称列表，已去空白、去重并保持配置顺序。
	Queues []string
	// Balance 是负载均衡策略，只允许 false/simple/auto。
	Balance string
	// MinProcesses 是 supervisor 最小 worker 数。
	MinProcesses int
	// MaxProcesses 是 supervisor 最大 worker 数，必须大于等于 MinProcesses。
	MaxProcesses int
	// Tries 是单个任务最大尝试次数。
	Tries int
	// Timeout 是单个任务执行超时秒数。
	Timeout int
	// Sleep 是空队列轮询间隔秒数。
	Sleep int
	// MaxJobs 是 worker 达到指定任务数后的退出阈值，0 表示不限制。
	MaxJobs int
	// MaxTime 是 worker 达到指定运行秒数后的退出阈值，0 表示不限制。
	MaxTime int
	// RetryAfter 是任务可重试时间窗口秒数。
	RetryAfter int
	// Backoff 是失败重试退避秒数列表。
	Backoff []int
	// StopWhenEmpty 表示队列为空时 worker 是否退出。
	StopWhenEmpty bool
	// Memory 是兼容 Horizon 配置的内存限制字段，当前切片只解析和展示。
	Memory int
	// Nice 是兼容 Horizon 配置的进程优先级字段，当前 Go 实现只解析和展示。
	Nice int
	// AutoScalingStrategy 控制 auto balance 使用 time 还是 size workload，默认对齐 Laravel 的 time。
	AutoScalingStrategy string
	// BalanceMaxShift 限制 auto balance 单次扩缩容移动的 worker 数，默认 1。
	BalanceMaxShift int
	// BalanceCooldown 限制两次 auto balance 计算之间的最小间隔秒数，默认 3。
	BalanceCooldown int
}

// LoadConfig 从当前全局 config facade 解析 Horizon 静态配置。
// 需求背景：应用命令路径需要一个无参数入口，便于 horizon:list 从当前 Application 配置仓库读取
// horizon 命名空间。该函数只解析配置，不解析 queue/event/store 等运行期资源。
func LoadConfig() (Config, error) {
	return LoadConfigFrom(configpkg.Resolve())
}

// LoadConfigFrom 从显式配置源解析 Horizon 静态配置。
// 参数说明：source 是配置读取器，测试可传入 fake，生产默认传入 config facade。nil 会回退到
// configpkg.Resolve()。返回值是已经完成环境选择、defaults 合并和严格校验的配置视图。
func LoadConfigFrom(source ConfigReader) (Config, error) {
	if source == nil {
		source = configpkg.Resolve()
	}
	if err := rejectRemovedHorizonEnv(); err != nil {
		return Config{}, err
	}
	root := source.GetStringMap("horizon")
	if err := rejectRemovedRootConfig(root); err != nil {
		return Config{}, err
	}
	environment := resolveEnvironment(source, root)
	defaults := stringAnyMap(root["defaults"])
	environments := stringAnyMap(root["environments"])
	current, err := selectEnvironmentConfig(environment, environments)
	if err != nil {
		return Config{}, err
	}

	supervisors, err := parseSupervisors(defaults, current)
	if err != nil {
		return Config{}, err
	}

	observability, err := parseObservabilityConfig(stringAnyMap(root["observability"]))
	if err != nil {
		return Config{}, err
	}
	// Horizon 当前切片只解析静态配置，但仍需要在配置层严格校验 Payload Encoding。
	//
	// 需求背景：后续 Horizon store records 接入时会复用同一个字段；提前在 LoadConfigFrom
	// 中校验可以让 horizon:list、provider 或 manager 路径都一致暴露非法 HORIZON_ENCODING。
	resolvedEncoding, err := encodingpkg.ResolveWithDefault(source.GetString("encoding.default", encodingpkg.NameMsgpack), firstString(root["encoding"], ""))
	if err != nil {
		return Config{}, fmt.Errorf("horizon.encoding: %w", err)
	}

	return Config{
		Path:        normalizePath(firstString(root["path"], "horizon")),
		Environment: environment,
		Store:       firstString(root["use"], "redis"),
		Connection:  firstString(root["connection"], "default"),
		Prefix:      firstString(root["prefix"], "prismgo_horizon"),
		Encoding:    resolvedEncoding.Name(),
		HeartbeatTTL: time.Duration(firstPositiveInt(root["heartbeat_ttl_seconds"], 60)) *
			time.Second,
		Waits:           parseWaits(stringAnyMap(root["waits"])),
		Observability:   observability,
		FastTermination: firstBool(root["fast_termination"], false),
		MemoryLimit:     firstPositiveInt(root["memory_limit"], 128),
		Watch:           parseStringList(root["watch"]),
		Supervisors:     supervisors,
	}, nil
}

// DashboardPath 返回 Horizon Dashboard 的绝对路径。
func (c Config) DashboardPath() string {
	path := normalizePath(c.Path)
	if path == "" {
		path = "horizon"
	}
	return "/" + path
}

// APIPrefix 返回 Horizon 内部只读 API 的绝对路径前缀。
func (c Config) APIPrefix() string {
	return c.DashboardPath() + "/api"
}

// resolveEnvironment 按 horizon.environment -> app.env -> APP_ENV -> production 的顺序解析环境名。
// 设计思路：horizon.environment 允许 Horizon 独立覆盖应用环境；app.env 是项目级配置来源；
// APP_ENV 是缺失 app.env 时的兼容 fallback。
func resolveEnvironment(source ConfigReader, root map[string]any) string {
	if env := strings.TrimSpace(firstString(root["environment"], "")); env != "" {
		return env
	}
	if env := strings.TrimSpace(source.GetString("app.env")); env != "" {
		return env
	}
	if env := strings.TrimSpace(os.Getenv("APP_ENV")); env != "" {
		return env
	}
	return "production"
}

// selectEnvironmentConfig 按 Laravel Str::is 风格选择当前环境配置：精确匹配优先，再匹配通配模式。
func selectEnvironmentConfig(environment string, environments map[string]any) (map[string]any, error) {
	environment = strings.TrimSpace(environment)
	if environment == "" {
		environment = "production"
	}
	if len(environments) == 0 {
		return nil, nil
	}
	if current, ok := environments[environment]; ok {
		return stringAnyMap(current), nil
	}
	bestPattern := ""
	var best map[string]any
	for pattern, raw := range environments {
		pattern = strings.TrimSpace(pattern)
		if pattern == "" || pattern == environment || !strIs(pattern, environment) {
			continue
		}
		if bestPattern == "" || wildcardSpecificity(pattern) > wildcardSpecificity(bestPattern) {
			bestPattern = pattern
			best = stringAnyMap(raw)
		}
	}
	if bestPattern == "" {
		return nil, fmt.Errorf("horizon: no environment configuration matches %q", environment)
	}
	return best, nil
}

// strIs 实现 Laravel Str::is 的最小通配语义，支持 * 和 prod-* 这类配置键。
func strIs(pattern string, value string) bool {
	pattern = strings.TrimSpace(pattern)
	value = strings.TrimSpace(value)
	if pattern == "*" {
		return true
	}
	if !strings.Contains(pattern, "*") {
		return pattern == value
	}
	parts := strings.Split(pattern, "*")
	position := 0
	for i, part := range parts {
		if part == "" {
			continue
		}
		index := strings.Index(value[position:], part)
		if index < 0 {
			return false
		}
		if i == 0 && !strings.HasPrefix(value, part) {
			return false
		}
		position += index + len(part)
	}
	last := parts[len(parts)-1]
	return last == "" || strings.HasSuffix(value, last)
}

func wildcardSpecificity(pattern string) int {
	return len(strings.ReplaceAll(pattern, "*", ""))
}

func normalizePath(value string) string {
	value = strings.TrimSpace(value)
	value = strings.Trim(value, "/")
	if value == "" {
		return "horizon"
	}
	return value
}

func parseWaits(raw map[string]any) map[string]int {
	out := make(map[string]int, len(raw))
	for key, value := range raw {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		if n, ok := parseNonNegativeInt(value); ok {
			out[key] = n
		}
	}
	return out
}

// parseObservabilityConfig 解析 Horizon 观测成本配置。
// 逻辑说明：preset 只提供默认值，所有显式子字段都会继续严格解析并覆盖 preset。
// 这样业务可以从 production_light/minimal 出发逐项打开诊断能力，同时配置拼写错误会 fail fast。
func parseObservabilityConfig(raw map[string]any) (ObservabilityConfig, error) {
	cfg, _ := observabilityPresetConfig(ObservabilityPresetFull)
	if preset := strings.TrimSpace(firstString(raw["preset"], "")); preset != "" {
		selected, err := observabilityPresetConfig(preset)
		if err != nil {
			return ObservabilityConfig{}, err
		}
		cfg = selected
	}

	if err := rejectUnknownObservabilityFields(raw); err != nil {
		return ObservabilityConfig{}, err
	}

	// Parse bool fields
	if err := parseBoolFields(raw, map[string]*bool{
		"event_metrics":            &cfg.EventMetrics,
		"waits":                    &cfg.Waits,
		"batch_summaries":          &cfg.BatchSummaries,
		"process_health":           &cfg.ProcessHealth,
		"queue_lengths":            &cfg.QueueLengths,
		"failed_detail_enabled":    &cfg.FailedDetailEnabled,
		"poison_detail_enabled":    &cfg.PoisonDetailEnabled,
		"slow_job_detail_enabled":  &cfg.SlowJobDetailEnabled,
		"dynamic_sampling_enabled": &cfg.DynamicSamplingEnabled,
	}); err != nil {
		return ObservabilityConfig{}, err
	}

	// Parse int fields (non-negative)
	if err := parseIntFields(raw, map[string]*int{
		"queued_waits_max":                    &cfg.QueuedWaitsMax,
		"processing_spans_max":                &cfg.ProcessingSpansMax,
		"processing_cleanup_interval_seconds": &cfg.ProcessingCleanupIntervalSeconds,
		"max_events_per_second":               &cfg.MaxEventsPerSecond,
	}, strictNonNegativeInt); err != nil {
		return ObservabilityConfig{}, err
	}

	// Parse positive int fields
	if err := parseIntFields(raw, map[string]*int{
		"batch_size":            &cfg.BatchSize,
		"batch_summary_size":    &cfg.BatchSummarySize,
		"buffer_size":           &cfg.BufferSize,
		"sample_reservoir_size": &cfg.SampleReservoirSize,
		"max_aggregate_keys":    &cfg.MaxAggregateKeys,
	}, strictPositiveInt); err != nil {
		return ObservabilityConfig{}, err
	}

	if _, ok := raw["batch_summary_size"]; !ok {
		cfg.BatchSummarySize = cfg.BatchSize
	}

	// Parse duration fields
	if err := parseDurationFields(raw, map[string]*time.Duration{
		"metrics_window":              &cfg.MetricsWindow,
		"flush_interval":              &cfg.FlushInterval,
		"flush_timeout":               &cfg.FlushTimeout,
		"aggregate_key_ttl":           &cfg.AggregateKeyTTL,
		"event_metrics_retention":     &cfg.EventMetricsRetention,
		"high_value_detail_retention": &cfg.HighValueDetailRetention,
		"batch_summary_retention":     &cfg.BatchSummaryRetention,
		"diagnostics_retention":       &cfg.DiagnosticsRetention,
	}); err != nil {
		return ObservabilityConfig{}, err
	}

	// Parse special fields
	if err := parseSpecialObservabilityFields(raw, &cfg); err != nil {
		return ObservabilityConfig{}, err
	}

	return normalizeObservabilityConfig(cfg), nil
}

// parseBoolFields parses a map of bool field names to their target pointers.
func parseBoolFields(raw map[string]any, fields map[string]*bool) error {
	for field, target := range fields {
		value, ok := raw[field]
		if !ok {
			continue
		}
		parsed, err := strictBool("horizon: observability."+field, value)
		if err != nil {
			return err
		}
		*target = parsed
	}
	return nil
}

// parseIntFields parses a map of int field names to their target pointers using the provided validator.
func parseIntFields(raw map[string]any, fields map[string]*int, validator func(string, any) (int, error)) error {
	for field, target := range fields {
		value, ok := raw[field]
		if !ok {
			continue
		}
		parsed, err := validator("horizon: observability."+field, value)
		if err != nil {
			return err
		}
		*target = parsed
	}
	return nil
}

// parseDurationFields parses a map of duration field names to their target pointers.
func parseDurationFields(raw map[string]any, fields map[string]*time.Duration) error {
	for field, target := range fields {
		value, ok := raw[field]
		if !ok {
			continue
		}
		parsed, err := strictPositiveDuration("horizon: observability."+field, value)
		if err != nil {
			return err
		}
		*target = parsed
	}
	return nil
}

// parseSpecialObservabilityFields parses fields that require custom validation logic.
func parseSpecialObservabilityFields(raw map[string]any, cfg *ObservabilityConfig) error {
	if value, ok := raw["slow_job_threshold"]; ok {
		parsed, err := strictNonNegativeDuration("horizon: observability.slow_job_threshold", value)
		if err != nil {
			return err
		}
		cfg.SlowJobThreshold = parsed
	}

	if value, ok := raw["event_metrics_sample_rate"]; ok {
		parsed, err := strictSampleRate("horizon: observability.event_metrics_sample_rate", value)
		if err != nil {
			return err
		}
		cfg.EventMetricsSampleRate = parsed
	}

	if value, ok := raw["high_value_detail_sample_rate"]; ok {
		if text, isString := value.(string); !isString || strings.TrimSpace(text) != "" {
			parsed, err := strictSampleRate("horizon: observability.high_value_detail_sample_rate", value)
			if err != nil {
				return err
			}
			cfg.HighValueDetailSampleRate = &parsed
		}
	}

	if value, ok := raw["min_sample_rate"]; ok {
		parsed, err := strictSampleRate("horizon: observability.min_sample_rate", value)
		if err != nil {
			return err
		}
		cfg.MinSampleRate = parsed
	}

	if value, ok := raw["drop_policy"]; ok {
		parsed, err := strictDropPolicy("horizon: observability.drop_policy", value)
		if err != nil {
			return err
		}
		cfg.DropPolicy = parsed
	}

	return nil
}

func observabilityPresetConfig(preset string) (ObservabilityConfig, error) {
	switch preset {
	case "", ObservabilityPresetFull:
		return ObservabilityConfig{
			Preset:                           ObservabilityPresetFull,
			EventMetrics:                     true,
			Waits:                            true,
			BatchSummaries:                   true,
			ProcessHealth:                    true,
			QueueLengths:                     true,
			QueuedWaitsMax:                   10000,
			ProcessingSpansMax:               10000,
			ProcessingCleanupIntervalSeconds: 60,
			MetricsWindow:                    time.Minute,
			FlushInterval:                    time.Minute,
			FlushTimeout:                     5 * time.Second,
			BatchSize:                        500,
			BatchSummarySize:                 500,
			BufferSize:                       10000,
			DropPolicy:                       ObservabilityDropOldest,
			EventMetricsSampleRate:           1,
			SampleReservoirSize:              2048,
			MaxAggregateKeys:                 10000,
			AggregateKeyTTL:                  30 * time.Minute,
			EventMetricsRetention:            24 * time.Hour,
			HighValueDetailRetention:         24 * time.Hour,
			BatchSummaryRetention:            24 * time.Hour,
			DiagnosticsRetention:             24 * time.Hour,
			FailedDetailEnabled:              true,
			PoisonDetailEnabled:              true,
			SlowJobDetailEnabled:             true,
			SlowJobThreshold:                 30 * time.Second,
			DynamicSamplingEnabled:           true,
			MinSampleRate:                    0.01,
		}, nil
	case ObservabilityPresetProductionLight:
		return ObservabilityConfig{
			Preset:                           ObservabilityPresetProductionLight,
			EventMetrics:                     true,
			ProcessHealth:                    true,
			QueueLengths:                     true,
			QueuedWaitsMax:                   0,
			ProcessingSpansMax:               1000,
			ProcessingCleanupIntervalSeconds: 60,
			MetricsWindow:                    time.Minute,
			FlushInterval:                    time.Minute,
			FlushTimeout:                     5 * time.Second,
			BatchSize:                        500,
			BatchSummarySize:                 500,
			BufferSize:                       5000,
			DropPolicy:                       ObservabilityDropOldest,
			EventMetricsSampleRate:           0.1,
			SampleReservoirSize:              1024,
			MaxAggregateKeys:                 5000,
			AggregateKeyTTL:                  30 * time.Minute,
			EventMetricsRetention:            24 * time.Hour,
			HighValueDetailRetention:         24 * time.Hour,
			BatchSummaryRetention:            12 * time.Hour,
			DiagnosticsRetention:             24 * time.Hour,
			FailedDetailEnabled:              true,
			PoisonDetailEnabled:              true,
			SlowJobDetailEnabled:             true,
			SlowJobThreshold:                 30 * time.Second,
			DynamicSamplingEnabled:           true,
			MinSampleRate:                    0.01,
		}, nil
	case ObservabilityPresetMinimal:
		return ObservabilityConfig{
			Preset:                           ObservabilityPresetMinimal,
			ProcessHealth:                    true,
			QueueLengths:                     true,
			QueuedWaitsMax:                   0,
			ProcessingSpansMax:               0,
			ProcessingCleanupIntervalSeconds: 60,
			MetricsWindow:                    time.Minute,
			FlushInterval:                    time.Minute,
			FlushTimeout:                     5 * time.Second,
			BatchSize:                        250,
			BatchSummarySize:                 250,
			BufferSize:                       1000,
			DropPolicy:                       ObservabilityDropOldest,
			EventMetricsSampleRate:           0,
			SampleReservoirSize:              512,
			MaxAggregateKeys:                 1000,
			AggregateKeyTTL:                  30 * time.Minute,
			EventMetricsRetention:            12 * time.Hour,
			HighValueDetailRetention:         12 * time.Hour,
			BatchSummaryRetention:            12 * time.Hour,
			DiagnosticsRetention:             24 * time.Hour,
			SlowJobThreshold:                 30 * time.Second,
			DynamicSamplingEnabled:           true,
			MinSampleRate:                    0,
		}, nil
	default:
		return ObservabilityConfig{}, fmt.Errorf("horizon: observability.preset %q is invalid", preset)
	}
}

func normalizeObservabilityConfig(cfg ObservabilityConfig) ObservabilityConfig {
	if strings.TrimSpace(cfg.Preset) == "" {
		defaults, _ := observabilityPresetConfig(ObservabilityPresetFull)
		return defaults
	}
	if cfg.ProcessingCleanupIntervalSeconds <= 0 {
		cfg.ProcessingCleanupIntervalSeconds = 60
	}
	if cfg.MetricsWindow <= 0 {
		cfg.MetricsWindow = time.Minute
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = cfg.MetricsWindow
	}
	if cfg.FlushTimeout <= 0 {
		cfg.FlushTimeout = 5 * time.Second
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 500
	}
	if cfg.BatchSummarySize <= 0 {
		cfg.BatchSummarySize = cfg.BatchSize
	}
	if cfg.BufferSize <= 0 {
		cfg.BufferSize = 10000
	}
	if strings.TrimSpace(cfg.DropPolicy) == "" {
		cfg.DropPolicy = ObservabilityDropOldest
	}
	cfg.EventMetricsSampleRate = clampSampleRate(cfg.EventMetricsSampleRate)
	if cfg.HighValueDetailSampleRate != nil {
		value := clampSampleRate(*cfg.HighValueDetailSampleRate)
		cfg.HighValueDetailSampleRate = &value
	}
	if cfg.SampleReservoirSize <= 0 {
		cfg.SampleReservoirSize = 2048
	}
	if cfg.MaxAggregateKeys <= 0 {
		cfg.MaxAggregateKeys = 10000
	}
	if cfg.AggregateKeyTTL <= 0 {
		cfg.AggregateKeyTTL = 30 * time.Minute
	}
	if cfg.EventMetricsRetention <= 0 {
		cfg.EventMetricsRetention = 24 * time.Hour
	}
	if cfg.HighValueDetailRetention <= 0 {
		cfg.HighValueDetailRetention = 24 * time.Hour
	}
	if cfg.BatchSummaryRetention <= 0 {
		cfg.BatchSummaryRetention = 24 * time.Hour
	}
	if cfg.DiagnosticsRetention <= 0 {
		cfg.DiagnosticsRetention = 24 * time.Hour
	}
	cfg.MinSampleRate = clampSampleRate(cfg.MinSampleRate)
	return cfg
}

func parseStringList(value any) []string {
	var items []string
	switch typed := value.(type) {
	case nil:
		return nil
	case string:
		items = strings.Split(typed, ",")
	case []string:
		items = typed
	case []any:
		for _, item := range typed {
			items = append(items, fmt.Sprint(item))
		}
	default:
		items = []string{fmt.Sprint(value)}
	}
	return normalizeStrings(items)
}

func rejectRemovedRootConfig(root map[string]any) error {
	removed := map[string]string{
		"trim":          "use horizon.observability event_metrics_retention/high_value_detail_retention/batch_summary_retention/diagnostics_retention",
		"silenced":      "High-value Horizon job detail is no longer filtered by silenced rules",
		"silenced_tags": "High-value Horizon job detail is no longer filtered by silenced tag rules",
	}
	for field, replacement := range removed {
		if _, ok := root[field]; ok {
			return fmt.Errorf("horizon: %s has been removed; %s", field, replacement)
		}
	}
	if metrics := stringAnyMap(root["metrics"]); len(metrics) > 0 {
		if _, ok := metrics["trim_snapshots"]; ok {
			return fmt.Errorf("horizon: metrics.trim_snapshots has been removed; use horizon.observability.event_metrics_retention")
		}
	}
	return nil
}

func rejectRemovedHorizonEnv() error {
	removed := map[string]string{
		"HORIZON_TRIM_RECENT":                          "HORIZON_OBSERVABILITY_EVENT_METRICS_RETENTION",
		"HORIZON_TRIM_FAILED":                          "HORIZON_OBSERVABILITY_HIGH_VALUE_DETAIL_RETENTION",
		"HORIZON_TRIM_MONITORED":                       "HORIZON_OBSERVABILITY_HIGH_VALUE_DETAIL_RETENTION",
		"HORIZON_METRICS_TRIM_SNAPSHOTS_JOB":           "HORIZON_OBSERVABILITY_EVENT_METRICS_RETENTION",
		"HORIZON_METRICS_TRIM_SNAPSHOTS_QUEUE":         "HORIZON_OBSERVABILITY_EVENT_METRICS_RETENTION",
		"HORIZON_OBSERVABILITY_RECENT_JOBS":            "HORIZON_OBSERVABILITY_FAILED_DETAIL_ENABLED or HORIZON_OBSERVABILITY_POISON_DETAIL_ENABLED",
		"HORIZON_OBSERVABILITY_RECENT_JOBS_MAX":        "HORIZON_OBSERVABILITY_BUFFER_SIZE or HORIZON_OBSERVABILITY_MAX_AGGREGATE_KEYS",
		"HORIZON_OBSERVABILITY_JOB_HISTORY":            "HORIZON_OBSERVABILITY_EVENT_METRICS",
		"HORIZON_OBSERVABILITY_QUEUE_HISTORY":          "HORIZON_OBSERVABILITY_EVENT_METRICS",
		"HORIZON_OBSERVABILITY_SUCCESS_DETAIL_ENABLED": "no replacement; Successful Horizon job detail is not collected",
		"HORIZON_OBSERVABILITY_SUCCESS_SAMPLE_RATE":    "no replacement; Successful Horizon job detail is not collected",
		"HORIZON_SILENCED":                             "no replacement; silenced rules were removed",
		"HORIZON_SILENCED_TAGS":                        "no replacement; silenced tag rules were removed",
	}
	for env, replacement := range removed {
		if _, ok := os.LookupEnv(env); ok {
			return fmt.Errorf("horizon: env %s has been removed; use %s", env, replacement)
		}
	}
	return nil
}

func rejectUnknownObservabilityFields(raw map[string]any) error {
	allowed := map[string]struct{}{
		"preset":                              {},
		"event_metrics":                       {},
		"waits":                               {},
		"batch_summaries":                     {},
		"process_health":                      {},
		"queue_lengths":                       {},
		"queued_waits_max":                    {},
		"processing_spans_max":                {},
		"processing_cleanup_interval_seconds": {},
		"metrics_window":                      {},
		"flush_interval":                      {},
		"flush_timeout":                       {},
		"batch_size":                          {},
		"batch_summary_size":                  {},
		"buffer_size":                         {},
		"max_events_per_second":               {},
		"drop_policy":                         {},
		"event_metrics_sample_rate":           {},
		"high_value_detail_sample_rate":       {},
		"sample_reservoir_size":               {},
		"max_aggregate_keys":                  {},
		"aggregate_key_ttl":                   {},
		"event_metrics_retention":             {},
		"high_value_detail_retention":         {},
		"batch_summary_retention":             {},
		"diagnostics_retention":               {},
		"failed_detail_enabled":               {},
		"poison_detail_enabled":               {},
		"slow_job_detail_enabled":             {},
		"slow_job_threshold":                  {},
		"dynamic_sampling_enabled":            {},
		"min_sample_rate":                     {},
	}
	removed := map[string]string{
		"recent_jobs":            "use high-value detail fields for failed/poison/slow diagnostics",
		"recent_jobs_max":        "use buffer_size, sample_reservoir_size, max_aggregate_keys, and retention fields",
		"job_history":            "job history has been removed; use event_metrics queue windows",
		"queue_history":          "queue history now reads event_metrics windows",
		"success_detail_enabled": "Successful Horizon job detail has been removed and is not collected",
		"success_sample_rate":    "Successful Horizon job detail has been removed and is not collected",
		"silenced":               "silenced rules have been removed",
		"silenced_tags":          "silenced tag rules have been removed",
	}
	for field := range raw {
		if _, ok := allowed[field]; ok {
			continue
		}
		if replacement, ok := removed[field]; ok {
			return fmt.Errorf("horizon: observability.%s has been removed; %s", field, replacement)
		}
		return fmt.Errorf("horizon: observability.%s is unknown", field)
	}
	return nil
}

// parseSupervisors 合并 defaults 和当前 environment 下的 supervisor 配置。
// 逻辑说明：同名 supervisor 以 environment 配置覆盖 defaults；只存在于 environment 中的
// supervisor 也会被解析，便于按环境新增队列消费组。
func parseSupervisors(defaults, current map[string]any) (map[string]SupervisorConfig, error) {
	names := make(map[string]struct{}, len(defaults)+len(current))
	for name := range defaults {
		names[name] = struct{}{}
	}
	for name := range current {
		names[name] = struct{}{}
	}
	out := make(map[string]SupervisorConfig, len(names))
	for name := range names {
		merged := mergeMaps(stringAnyMap(defaults[name]), stringAnyMap(current[name]))
		supervisor, err := parseSupervisor(name, merged)
		if err != nil {
			return nil, err
		}
		out[name] = supervisor
	}
	return out, nil
}

// parseSupervisor 把单个 supervisor 的原始 map 解析为强类型配置。
// 参数说明：name 用于错误信息和展示；raw 是 defaults 与 environment 合并后的配置。
// 设计原因：Horizon 管理的是长驻 worker，配置错误必须在命令入口 fail fast，而不是运行中静默回退。
func parseSupervisor(name string, raw map[string]any) (SupervisorConfig, error) {
	if err := rejectCamelCaseSupervisorFields(name, raw); err != nil {
		return SupervisorConfig{}, err
	}
	queues := parseQueues(raw["queue"])
	minProcesses, err := intField(name, raw, "min_processes")
	if err != nil {
		return SupervisorConfig{}, err
	}
	maxProcesses, err := intField(name, raw, "max_processes")
	if err != nil {
		return SupervisorConfig{}, err
	}
	tries, err := intField(name, raw, "tries")
	if err != nil {
		return SupervisorConfig{}, err
	}
	timeout, err := intField(name, raw, "timeout")
	if err != nil {
		return SupervisorConfig{}, err
	}
	sleep, err := intField(name, raw, "sleep")
	if err != nil {
		return SupervisorConfig{}, err
	}
	maxJobs, err := intField(name, raw, "max_jobs")
	if err != nil {
		return SupervisorConfig{}, err
	}
	maxTime, err := intField(name, raw, "max_time")
	if err != nil {
		return SupervisorConfig{}, err
	}
	retryAfter, err := intField(name, raw, "retry_after")
	if err != nil {
		return SupervisorConfig{}, err
	}
	memory, err := intField(name, raw, "memory")
	if err != nil {
		return SupervisorConfig{}, err
	}
	nice, err := intField(name, raw, "nice")
	if err != nil {
		return SupervisorConfig{}, err
	}
	autoScalingStrategy, err := scalingStrategyField(name, raw)
	if err != nil {
		return SupervisorConfig{}, err
	}
	balanceMaxShift, err := intAliasField(name, raw, "balance_max_shift", "", 1)
	if err != nil {
		return SupervisorConfig{}, err
	}
	balanceCooldown, err := intAliasField(name, raw, "balance_cooldown", "", 3)
	if err != nil {
		return SupervisorConfig{}, err
	}
	backoff, err := parseBackoffField(name, raw["backoff"])
	if err != nil {
		return SupervisorConfig{}, err
	}
	stopWhenEmpty, err := boolField(name, raw["stop_when_empty"])
	if err != nil {
		return SupervisorConfig{}, err
	}
	supervisor := SupervisorConfig{
		Name:                name,
		Connection:          strings.TrimSpace(firstString(raw["connection"], "")),
		Queues:              queues,
		Balance:             normalizeBalance(firstString(raw["balance"], BalanceFalse)),
		MinProcesses:        minProcesses,
		MaxProcesses:        maxProcesses,
		Tries:               tries,
		Timeout:             timeout,
		Sleep:               sleep,
		MaxJobs:             maxJobs,
		MaxTime:             maxTime,
		RetryAfter:          retryAfter,
		Backoff:             backoff,
		StopWhenEmpty:       stopWhenEmpty,
		Memory:              memory,
		Nice:                nice,
		AutoScalingStrategy: autoScalingStrategy,
		BalanceMaxShift:     balanceMaxShift,
		BalanceCooldown:     balanceCooldown,
	}
	if supervisor.Connection == "" {
		return SupervisorConfig{}, fmt.Errorf("horizon: supervisor %q connection is required", name)
	}
	if len(supervisor.Queues) == 0 {
		return SupervisorConfig{}, fmt.Errorf("horizon: supervisor %q queue is required", name)
	}
	if supervisor.Balance == "" {
		return SupervisorConfig{}, fmt.Errorf("horizon: supervisor %q balance is invalid", name)
	}
	if supervisor.MaxProcesses < supervisor.MinProcesses {
		return SupervisorConfig{}, fmt.Errorf("horizon: supervisor %q max_processes must be >= min_processes", name)
	}
	return supervisor, nil
}

// scalingStrategyField 解析 Laravel Horizon auto scaling strategy 的 snake_case/camelCase 兼容字段。
// 参数说明：supervisor 用于错误定位；raw 是合并后的 supervisor 配置。snake_case 优先，便于本项目配置风格覆盖迁移字段。
func scalingStrategyField(supervisor string, raw map[string]any) (string, error) {
	field := "auto_scaling_strategy"
	value, ok := raw[field]
	if !ok || strings.TrimSpace(fmt.Sprint(value)) == "" {
		return AutoScalingStrategyTime, nil
	}
	switch strings.TrimSpace(strings.ToLower(fmt.Sprint(value))) {
	case AutoScalingStrategyTime:
		return AutoScalingStrategyTime, nil
	case AutoScalingStrategySize:
		return AutoScalingStrategySize, nil
	default:
		return "", fmt.Errorf("horizon: supervisor %q %s must be time or size", supervisor, field)
	}
}

// intAliasField 解析拥有 snake_case/camelCase 两种命名的非负整数配置。
// 设计思路：issue 07 需要同时兼容 Prismgo 配置风格与 Laravel Horizon 文档字段；错误保留实际命中的字段名，便于排查。
func intAliasField(supervisor string, raw map[string]any, snake string, camel string, fallback int) (int, error) {
	field := snake
	value, ok := raw[field]
	if !ok && camel != "" {
		field = camel
		value, ok = raw[field]
	}
	if !ok {
		return fallback, nil
	}
	n, valid := parseNonNegativeInt(value)
	if !valid {
		return 0, fmt.Errorf("horizon: supervisor %q %s must be a non-negative integer", supervisor, field)
	}
	return n, nil
}

func rejectCamelCaseSupervisorFields(supervisor string, raw map[string]any) error {
	replacements := map[string]string{
		"maxProcesses":        "max_processes",
		"minProcesses":        "min_processes",
		"retryAfter":          "retry_after",
		"maxJobs":             "max_jobs",
		"maxTime":             "max_time",
		"stopWhenEmpty":       "stop_when_empty",
		"autoScalingStrategy": "auto_scaling_strategy",
		"balanceMaxShift":     "balance_max_shift",
		"balanceCooldown":     "balance_cooldown",
	}
	for field, replacement := range replacements {
		if _, ok := raw[field]; ok {
			return fmt.Errorf("horizon: supervisor %q field %s is not supported; use %s", supervisor, field, replacement)
		}
	}
	return nil
}

// intField 读取单个非负整数字段，缺失时返回 0。
// 参数说明：supervisor 用于错误上下文；raw 是 supervisor 原始配置；field 是配置字段名。
func intField(supervisor string, raw map[string]any, field string) (int, error) {
	value, ok := raw[field]
	if !ok {
		return 0, nil
	}
	n, valid := parseNonNegativeInt(value)
	if !valid {
		return 0, fmt.Errorf("horizon: supervisor %q %s must be a non-negative integer", supervisor, field)
	}
	return n, nil
}

// parseQueues 兼容逗号字符串、[]string、[]any 和单个标量队列配置。
// 逻辑说明：解析后统一 trim、去空项和去重，保证 horizon:list 与后续 worker 使用同一种队列视图。
func parseQueues(value any) []string {
	var items []string
	switch typed := value.(type) {
	case string:
		items = strings.Split(typed, ",")
	case []string:
		items = typed
	case []any:
		for _, item := range typed {
			items = append(items, fmt.Sprint(item))
		}
	default:
		if value != nil {
			items = []string{fmt.Sprint(value)}
		}
	}
	return normalizeStrings(items)
}

// normalizeBalance 把配置中的 balance 收敛到受支持的策略常量。
// 设计原因：balance 会影响后续 worker 扩缩容策略，未知值必须被上层 parseSupervisor 判定为错误。
func normalizeBalance(value string) string {
	switch strings.TrimSpace(strings.ToLower(value)) {
	case "", "false":
		return BalanceFalse
	case "simple":
		return BalanceSimple
	case "auto":
		return BalanceAuto
	default:
		return ""
	}
}

// parseBackoffField 解析失败重试退避配置。
// 参数说明：supervisor 用于错误上下文；value 可为数字、逗号字符串或数组。所有项必须是非负整数，
// 任一非法项都会让配置解析失败。
func parseBackoffField(supervisor string, value any) ([]int, error) {
	switch typed := value.(type) {
	case nil:
		return nil, nil
	case []int:
		out := append([]int(nil), typed...)
		for _, item := range out {
			if item < 0 {
				return nil, fmt.Errorf("horizon: supervisor %q backoff must contain non-negative integers", supervisor)
			}
		}
		return out, nil
	case []any:
		out := make([]int, 0, len(typed))
		for _, item := range typed {
			n, ok := parseNonNegativeInt(item)
			if !ok {
				return nil, fmt.Errorf("horizon: supervisor %q backoff must contain non-negative integers", supervisor)
			}
			out = append(out, n)
		}
		return out, nil
	case string:
		parts := strings.Split(typed, ",")
		out := make([]int, 0, len(parts))
		for _, part := range parts {
			if strings.TrimSpace(part) == "" {
				continue
			}
			n, ok := parseNonNegativeInt(part)
			if !ok {
				return nil, fmt.Errorf("horizon: supervisor %q backoff must contain non-negative integers", supervisor)
			}
			out = append(out, n)
		}
		return out, nil
	default:
		n, ok := parseNonNegativeInt(value)
		if !ok {
			return nil, fmt.Errorf("horizon: supervisor %q backoff must contain non-negative integers", supervisor)
		}
		return []int{n}, nil
	}
}

// stringAnyMap 安全地把配置节点转换为 map[string]any。
// 逻辑说明：非 map 节点被视为缺失配置，交由后续必填字段校验决定是否报错。
func stringAnyMap(value any) map[string]any {
	switch typed := value.(type) {
	case map[string]any:
		return typed
	default:
		return nil
	}
}

// mergeMaps 合并两个浅层配置 map，override 中的字段覆盖 base。
// 设计原因：supervisor 配置当前只有一层字段，浅合并能清晰表达 environment 覆盖 defaults 的规则。
func mergeMaps(base, override map[string]any) map[string]any {
	out := make(map[string]any, len(base)+len(override))
	for key, value := range base {
		out[key] = value
	}
	for key, value := range override {
		out[key] = value
	}
	return out
}

// firstString 读取配置字符串，空白值回退到 fallback。
// 使用方式：用于 store、prefix、connection、balance 等字符串配置的默认值处理。
func firstString(value any, fallback string) string {
	if value == nil {
		return fallback
	}
	text := strings.TrimSpace(fmt.Sprint(value))
	if text == "" {
		return fallback
	}
	return text
}

// firstNonNegativeInt 读取非负整数配置，非法值回退到 fallback。
func firstNonNegativeInt(value any, fallback int) int {
	n, ok := parseNonNegativeInt(value)
	if !ok {
		return fallback
	}
	return n
}

// firstPositiveInt 读取正整数配置，缺失、零值或非法值回退到 fallback。
func firstPositiveInt(value any, fallback int) int {
	n, ok := parseNonNegativeInt(value)
	if !ok || n <= 0 {
		return fallback
	}
	return n
}

func firstBool(value any, fallback bool) bool {
	switch typed := value.(type) {
	case nil:
		return fallback
	case bool:
		return typed
	case string:
		switch strings.TrimSpace(strings.ToLower(typed)) {
		case "1", "true", "yes", "on":
			return true
		case "0", "false", "no", "off", "":
			return false
		default:
			return fallback
		}
	default:
		text := strings.TrimSpace(strings.ToLower(fmt.Sprint(value)))
		return text == "1" || text == "true" || text == "yes" || text == "on"
	}
}

// strictBool 解析必须合法的布尔配置。
// 参数说明：field 用于错误定位；value 支持 bool 和常见字符串布尔值。
// 设计原因：当 preset 缺失时，observability 子开关会参与最终合并，因此非法布尔值必须明确报错。
func strictBool(field string, value any) (bool, error) {
	switch typed := value.(type) {
	case bool:
		return typed, nil
	case string:
		switch strings.TrimSpace(strings.ToLower(typed)) {
		case "1", "true", "yes", "on":
			return true, nil
		case "0", "false", "no", "off":
			return false, nil
		}
	}
	return false, fmt.Errorf("%s must be a boolean", field)
}

// parseNonNegativeInt 把常见配置来源中的数字解析为非负整数。
// 逻辑说明：nil 和空字符串表示字段缺失，按 0 处理；负数和无法解析的值返回 false。
func parseNonNegativeInt(value any) (int, bool) {
	switch typed := value.(type) {
	case nil:
		return 0, true
	case int:
		return typed, typed >= 0
	case int64:
		return int(typed), typed >= 0
	case float64:
		return int(typed), typed >= 0
	case string:
		if strings.TrimSpace(typed) == "" {
			return 0, true
		}
		n, err := strconv.Atoi(strings.TrimSpace(typed))
		return n, err == nil && n >= 0
	default:
		n, err := strconv.Atoi(strings.TrimSpace(fmt.Sprint(value)))
		return n, err == nil && n >= 0
	}
}

func strictNonNegativeInt(field string, value any) (int, error) {
	n, ok := parseNonNegativeInt(value)
	if !ok {
		return 0, fmt.Errorf("%s must be a non-negative integer", field)
	}
	return n, nil
}

func strictPositiveInt(field string, value any) (int, error) {
	n, ok := parseNonNegativeInt(value)
	if !ok || n <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer", field)
	}
	return n, nil
}

func strictSampleRate(field string, value any) (float64, error) {
	var parsed float64
	var err error
	switch typed := value.(type) {
	case float64:
		parsed = typed
	case float32:
		parsed = float64(typed)
	case int:
		parsed = float64(typed)
	case int64:
		parsed = float64(typed)
	case string:
		if strings.TrimSpace(typed) == "" {
			return 0, fmt.Errorf("%s must be between 0 and 1", field)
		}
		parsed, err = strconv.ParseFloat(strings.TrimSpace(typed), 64)
	default:
		parsed, err = strconv.ParseFloat(strings.TrimSpace(fmt.Sprint(value)), 64)
	}
	if err != nil || parsed < 0 || parsed > 1 {
		return 0, fmt.Errorf("%s must be between 0 and 1", field)
	}
	return parsed, nil
}

func strictPositiveDuration(field string, value any) (time.Duration, error) {
	switch typed := value.(type) {
	case time.Duration:
		if typed > 0 {
			return typed, nil
		}
	case int:
		if typed > 0 {
			return time.Duration(typed) * time.Second, nil
		}
	case int64:
		if typed > 0 {
			return time.Duration(typed) * time.Second, nil
		}
	case float64:
		if typed > 0 {
			return time.Duration(typed * float64(time.Second)), nil
		}
	case string:
		text := strings.TrimSpace(typed)
		if text == "" {
			break
		}
		if n, err := strconv.Atoi(text); err == nil && n > 0 {
			return time.Duration(n) * time.Second, nil
		}
		if duration, err := time.ParseDuration(text); err == nil && duration > 0 {
			return duration, nil
		}
	}
	return 0, fmt.Errorf("%s must be a positive duration", field)
}

func strictNonNegativeDuration(field string, value any) (time.Duration, error) {
	switch typed := value.(type) {
	case time.Duration:
		if typed >= 0 {
			return typed, nil
		}
	case int:
		if typed >= 0 {
			return time.Duration(typed) * time.Second, nil
		}
	case int64:
		if typed >= 0 {
			return time.Duration(typed) * time.Second, nil
		}
	case float64:
		if typed >= 0 {
			return time.Duration(typed * float64(time.Second)), nil
		}
	case string:
		text := strings.TrimSpace(typed)
		if text == "" {
			break
		}
		if n, err := strconv.Atoi(text); err == nil && n >= 0 {
			return time.Duration(n) * time.Second, nil
		}
		if duration, err := time.ParseDuration(text); err == nil && duration >= 0 {
			return duration, nil
		}
	}
	return 0, fmt.Errorf("%s must be a non-negative duration", field)
}

func strictDropPolicy(field string, value any) (string, error) {
	switch strings.TrimSpace(strings.ToLower(fmt.Sprint(value))) {
	case ObservabilityDropOldest:
		return ObservabilityDropOldest, nil
	case ObservabilityDropNewest:
		return ObservabilityDropNewest, nil
	default:
		return "", fmt.Errorf("%s must be %s or %s", field, ObservabilityDropOldest, ObservabilityDropNewest)
	}
}

func clampSampleRate(value float64) float64 {
	if value < 0 {
		return 0
	}
	if value > 1 {
		return 1
	}
	return value
}

// boolField 解析 stop_when_empty 布尔配置。
// 参数说明：supervisor 用于错误上下文；value 支持 bool 和常见字符串布尔值。其他类型按配置错误处理。
func boolField(supervisor string, value any) (bool, error) {
	switch typed := value.(type) {
	case nil:
		return false, nil
	case bool:
		return typed, nil
	case string:
		switch strings.TrimSpace(strings.ToLower(typed)) {
		case "1", "true", "yes", "on":
			return true, nil
		case "", "0", "false", "no", "off":
			return false, nil
		}
	}
	return false, fmt.Errorf("horizon: supervisor %q stop_when_empty must be a boolean", supervisor)
}

// normalizeStrings 清理字符串列表，去除空白项并按首次出现顺序去重。
// 使用方式：当前用于队列名称解析，后续也可复用到标签类配置。
func normalizeStrings(values []string) []string {
	out := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}
