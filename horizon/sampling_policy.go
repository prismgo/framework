package horizon

import "time"

const (
	// SamplingStateNormal 表示观测链路压力低，使用显式配置采样率。
	SamplingStateNormal = "normal"
	// SamplingStateWarming 表示观测链路开始接近高水位，轻度降低采样率。
	SamplingStateWarming = "warming"
	// SamplingStatePressured 表示观测链路处于高压力，明显降低采样率。
	SamplingStatePressured = "pressured"
	// SamplingStateDegraded 表示观测链路已降级或接近熔断，可低于 min_sample_rate。
	SamplingStateDegraded = "degraded"
)

// SamplingPressure 描述动态采样策略需要的观测链路压力输入。
//
// 使用方式：collector/manager 从内存状态、flusher diagnostics 和入口速率构造该结构；
// 策略只负责把压力映射为当前实际采样率，不读取 Store 或队列 backend。
//
// 设计原因：把压力输入收敛成独立 DTO，避免采样策略直接耦合 collector、flusher 或 Store 实现，
// 也便于测试覆盖每一种压力来源。
type SamplingPressure struct {
	// BufferUtilization 是 collector 有界 buffer 的使用率，范围通常为 0..1。
	BufferUtilization float64
	// ReservoirUtilization 是 runtime 样本池使用率，范围通常为 0..1。
	ReservoirUtilization float64
	// AggregateKeyCount 是当前 event_metrics 聚合 key 数量。
	AggregateKeyCount int
	// MaxAggregateKeys 是允许的 event_metrics 聚合 key 上限；0 表示不上报该压力来源。
	MaxAggregateKeys int
	// FlushLag 是当前 flush 距离上次 flush/snapshot 的滞后时间。
	FlushLag time.Duration
	// FlushInterval 是配置的 flush 周期，用于解释 FlushLag 压力。
	FlushInterval time.Duration
	// FlushDuration 是最近一次 Store flush 耗时。
	FlushDuration time.Duration
	// FlushTimeout 是配置的单次 Store flush 超时，用于解释 FlushDuration 压力。
	FlushTimeout time.Duration
	// FlushErrorStreak 是连续 flush 失败次数。
	FlushErrorStreak int
	// EventRate 是 collector 入口观察到的事件速率（events per second，浮点精度）。
	//
	// 设计原因：使用 float64 而非 int64，避免低流量场景下整数除法截断导致压力被低估。
	// 例如 55 事件/60 秒 = 0.917 EPS，int64 会截断为 0，动态采样无法感知压力。
	EventRate float64
	// MaxEventsPerSecond 是配置的入口事件速率上限；0 表示不上报该压力来源。
	MaxEventsPerSecond int
	// DropRate 是当前滑动窗口中已知丢弃事件占比。
	DropRate float64
}

// SamplingPolicyResult 是动态采样策略对 event_metrics 和 high-value detail 的当前决策。
//
// 使用方式：事件入口把该结果写入 SamplingDecision，flusher 再把实际采样率保存到 window 元数据。
// 设计原因：event_metrics 与 high-value detail 有不同基线，但必须由同一压力状态协调降采样。
type SamplingPolicyResult struct {
	// State 是 normal、warming、pressured 或 degraded。
	State string
	// EventMetricsRate 是本次事件实际使用的 event_metrics 采样率。
	EventMetricsRate float64
	// HighValueDetailRate 是本次事件实际使用的 High-value Horizon job detail 采样率。
	HighValueDetailRate float64
}

// EvaluateSamplingPolicy 根据当前压力计算实际采样率。
//
// 语义说明：event_metrics_sample_rate 和 high_value_detail_sample_rate 是显式上限；
// dynamic_sampling_enabled=false 时固定使用配置值。event_metrics_sample_rate=0 会保持 0，
// high-value 未显式配置时也回落为 0。
func EvaluateSamplingPolicy(cfg ObservabilityConfig, pressure SamplingPressure) SamplingPolicyResult {
	cfg = normalizeObservabilityConfig(cfg)
	eventBase := clampSampleRate(cfg.EventMetricsSampleRate)
	highValueBase := cfg.EffectiveHighValueDetailSampleRate(eventBase)
	if !cfg.DynamicSamplingEnabled {
		return SamplingPolicyResult{
			State:               SamplingStateNormal,
			EventMetricsRate:    eventBase,
			HighValueDetailRate: highValueBase,
		}
	}
	state := samplingStateForPressure(cfg, pressure)
	return SamplingPolicyResult{
		State:               state,
		EventMetricsRate:    applySamplingState(eventBase, cfg.MinSampleRate, state),
		HighValueDetailRate: applySamplingState(highValueBase, cfg.MinSampleRate, state),
	}
}

func samplingStateForPressure(cfg ObservabilityConfig, pressure SamplingPressure) string {
	state := SamplingStateNormal
	state = maxSamplingState(state, stateForUtilization(pressure.BufferUtilization))
	state = maxSamplingState(state, stateForUtilization(pressure.ReservoirUtilization))
	state = maxSamplingState(state, stateForAggregateKeys(pressure.AggregateKeyCount, pressure.MaxAggregateKeys))
	state = maxSamplingState(state, stateForFlushLag(pressure.FlushLag, firstPositiveDuration(pressure.FlushInterval, cfg.FlushInterval)))
	state = maxSamplingState(state, stateForFlushDuration(pressure.FlushDuration, firstPositiveDuration(pressure.FlushTimeout, cfg.FlushTimeout)))
	state = maxSamplingState(state, stateForFlushErrors(pressure.FlushErrorStreak))
	state = maxSamplingState(state, stateForEventRate(pressure.EventRate, firstPositiveSamplingInt(pressure.MaxEventsPerSecond, cfg.MaxEventsPerSecond)))
	state = maxSamplingState(state, stateForDropRate(pressure.DropRate))
	return state
}

func stateForUtilization(value float64) string {
	switch {
	case value >= 0.90:
		return SamplingStateDegraded
	case value >= 0.70:
		return SamplingStatePressured
	case value >= 0.50:
		return SamplingStateWarming
	default:
		return SamplingStateNormal
	}
}

func stateForAggregateKeys(count int, max int) string {
	if count <= 0 || max <= 0 {
		return SamplingStateNormal
	}
	return stateForUtilization(float64(count) / float64(max))
}

func stateForFlushLag(lag time.Duration, interval time.Duration) string {
	if lag <= 0 || interval <= 0 {
		return SamplingStateNormal
	}
	switch {
	case lag >= 3*interval:
		return SamplingStateDegraded
	case lag >= 2*interval:
		return SamplingStatePressured
	case lag >= interval:
		return SamplingStateWarming
	default:
		return SamplingStateNormal
	}
}

func stateForFlushDuration(duration time.Duration, timeout time.Duration) string {
	if duration <= 0 || timeout <= 0 {
		return SamplingStateNormal
	}
	return stateForUtilization(float64(duration) / float64(timeout))
}

func stateForFlushErrors(streak int) string {
	switch {
	case streak >= 3:
		return SamplingStateDegraded
	case streak > 0:
		return SamplingStatePressured
	default:
		return SamplingStateNormal
	}
}

// stateForEventRate 根据事件速率与上限的利用率推断压力状态。
//
// 参数说明：rate 为 float64 EPS（events per second），支持低流量下的精确压力判断。
// 当 rate 或 max 为 0 时视为无压力来源，返回 normal。
func stateForEventRate(rate float64, max int) string {
	if rate <= 0 || max <= 0 {
		return SamplingStateNormal
	}
	return stateForUtilization(rate / float64(max))
}

func stateForDropRate(rate float64) string {
	switch {
	case rate >= 0.10:
		return SamplingStateDegraded
	case rate > 0:
		return SamplingStatePressured
	default:
		return SamplingStateNormal
	}
}

func applySamplingState(base float64, minRate float64, state string) float64 {
	base = clampSampleRate(base)
	if base <= 0 {
		return 0
	}
	minRate = clampSampleRate(minRate)
	multiplier := 1.0
	switch state {
	case SamplingStateWarming:
		multiplier = 0.5
	case SamplingStatePressured:
		multiplier = 0.25
	case SamplingStateDegraded:
		multiplier = 0.1
	}
	rate := base * multiplier
	if state != SamplingStateDegraded && minRate > 0 && rate < minRate {
		rate = minRate
	}
	if rate > base {
		return base
	}
	return clampSampleRate(rate)
}

func maxSamplingState(left string, right string) string {
	if samplingStateRank(right) > samplingStateRank(left) {
		return right
	}
	return left
}

func samplingStateRank(state string) int {
	switch state {
	case SamplingStateDegraded:
		return 3
	case SamplingStatePressured:
		return 2
	case SamplingStateWarming:
		return 1
	default:
		return 0
	}
}

func firstPositiveDuration(values ...time.Duration) time.Duration {
	for _, value := range values {
		if value > 0 {
			return value
		}
	}
	return 0
}

func firstPositiveSamplingInt(values ...int) int {
	for _, value := range values {
		if value > 0 {
			return value
		}
	}
	return 0
}
