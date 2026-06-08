package horizon

import (
	"sort"
	"time"
)

// EventMetricEstimateWindow 保存单个 event_metrics window 的估算元数据。
//
// 使用方式：读模型把 Store 中的 EventMetricWindow 投影为该结构，前端可直接展示窗口边界、
// 采样率、样本量和估算总量。
// 设计原因：估算必须按窗口独立计算，不能跨窗口平均采样率后反推总量。
type EventMetricEstimateWindow struct {
	// WindowStart 是事件发生时间归属的窗口开始时间。
	WindowStart time.Time `json:"window_start"`
	// WindowEnd 是事件发生时间归属的窗口结束时间。
	WindowEnd time.Time `json:"window_end"`
	// EffectiveSampleRate 是该窗口实际使用的采样率；0 表示该窗口没有可估算观测。
	EffectiveSampleRate float64 `json:"effective_sample_rate"`
	// SampledCount 是该窗口实际采样命中的事件数量。
	SampledCount int64 `json:"sampled_count"`
	// EstimatedTotal 是按该窗口 EffectiveSampleRate 估算的原始事件总量。
	EstimatedTotal int64 `json:"estimated_total"`
	// Quality 是 exact、estimated、degraded、unknown 或 partial。
	Quality string `json:"quality"`
	// Degraded 表示该窗口存在可量化降级，估算可展示但需要提示质量下降。
	Degraded bool `json:"degraded"`
}

// EventMetricEstimate 汇总多个 event_metrics window 的估算结果。
//
// 使用方式：queue/job history 读模型先调用 EstimateEventMetricWindows，再把总量和窗口元数据暴露给展示层。
// 设计原因：跨窗口汇总必须保留每个窗口的采样元数据，否则无法区分 exact、estimated 和 unknown。
type EventMetricEstimate struct {
	// SampledCount 是所有窗口实际采样命中事件数之和。
	SampledCount int64 `json:"sampled_count"`
	// EstimatedTotal 是所有可估算窗口逐个估算后的总和。
	EstimatedTotal int64 `json:"estimated_total"`
	// Quality 是汇总后的最差质量状态。
	Quality string `json:"quality"`
	// Degraded 表示任一窗口存在降级。
	Degraded bool `json:"degraded"`
	// Windows 保存参与汇总的每个窗口估算元数据。
	Windows []EventMetricEstimateWindow `json:"windows"`
}

// RuntimePercentileEstimate 保存 runtime P95/P99 的样本估算元数据。
//
// 使用方式：读模型只在 runtime 样本量满足阈值时展示 P95/P99；样本不足时 Quality 为 unknown。
// 设计原因：百分位只能来自样本分布、reservoir 或 histogram，不能通过 sample_rate 反推。
type RuntimePercentileEstimate struct {
	// P95 是基于样本分布计算的 95 分位 runtime 毫秒值。
	P95 int64 `json:"p95_ms"`
	// P99 是基于样本分布计算的 99 分位 runtime 毫秒值。
	P99 int64 `json:"p99_ms"`
	// SampledCount 是参与百分位计算的样本数量。
	SampledCount int64 `json:"sampled_count"`
	// Quality 是 estimated 或 unknown。
	Quality string `json:"quality"`
}

// EstimateEventMetricWindows 按 window 各自采样率估算后再汇总。
//
// 语义说明：effective_sample_rate=0 的窗口标记为 unknown，不参与 estimated_total 求和；
// sampled_count=0 且采样率大于 0 的窗口可以估算为 0。
func EstimateEventMetricWindows(windows []EventMetricWindow) EventMetricEstimate {
	out := EventMetricEstimate{
		Quality: EventMetricQualityExact,
		Windows: make([]EventMetricEstimateWindow, 0, len(windows)),
	}
	for _, window := range windows {
		item := estimateEventMetricWindow(window)
		out.Windows = append(out.Windows, item)
		out.SampledCount += item.SampledCount
		if item.Quality != EventMetricQualityUnknown {
			out.EstimatedTotal += item.EstimatedTotal
		}
		out.Quality = mergeEventMetricQuality(out.Quality, item.Quality)
		out.Degraded = out.Degraded || item.Degraded
	}
	return out
}

func estimateEventMetricWindow(window EventMetricWindow) EventMetricEstimateWindow {
	sampled := window.SampleCount
	if sampled == 0 {
		sampled = window.Processed + window.Failed + window.Released + window.Poison + window.Queued
	}
	quality := window.Quality
	if quality == "" {
		quality = eventMetricQualityForWindow(window.Estimated, window.Degraded, window.Partial, window.Unknown)
	}
	if window.EffectiveSampleRate == 0 {
		quality = EventMetricQualityUnknown
	}
	estimated := window.EstimatedTotal
	if estimated == 0 && quality != EventMetricQualityUnknown {
		estimated = sampled
		if window.EffectiveSampleRate > 0 && window.EffectiveSampleRate < 1 {
			estimated = int64(float64(sampled)/window.EffectiveSampleRate + 0.5)
		}
	}
	return EventMetricEstimateWindow{
		WindowStart:         window.WindowStart,
		WindowEnd:           window.WindowEnd,
		EffectiveSampleRate: window.EffectiveSampleRate,
		SampledCount:        sampled,
		EstimatedTotal:      estimated,
		Quality:             quality,
		Degraded:            window.Degraded || quality == EventMetricQualityDegraded,
	}
}

func mergeEventMetricQuality(current string, next string) string {
	if samplingQualityRank(next) > samplingQualityRank(current) {
		return next
	}
	return current
}

// EstimateRuntimePercentiles 只从 runtime 样本分布计算 P95/P99。
func EstimateRuntimePercentiles(samples []int64, minSamples int) RuntimePercentileEstimate {
	if minSamples <= 0 {
		minSamples = 1
	}
	if len(samples) < minSamples {
		return RuntimePercentileEstimate{
			SampledCount: int64(len(samples)),
			Quality:      EventMetricQualityUnknown,
		}
	}
	sorted := append([]int64(nil), samples...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	return RuntimePercentileEstimate{
		P95:          percentile(sorted, 0.95),
		P99:          percentile(sorted, 0.99),
		SampledCount: int64(len(sorted)),
		Quality:      EventMetricQualityEstimated,
	}
}

func percentile(sorted []int64, quantile float64) int64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)-1)*quantile + 0.5)
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func samplingQualityRank(quality string) int {
	switch quality {
	case EventMetricQualityPartial:
		return 4
	case EventMetricQualityUnknown:
		return 3
	case EventMetricQualityDegraded:
		return 2
	case EventMetricQualityEstimated:
		return 1
	default:
		return 0
	}
}
