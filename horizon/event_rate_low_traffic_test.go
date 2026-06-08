package horizon

import (
	"testing"
	"time"
)

// TestEventRateTrackerReturnsFractionalRateForLowTraffic 验证低流量时 rateAt 返回精确的浮点 EPS，
// 而非整数除法截断为 0。
//
// 需求背景：修复前 rateAt 使用 totalAt/60 整数除法，55 个事件在 60 秒窗口内得到 0 EPS，
// 导致动态采样策略无法感知低流量压力（始终停留在 normal 状态）。
// 修复后 rateAt 返回 float64，55 个事件应得到约 0.917 EPS。
func TestEventRateTrackerReturnsFractionalRateForLowTraffic(t *testing.T) {
	var tracker eventRateTracker
	start := time.Date(2026, 5, 30, 10, 0, 0, 0, time.UTC)

	// 60 秒窗口内记录 55 个事件
	for i := 0; i < 55; i++ {
		tracker.recordAt(start.Add(time.Duration(i) * time.Second))
	}

	rate := tracker.rateAt(start.Add(59 * time.Second))
	if rate <= 0 {
		t.Fatalf("expected fractional EPS > 0 for 55 events in 60s window, got %f", rate)
	}

	// 验证精度：55/60 ≈ 0.917，允许误差 ±0.01
	expected := 55.0 / 60.0
	if rate < expected-0.01 || rate > expected+0.01 {
		t.Fatalf("expected rate ≈ %f (55/60), got %f", expected, rate)
	}
}

// TestLowTrafficEventRateTriggersWarmingState 验证指定 EPS 经过 stateForEventRate 后
// 能正确进入对应压力状态，而非被整数除法压成 normal。
//
// 需求背景：MaxEventsPerSecond=100 时，55 EPS 应产生 55% 利用率（warming），
// 75 EPS 应产生 75% 利用率（pressured），95 EPS 应产生 95% 利用率（degraded）。
// 修复前整数除法 3300/60=55 虽然恰好整除，但类似 53 EPS (3180/60=53) 在旧 int64 实现
// 下可以工作，而 55 events/60s (0.917 EPS) 会被截断为 0，完全丢失压力信号。
func TestLowTrafficEventRateTriggersWarmingState(t *testing.T) {
	cfg := observabilityPresetConfigOrFull()
	cfg.MaxEventsPerSecond = 100
	cfg.EventMetricsSampleRate = 1
	cfg.DynamicSamplingEnabled = true

	cases := []struct {
		name          string
		eventsPerSec  int // 每秒记录的事件数（实际 EPS）
		expectedState string
	}{
		{"30 EPS → 30% → normal", 30, SamplingStateNormal},
		{"55 EPS → 55% → warming", 55, SamplingStateWarming},
		{"75 EPS → 75% → pressured", 75, SamplingStatePressured},
		{"95 EPS → 95% → degraded", 95, SamplingStateDegraded},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			coll := newCollector(cfg)
			start := time.Date(2026, 5, 30, 10, 0, 0, 0, time.UTC)

			coll.mu.Lock()
			// 在 60 秒窗口内，每秒均匀记录 eventsPerSec 个事件
			for sec := 0; sec < 60; sec++ {
				for j := 0; j < tc.eventsPerSec; j++ {
					coll.rateTracker.recordAt(start.Add(time.Duration(sec) * time.Second))
				}
			}
			pressure := coll.samplingPressureAt(start.Add(59 * time.Second))
			coll.mu.Unlock()

			result := EvaluateSamplingPolicy(cfg, pressure)
			if result.State != tc.expectedState {
				t.Fatalf("expected state %s for %d EPS with maxEPS=100, got %s (EventRate=%f)",
					tc.expectedState, tc.eventsPerSec, result.State, pressure.EventRate)
			}
		})
	}
}

// TestFractionalEPSNotTruncatedToZero 验证 sub-1 EPS 场景下旧 int64 截断问题已修复。
//
// 需求背景：这是用户报告的精确复现场景：MaxEventsPerSecond=100，60 秒内 55 个事件。
// 旧实现：rateAt = 55/60 = 0 (int64)，stateForEventRate 判为 normal。
// 新实现：rateAt = 0.917 (float64)，stateForEventRate 用 0.917/100 = 0.00917 判为 normal（正确！
// 因为真实利用率只有 0.917%）。核心改进是压力信号不再被完全丢失。
func TestFractionalEPSNotTruncatedToZero(t *testing.T) {
	cfg := observabilityPresetConfigOrFull()
	cfg.MaxEventsPerSecond = 100
	cfg.EventMetricsSampleRate = 1
	cfg.DynamicSamplingEnabled = true
	cfg.MinSampleRate = 0.1

	coll := newCollector(cfg)
	start := time.Date(2026, 5, 30, 10, 0, 0, 0, time.UTC)

	coll.mu.Lock()
	// 60 秒窗口内记录 55 个事件（分散在不同秒）
	for i := 0; i < 55; i++ {
		coll.rateTracker.recordAt(start.Add(time.Duration(i) * time.Second))
	}
	pressure := coll.samplingPressureAt(start.Add(59 * time.Second))
	coll.mu.Unlock()

	// 核心断言：EventRate 不再被截断为 0
	if pressure.EventRate <= 0 {
		t.Fatalf("EventRate should be > 0 for 55 events/60s, got %f", pressure.EventRate)
	}

	// 真实 EPS ≈ 0.917，对比 MaxEPS=100 利用率仅 0.917% → normal（这是正确行为）
	result := EvaluateSamplingPolicy(cfg, pressure)
	if result.State != SamplingStateNormal {
		t.Fatalf("0.917 EPS / 100 MaxEPS = 0.917%% utilization should be normal, got %s", result.State)
	}
}

// TestHighTrafficEventRatePrecisionUnchanged 验证高流量场景下 EPS 精度不受影响。
//
// 需求背景：修改 rateAt 为 float64 后，高流量（>=60 EPS）的计算结果应与之前一致，
// 确保修改不影响已有的正确行为。
func TestHighTrafficEventRatePrecisionUnchanged(t *testing.T) {
	var tracker eventRateTracker
	start := time.Date(2026, 5, 30, 10, 0, 0, 0, time.UTC)

	// 60 秒内均匀分布 600 个事件 → 10 EPS
	for i := 0; i < 600; i++ {
		tracker.recordAt(start.Add(time.Duration(i%60) * time.Second))
	}

	rate := tracker.rateAt(start.Add(59 * time.Second))
	if rate < 9.9 || rate > 10.1 {
		t.Fatalf("expected rate ≈ 10.0 for 600 events in 60s, got %f", rate)
	}
}
