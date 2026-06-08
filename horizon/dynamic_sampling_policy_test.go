package horizon

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/prismgo/framework/queue"
)

func TestDynamicSamplingPolicyLowersRatesUnderPressureWithoutExceedingBaselines(t *testing.T) {
	// 需求背景：dynamic sampling contract 要求 event_metrics_sample_rate 作为显式上限，动态采样只能在压力升高时降低实际采样率。
	cfg := observabilityPresetConfigOrFull()
	cfg.EventMetricsSampleRate = 0.8
	highValueRate := 0.6
	cfg.HighValueDetailSampleRate = &highValueRate
	cfg.MinSampleRate = 0.2
	cfg.DynamicSamplingEnabled = true

	normal := EvaluateSamplingPolicy(cfg, SamplingPressure{
		BufferUtilization:    0.49,
		ReservoirUtilization: 0.25,
		AggregateKeyCount:    10,
		FlushLag:             100 * time.Millisecond,
		EventRate:            100,
	})
	if normal.State != SamplingStateNormal || normal.EventMetricsRate != 0.8 || normal.HighValueDetailRate != 0.6 {
		t.Fatalf("normal pressure should keep configured baselines, got %#v", normal)
	}

	pressured := EvaluateSamplingPolicy(cfg, SamplingPressure{
		BufferUtilization:    0.82,
		ReservoirUtilization: 0.75,
		AggregateKeyCount:    8500,
		MaxAggregateKeys:     10000,
		FlushLag:             3 * time.Second,
		EventRate:            850,
		MaxEventsPerSecond:   1000,
	})
	if pressured.State != SamplingStatePressured {
		t.Fatalf("expected pressured state, got %#v", pressured)
	}
	if pressured.EventMetricsRate >= normal.EventMetricsRate || pressured.EventMetricsRate < cfg.MinSampleRate {
		t.Fatalf("pressured event_metrics rate should be lowered but respect min_sample_rate, got %#v", pressured)
	}
	if pressured.HighValueDetailRate >= normal.HighValueDetailRate || pressured.HighValueDetailRate < cfg.MinSampleRate {
		t.Fatalf("pressured high-value rate should be lowered but respect min_sample_rate, got %#v", pressured)
	}

	fixed := cfg
	fixed.DynamicSamplingEnabled = false
	disabled := EvaluateSamplingPolicy(fixed, SamplingPressure{BufferUtilization: 0.95, FlushErrorStreak: 3})
	if disabled.State != SamplingStateNormal || disabled.EventMetricsRate != 0.8 || disabled.HighValueDetailRate != 0.6 {
		t.Fatalf("disabled dynamic sampling should keep fixed configured rates, got %#v", disabled)
	}
}

func TestDynamicSamplingPolicyKeepsEventMetricsZeroButAllowsExplicitHighValueRate(t *testing.T) {
	// 需求背景：event_metrics_sample_rate=0 是合法显式值；只有显式 high_value_detail_sample_rate 才能独立采集诊断明细。
	cfg := observabilityPresetConfigOrFull()
	cfg.EventMetricsSampleRate = 0
	cfg.HighValueDetailSampleRate = nil
	cfg.DynamicSamplingEnabled = true

	fallback := EvaluateSamplingPolicy(cfg, SamplingPressure{BufferUtilization: 0.95})
	if fallback.EventMetricsRate != 0 || fallback.HighValueDetailRate != 0 {
		t.Fatalf("zero event_metrics sample rate should keep fallback high-value at 0, got %#v", fallback)
	}

	explicitRate := 0.5
	cfg.HighValueDetailSampleRate = &explicitRate
	normal := EvaluateSamplingPolicy(cfg, SamplingPressure{})
	if normal.EventMetricsRate != 0 || normal.HighValueDetailRate != 0.5 {
		t.Fatalf("explicit high-value rate should be independent from zero event_metrics rate, got %#v", normal)
	}

	degraded := EvaluateSamplingPolicy(cfg, SamplingPressure{BufferUtilization: 0.95})
	if degraded.State != SamplingStateDegraded || degraded.EventMetricsRate != 0 ||
		degraded.HighValueDetailRate <= 0 || degraded.HighValueDetailRate >= normal.HighValueDetailRate {
		t.Fatalf("degraded pressure should keep event_metrics at 0 and lower explicit high-value rate, got %#v", degraded)
	}
}

func TestDynamicSamplingPolicyClassifiesPressureSources(t *testing.T) {
	// 需求背景：动态采样压力来源包含 buffer、flush lag/error、flush duration、event rate 和 drop rate。
	cfg := observabilityPresetConfigOrFull()
	cfg.EventMetricsSampleRate = 0.8
	cfg.MinSampleRate = 0.2
	cfg.FlushInterval = time.Second
	cfg.FlushTimeout = time.Second
	cfg.MaxEventsPerSecond = 100

	cases := []struct {
		name     string
		pressure SamplingPressure
		state    string
	}{
		{name: "warming buffer", pressure: SamplingPressure{BufferUtilization: 0.55}, state: SamplingStateWarming},
		{name: "pressured flush lag", pressure: SamplingPressure{FlushLag: 2 * time.Second}, state: SamplingStatePressured},
		{name: "degraded flush lag", pressure: SamplingPressure{FlushLag: 3 * time.Second}, state: SamplingStateDegraded},
		{name: "pressured flush error", pressure: SamplingPressure{FlushErrorStreak: 1}, state: SamplingStatePressured},
		{name: "degraded flush error", pressure: SamplingPressure{FlushErrorStreak: 3}, state: SamplingStateDegraded},
		{name: "warming flush duration", pressure: SamplingPressure{FlushDuration: 550 * time.Millisecond}, state: SamplingStateWarming},
		{name: "warming event rate", pressure: SamplingPressure{EventRate: 55}, state: SamplingStateWarming},
		{name: "pressured drop rate", pressure: SamplingPressure{DropRate: 0.01}, state: SamplingStatePressured},
		{name: "degraded drop rate", pressure: SamplingPressure{DropRate: 0.10}, state: SamplingStateDegraded},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result := EvaluateSamplingPolicy(cfg, tc.pressure)
			if result.State != tc.state {
				t.Fatalf("expected state %s, got %#v", tc.state, result)
			}
		})
	}
}

func TestCollectorInputFromEventUsesDynamicSamplingRates(t *testing.T) {
	// 需求背景：生产事件入口必须使用动态采样后的实际 rate，不能把配置上限当作窗口有效采样率。
	cfg := observabilityPresetConfigOrFull()
	cfg.EventMetricsSampleRate = 1
	cfg.DynamicSamplingEnabled = true
	cfg.MinSampleRate = 0.2
	coll := newCollector(cfg)

	input := coll.inputFromEventWithPressure(queue.JobProcessed{
		Connection: "redis",
		Queue:      "default",
		JobID:      "job-1",
		JobName:    "IntegrationJob",
		Duration:   25 * time.Millisecond,
	}, SamplingPressure{BufferUtilization: 0.82})

	if input.Sampling.EventMetricsSampleRate >= 1 || input.Sampling.EventMetricsSampleRate < cfg.MinSampleRate {
		t.Fatalf("event input should use lowered dynamic event_metrics rate, got %#v", input.Sampling)
	}
	if input.Sampling.HighValueDetailRate != input.Sampling.EventMetricsSampleRate {
		t.Fatalf("missing high-value override should fall back to current actual event_metrics rate, got %#v", input.Sampling)
	}
	if !input.Sampling.Estimated {
		t.Fatalf("dynamic sampling below 1 must mark derived metrics estimated, got %#v", input.Sampling)
	}
}

func TestCollectorSamplingPressureReportsRequiredInputs(t *testing.T) {
	// 需求背景：动态采样的压力输入至少要包含 buffer、reservoir、aggregate key、event rate 和 drop rate。
	cfg := observabilityPresetConfigOrFull()
	cfg.BufferSize = 2
	cfg.SampleReservoirSize = 4
	cfg.MaxAggregateKeys = 8
	cfg.MaxEventsPerSecond = 1
	coll := newCollector(cfg)

	coll.mu.Lock()
	coll.memState.BufferUsed = 1
	coll.rtSamples = []int64{10, 20}
	coll.aggKeys["redis:default:A"] = &aggregateKeyState{key: "redis:default:A", lastActive: time.Now()}
	coll.rateTracker.recordAt(time.Now())
	coll.rateTracker.recordAt(time.Now())
	coll.drops[MemoryDropRateLimited] = 1
	coll.mu.Unlock()

	pressure := coll.SamplingPressure()
	if pressure.BufferUtilization != 0.5 ||
		pressure.ReservoirUtilization != 0.5 ||
		pressure.AggregateKeyCount != 1 ||
		pressure.MaxAggregateKeys != 8 ||
		pressure.EventRate == 0 ||
		pressure.DropRate == 0 {
		t.Fatalf("collector pressure missing required inputs: %#v", pressure)
	}
}

func TestEventMetricWindowWithZeroEffectiveSampleRateIsUnknown(t *testing.T) {
	// 需求背景：effective_sample_rate=0 表示未采样观测，读模型必须为 unknown，不能伪装为 exact 或 estimated 0。
	window := eventMetricWindowFromIncrement(EventMetricIncrement{
		WindowStart:         time.Date(2026, 5, 15, 10, 0, 0, 0, time.UTC),
		WindowEnd:           time.Date(2026, 5, 15, 10, 1, 0, 0, time.UTC),
		Connection:          "redis",
		Queue:               "default",
		Samples:             0,
		EffectiveSampleRate: 0,
	})
	if !window.Unknown || window.Quality != EventMetricQualityUnknown || window.EffectiveSampleRate != 0 {
		t.Fatalf("zero effective sample rate should remain unknown with rate 0, got %#v", window)
	}
}

func TestEstimateEventMetricWindowsSumsPerWindowEstimates(t *testing.T) {
	// 需求背景：跨窗口聚合必须先按各自 effective_sample_rate 估算，再求和，不能用平均采样率反推总量。
	windows := []EventMetricWindow{
		{
			WindowStart:         time.Date(2026, 5, 15, 10, 0, 0, 0, time.UTC),
			WindowEnd:           time.Date(2026, 5, 15, 10, 1, 0, 0, time.UTC),
			Connection:          "redis",
			Queue:               "default",
			SampleCount:         10,
			EffectiveSampleRate: 0.5,
			Quality:             EventMetricQualityEstimated,
		},
		{
			WindowStart:         time.Date(2026, 5, 15, 10, 1, 0, 0, time.UTC),
			WindowEnd:           time.Date(2026, 5, 15, 10, 2, 0, 0, time.UTC),
			Connection:          "redis",
			Queue:               "default",
			SampleCount:         10,
			EffectiveSampleRate: 0.1,
			Quality:             EventMetricQualityEstimated,
		},
	}

	estimate := EstimateEventMetricWindows(windows)
	if estimate.Quality != EventMetricQualityEstimated ||
		estimate.SampledCount != 20 ||
		estimate.EstimatedTotal != 120 ||
		len(estimate.Windows) != 2 {
		t.Fatalf("expected per-window estimate total 120, got %#v", estimate)
	}
}

func TestRuntimePercentilesUseSamplesAndReturnUnknownWhenSparse(t *testing.T) {
	// 需求背景：P95/P99 只能基于样本分布估算；样本不足时返回 unknown，不能用 sample_rate 反推。
	sparse := EstimateRuntimePercentiles([]int64{10, 20}, 5)
	if sparse.Quality != EventMetricQualityUnknown || sparse.P95 != 0 || sparse.P99 != 0 {
		t.Fatalf("sparse percentile samples should be unknown, got %#v", sparse)
	}

	samples := []int64{100, 20, 60, 40, 80}
	estimate := EstimateRuntimePercentiles(samples, 5)
	if estimate.Quality != EventMetricQualityEstimated || estimate.SampledCount != int64(len(samples)) {
		t.Fatalf("percentiles should be estimated from samples, got %#v", estimate)
	}
	sorted := append([]int64(nil), samples...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	if estimate.P95 != sorted[4] || estimate.P99 != sorted[4] {
		t.Fatalf("percentiles should come from sample distribution, got %#v sorted=%v", estimate, sorted)
	}
}

func TestCollectorKeepsEventMetricsZeroWhileAllowingExplicitHighValueDetail(t *testing.T) {
	// 需求背景：event_metrics_sample_rate=0 不能采集 event-derived counters；显式 high-value 采样率仍可采集诊断明细。
	cfg := observabilityPresetConfigOrFull()
	cfg.EventMetrics = true
	cfg.EventMetricsSampleRate = 0
	highValueRate := 1.0
	cfg.HighValueDetailSampleRate = &highValueRate
	cfg.FailedDetailEnabled = true
	cfg.BufferSize = 10

	coll := newCollector(cfg)
	coll.Start(context.Background())
	defer coll.Stop()

	_ = coll.Collect(context.Background(), CollectorInput{
		Event:        queue.EventJobFailed,
		Connection:   "redis",
		Queue:        "default",
		JobID:        "job-failed",
		JobName:      "ImportantJob",
		ErrorSummary: "boom",
		OccurredAt:   time.Now(),
		Sampling: SamplingDecision{
			EventMetricsSampled:    true,
			EventMetricsSampleRate: 0,
			HighValueDetailSampled: true,
			HighValueDetailRate:    1,
			Estimated:              true,
		},
	})
	time.Sleep(50 * time.Millisecond)

	snapshot := coll.FlushSnapshot(time.Now())
	if len(snapshot.windows) != 0 {
		t.Fatalf("event_metrics windows should stay empty when configured sample rate is 0, got %#v", snapshot.windows)
	}
	if len(snapshot.details) != 1 || snapshot.details[0].Kind != HighValueDetailFailed {
		t.Fatalf("explicit high-value detail should still be collected, got %#v", snapshot.details)
	}
}

func TestPercentileBoundary(t *testing.T) {
	// 需求背景：dynamic sampling contract 的 percentile 函数用最近秩法计算百分位数。
	// 本测试覆盖边界：空切片、单元素、极端分位数、P95/P99 在小样本上的分歧。
	// 百分位数按 idx=int((len(sorted)-1)*quantile + 0.5) 计算，再 clamp 到 [0, len-1]。

	// 空切片 → 0
	if p := percentile(nil, 0.5); p != 0 {
		t.Fatalf("nil slice: got %d, want 0", p)
	}
	if p := percentile([]int64{}, 0.95); p != 0 {
		t.Fatalf("empty slice: got %d, want 0", p)
	}

	// 单元素 → 始终返回该元素
	if p := percentile([]int64{42}, 0.0); p != 42 {
		t.Fatalf("single at 0.0: got %d, want 42", p)
	}
	if p := percentile([]int64{42}, 0.5); p != 42 {
		t.Fatalf("single at 0.5: got %d, want 42", p)
	}
	if p := percentile([]int64{42}, 1.0); p != 42 {
		t.Fatalf("single at 1.0: got %d, want 42", p)
	}

	// 10 元素切片，用于极端分位数测试
	sorted := []int64{10, 20, 30, 40, 50, 60, 70, 80, 90, 100}

	// quantile < 0 → 下标被 clamp 到 0
	if p := percentile(sorted, -0.5); p != sorted[0] {
		t.Fatalf("negative quantile: got %d, want %d (clamped to min)", p, sorted[0])
	}

	// quantile > 1 → 下标被 clamp 到 len-1
	if p := percentile(sorted, 2.0); p != sorted[len(sorted)-1] {
		t.Fatalf("over-1 quantile: got %d, want %d (clamped to max)", p, sorted[len(sorted)-1])
	}

	// Small sample P95 vs P99 divergence:
	// 12 元素，idx_P95 = int(11*0.95+0.5) = int(10.95) = 10 → sorted[10]=110
	// 12 元素，idx_P99 = int(11*0.99+0.5) = int(11.39) = 11 → sorted[11]=120
	sorted12 := []int64{10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120}
	p95 := percentile(sorted12, 0.95)
	p99 := percentile(sorted12, 0.99)
	if p95 != 110 || p99 != 120 {
		t.Fatalf("P95 vs P99 divergence: P95=%d (want 110), P99=%d (want 120)", p95, p99)
	}
}
