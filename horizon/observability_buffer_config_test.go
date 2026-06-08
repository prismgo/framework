package horizon

import (
	"strings"
	"testing"
	"time"
)

func TestLoadConfigParsesObservabilityBufferDefaultsAndPresets(t *testing.T) {
	// 需求背景：buffer config contract 把 preset 收敛为默认值来源，解析后的 buffer、flush、sampling、
	// retention 和 high-value detail 字段必须成为独立强类型配置。
	base := map[string]any{
		"defaults":     map[string]any{"supervisor-default": validSupervisorConfig()},
		"environments": map[string]any{"production": map[string]any{}},
	}
	full, err := LoadConfigFrom(fakeConfig{maps: map[string]map[string]any{"horizon": base}})
	if err != nil {
		t.Fatalf("load full defaults: %v", err)
	}
	if !full.Observability.Enabled(ObservabilityEventMetrics) ||
		!full.Observability.Enabled(ObservabilityWaits) ||
		!full.Observability.Enabled(ObservabilityBatchSummaries) ||
		!full.Observability.Enabled(ObservabilityProcessHealth) ||
		!full.Observability.Enabled(ObservabilityQueueLengths) {
		t.Fatalf("full preset should enable current capabilities: %#v", full.Observability)
	}
	if full.Observability.EventMetricsSampleRate != 1 ||
		full.Observability.HighValueDetailSampleRate != nil ||
		full.Observability.EffectiveHighValueDetailSampleRate(0.25) != 0.25 ||
		full.Observability.MetricsWindow != time.Minute ||
		full.Observability.FlushInterval != time.Minute ||
		full.Observability.FlushTimeout != 5*time.Second ||
		full.Observability.BatchSize != 500 ||
		full.Observability.BatchSummarySize != full.Observability.BatchSize ||
		full.Observability.BufferSize != 10000 ||
		full.Observability.SampleReservoirSize != 2048 ||
		full.Observability.MaxAggregateKeys != 10000 ||
		full.Observability.AggregateKeyTTL != 30*time.Minute ||
		full.Observability.EventMetricsRetention != 24*time.Hour ||
		full.Observability.HighValueDetailRetention != 24*time.Hour ||
		full.Observability.BatchSummaryRetention != 24*time.Hour ||
		full.Observability.DiagnosticsRetention != 24*time.Hour {
		t.Fatalf("unexpected full observability defaults: %#v", full.Observability)
	}

	lightRoot := mergeTestMap(base, map[string]any{"observability": map[string]any{"preset": ObservabilityPresetProductionLight}})
	light, err := LoadConfigFrom(fakeConfig{maps: map[string]map[string]any{"horizon": lightRoot}})
	if err != nil {
		t.Fatalf("load production_light: %v", err)
	}
	if !light.Observability.Enabled(ObservabilityEventMetrics) || light.Observability.EventMetricsSampleRate >= 1 ||
		light.Observability.Enabled(ObservabilityWaits) || light.Observability.Enabled(ObservabilityBatchSummaries) {
		t.Fatalf("production_light should keep sampled event metrics and skip higher cost defaults: %#v", light.Observability)
	}

	minimalRoot := mergeTestMap(base, map[string]any{"observability": map[string]any{"preset": ObservabilityPresetMinimal}})
	minimal, err := LoadConfigFrom(fakeConfig{maps: map[string]map[string]any{"horizon": minimalRoot}})
	if err != nil {
		t.Fatalf("load minimal: %v", err)
	}
	if minimal.Observability.Enabled(ObservabilityEventMetrics) || minimal.Observability.Enabled(ObservabilityWaits) ||
		!minimal.Observability.Enabled(ObservabilityProcessHealth) || !minimal.Observability.Enabled(ObservabilityQueueLengths) {
		t.Fatalf("minimal should keep only process health and queue lengths: %#v", minimal.Observability)
	}
}

func TestLoadConfigObservabilityPresetAllowsStrictExplicitOverrides(t *testing.T) {
	// 需求背景：preset 现在只提供默认值；显式子选项必须覆盖 preset，且即使 preset 存在也要严格校验。
	root := map[string]any{
		"observability": map[string]any{
			"preset":                        ObservabilityPresetProductionLight,
			"event_metrics":                 false,
			"waits":                         true,
			"batch_summaries":               true,
			"event_metrics_sample_rate":     0,
			"high_value_detail_sample_rate": 0.75,
			"failed_detail_enabled":         true,
			"poison_detail_enabled":         true,
			"slow_job_detail_enabled":       true,
			"slow_job_threshold":            "3s",
			"metrics_window":                "30s",
			"flush_interval":                "45s",
			"flush_timeout":                 "2s",
			"batch_size":                    25,
			"batch_summary_size":            7,
			"buffer_size":                   50,
			"max_events_per_second":         100,
			"drop_policy":                   "drop_newest",
			"sample_reservoir_size":         99,
			"max_aggregate_keys":            88,
			"aggregate_key_ttl":             "10m",
			"event_metrics_retention":       "2h",
			"high_value_detail_retention":   "3h",
			"batch_summary_retention":       "4h",
			"diagnostics_retention":         "5h",
			"dynamic_sampling_enabled":      false,
			"min_sample_rate":               0,
		},
		"defaults":     map[string]any{"supervisor-default": validSupervisorConfig()},
		"environments": map[string]any{"production": map[string]any{}},
	}
	loaded, err := LoadConfigFrom(fakeConfig{maps: map[string]map[string]any{"horizon": root}})
	if err != nil {
		t.Fatalf("load explicit observability overrides: %v", err)
	}
	obs := loaded.Observability
	if obs.Enabled(ObservabilityEventMetrics) || obs.Enabled(ObservabilityWaits) ||
		!obs.Enabled(ObservabilityBatchSummaries) {
		t.Fatalf("explicit booleans should override preset defaults: %#v", obs)
	}
	if obs.EventMetricsSampleRate != 0 || obs.HighValueDetailSampleRate == nil || *obs.HighValueDetailSampleRate != 0.75 ||
		obs.EffectiveHighValueDetailSampleRate(0) != 0.75 {
		t.Fatalf("sampling overrides were not parsed: %#v", obs)
	}
	if !obs.Enabled(ObservabilityHighValueDetail) || !obs.Enabled(ObservabilityFailedDetail) ||
		!obs.Enabled(ObservabilityPoisonDetail) || !obs.Enabled(ObservabilitySlowJobDetail) {
		t.Fatalf("explicit high-value detail exception should be enabled: %#v", obs)
	}
	if obs.SlowJobThreshold != 3*time.Second || obs.MetricsWindow != 30*time.Second ||
		obs.FlushInterval != 45*time.Second || obs.FlushTimeout != 2*time.Second ||
		obs.BatchSize != 25 || obs.BatchSummarySize != 7 || obs.BufferSize != 50 || obs.MaxEventsPerSecond != 100 ||
		obs.DropPolicy != ObservabilityDropNewest || obs.SampleReservoirSize != 99 ||
		obs.MaxAggregateKeys != 88 || obs.AggregateKeyTTL != 10*time.Minute ||
		obs.EventMetricsRetention != 2*time.Hour || obs.HighValueDetailRetention != 3*time.Hour ||
		obs.BatchSummaryRetention != 4*time.Hour || obs.DiagnosticsRetention != 5*time.Hour ||
		obs.DynamicSamplingEnabled || obs.MinSampleRate != 0 {
		t.Fatalf("buffer and retention overrides were not parsed: %#v", obs)
	}
}

func TestLoadConfigObservabilityBatchSummarySizeDefaultsToBatchSize(t *testing.T) {
	root := map[string]any{
		"observability": map[string]any{"batch_size": 37},
		"defaults":      map[string]any{"supervisor-default": validSupervisorConfig()},
		"environments":  map[string]any{"production": map[string]any{}},
	}
	loaded, err := LoadConfigFrom(fakeConfig{maps: map[string]map[string]any{"horizon": root}})
	if err != nil {
		t.Fatalf("load observability batch defaults: %v", err)
	}
	if loaded.Observability.BatchSummarySize != 37 {
		t.Fatalf("batch_summary_size should default to batch_size, got %#v", loaded.Observability)
	}
}

func TestLoadConfigRejectsRemovedObservabilityConfigAndEnv(t *testing.T) {
	// 需求背景：旧 recent/history/silenced/trim 配置已经从新观测合同移除，必须 fail fast 并提示迁移方向。
	cases := map[string]map[string]any{
		"recent_jobs":            {"observability": map[string]any{"recent_jobs": true}},
		"job_history":            {"observability": map[string]any{"job_history": true}},
		"queue_history":          {"observability": map[string]any{"queue_history": true}},
		"success_sample_rate":    {"observability": map[string]any{"success_sample_rate": 1}},
		"success_detail_enabled": {"observability": map[string]any{"success_detail_enabled": true}},
		"silenced":               {"silenced": []any{"App\\Jobs\\Quiet"}},
		"silenced_tags":          {"silenced_tags": []any{"quiet"}},
		"trim":                   {"trim": map[string]any{"recent": 60}},
		"metrics trim":           {"metrics": map[string]any{"trim_snapshots": map[string]any{"job": 24}}},
	}
	for name, extra := range cases {
		t.Run(name, func(t *testing.T) {
			root := mergeTestMap(map[string]any{
				"defaults":     map[string]any{"supervisor-default": validSupervisorConfig()},
				"environments": map[string]any{"production": map[string]any{}},
			}, extra)
			_, err := LoadConfigFrom(fakeConfig{maps: map[string]map[string]any{"horizon": root}})
			if err == nil {
				t.Fatal("expected removed config field to fail")
			}
			if !strings.Contains(err.Error(), "removed") {
				t.Fatalf("error should identify removed config, got %v", err)
			}
		})
	}

	t.Setenv("HORIZON_OBSERVABILITY_RECENT_JOBS", "true")
	_, err := LoadConfigFrom(fakeConfig{maps: map[string]map[string]any{"horizon": {
		"defaults":     map[string]any{"supervisor-default": validSupervisorConfig()},
		"environments": map[string]any{"production": map[string]any{}},
	}}})
	if err == nil || !strings.Contains(err.Error(), "HORIZON_OBSERVABILITY_RECENT_JOBS") || !strings.Contains(err.Error(), "removed") {
		t.Fatalf("expected removed env error, got %v", err)
	}
}

func TestLoadConfigRejectsInvalidObservabilityBufferValuesAndUnknownFields(t *testing.T) {
	cases := map[string]map[string]any{
		"unknown field":       {"mystery": true},
		"invalid bool":        {"preset": ObservabilityPresetFull, "event_metrics": "maybe"},
		"invalid sample rate": {"event_metrics_sample_rate": 1.1},
		"negative sample":     {"high_value_detail_sample_rate": -0.1},
		"zero duration":       {"metrics_window": 0},
		"bad duration":        {"flush_interval": "soon"},
		"zero batch size":     {"batch_size": 0},
		"zero summary size":   {"batch_summary_size": 0},
		"bad drop policy":     {"drop_policy": "block_worker"},
		"bad min sample":      {"min_sample_rate": 2},
	}
	for name, observability := range cases {
		t.Run(name, func(t *testing.T) {
			root := map[string]any{
				"observability": observability,
				"defaults":      map[string]any{"supervisor-default": validSupervisorConfig()},
				"environments":  map[string]any{"production": map[string]any{}},
			}
			_, err := LoadConfigFrom(fakeConfig{maps: map[string]map[string]any{"horizon": root}})
			if err == nil {
				t.Fatal("expected invalid observability config to fail")
			}
			if !strings.Contains(err.Error(), "observability") {
				t.Fatalf("error should identify observability config, got %v", err)
			}
		})
	}
}

func TestLoadConfigObservabilityCoversAllSampleRateAndDurationInputTypes(t *testing.T) {
	// 需求背景：strictSampleRate、strictPositiveDuration 需要覆盖 float32、
	// int、int64、float64 等 yaml/json 反序列化常见类型，确保所有分支被测试。
	t.Run("sample rate as int", func(t *testing.T) {
		root := map[string]any{
			"observability": map[string]any{"event_metrics_sample_rate": 0},
			"defaults":      map[string]any{"supervisor-default": validSupervisorConfig()},
			"environments":  map[string]any{"production": map[string]any{}},
		}
		loaded, err := LoadConfigFrom(fakeConfig{maps: map[string]map[string]any{"horizon": root}})
		if err != nil {
			t.Fatalf("int 0 sample rate should be valid: %v", err)
		}
		if loaded.Observability.EventMetricsSampleRate != 0 {
			t.Fatalf("expected sample rate 0, got %v", loaded.Observability.EventMetricsSampleRate)
		}
	})
	t.Run("sample rate as int64", func(t *testing.T) {
		root := map[string]any{
			"observability": map[string]any{"event_metrics_sample_rate": int64(1)},
			"defaults":      map[string]any{"supervisor-default": validSupervisorConfig()},
			"environments":  map[string]any{"production": map[string]any{}},
		}
		loaded, err := LoadConfigFrom(fakeConfig{maps: map[string]map[string]any{"horizon": root}})
		if err != nil {
			t.Fatalf("int64 sample rate should be valid: %v", err)
		}
		if loaded.Observability.EventMetricsSampleRate != 1 {
			t.Fatalf("expected sample rate 1, got %v", loaded.Observability.EventMetricsSampleRate)
		}
	})
	t.Run("duration as int", func(t *testing.T) {
		root := map[string]any{
			"observability": map[string]any{"metrics_window": 30},
			"defaults":      map[string]any{"supervisor-default": validSupervisorConfig()},
			"environments":  map[string]any{"production": map[string]any{}},
		}
		loaded, err := LoadConfigFrom(fakeConfig{maps: map[string]map[string]any{"horizon": root}})
		if err != nil {
			t.Fatalf("int duration should be valid: %v", err)
		}
		if loaded.Observability.MetricsWindow != 30*time.Second {
			t.Fatalf("expected 30s, got %v", loaded.Observability.MetricsWindow)
		}
	})
	t.Run("duration as float64", func(t *testing.T) {
		root := map[string]any{
			"observability": map[string]any{"metrics_window": 45.0},
			"defaults":      map[string]any{"supervisor-default": validSupervisorConfig()},
			"environments":  map[string]any{"production": map[string]any{}},
		}
		loaded, err := LoadConfigFrom(fakeConfig{maps: map[string]map[string]any{"horizon": root}})
		if err != nil {
			t.Fatalf("float64 duration should be valid: %v", err)
		}
		if loaded.Observability.MetricsWindow != 45*time.Second {
			t.Fatalf("expected 45s, got %v", loaded.Observability.MetricsWindow)
		}
	})
	t.Run("duration as int64", func(t *testing.T) {
		root := map[string]any{
			"observability": map[string]any{"metrics_window": int64(15)},
			"defaults":      map[string]any{"supervisor-default": validSupervisorConfig()},
			"environments":  map[string]any{"production": map[string]any{}},
		}
		loaded, err := LoadConfigFrom(fakeConfig{maps: map[string]map[string]any{"horizon": root}})
		if err != nil {
			t.Fatalf("int64 duration should be valid: %v", err)
		}
		if loaded.Observability.MetricsWindow != 15*time.Second {
			t.Fatalf("expected 15s, got %v", loaded.Observability.MetricsWindow)
		}
	})
	t.Run("bool string false", func(t *testing.T) {
		root := map[string]any{
			"observability": map[string]any{"event_metrics": "false"},
			"defaults":      map[string]any{"supervisor-default": validSupervisorConfig()},
			"environments":  map[string]any{"production": map[string]any{}},
		}
		_, err := LoadConfigFrom(fakeConfig{maps: map[string]map[string]any{"horizon": root}})
		if err != nil {
			t.Fatalf("string false should be valid bool: %v", err)
		}
	})
	t.Run("drop policy newest", func(t *testing.T) {
		root := map[string]any{
			"observability": map[string]any{"drop_policy": "drop_newest"},
			"defaults":      map[string]any{"supervisor-default": validSupervisorConfig()},
			"environments":  map[string]any{"production": map[string]any{}},
		}
		loaded, err := LoadConfigFrom(fakeConfig{maps: map[string]map[string]any{"horizon": root}})
		if err != nil {
			t.Fatalf("drop_newest should be valid: %v", err)
		}
		if loaded.Observability.DropPolicy != ObservabilityDropNewest {
			t.Fatalf("expected drop_newest, got %v", loaded.Observability.DropPolicy)
		}
	})
}

func TestObservabilityContractsZeroDivisionAndNoDropEdgeCases(t *testing.T) {
	// 需求背景：BufferUtilization、AggregateKeyUtilization 的除零保护分支
	// 和 HasDrops 的快速出口需要显式覆盖，确保未来重构不会改变防御行为。

	// BufferSize=0 时 BufferUtilization 应返回 0
	zeroBuf := ObservabilityMemoryState{BufferSize: 0, BufferUsed: 10}
	if zeroBuf.BufferUtilization() != 0 {
		t.Fatalf("zero buffer capacity should return 0, got %v", zeroBuf.BufferUtilization())
	}

	// MaxAggregateKeys=0 时 AggregateKeyUtilization 应返回 0
	zeroAgg := ObservabilityMemoryState{MaxAggregateKeys: 0, AggregateKeyCount: 5}
	if zeroAgg.AggregateKeyUtilization() != 0 {
		t.Fatalf("zero aggregate key capacity should return 0, got %v", zeroAgg.AggregateKeyUtilization())
	}

	// 无丢弃诊断时 HasDrops 应返回 false
	noDrops := FlushBatch{}
	if noDrops.HasDrops() {
		t.Fatal("empty batch should not report drops")
	}

	// Count=0 的诊断不算丢弃
	zeroCount := FlushBatch{Diagnostics: []ObservabilityDiagnostic{{Reason: "test", Count: 0}}}
	if zeroCount.HasDrops() {
		t.Fatal("zero-count diagnostic should not report drops")
	}
}

func TestLoadConfigObservabilitySampleRateFloat32AndEdgeTypes(t *testing.T) {
	// 需求背景：strictSampleRate 和 strictPositiveDuration 的 float32、string-empty
	// 和 time.Duration 分支需要独立测试覆盖。
	t.Run("sample rate as float32", func(t *testing.T) {
		root := map[string]any{
			"observability": map[string]any{"event_metrics_sample_rate": float32(0.5)},
			"defaults":      map[string]any{"supervisor-default": validSupervisorConfig()},
			"environments":  map[string]any{"production": map[string]any{}},
		}
		loaded, err := LoadConfigFrom(fakeConfig{maps: map[string]map[string]any{"horizon": root}})
		if err != nil {
			t.Fatalf("float32 sample rate should be valid: %v", err)
		}
		if loaded.Observability.EventMetricsSampleRate != 0.5 {
			t.Fatalf("expected 0.5, got %v", loaded.Observability.EventMetricsSampleRate)
		}
	})
	t.Run("duration as time.Duration type", func(t *testing.T) {
		root := map[string]any{
			"observability": map[string]any{"metrics_window": time.Minute},
			"defaults":      map[string]any{"supervisor-default": validSupervisorConfig()},
			"environments":  map[string]any{"production": map[string]any{}},
		}
		loaded, err := LoadConfigFrom(fakeConfig{maps: map[string]map[string]any{"horizon": root}})
		if err != nil {
			t.Fatalf("time.Duration should be valid: %v", err)
		}
		if loaded.Observability.MetricsWindow != time.Minute {
			t.Fatalf("expected 1m, got %v", loaded.Observability.MetricsWindow)
		}
	})
	t.Run("drop policy old style as valid", func(t *testing.T) {
		root := map[string]any{
			"observability": map[string]any{"drop_policy": "drop_oldest"},
			"defaults":      map[string]any{"supervisor-default": validSupervisorConfig()},
			"environments":  map[string]any{"production": map[string]any{}},
		}
		loaded, err := LoadConfigFrom(fakeConfig{maps: map[string]map[string]any{"horizon": root}})
		if err != nil {
			t.Fatalf("drop_oldest should be valid: %v", err)
		}
		if loaded.Observability.DropPolicy != ObservabilityDropOldest {
			t.Fatalf("expected drop_oldest, got %v", loaded.Observability.DropPolicy)
		}
	})
}
