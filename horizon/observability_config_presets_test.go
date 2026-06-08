package horizon

import (
	"strings"
	"testing"
)

func TestLoadConfigParsesObservabilityDefaultsAndPresets(t *testing.T) {
	// 需求背景：未提供 preset 和子开关时，应默认回退到 full；
	// production_light/minimal 仍由显式 preset 选择，不能随 APP_ENV 自动切换。
	// buffer config contract 后 recent_jobs/job_history/queue_history 已移除，改用新观测能力集合。
	base := map[string]any{
		"defaults":     map[string]any{"supervisor-default": validSupervisorConfig()},
		"environments": map[string]any{"production": map[string]any{}},
	}
	full, err := LoadConfigFrom(fakeConfig{maps: map[string]map[string]any{"horizon": base}})
	if err != nil {
		t.Fatalf("load full defaults: %v", err)
	}
	if full.Observability.Preset != ObservabilityPresetFull ||
		!full.Observability.Enabled(ObservabilityEventMetrics) ||
		!full.Observability.Enabled(ObservabilityWaits) ||
		!full.Observability.Enabled(ObservabilityBatchSummaries) ||
		!full.Observability.Enabled(ObservabilityProcessHealth) ||
		!full.Observability.Enabled(ObservabilityQueueLengths) {
		t.Fatalf("full defaults did not enable all observability abilities: %#v", full.Observability)
	}
	if full.Observability.QueuedWaitsMax != 10000 ||
		full.Observability.ProcessingSpansMax != 10000 ||
		full.Observability.ProcessingCleanupIntervalSeconds != 60 {
		t.Fatalf("unexpected full limits: %#v", full.Observability)
	}

	lightRoot := mergeTestMap(base, map[string]any{"observability": map[string]any{"preset": ObservabilityPresetProductionLight}})
	light, err := LoadConfigFrom(fakeConfig{maps: map[string]map[string]any{"horizon": lightRoot}})
	if err != nil {
		t.Fatalf("load production_light: %v", err)
	}
	for _, feature := range []ObservabilityFeature{ObservabilityProcessHealth, ObservabilityQueueLengths, ObservabilityEventMetrics} {
		if !light.Observability.Enabled(feature) {
			t.Fatalf("production_light should enable %s: %#v", feature, light.Observability)
		}
	}
	for _, feature := range []ObservabilityFeature{ObservabilityWaits, ObservabilityBatchSummaries} {
		if light.Observability.Enabled(feature) {
			t.Fatalf("production_light should disable %s: %#v", feature, light.Observability)
		}
	}

	minimalRoot := mergeTestMap(base, map[string]any{"observability": map[string]any{"preset": ObservabilityPresetMinimal}})
	minimal, err := LoadConfigFrom(fakeConfig{maps: map[string]map[string]any{"horizon": minimalRoot}})
	if err != nil {
		t.Fatalf("load minimal: %v", err)
	}
	if !minimal.Observability.Enabled(ObservabilityProcessHealth) || !minimal.Observability.Enabled(ObservabilityQueueLengths) {
		t.Fatalf("minimal should keep core health and queue lengths: %#v", minimal.Observability)
	}
	if minimal.Observability.Enabled(ObservabilityEventMetrics) ||
		minimal.Observability.Enabled(ObservabilityBatchSummaries) {
		t.Fatalf("minimal should disable high-cost details: %#v", minimal.Observability)
	}
}

func TestLoadConfigObservabilitySwitchesApplyWhenPresetMissing(t *testing.T) {
	// 逻辑说明：只有 preset 字段缺失时，布尔子开关才参与合并；
	// 这里显式关闭多项能力，验证它们会覆盖 full 基线。
	// buffer config contract 后使用新观测能力字段替代已移除的 recent_jobs/job_history/queue_history。
	root := map[string]any{
		"observability": map[string]any{
			"event_metrics":   false,
			"waits":           false,
			"batch_summaries": false,
			"process_health":  false,
			"queue_lengths":   false,
		},
		"defaults":     map[string]any{"supervisor-default": validSupervisorConfig()},
		"environments": map[string]any{"production": map[string]any{}},
	}
	loaded, err := LoadConfigFrom(fakeConfig{maps: map[string]map[string]any{"horizon": root}})
	if err != nil {
		t.Fatalf("load observability without preset: %v", err)
	}
	if loaded.Observability.Enabled(ObservabilityEventMetrics) ||
		loaded.Observability.Enabled(ObservabilityWaits) ||
		loaded.Observability.Enabled(ObservabilityBatchSummaries) ||
		loaded.Observability.Enabled(ObservabilityProcessHealth) ||
		loaded.Observability.Enabled(ObservabilityQueueLengths) {
		t.Fatalf("boolean switches should override defaults when preset is missing: %#v", loaded.Observability)
	}
}

func TestLoadConfigObservabilityEmptyPresetTreatsAsUnset(t *testing.T) {
	// 逻辑说明：preset 只有在字段有非空值时才锁定布尔能力；
	// 空字符串应与未设置等价，仍允许子开关覆盖 full 基线。
	root := map[string]any{
		"observability": map[string]any{
			"preset":          "",
			"event_metrics":   false,
			"process_health":  false,
			"queue_lengths":   false,
			"batch_summaries": false,
		},
		"defaults":     map[string]any{"supervisor-default": validSupervisorConfig()},
		"environments": map[string]any{"production": map[string]any{}},
	}
	loaded, err := LoadConfigFrom(fakeConfig{maps: map[string]map[string]any{"horizon": root}})
	if err != nil {
		t.Fatalf("load observability with empty preset: %v", err)
	}
	if loaded.Observability.Enabled(ObservabilityEventMetrics) ||
		loaded.Observability.Enabled(ObservabilityProcessHealth) ||
		loaded.Observability.Enabled(ObservabilityQueueLengths) ||
		loaded.Observability.Enabled(ObservabilityBatchSummaries) {
		t.Fatalf("empty preset should behave like unset preset: %#v", loaded.Observability)
	}
}

func TestLoadConfigObservabilityPresetFieldPresenceOverridesBooleanSwitchesAndKeepsLimits(t *testing.T) {
	// 逻辑说明：buffer config contract 起 preset 只提供默认值，所有显式子选项都必须覆盖 preset；
	// 这里显式提供 full preset 同时修改子选项，验证子字段仍然生效。
	root := map[string]any{
		"observability": map[string]any{
			"preset":                              ObservabilityPresetFull,
			"event_metrics":                       false,
			"waits":                               false,
			"batch_summaries":                     false,
			"process_health":                      false,
			"queue_lengths":                       false,
			"queued_waits_max":                    456,
			"processing_spans_max":                0,
			"processing_cleanup_interval_seconds": 10,
			"event_metrics_sample_rate":           0.5,
			"buffer_size":                         123,
		},
		"defaults":     map[string]any{"supervisor-default": validSupervisorConfig()},
		"environments": map[string]any{"production": map[string]any{}},
	}
	loaded, err := LoadConfigFrom(fakeConfig{maps: map[string]map[string]any{"horizon": root}})
	if err != nil {
		t.Fatalf("load explicit observability preset: %v", err)
	}
	// 显式子选项覆盖 preset
	if loaded.Observability.Enabled(ObservabilityEventMetrics) ||
		loaded.Observability.Enabled(ObservabilityWaits) ||
		loaded.Observability.Enabled(ObservabilityBatchSummaries) ||
		loaded.Observability.Enabled(ObservabilityProcessHealth) ||
		loaded.Observability.Enabled(ObservabilityQueueLengths) {
		t.Fatalf("explicit booleans should override preset defaults: %#v", loaded.Observability)
	}
	if loaded.Observability.QueuedWaitsMax != 456 {
		t.Fatalf("explicit limits should still apply: %#v", loaded.Observability)
	}
	if loaded.Observability.ProcessingSpansMax != 0 || loaded.Observability.Enabled(ObservabilityProcessingSpans) {
		t.Fatalf("processing_spans_max=0 should disable span storage: %#v", loaded.Observability)
	}
	if loaded.Observability.ProcessingCleanupIntervalSeconds != 10 {
		t.Fatalf("processing cleanup interval should still be configurable: %#v", loaded.Observability)
	}
	if loaded.Observability.EventMetricsSampleRate != 0.5 || loaded.Observability.BufferSize != 123 {
		t.Fatalf("numeric fields should override preset: %#v", loaded.Observability)
	}
}

func TestLoadConfigObservabilityLimitsCanDisablePresetFeatures(t *testing.T) {
	// 逻辑说明：preset 只决定布尔能力集合；当上限类参数显式收紧到 0 时，
	// 依赖 waits / processing spans 的最终可用性仍要被上限继续约束。
	// buffer config contract 后用 queued_waits_max/processing_spans_max 替代已移除的 recent_jobs_max。
	root := map[string]any{
		"observability": map[string]any{
			"preset":               ObservabilityPresetFull,
			"queued_waits_max":     0,
			"processing_spans_max": 0,
		},
		"defaults":     map[string]any{"supervisor-default": validSupervisorConfig()},
		"environments": map[string]any{"production": map[string]any{}},
	}
	loaded, err := LoadConfigFrom(fakeConfig{maps: map[string]map[string]any{"horizon": root}})
	if err != nil {
		t.Fatalf("load full preset with explicit limits: %v", err)
	}
	if loaded.Observability.Enabled(ObservabilityWaits) ||
		loaded.Observability.Enabled(ObservabilityProcessingSpans) {
		t.Fatalf("limits should still be able to disable dependent features: %#v", loaded.Observability)
	}
}

func TestLoadConfigObservabilityStrictValidationRegardlessOfPreset(t *testing.T) {
	// 需求背景：buffer config contract 起所有显式子选项必须严格解析，即使 preset 存在也不能忽略非法值；
	// 非法布尔、整数、浮点、duration 都会让配置加载失败。
	root := map[string]any{
		"observability": map[string]any{
			"preset":        ObservabilityPresetProductionLight,
			"event_metrics": "maybe",
			"buffer_size":   "many",
		},
		"defaults":     map[string]any{"supervisor-default": validSupervisorConfig()},
		"environments": map[string]any{"production": map[string]any{}},
	}
	_, err := LoadConfigFrom(fakeConfig{maps: map[string]map[string]any{"horizon": root}})
	if err == nil {
		t.Fatal("invalid explicit values should fail even when preset is present")
	}
	if !strings.Contains(err.Error(), "observability") {
		t.Fatalf("error should identify observability config, got %v", err)
	}
}

func TestLoadConfigRejectsInvalidObservabilityConfig(t *testing.T) {
	// 需求背景：preset 缺失时，布尔子开关会参与合并，因此其非法值仍需 fail fast；
	// preset 和上限类参数本身非法时也必须直接报错。
	// buffer config contract 后使用新字段替代已移除的 recent_jobs_max。
	cases := map[string]map[string]any{
		"invalid preset":              {"preset": "tiny"},
		"invalid bool without preset": {"event_metrics": "maybe"},
		"invalid int":                 {"buffer_size": "many"},
		"negative int":                {"queued_waits_max": -1},
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
