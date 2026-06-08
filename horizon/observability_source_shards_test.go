package horizon

import (
	"context"
	"testing"
	"time"
)

func TestEventMetricWindowsPreserveInstanceShardsAndAggregateQueues(t *testing.T) {
	// 需求背景：多实例 Horizon 会在同一 namespace/environment 下写入相同 supervisor/queue/job
	// 的 event_metrics；Store 必须保留 host/source 分片，读模型再按事件窗口聚合，不能用
	// flush_at 或全局 snapshot 覆盖来表达多实例指标。
	ctx := context.Background()
	now := time.Date(2026, 5, 15, 10, 0, 0, 0, time.UTC)
	store := NewMemoryStore(StoreOptions{Prefix: "tenant-a"})
	windows := []EventMetricWindow{
		{
			WindowStart:         now,
			WindowEnd:           now.Add(time.Minute),
			FlushAt:             now.Add(10 * time.Second),
			MetricsWindowMS:     int64(time.Minute / time.Millisecond),
			SourcePrefix:        "tenant-a",
			SourceHost:          "host-a",
			SourceEnvironment:   "production",
			SourceSupervisor:    "supervisor-default",
			Connection:          "redis",
			Queue:               "default",
			JobName:             "EmailJob",
			Processed:           10,
			SampleCount:         10,
			RuntimeSampleCount:  10,
			EffectiveSampleRate: 0.5,
			EstimatedTotal:      20,
			Estimated:           true,
			Quality:             EventMetricQualityEstimated,
		},
		{
			WindowStart:         now,
			WindowEnd:           now.Add(time.Minute),
			FlushAt:             now.Add(20 * time.Second),
			MetricsWindowMS:     int64(time.Minute / time.Millisecond),
			SourcePrefix:        "tenant-a",
			SourceHost:          "host-b",
			SourceEnvironment:   "production",
			SourceSupervisor:    "supervisor-default",
			Connection:          "redis",
			Queue:               "default",
			JobName:             "EmailJob",
			Processed:           5,
			SampleCount:         5,
			RuntimeSampleCount:  5,
			EffectiveSampleRate: 1,
			EstimatedTotal:      5,
			Quality:             EventMetricQualityExact,
		},
		{
			WindowStart:         now,
			WindowEnd:           now.Add(time.Minute),
			FlushAt:             now.Add(15 * time.Second),
			MetricsWindowMS:     int64(time.Minute / time.Millisecond),
			SourcePrefix:        "tenant-a",
			SourceHost:          "host-a",
			SourceEnvironment:   "staging",
			SourceSupervisor:    "supervisor-default",
			Connection:          "redis",
			Queue:               "default",
			JobName:             "EmailJob",
			Processed:           2,
			SampleCount:         2,
			RuntimeSampleCount:  2,
			EffectiveSampleRate: 1,
			EstimatedTotal:      2,
			Quality:             EventMetricQualityExact,
		},
		{
			WindowStart:         now,
			WindowEnd:           now.Add(time.Minute),
			FlushAt:             now.Add(25 * time.Second),
			MetricsWindowMS:     int64(time.Minute / time.Millisecond),
			SourcePrefix:        "tenant-a",
			SourceHost:          "host-a",
			SourceEnvironment:   "production",
			SourceSupervisor:    "supervisor-default",
			Connection:          "redis",
			Queue:               "default",
			JobName:             "_overflow",
			Processed:           3,
			SampleCount:         3,
			RuntimeSampleCount:  3,
			EffectiveSampleRate: 1,
			EstimatedTotal:      3,
			Degraded:            true,
			Quality:             EventMetricQualityDegraded,
		},
	}
	if err := store.AppendEventMetricWindows(ctx, windows, 0); err != nil {
		t.Fatalf("append event metric windows: %v", err)
	}

	page, err := store.EventMetricWindows(ctx, EventMetricWindowQuery{Page: PageRequest{Page: 1, PageSize: 10}})
	if err != nil {
		t.Fatalf("read event metric windows: %v", err)
	}
	if page.Total != len(windows) {
		t.Fatalf("windows should be appended per source shard, got total=%d items=%#v", page.Total, page.Items)
	}

	diagnostics := metricsWindowDiagnostics(page.Items)
	if hasDiagnostic(diagnostics, metricsWindowInconsistent) {
		t.Fatalf("same metrics_window with different flush_at or environment must not be drift: %#v", diagnostics)
	}
	models := buildEventMetricReadModels(page.Items, diagnostics)
	model := findEventMetricEstimate(t, models, "redis:default")
	if model.Estimate.EstimatedTotal != 30 || model.Estimate.SampledCount != 20 ||
		model.Estimate.Quality != EventMetricQualityDegraded || !model.Estimate.Degraded {
		t.Fatalf("aggregate should sum per-source/window estimates and preserve degraded overflow, got %#v", model.Estimate)
	}
	if len(model.SourceDetails) != 4 {
		t.Fatalf("read model should retain source details for drill-down, got %#v", model.SourceDetails)
	}
	if !containsSourceDetail(model.SourceDetails, "tenant-a", "host-b", "production", "supervisor-default", "redis", "default", "EmailJob") {
		t.Fatalf("missing host-b source detail: %#v", model.SourceDetails)
	}
	if !containsSourceDetail(model.SourceDetails, "tenant-a", "host-a", "production", "supervisor-default", "redis", "default", "_overflow") {
		t.Fatalf("overflow must keep source dimensions and stay distinguishable: %#v", model.SourceDetails)
	}
}

func TestStaleSourceShardMarksAggregateUnknown(t *testing.T) {
	// 需求背景：多实例读取时，stale runtime 的旧窗口不能被当成当前窗口的 0 或完整事实；
	// read model 应保留已知窗口值，同时用 unknown/degraded 告诉调用方该来源分片不可量化。
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Minute)
	store := NewMemoryStore(StoreOptions{Prefix: "tenant-a", HeartbeatTTL: 30 * time.Second})
	if err := store.HeartbeatSupervisor(ctx, SupervisorState{
		Name:            "supervisor-default",
		Host:            "host-a",
		Environment:     "production",
		Status:          SupervisorRunning,
		Connection:      "redis",
		Queues:          []string{"default"},
		LastHeartbeatAt: now.Add(-2 * time.Minute),
	}); err != nil {
		t.Fatalf("heartbeat stale supervisor: %v", err)
	}
	if err := store.AppendEventMetricWindows(ctx, []EventMetricWindow{{
		WindowStart:         now.Add(-time.Minute),
		WindowEnd:           now,
		FlushAt:             now,
		MetricsWindowMS:     int64(time.Minute / time.Millisecond),
		SourcePrefix:        "tenant-a",
		SourceHost:          "host-a",
		SourceEnvironment:   "production",
		SourceSupervisor:    "supervisor-default",
		Connection:          "redis",
		Queue:               "default",
		JobName:             "EmailJob",
		Processed:           4,
		SampleCount:         4,
		RuntimeSampleCount:  4,
		EffectiveSampleRate: 1,
		EstimatedTotal:      4,
		Quality:             EventMetricQualityExact,
	}}, 0); err != nil {
		t.Fatalf("append event metric windows: %v", err)
	}

	current, err := buildMetricsCurrentReadModel(ctx, nil, store, EventMetricWindowQuery{Page: PageRequest{Page: 1, PageSize: 10}})
	if err != nil {
		t.Fatalf("build current metrics: %v", err)
	}
	if !hasDiagnostic(current.Diagnostics.Items, metricsSourceStale) {
		t.Fatalf("expected stale source diagnostic, got %#v", current.Diagnostics.Items)
	}
	model := findEventMetricEstimate(t, current.Estimates, "redis:default")
	if model.Estimate.Quality != EventMetricQualityUnknown || !model.Estimate.Degraded || model.Estimate.EstimatedTotal != 4 {
		t.Fatalf("stale source should preserve known estimate but mark aggregate unknown: %#v", model.Estimate)
	}
	if len(model.SourceDetails) != 1 || !model.SourceDetails[0].Unknown || model.SourceDetails[0].Quality != EventMetricQualityUnknown {
		t.Fatalf("stale source detail should be unknown: %#v", model.SourceDetails)
	}
}

func TestMissingFreshSourceShardMarksAggregateUnknown(t *testing.T) {
	// 需求背景：当 Store 里有 fresh supervisor heartbeat，但当前 event_metrics window
	// 缺少该 supervisor 的 queue 分片时，聚合结果必须显式 unknown，而不是把缺失实例当作 0。
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Minute)
	store := NewMemoryStore(StoreOptions{Prefix: "tenant-a", HeartbeatTTL: time.Minute})
	for _, supervisor := range []SupervisorState{
		{Name: "supervisor-default", Host: "host-a", Environment: "production", Status: SupervisorRunning, Connection: "redis", Queues: []string{"default"}, LastHeartbeatAt: now},
		{Name: "supervisor-default", Host: "host-b", Environment: "production", Status: SupervisorRunning, Connection: "redis", Queues: []string{"default"}, LastHeartbeatAt: now},
	} {
		if err := store.HeartbeatSupervisor(ctx, supervisor); err != nil {
			t.Fatalf("heartbeat supervisor: %v", err)
		}
	}
	if err := store.AppendEventMetricWindows(ctx, []EventMetricWindow{{
		WindowStart:         now.Add(-time.Minute),
		WindowEnd:           now,
		FlushAt:             now,
		MetricsWindowMS:     int64(time.Minute / time.Millisecond),
		SourcePrefix:        "tenant-a",
		SourceHost:          "host-a",
		SourceEnvironment:   "production",
		SourceSupervisor:    "supervisor-default",
		Connection:          "redis",
		Queue:               "default",
		JobName:             "EmailJob",
		Processed:           8,
		SampleCount:         8,
		RuntimeSampleCount:  8,
		EffectiveSampleRate: 1,
		EstimatedTotal:      8,
		Quality:             EventMetricQualityExact,
	}}, 0); err != nil {
		t.Fatalf("append event metric windows: %v", err)
	}

	current, err := buildMetricsCurrentReadModel(ctx, nil, store, EventMetricWindowQuery{Page: PageRequest{Page: 1, PageSize: 10}})
	if err != nil {
		t.Fatalf("build current metrics: %v", err)
	}
	if !hasDiagnostic(current.Diagnostics.Items, metricsSourceMissing) {
		t.Fatalf("expected missing source diagnostic, got %#v", current.Diagnostics.Items)
	}
	model := findEventMetricEstimate(t, current.Estimates, "redis:default")
	if model.Estimate.Quality != EventMetricQualityUnknown || !model.Estimate.Degraded || model.Estimate.EstimatedTotal != 8 {
		t.Fatalf("missing source should preserve known estimate but mark aggregate unknown: %#v", model.Estimate)
	}
}

func containsSourceDetail(items []EventMetricSourceDetail, prefix, host, environment, supervisor, connection, queue, jobName string) bool {
	for _, item := range items {
		if item.SourcePrefix == prefix &&
			item.SourceHost == host &&
			item.SourceEnvironment == environment &&
			item.SourceSupervisor == supervisor &&
			item.Connection == connection &&
			item.Queue == queue &&
			item.JobName == jobName {
			return true
		}
	}
	return false
}
