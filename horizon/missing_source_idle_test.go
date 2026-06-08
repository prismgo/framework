package horizon

import (
	"context"
	"testing"
	"time"
)

// TestMissingSourceDiagnosticsSkipsIdleSupervisor 验证刚启动或空闲的 supervisor
// （其 queue 在 Store 中从未有过窗口）不会被误报为 missing。
//
// 需求背景：修复前 missingSourceDiagnostics 只要看到 fresh supervisor 声明的 queue
// 不在当前窗口集合就报 missing，无法区分"pipeline 断了"和"queue 暂时没有事件"。
// 修复后采用组合策略：先查同 queue 是否有其他来源窗口（快速判断），
// 不确定时再查 Store 历史窗口（兜底），只有历史存在但当前缺失才报 missing。
func TestMissingSourceDiagnosticsSkipsIdleSupervisor(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Minute)
	store := NewMemoryStore(StoreOptions{Prefix: "tenant-a", HeartbeatTTL: time.Minute})

	// 注册两个 fresh supervisor，host-a 有窗口，host-b 没有
	for _, supervisor := range []SupervisorState{
		{Name: "supervisor-default", Host: "host-a", Environment: "production", Status: SupervisorRunning, Connection: "redis", Queues: []string{"default"}, LastHeartbeatAt: now},
		{Name: "supervisor-default", Host: "host-b", Environment: "production", Status: SupervisorRunning, Connection: "redis", Queues: []string{"default"}, LastHeartbeatAt: now},
	} {
		if err := store.HeartbeatSupervisor(ctx, supervisor); err != nil {
			t.Fatalf("heartbeat supervisor: %v", err)
		}
	}

	// 只写入 host-a 的窗口
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
		EffectiveSampleRate: 1,
		EstimatedTotal:      8,
		Quality:             EventMetricQualityExact,
	}}, 0); err != nil {
		t.Fatalf("append windows: %v", err)
	}

	// 获取当前窗口
	windows, err := store.EventMetricWindows(ctx, EventMetricWindowQuery{Page: PageRequest{Page: 1, PageSize: 100}})
	if err != nil {
		t.Fatalf("get windows: %v", err)
	}

	// 获取 supervisors
	supervisors, err := store.Supervisors(ctx, now)
	if err != nil {
		t.Fatalf("get supervisors: %v", err)
	}

	// host-b 的 queue "default" 在 Store 中没有历史窗口（只有 host-a 的）
	// 但同 queue "redis:default" 有 host-a 的窗口存在
	// → host-b 应报 missing（同 queue 有其他来源但缺 host-b）
	diags := missingSourceDiagnostics(ctx, store, supervisors, windows.Items)
	if !hasDiagnostic(diags, metricsSourceMissing) {
		t.Fatalf("expected missing diagnostic for host-b (same queue has other source), got %#v", diags)
	}
}

// TestMissingSourceDiagnosticsSkipsNeverProducedQueue 验证 supervisor 的 queue
// 在 Store 中从未有过任何窗口，且当前窗口集合中该 queue 也没有任何来源时，不报 missing。
//
// 需求背景：刚启动的 supervisor 或空闲 queue 不应被误报为 pipeline 断裂。
func TestMissingSourceDiagnosticsSkipsNeverProducedQueue(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Minute)
	store := NewMemoryStore(StoreOptions{Prefix: "tenant-a", HeartbeatTTL: time.Minute})

	// 注册一个 fresh supervisor，其 queue "notifications" 从未有过窗口
	supervisor := SupervisorState{
		Name: "supervisor-notif", Host: "host-a", Environment: "production",
		Status: SupervisorRunning, Connection: "redis", Queues: []string{"notifications"},
		LastHeartbeatAt: now,
	}
	if err := store.HeartbeatSupervisor(ctx, supervisor); err != nil {
		t.Fatalf("heartbeat supervisor: %v", err)
	}

	// Store 中写入其他 queue 的窗口（default），但不写 notifications
	if err := store.AppendEventMetricWindows(ctx, []EventMetricWindow{{
		WindowStart:         now.Add(-time.Minute),
		WindowEnd:           now,
		FlushAt:             now,
		MetricsWindowMS:     int64(time.Minute / time.Millisecond),
		SourcePrefix:        "tenant-a",
		SourceHost:          "host-a",
		SourceEnvironment:   "production",
		SourceSupervisor:    "supervisor-notif",
		Connection:          "redis",
		Queue:               "default",
		JobName:             "EmailJob",
		Processed:           5,
		SampleCount:         5,
		EffectiveSampleRate: 1,
		EstimatedTotal:      5,
		Quality:             EventMetricQualityExact,
	}}, 0); err != nil {
		t.Fatalf("append windows: %v", err)
	}

	windows, err := store.EventMetricWindows(ctx, EventMetricWindowQuery{Page: PageRequest{Page: 1, PageSize: 100}})
	if err != nil {
		t.Fatalf("get windows: %v", err)
	}

	supervisors, err := store.Supervisors(ctx, now)
	if err != nil {
		t.Fatalf("get supervisors: %v", err)
	}

	// supervisor 声明了 "notifications" queue，但 Store 中从未有过该 queue 的窗口
	// 且当前窗口中 "redis:notifications" 也没有任何其他来源 → 不报 missing
	diags := missingSourceDiagnostics(ctx, store, supervisors, windows.Items)
	if hasDiagnostic(diags, metricsSourceMissing) {
		t.Fatalf("should not report missing for queue that never produced windows, got %#v", diags)
	}
}

// TestMissingSourceDiagnosticsReportsWhenStoreHasHistory 验证 Store 中存在历史窗口
// 但当前窗口集合中缺失时，仍然正确报告 missing。
//
// 需求背景：pipeline 断裂的场景 — supervisor 之前正常产窗口，但当前窗口集合里看不到。
func TestMissingSourceDiagnosticsReportsWhenStoreHasHistory(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Minute)
	store := NewMemoryStore(StoreOptions{Prefix: "tenant-a", HeartbeatTTL: time.Minute})

	// 注册 fresh supervisor
	supervisor := SupervisorState{
		Name: "supervisor-default", Host: "host-a", Environment: "production",
		Status: SupervisorRunning, Connection: "redis", Queues: []string{"default"},
		LastHeartbeatAt: now,
	}
	if err := store.HeartbeatSupervisor(ctx, supervisor); err != nil {
		t.Fatalf("heartbeat supervisor: %v", err)
	}

	// Store 中有该 supervisor 的历史窗口（前一个窗口周期）
	if err := store.AppendEventMetricWindows(ctx, []EventMetricWindow{{
		WindowStart:         now.Add(-2 * time.Minute),
		WindowEnd:           now.Add(-time.Minute),
		FlushAt:             now.Add(-time.Minute),
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
		EffectiveSampleRate: 1,
		EstimatedTotal:      10,
		Quality:             EventMetricQualityExact,
	}}, 0); err != nil {
		t.Fatalf("append windows: %v", err)
	}

	// 当前窗口集合为空（模拟 pipeline 断裂：Store 有历史但当前没有窗口）
	supervisors, err := store.Supervisors(ctx, now)
	if err != nil {
		t.Fatalf("get supervisors: %v", err)
	}

	// 传入空窗口集合 — supervisor 的 queue 在 Store 有历史但当前缺失 → 报 missing
	diags := missingSourceDiagnostics(ctx, store, supervisors, nil)
	if !hasDiagnostic(diags, metricsSourceMissing) {
		t.Fatalf("should report missing when Store has history but current windows are empty, got %#v", diags)
	}
}
