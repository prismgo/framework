package horizon

import (
	"context"
	"strings"
	"testing"
	"time"

	horizoncmd "github.com/prismgo/framework/horizon/cmd"
)

func TestSnapshotSkipsQueueLengthsWithoutOverwritingPreviousSample(t *testing.T) {
	// 需求背景：queue_lengths=false 表示本次 snapshot 跳过采样，不代表队列长度为 0；
	// Store 中上一份成功样本必须保留，CLI 摘要也要能区分 skipped 和 count=0。
	ctx := context.Background()
	now := time.Date(2026, 5, 14, 13, 0, 0, 0, time.UTC)
	store := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})
	previous := QueueLengthSnapshot{CapturedAt: now.Add(-time.Minute), Queues: []QueueLengthBucket{{Connection: "redis", Queue: "default", Size: 9}}}
	if err := store.SaveQueueLengthSnapshot(ctx, previous); err != nil {
		t.Fatalf("seed queue lengths: %v", err)
	}
	obs, _ := observabilityPresetConfig(ObservabilityPresetFull)
	obs.QueueLengths = false
	manager, _ := NewManager(Config{
		Store:         "memory",
		Observability: obs,
		Supervisors: map[string]SupervisorConfig{
			"supervisor-default": {Name: "supervisor-default", Connection: "redis", Queues: []string{"default"}},
		},
	}, WithStoreFactory(staticStoreResolver{store: store}), WithQueueManager(&fakeRuntimeQueueManager{connections: map[string]*fakeQueueConnection{
		"redis": {sizes: map[string]int64{"default": 0}},
	}}))
	runtime := &runtimeCommandAdapter{manager: manager, store: store}

	summary, err := runtime.Snapshot(ctx, now)
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if summary.QueueLengthStatus != horizoncmd.SnapshotStatusSkipped || summary.QueueLengthCount != 0 {
		t.Fatalf("queue length summary should be skipped, got %#v", summary)
	}
	read, err := store.QueueLengthSnapshot(ctx)
	if err != nil {
		t.Fatalf("read queue lengths: %v", err)
	}
	if len(read.Queues) != 1 || read.Queues[0].Size != 9 || !read.CapturedAt.Equal(previous.CapturedAt) {
		t.Fatalf("skipped queue lengths should not overwrite previous sample: %#v", read)
	}

	output := runHorizonCommand(t, horizoncmd.NewSnapshotCommand(newRuntimeLoader(func() (*Manager, error) { return manager, nil })), runtimeInput{})
	if !strings.Contains(output, "Queue Lengths: skipped") {
		t.Fatalf("snapshot command should report skipped queue lengths, got:\n%s", output)
	}
}

func TestSnapshotMinimalSkipsHighCostMetricsWithoutOverwritingPreviousSnapshot(t *testing.T) {
	// 需求背景：minimal 只保留核心健康和队列长度；高成本 metrics 大 JSON、waits
	// 和 batch summaries 都应跳过写入，不能把旧数据覆盖为空。
	ctx := context.Background()
	now := time.Date(2026, 5, 14, 14, 0, 0, 0, time.UTC)
	store := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})
	previousWindow := EventMetricWindow{
		WindowStart: now.Add(-time.Hour).Truncate(time.Minute),
		WindowEnd:   now.Add(-time.Hour).Truncate(time.Minute).Add(time.Minute),
		FlushAt:     now.Add(-time.Hour),
		Connection:  "redis",
		Queue:       "default",
		Processed:   7,
		Quality:     EventMetricQualityExact,
	}
	if err := store.AppendEventMetricWindows(ctx, []EventMetricWindow{previousWindow}, 24*time.Hour); err != nil {
		t.Fatalf("seed event windows: %v", err)
	}
	obs, _ := observabilityPresetConfig(ObservabilityPresetMinimal)
	manager, _ := NewManager(Config{
		Store:         "memory",
		Observability: obs,
		Supervisors: map[string]SupervisorConfig{
			"supervisor-default": {Name: "supervisor-default", Connection: "redis", Queues: []string{"default"}},
		},
	}, WithStoreFactory(staticStoreResolver{store: store}), WithQueueManager(&fakeRuntimeQueueManager{connections: map[string]*fakeQueueConnection{
		"redis": {sizes: map[string]int64{"default": 3}},
	}}))
	runtime := &runtimeCommandAdapter{manager: manager, store: store}

	summary, err := runtime.Snapshot(ctx, now)
	if err != nil {
		t.Fatalf("snapshot minimal: %v", err)
	}
	if summary.QueueLengthStatus != horizoncmd.SnapshotStatusEnabled || summary.QueueLengthCount != 1 {
		t.Fatalf("minimal should still sample queue lengths: %#v", summary)
	}
	if summary.MetricsStatus != horizoncmd.SnapshotStatusSkipped ||
		summary.WaitsStatus != horizoncmd.SnapshotStatusSkipped ||
		summary.BatchSummariesStatus != horizoncmd.SnapshotStatusSkipped {
		t.Fatalf("minimal should skip high-cost metrics: %#v", summary)
	}
	read, err := store.EventMetricWindows(ctx, EventMetricWindowQuery{Page: PageRequest{Page: 1, PageSize: 10}})
	if err != nil {
		t.Fatalf("read event windows: %v", err)
	}
	if read.Total == 0 || read.Items[0].Processed != 7 {
		t.Fatalf("minimal metrics skip should not affect event windows: %#v", read)
	}
}

func TestSnapshotProductionLightPersistsHistoryFromMetricsDetails(t *testing.T) {
	// 逻辑说明：production_light 保留 event_metrics，但跳过 waits 和 batch summaries；
	// history read model 从同一份 event_metrics window 聚合生成。
	ctx := context.Background()
	now := time.Date(2026, 5, 14, 15, 0, 0, 0, time.UTC)
	store := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})
	obs, _ := observabilityPresetConfig(ObservabilityPresetProductionLight)
	manager, _ := NewManager(Config{
		Store:         "memory",
		Observability: obs,
		Supervisors: map[string]SupervisorConfig{
			"supervisor-default": {Name: "supervisor-default", Connection: "redis", Queues: []string{"default"}},
		},
	}, WithStoreFactory(staticStoreResolver{store: store}), WithQueueManager(&fakeRuntimeQueueManager{connections: map[string]*fakeQueueConnection{
		"redis": {sizes: map[string]int64{"default": 0}},
	}}))
	// 通过 collector 注入事件替代旧采集入口
	coll := manager.Collector()
	coll.Start(ctx)
	defer coll.Stop()
	_ = coll.Collect(ctx, CollectorInput{
		Event: "queue.job_processed", Connection: "redis", Queue: "default",
		JobID: "job-1", JobName: "EmailJob", Runtime: 50 * time.Millisecond, OccurredAt: now,
		Sampling: SamplingDecision{EventMetricsSampled: true, EventMetricsSampleRate: 1.0},
	})
	time.Sleep(100 * time.Millisecond)
	runtime := &runtimeCommandAdapter{manager: manager, store: store}

	summary, err := runtime.Snapshot(ctx, now)
	if err != nil {
		t.Fatalf("snapshot production_light: %v", err)
	}
	if summary.MetricsStatus != horizoncmd.SnapshotStatusEnabled ||
		summary.WaitsStatus != horizoncmd.SnapshotStatusSkipped ||
		summary.BatchSummariesStatus != horizoncmd.SnapshotStatusSkipped {
		t.Fatalf("unexpected production_light summary: %#v", summary)
	}
	windows, err := store.EventMetricWindows(ctx, EventMetricWindowQuery{Page: PageRequest{Page: 1, PageSize: 10}})
	if err != nil {
		t.Fatalf("read event windows: %v", err)
	}
	if windows.Total == 0 {
		t.Fatalf("production_light should persist event windows: %#v", windows)
	}
	batches, err := store.Batches(ctx, "")
	if err != nil {
		t.Fatalf("read batches: %v", err)
	}
	if len(batches) != 0 {
		t.Fatalf("batch summaries should be skipped: %#v", batches)
	}
}

func TestSnapshotPersistsHistoryFromEventMetricsWithoutLegacyHistorySwitches(t *testing.T) {
	// 需求背景：historical scenario 34 要求删除 recent_jobs/job_history/queue_history 独立能力判断；
	// queue history read model 必须从 event_metrics window 数据生成，且不得再生成 job_history。
	ctx := context.Background()
	now := time.Date(2026, 5, 15, 9, 0, 0, 0, time.UTC)
	store := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})
	obs, _ := observabilityPresetConfig(ObservabilityPresetFull)
	manager, _ := NewManager(Config{
		Store:         "memory",
		Observability: obs,
		Supervisors: map[string]SupervisorConfig{
			"supervisor-default": {Name: "supervisor-default", Connection: "redis", Queues: []string{"default"}},
		},
	}, WithStoreFactory(staticStoreResolver{store: store}), WithQueueManager(&fakeRuntimeQueueManager{connections: map[string]*fakeQueueConnection{
		"redis": {sizes: map[string]int64{"default": 0}},
	}}))
	coll := manager.Collector()
	coll.Start(ctx)
	defer coll.Stop()
	_ = coll.Collect(ctx, CollectorInput{
		Event: "queue.job_processed", Connection: "redis", Queue: "default",
		JobID: "job-1", JobName: "EmailJob", Runtime: 40 * time.Millisecond, OccurredAt: now,
		Sampling: SamplingDecision{EventMetricsSampled: true, EventMetricsSampleRate: 1.0},
	})
	time.Sleep(100 * time.Millisecond)
	runtime := &runtimeCommandAdapter{manager: manager, store: store}

	summary, err := runtime.Snapshot(ctx, now)
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if summary.MetricsStatus != horizoncmd.SnapshotStatusEnabled {
		t.Fatalf("history read model should follow event_metrics, got %#v", summary)
	}
	// MetricsHistory 写入已随 SaveMetricsSnapshot 移除；改用 EventMetricWindows 验证。
	windows, err := store.EventMetricWindows(ctx, EventMetricWindowQuery{Page: PageRequest{Page: 1, PageSize: 10}})
	if err != nil {
		t.Fatalf("read event windows: %v", err)
	}
	if windows.Total == 0 {
		t.Fatalf("event windows should be persisted from event_metrics: %#v", windows)
	}
}
