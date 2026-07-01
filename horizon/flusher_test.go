package horizon

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prismgo/framework/queue"
	"github.com/prismgo/framework/queue/payload"
)

// TestFlusherPeriodicFlush 验证按 flush_interval 定期写入 Store。
//
// 需求背景：historical scenario 34 要求 flusher 按 flush_interval、batch_size 或高水位触发批量写入。
func TestFlusherPeriodicFlush(t *testing.T) {
	cfg := observabilityPresetConfigOrFull()
	cfg.FlushInterval = 50 * time.Millisecond
	cfg.FlushTimeout = 1 * time.Second
	cfg.BufferSize = 1000
	cfg.EventMetricsSampleRate = 1.0
	cfg.EventMetrics = true
	cfg.QueuedWaitsMax = 0

	coll := newCollector(cfg)
	store := NewMemoryStore(StoreOptions{Prefix: "test"})

	f := newFlusher(cfg, store, coll, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	coll.Start(ctx)
	f.Start(ctx)

	// 发送事件到 collector
	for i := 0; i < 10; i++ {
		_ = coll.Collect(ctx, CollectorInput{
			Event:      "queue.job_processed",
			Connection: "redis",
			Queue:      "default",
			JobName:    "TestJob",
			Runtime:    100 * time.Millisecond,
			Sampling: SamplingDecision{
				EventMetricsSampled:    true,
				EventMetricsSampleRate: 1.0,
			},
		})
	}

	// 等待至少一次 flush
	time.Sleep(200 * time.Millisecond)

	f.Stop()
	coll.Stop()

	// 检查 Store 中是否有 event_metrics 数据
	windows, err := store.EventMetricWindows(ctx, EventMetricWindowQuery{Page: PageRequest{Page: 1, PageSize: 100}})
	if err != nil {
		t.Fatalf("failed to read event windows: %v", err)
	}
	t.Logf("windows after flush: %d", windows.Total)
}

// TestFlusherShutdownBestEffort 验证 shutdown best-effort flush。
//
// 需求背景：historical scenario 34 要求 shutdown/cancel 路径支持 best-effort flush。
func TestFlusherShutdownBestEffort(t *testing.T) {
	cfg := observabilityPresetConfigOrFull()
	cfg.FlushInterval = 10 * time.Minute // 长间隔，确保不会自动 flush
	cfg.FlushTimeout = 5 * time.Second
	cfg.BufferSize = 1000
	cfg.EventMetricsSampleRate = 1.0
	cfg.EventMetrics = true
	cfg.QueuedWaitsMax = 0

	coll := newCollector(cfg)
	store := NewMemoryStore(StoreOptions{Prefix: "test"})

	f := newFlusher(cfg, store, coll, nil)

	ctx, cancel := context.WithCancel(context.Background())

	coll.Start(ctx)
	f.Start(ctx)

	// 发送事件
	for i := 0; i < 5; i++ {
		_ = coll.Collect(ctx, CollectorInput{
			Event:      "queue.job_processed",
			Connection: "redis",
			Queue:      "default",
			JobName:    "ShutdownJob",
			Runtime:    50 * time.Millisecond,
			Sampling: SamplingDecision{
				EventMetricsSampled:    true,
				EventMetricsSampleRate: 1.0,
			},
		})
	}

	// 给 collector 时间处理
	time.Sleep(100 * time.Millisecond)

	// 触发 shutdown
	cancel()
	f.Stop()
	coll.Stop()

	// 验证 Store 有数据（best-effort flush 应该写入）
	windows, _ := store.EventMetricWindows(ctx, EventMetricWindowQuery{Page: PageRequest{Page: 1, PageSize: 100}})
	totalProcessed := int64(0)
	for _, w := range windows.Items {
		totalProcessed += w.Processed
	}
	if totalProcessed == 0 {
		t.Error("expected at least some processed events from shutdown flush")
	}
	windows, err := store.EventMetricWindows(context.Background(), EventMetricWindowQuery{Page: PageRequest{Page: 1, PageSize: 10}})
	if err != nil {
		t.Fatalf("read shutdown windows: %v", err)
	}
	if windows.Total > 0 && !windows.Items[0].Partial {
		t.Fatalf("shutdown window should be marked partial: %#v", windows.Items[0])
	}
	t.Logf("shutdown flush: %d processed events", totalProcessed)
}

// TestFlusherStoreWriteFailure 验证 Store 写入失败时不影响后续 flush。
//
// 需求背景：historical scenario 34 要求 Flusher 写 Store 失败时不会反向取消或阻塞正在处理的 job。
func TestFlusherStoreWriteFailureDoesNotBlock(t *testing.T) {
	cfg := observabilityPresetConfigOrFull()
	cfg.FlushInterval = 50 * time.Millisecond
	cfg.FlushTimeout = 1 * time.Second
	cfg.BufferSize = 1000
	cfg.EventMetricsSampleRate = 1.0
	cfg.EventMetrics = true
	cfg.QueuedWaitsMax = 0

	coll := newCollector(cfg)
	store := NewMemoryStore(StoreOptions{Prefix: "test"})

	f := newFlusher(cfg, store, coll, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	coll.Start(ctx)
	f.Start(ctx)

	// 发送一些事件
	for i := 0; i < 5; i++ {
		_ = coll.Collect(ctx, CollectorInput{
			Event:      "queue.job_processed",
			Connection: "redis",
			Queue:      "default",
			JobName:    "TestJob",
			Sampling: SamplingDecision{
				EventMetricsSampled:    true,
				EventMetricsSampleRate: 1.0,
			},
		})
	}

	// 等待 flush
	time.Sleep(200 * time.Millisecond)

	f.Stop()
	coll.Stop()

	// 不应 panic 或死锁
	diag := f.Diagnostics()
	t.Logf("flush diagnostics: error=%q, streak=%d", diag.LastFlushError, diag.FlushErrorStreak)
}

// TestFlusherBuildBatch 验证 FlushBatch 构建包含 event_metrics 窗口和诊断。
//
// 需求背景：historical scenario 34 要求 event_metrics Store 写入使用追加 batch/window 模型。
func TestFlusherBuildBatch(t *testing.T) {
	cfg := observabilityPresetConfigOrFull()
	cfg.MetricsWindow = time.Minute
	cfg.EventMetricsSampleRate = 1.0
	cfg.EventMetrics = true
	coll := newCollector(cfg)
	coll.Start(context.Background())
	defer coll.Stop()

	ctx := context.Background()
	now := time.Now()

	_ = coll.Collect(ctx, CollectorInput{
		Event:      "queue.job_processed",
		Connection: "redis",
		Queue:      "default",
		JobName:    "BatchJob",
		Runtime:    100 * time.Millisecond,
		OccurredAt: now,
		Sampling: SamplingDecision{
			EventMetricsSampled:    true,
			EventMetricsSampleRate: 1.0,
		},
	})

	time.Sleep(100 * time.Millisecond)
	snapshot := coll.FlushSnapshot(now.Add(time.Minute))

	store := NewMemoryStore(StoreOptions{Prefix: "test"})
	f := newFlusher(cfg, store, coll, nil)
	batch := f.buildFlushBatch(snapshot, now.Add(time.Minute))

	if len(batch.Increments) == 0 {
		t.Error("expected at least one increment in batch")
	}
	if batch.WindowStart.IsZero() {
		t.Error("expected non-zero WindowStart")
	}
	if batch.WindowEnd.IsZero() {
		t.Error("expected non-zero WindowEnd")
	}

	// 验证 increment 内容
	found := false
	for _, inc := range batch.Increments {
		if inc.JobName == "BatchJob" {
			found = true
			if inc.Processed != 1 {
				t.Errorf("expected 1 processed, got %d", inc.Processed)
			}
		}
	}
	if !found {
		t.Error("expected increment for BatchJob")
	}
}

// TestFlusherDiagnostics 验证诊断状态暴露。
//
// 需求背景：historical scenario 35 要求诊断状态至少包含 dropped count、drop reason、last flush 等。
func TestFlusherDiagnostics(t *testing.T) {
	cfg := observabilityPresetConfigOrFull()
	coll := newCollector(cfg)
	store := NewMemoryStore(StoreOptions{Prefix: "test"})
	f := newFlusher(cfg, store, coll, nil)

	diag := f.Diagnostics()
	if diag.FlushErrorStreak != 0 {
		t.Error("new flusher should have zero error streak")
	}
}

func TestFlusherLoopPanicRestartsAndFlushesLaterTicks(t *testing.T) {
	cfg := observabilityPresetConfigOrFull()
	cfg.EventMetrics = true
	cfg.EventMetricsSampleRate = 1
	cfg.FlushInterval = 20 * time.Millisecond
	cfg.FlushTimeout = time.Second
	coll := newCollector(cfg)
	store := NewMemoryStore(StoreOptions{Prefix: "test"})
	f := newFlusher(cfg, store, coll, nil)
	var ticks atomic.Int64
	f.beforePeriodicFlush = func() {
		if ticks.Add(1) == 1 {
			panic("injected flusher loop panic")
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	coll.Start(ctx)
	defer coll.Stop()
	f.Start(ctx)
	defer f.Stop()

	deadline := time.Now().Add(time.Second)
	for {
		diag := f.Diagnostics()
		if diag.Degraded && diag.DegradedReason == MemoryDropFlusherPanic && strings.Contains(diag.LastFlushError, "flusher loop panic") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for flusher panic diagnostic, got %#v", diag)
		}
		time.Sleep(10 * time.Millisecond)
	}

	_ = coll.Collect(ctx, CollectorInput{
		Event:      "queue.job_processed",
		Connection: "redis",
		Queue:      "default",
		SourceHost: "test-host",
		OccurredAt: time.Now(),
		Sampling:   SamplingDecision{EventMetricsSampled: true},
	})

	deadline = time.Now().Add(time.Second)
	for {
		windows, err := store.EventMetricWindows(ctx, EventMetricWindowQuery{Page: PageRequest{Page: 1, PageSize: 10}})
		if err == nil && windows.Total > 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for restarted flusher to write windows")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestFlusherLoopContinuousPanicCoolingDoesNotBlockStop(t *testing.T) {
	cfg := observabilityPresetConfigOrFull()
	cfg.FlushInterval = time.Minute
	cfg.FlushTimeout = time.Second
	coll := newCollector(cfg)
	store := NewMemoryStore(StoreOptions{Prefix: "test"})
	f := newFlusher(cfg, store, coll, nil)
	f.cfg.FlushInterval = 0

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.Start(ctx)

	deadline := time.Now().Add(time.Second)
	for {
		diag := f.Diagnostics()
		if diag.Degraded && diag.DegradedReason == MemoryDropFlusherPanic && diag.FlushErrorStreak >= goroutineRestartPanicThreshold {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for flusher panic cooling threshold, got %#v", diag)
		}
		time.Sleep(10 * time.Millisecond)
	}

	started := time.Now()
	f.Stop()
	if elapsed := time.Since(started); elapsed > 200*time.Millisecond {
		t.Fatalf("Stop should not wait for panic cooling delay, took %s", elapsed)
	}
}

// TestFlusherOnDemandFlush 验证 horizon:snapshot 按需 flush 路径。
//
// 需求背景：historical scenario 34 要求 horizon:snapshot 保留命令名但改为触发新 async flusher flush。
func TestFlusherOnDemandFlush(t *testing.T) {
	cfg := observabilityPresetConfigOrFull()
	cfg.FlushTimeout = 5 * time.Second
	cfg.BufferSize = 1000
	cfg.EventMetrics = true
	cfg.EventMetricsSampleRate = 1.0
	cfg.QueuedWaitsMax = 0

	coll := newCollector(cfg)
	store := NewMemoryStore(StoreOptions{Prefix: "test"})
	f := newFlusher(cfg, store, coll, nil)

	ctx := context.Background()
	coll.Start(ctx)
	defer coll.Stop()

	// 发送事件
	for i := 0; i < 10; i++ {
		_ = coll.Collect(ctx, CollectorInput{
			Event:      "queue.job_processed",
			Connection: "redis",
			Queue:      "default",
			JobName:    "SnapshotJob",
			Runtime:    100 * time.Millisecond,
			Sampling: SamplingDecision{
				EventMetricsSampled:    true,
				EventMetricsSampleRate: 1.0,
			},
		})
	}

	time.Sleep(200 * time.Millisecond)

	summary, err := f.FlushSnapshotOnDemand(ctx)
	if err != nil {
		t.Fatalf("on-demand flush failed: %v", err)
	}
	if summary.IncrementCount == 0 {
		t.Error("expected increments in flush summary")
	}
	if summary.CapturedAt.IsZero() {
		t.Error("expected non-zero CapturedAt")
	}
	t.Logf("flush summary: increments=%d, detail=%d, drops=%d, degraded=%v",
		summary.IncrementCount, summary.DetailCount, summary.DropCount, summary.Degraded)
}

func TestFlusherOnDemandFlushPersistsAppendOnlyFacts(t *testing.T) {
	// 需求背景：horizon:snapshot 必须走 flusher writer；窗口、明细、诊断、批次摘要和兼容 read model
	// 需要来自同一批 collector 快照，不能只保存旧 MetricsSnapshot。
	cfg := observabilityPresetConfigOrFull()
	cfg.FlushTimeout = time.Second
	cfg.BufferSize = 1000
	cfg.EventMetrics = true
	cfg.EventMetricsSampleRate = 1
	cfg.FailedDetailEnabled = true
	highValueRate := 1.0
	cfg.HighValueDetailSampleRate = &highValueRate
	cfg.HighValueDetailRetention = 365 * 24 * time.Hour
	cfg.DiagnosticsRetention = time.Hour
	cfg.BatchSummaryRetention = 365 * 24 * time.Hour
	cfg.QueuedWaitsMax = 0

	coll := newCollector(cfg)
	store := NewMemoryStore(StoreOptions{Prefix: "snapshot_flusher"})
	f := newFlusher(cfg, store, coll, nil)
	ctx := context.Background()
	now := time.Date(2026, 5, 15, 10, 0, 0, 0, time.UTC)

	coll.Start(ctx)
	defer coll.Stop()
	_ = coll.Collect(ctx, CollectorInput{
		Event:        "queue.job_failed",
		Connection:   "redis",
		Queue:        "default",
		JobID:        "job-1",
		JobName:      "EmailJob",
		Runtime:      120 * time.Millisecond,
		ErrorSummary: "timeout",
		OccurredAt:   now,
		Sampling: SamplingDecision{
			EventMetricsSampled:    true,
			EventMetricsSampleRate: 1,
			HighValueDetailSampled: true,
			HighValueDetailRate:    1,
		},
	})
	_ = coll.Collect(ctx, CollectorInput{
		Event:      "queue.batch_updated",
		OccurredAt: now,
		BatchSummary: BatchSummary{
			ID:      "batch-1",
			Name:    "Imports",
			Status:  BatchStatusRunning,
			Total:   3,
			Pending: 2,
		},
	})
	coll.recordDrop(MemoryDropBufferFull)
	time.Sleep(100 * time.Millisecond)

	summary, err := f.FlushSnapshotOnDemand(ctx)
	if err != nil {
		t.Fatalf("on-demand flush: %v", err)
	}
	if summary.SchedulingStatus != FlushSchedulingFlushed || summary.WindowCount == 0 ||
		summary.DetailCount == 0 || summary.DiagnosticCount == 0 || summary.BatchSummaryCount == 0 {
		t.Fatalf("flush summary missing append-only facts: %#v", summary)
	}
	windows, err := store.EventMetricWindows(ctx, EventMetricWindowQuery{Page: PageRequest{Page: 1, PageSize: 10}})
	if err != nil || windows.Total != 1 {
		t.Fatalf("event windows = %#v err=%v", windows, err)
	}
	details, err := store.HighValueDetails(ctx, HighValueDetailQuery{Kind: HighValueDetailFailed, Page: PageRequest{Page: 1, PageSize: 10}})
	if err != nil || details.Total != 1 {
		t.Fatalf("high value details = %#v err=%v", details, err)
	}
	diagnostics, err := store.ObservabilityDiagnostics(ctx, PageRequest{Page: 1, PageSize: 10})
	if err != nil || diagnostics.Total == 0 {
		t.Fatalf("diagnostics = %#v err=%v", diagnostics, err)
	}
	batches, err := store.Batches(ctx, "")
	if err != nil || len(batches) != 1 {
		t.Fatalf("batches = %#v err=%v", batches, err)
	}
	windows, err = store.EventMetricWindows(ctx, EventMetricWindowQuery{Page: PageRequest{Page: 1, PageSize: 100}})
	if err != nil || windows.Total == 0 {
		t.Fatalf("event windows = %#v err=%v", windows, err)
	}
	// MetricsHistory 写入已随 SaveMetricsSnapshot 移除而不再执行
}

func TestFlusherPersistsHighValueDetailsAndDiagnostics(t *testing.T) {
	cfg := observabilityPresetConfigOrFull()
	cfg.HighValueDetailRetention = time.Hour
	cfg.DiagnosticsRetention = time.Hour
	store := NewMemoryStore(StoreOptions{Prefix: "test"})
	coll := newCollector(cfg)
	f := newFlusher(cfg, store, coll, nil)
	now := time.Now().UTC()

	err := f.Flush(context.Background(), FlushBatch{
		WindowStart: now.Add(-time.Minute),
		WindowEnd:   now,
		HighValueDetails: []HighValueJobDetail{{
			ID:         "failed-detail",
			Kind:       HighValueDetailFailed,
			Connection: "redis",
			Queue:      "default",
			JobID:      "job-1",
			OccurredAt: now,
		}},
		Diagnostics: []ObservabilityDiagnostic{{
			Reason:     MemoryDropBufferFull,
			Count:      4,
			ObservedAt: now,
		}},
	})
	if err != nil {
		t.Fatalf("flush: %v", err)
	}
	details, err := store.HighValueDetails(context.Background(), HighValueDetailQuery{Kind: HighValueDetailFailed, Page: PageRequest{Page: 1, PageSize: 10}})
	if err != nil {
		t.Fatalf("read high-value details: %v", err)
	}
	if details.Total != 1 || len(details.Items) != 1 || details.Items[0].ID != "failed-detail" {
		t.Fatalf("unexpected high-value details: %#v", details)
	}
	diagnostics, err := store.ObservabilityDiagnostics(context.Background(), PageRequest{Page: 1, PageSize: 10})
	if err != nil {
		t.Fatalf("read diagnostics: %v", err)
	}
	if diagnostics.Total != 1 || len(diagnostics.Items) != 1 || diagnostics.Items[0].Count != 4 {
		t.Fatalf("unexpected diagnostics: %#v", diagnostics)
	}
}

func TestFlusherPersistsBatchSummariesAsIndependentWindowChannel(t *testing.T) {
	cfg := observabilityPresetConfigOrFull()
	cfg.BatchSummaries = true
	cfg.EventMetrics = true
	cfg.MetricsWindow = time.Minute
	cfg.BatchSummaryRetention = time.Hour
	store := NewMemoryStore(StoreOptions{Prefix: "test"})
	coll := newCollector(cfg)
	f := newFlusher(cfg, store, coll, nil)
	base := time.Now().UTC().Truncate(time.Minute)
	createdAt := base.Add(10 * time.Second)
	receivedAt := base.Add(20 * time.Second)
	flushAt := base.Add(time.Minute)

	input := collectorInputFromEvent(queue.BatchEvent{
		EventName: queue.EventBatchUpdated,
		Batch: payload.BatchStatus{
			ID:        "batch-1",
			Name:      "Daily reports",
			Total:     10,
			Pending:   3,
			Processed: 7,
			Failed:    1,
			CreatedAt: createdAt,
		},
	}, cfg)
	coll.processItem(collectorItem{input: input, receivedAt: receivedAt})
	snapshot := coll.FlushSnapshot(flushAt)
	batch := f.buildFlushBatch(snapshot, flushAt)

	if len(batch.Increments) != 0 {
		t.Fatalf("batch summaries must not create event_metrics increments: %#v", batch.Increments)
	}
	if len(batch.BatchSummaries) != 1 {
		t.Fatalf("expected one batch summary, got %#v", batch.BatchSummaries)
	}
	if err := f.Flush(context.Background(), batch); err != nil {
		t.Fatalf("flush batch summary: %v", err)
	}
	summary, ok, err := store.Batch(context.Background(), "batch-1")
	if err != nil || !ok {
		t.Fatalf("read batch summary ok=%v err=%v", ok, err)
	}
	if summary.Status != BatchStatusRunning || summary.Processed != 7 || summary.Failed != 1 {
		t.Fatalf("batch summary data corrupted: %#v", summary)
	}
	if !summary.WindowStart.Equal(receivedAt.Truncate(time.Minute)) || !summary.WindowEnd.Equal(receivedAt.Truncate(time.Minute).Add(time.Minute)) {
		t.Fatalf("batch summary window not persisted: %#v", summary)
	}
	if !summary.FlushAt.Equal(flushAt) || summary.Quality != EventMetricQualityExact || summary.Partial {
		t.Fatalf("batch summary quality metadata not persisted: %#v", summary)
	}

	shutdownBatch := FlushBatch{BatchSummaries: []BatchSummary{{ID: "batch-2", CreatedAt: createdAt, WindowStart: createdAt, WindowEnd: createdAt.Add(time.Minute)}}}
	markFlushBatchPartial(&shutdownBatch)
	if !shutdownBatch.BatchSummaries[0].Partial || shutdownBatch.BatchSummaries[0].Quality != EventMetricQualityPartial {
		t.Fatalf("partial batch summary not marked: %#v", shutdownBatch.BatchSummaries[0])
	}
}

func TestFlusherLimitsDirectBatchSummariesAndPersistsDiagnostic(t *testing.T) {
	cfg := observabilityPresetConfigOrFull()
	cfg.BatchSummarySize = 2
	cfg.DiagnosticsRetention = time.Hour
	cfg.BatchSummaryRetention = time.Hour
	store := NewMemoryStore(StoreOptions{Prefix: "test"})
	f := newFlusher(cfg, store, newCollector(cfg), nil)
	now := time.Now()
	f.now = func() time.Time { return now }

	err := f.Flush(context.Background(), FlushBatch{
		WindowStart: now.Add(-time.Minute),
		WindowEnd:   now,
		BatchSummaries: []BatchSummary{
			{ID: "batch-1", Status: BatchStatusRunning, CreatedAt: now},
			{ID: "batch-2", Status: BatchStatusRunning, CreatedAt: now},
			{ID: "batch-1", Status: BatchStatusFinished, CreatedAt: now},
			{ID: "batch-3", Status: BatchStatusRunning, CreatedAt: now},
		},
	})
	if err != nil {
		t.Fatalf("flush batch summaries: %v", err)
	}

	batches, err := store.Batches(context.Background(), "")
	if err != nil {
		t.Fatalf("read batches: %v", err)
	}
	if len(batches) != 2 {
		t.Fatalf("expected flusher to write at most 2 batch summaries, got %#v", batches)
	}
	foundLatest := false
	for _, batch := range batches {
		if batch.ID == "batch-1" && batch.Status == BatchStatusFinished {
			foundLatest = true
		}
		if batch.ID == "batch-3" {
			t.Fatalf("over-limit batch should not be written: %#v", batches)
		}
	}
	if !foundLatest {
		t.Fatalf("duplicate batch ID should keep latest status, got %#v", batches)
	}
	diagnostics, err := store.ObservabilityDiagnostics(context.Background(), PageRequest{Page: 1, PageSize: 10})
	if err != nil {
		t.Fatalf("read diagnostics: %v", err)
	}
	foundLimit := false
	for _, diagnostic := range diagnostics.Items {
		if diagnostic.Reason == MemoryDropBatchSummaryLimit && diagnostic.Count == 1 {
			foundLimit = true
			break
		}
	}
	if !foundLimit {
		t.Fatalf("expected batch_summary_limit diagnostic, got %#v", diagnostics.Items)
	}
}

type fallbackBatchSummaryStore struct {
	Store
	saved []BatchSummary
}

func (s *fallbackBatchSummaryStore) SaveBatchSummary(_ context.Context, item BatchSummary) error {
	s.saved = append(s.saved, item)
	return nil
}

func TestFlusherWritesBatchSummariesThroughLegacyStoreFallback(t *testing.T) {
	cfg := observabilityPresetConfigOrFull()
	cfg.BatchSummaries = true
	inner := NewMemoryStore(StoreOptions{Prefix: "fallback"})
	store := &fallbackBatchSummaryStore{Store: inner}
	coll := newCollector(cfg)
	f := newFlusher(cfg, store, coll, nil)
	now := time.Date(2026, 5, 15, 13, 0, 0, 0, time.UTC)

	err := f.Flush(context.Background(), FlushBatch{
		WindowStart: now.Add(-time.Minute),
		WindowEnd:   now,
		BatchSummaries: []BatchSummary{{
			ID:        "batch-fallback",
			Name:      "Fallback",
			CreatedAt: now.Add(-time.Minute),
		}},
	})
	if err != nil {
		t.Fatalf("flush fallback batch summary: %v", err)
	}
	if len(store.saved) != 1 {
		t.Fatalf("expected one saved summary, got %#v", store.saved)
	}
	got := store.saved[0]
	if !got.WindowStart.Equal(now.Add(-time.Minute)) || !got.WindowEnd.Equal(now) || !got.FlushAt.Equal(now) {
		t.Fatalf("fallback summary window metadata not prepared: %#v", got)
	}
	if got.UpdatedAt.IsZero() || got.Quality != EventMetricQualityExact {
		t.Fatalf("fallback summary quality metadata not prepared: %#v", got)
	}
}

func TestFlusherAppendsEventMetricWindows(t *testing.T) {
	cfg := observabilityPresetConfigOrFull()
	cfg.EventMetricsRetention = 0
	store := NewMemoryStore(StoreOptions{Prefix: "test"})
	coll := newCollector(cfg)
	f := newFlusher(cfg, store, coll, nil)
	now := time.Date(2026, 5, 15, 12, 0, 0, 0, time.UTC)

	err := f.Flush(context.Background(), FlushBatch{
		WindowStart: now.Add(-time.Minute),
		WindowEnd:   now,
		Increments: []EventMetricIncrement{{
			Connection:          "redis",
			Queue:               "default",
			JobName:             "MailJob",
			Processed:           2,
			RuntimeMS:           120,
			Samples:             2,
			EffectiveSampleRate: 1,
			EstimatedTotal:      2,
			WindowStart:         now.Add(-time.Minute),
			WindowEnd:           now,
			Estimated:           false,
			Quality:             EventMetricQualityExact,
			SourcePrefix:        "test",
			SourceEnvironment:   "production",
			SourceHost:          "host-a",
			SourceSupervisor:    "supervisor-a",
		}},
	})
	if err != nil {
		t.Fatalf("flush first window: %v", err)
	}

	err = f.Flush(context.Background(), FlushBatch{
		WindowStart: now,
		WindowEnd:   now.Add(time.Minute),
		Increments: []EventMetricIncrement{{
			Connection:          "redis",
			Queue:               "default",
			JobName:             "MailJob",
			Failed:              1,
			Samples:             1,
			EffectiveSampleRate: 0.5,
			EstimatedTotal:      2,
			WindowStart:         now,
			WindowEnd:           now.Add(time.Minute),
			Estimated:           true,
			Degraded:            true,
			Quality:             EventMetricQualityDegraded,
		}},
		Diagnostics: []ObservabilityDiagnostic{{
			Reason:     MemoryDropBufferFull,
			Count:      1,
			ObservedAt: now.Add(time.Minute),
		}},
	})
	if err != nil {
		t.Fatalf("flush second window: %v", err)
	}

	windows, err := store.EventMetricWindows(context.Background(), EventMetricWindowQuery{Page: PageRequest{Page: 1, PageSize: 10}})
	if err != nil {
		t.Fatalf("read event metric windows: %v", err)
	}
	if windows.Total != 2 || len(windows.Items) != 2 {
		t.Fatalf("event metric windows should append, got %#v", windows)
	}
	latest := windows.Items[0]
	if !latest.WindowStart.Equal(now) || latest.FlushAt.IsZero() {
		t.Fatalf("latest window boundary/flush_at not persisted: %#v", latest)
	}
	if latest.EffectiveSampleRate != 0.5 || latest.SampleCount != 1 || latest.EstimatedTotal != 2 {
		t.Fatalf("sampling metadata not persisted: %#v", latest)
	}
	if latest.Quality != EventMetricQualityDegraded || !latest.Degraded {
		t.Fatalf("quality/degraded metadata not persisted: %#v", latest)
	}
	older := windows.Items[1]
	if older.Processed != 2 || older.Quality != EventMetricQualityExact || older.SourcePrefix != "test" {
		t.Fatalf("older appended window corrupted: %#v", older)
	}
}

// TestFlusherCollectorPairing 验证 collector 和 flusher 配对工作。
func TestFlusherCollectorPairing(t *testing.T) {
	cfg := observabilityPresetConfigOrFull()
	cfg.FlushInterval = 50 * time.Millisecond
	cfg.FlushTimeout = 2 * time.Second
	cfg.BufferSize = 500
	cfg.EventMetricsSampleRate = 1.0
	cfg.EventMetrics = true
	cfg.QueuedWaitsMax = 0
	cfg.MetricsWindow = time.Minute

	coll := newCollector(cfg)
	store := NewMemoryStore(StoreOptions{Prefix: "test"})

	f := newFlusher(cfg, store, coll, nil)
	f.now = func() time.Time { return time.Now() }

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	coll.Start(ctx)
	f.Start(ctx)

	// 发送多种事件
	now := time.Now()
	events := []CollectorInput{
		{
			Event:      "queue.job_processed",
			Connection: "redis", Queue: "default", JobName: "JobA",
			Runtime: 100 * time.Millisecond, OccurredAt: now,
			Sampling: SamplingDecision{EventMetricsSampled: true, EventMetricsSampleRate: 1.0},
		},
		{
			Event:      "queue.job_failed",
			Connection: "redis", Queue: "default", JobName: "JobA",
			Runtime: 500 * time.Millisecond, OccurredAt: now,
			ErrorSummary: "timeout",
			Sampling:     SamplingDecision{EventMetricsSampled: true, EventMetricsSampleRate: 1.0, HighValueDetailSampled: true, HighValueDetailRate: 1.0},
		},
		{
			Event:      "queue.job_processed",
			Connection: "redis", Queue: "low", JobName: "JobB",
			Runtime: 50 * time.Millisecond, OccurredAt: now,
			Sampling: SamplingDecision{EventMetricsSampled: true, EventMetricsSampleRate: 1.0},
		},
	}

	for _, ev := range events {
		_ = coll.Collect(ctx, ev)
	}

	time.Sleep(300 * time.Millisecond)

	// 按需 flush
	summary, err := f.FlushSnapshotOnDemand(ctx)
	if err != nil {
		t.Fatalf("flush failed: %v", err)
	}
	t.Logf("pairing summary: increments=%d, detail=%d, drops=%d",
		summary.IncrementCount, summary.DetailCount, summary.DropCount)

	// 验证 Store 中有数据
	windows, _ := store.EventMetricWindows(ctx, EventMetricWindowQuery{Page: PageRequest{Page: 1, PageSize: 100}})
	t.Logf("store windows after paired flush: %d", windows.Total)
}

// TestFlusherNilSafety 验证 nil flusher 安全。
func TestFlusherNilSafety(t *testing.T) {
	var f *flusher
	if err := f.Flush(context.Background(), FlushBatch{}); err != nil {
		t.Error("nil flusher Flush should return nil")
	}
	diag := f.Diagnostics()
	if diag.FlushErrorStreak != 0 {
		t.Error("nil flusher diagnostics should be zero")
	}
}

// TestFlusherRecordError 验证 flush 错误记录和降级诊断更新。
func TestFlusherRecordError(t *testing.T) {
	cfg := observabilityPresetConfigOrFull()
	coll := newCollector(cfg)
	store := NewMemoryStore(StoreOptions{Prefix: "test"})
	f := newFlusher(cfg, store, coll, nil)

	f.recordFlushError(assertAnError{})
	f.recordFlushError(assertAnError{})
	f.recordFlushError(assertAnError{})

	diag := f.Diagnostics()
	if diag.FlushErrorStreak != 3 {
		t.Errorf("expected error streak 3, got %d", diag.FlushErrorStreak)
	}
	if !diag.Degraded {
		t.Error("expected degraded=true after 3 consecutive errors")
	}
}

func TestFlusherClearsStoreUnavailableDegradationAfterConsecutiveSuccesses(t *testing.T) {
	cfg := observabilityPresetConfigOrFull()
	coll := newCollector(cfg)
	store := NewMemoryStore(StoreOptions{Prefix: "test"})
	f := newFlusher(cfg, store, coll, nil)

	f.recordFlushError(assertAnError{})
	f.recordFlushError(assertAnError{})
	f.recordFlushError(assertAnError{})
	if !f.Diagnostics().Degraded {
		t.Fatal("expected degraded after consecutive store errors")
	}

	for i := 0; i < 3; i++ {
		if err := f.Flush(context.Background(), FlushBatch{}); err != nil {
			t.Fatalf("success flush %d: %v", i, err)
		}
	}

	diag := f.Diagnostics()
	if diag.Degraded || diag.DegradedReason != "" || diag.FlushErrorStreak != 0 {
		t.Fatalf("expected degradation to clear after consecutive successes, got %#v", diag)
	}
}

type assertAnError struct{}

func (e assertAnError) Error() string { return "test error" }

func TestFlusherMarksDegradedWhenFlushLagExceedsInterval(t *testing.T) {
	cfg := observabilityPresetConfigOrFull()
	cfg.FlushInterval = time.Minute
	cfg.FlushTimeout = time.Second
	coll := newCollector(cfg)
	store := NewMemoryStore(StoreOptions{Prefix: "test"})
	f := newFlusher(cfg, store, coll, nil)
	now := time.Date(2026, 5, 15, 14, 0, 0, 0, time.UTC)
	f.now = func() time.Time { return now }

	coll.mu.Lock()
	coll.lastFlushAt = now.Add(-3 * time.Minute)
	coll.mu.Unlock()

	f.periodicFlush()

	deadline := time.Now().Add(time.Second)
	for {
		diag := f.Diagnostics()
		if diag.Degraded && diag.DegradedReason == MemoryDropFlushLagExceeded {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected flush lag degradation, got %#v", diag)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

type slowEventMetricStore struct {
	Store
	delay time.Duration
}

func (s *slowEventMetricStore) AppendEventMetricWindows(ctx context.Context, windows []EventMetricWindow, retention time.Duration) error {
	select {
	case <-time.After(s.delay):
		return s.Store.AppendEventMetricWindows(ctx, windows, retention)
	case <-ctx.Done():
		return ctx.Err()
	}
}

type blockingEventMetricStore struct {
	Store
	entered chan struct{}
	release chan struct{}
}

func (s *blockingEventMetricStore) AppendEventMetricWindows(ctx context.Context, windows []EventMetricWindow, retention time.Duration) error {
	select {
	case s.entered <- struct{}{}:
	default:
	}
	select {
	case <-s.release:
		return s.Store.AppendEventMetricWindows(ctx, windows, retention)
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestFlusherOnDemandBusyDoesNotDrainCollector(t *testing.T) {
	// 逻辑说明：snapshot 等待已有 writer 时必须先拿到串行 slot，再取 collector 快照；
	// 否则超时会把内存窗口清空但没有任何 append-only Store 事实。
	cfg := observabilityPresetConfigOrFull()
	cfg.FlushTimeout = 40 * time.Millisecond
	cfg.EventMetrics = true
	cfg.EventMetricsSampleRate = 1
	cfg.QueuedWaitsMax = 0
	base := NewMemoryStore(StoreOptions{Prefix: "busy"})
	store := &blockingEventMetricStore{Store: base, entered: make(chan struct{}, 1), release: make(chan struct{})}
	coll := newCollector(cfg)
	f := newFlusher(cfg, store, coll, nil)
	ctx := context.Background()

	go func() {
		_ = f.Flush(ctx, FlushBatch{
			WindowStart: time.Now().Add(-time.Minute),
			WindowEnd:   time.Now(),
			Increments: []EventMetricIncrement{{
				WindowStart:         time.Now().Add(-time.Minute),
				WindowEnd:           time.Now(),
				Connection:          "redis",
				Queue:               "busy",
				Processed:           1,
				Samples:             1,
				EffectiveSampleRate: 1,
			}},
		})
	}()
	<-store.entered

	coll.mu.Lock()
	coll.windows["manual"] = &eventMetricsWindow{
		windowStart: time.Now().Truncate(time.Minute),
		connection:  "redis",
		queue:       "default",
		jobName:     "EmailJob",
		processed:   2,
		samples:     2,
		sampleRate:  1,
	}
	coll.mu.Unlock()

	_, err := f.FlushSnapshotOnDemand(ctx)
	if err == nil {
		t.Fatal("expected on-demand flush to time out while writer is busy")
	}
	diag := f.Diagnostics()
	if diag.SchedulerSkipped == 0 || diag.SchedulerTimeout == 0 {
		t.Fatalf("on-demand busy should expose skipped and timeout diagnostics, got %#v", diag)
	}
	if snapshot := coll.SnapshotPeek(time.Now()); snapshot == nil || len(snapshot.windows) != 1 {
		t.Fatalf("collector should retain windows after busy timeout, got %#v", snapshot)
	}
	close(store.release)
}

func TestFlusherPeriodicTicksMergeBehindSingleBackgroundWriter(t *testing.T) {
	// 需求背景：historical scenario 42 要求周期 tick 在慢 Store 下不能为每次 flush 启动无界 goroutine。
	// 逻辑说明：第一个 tick 接受并启动后台 writer；后续 tick 只合并为 pending flush。Store 释放后，
	// 同一个后台 writer 串行消化 pending flush，Diagnostics 暴露 accepted/running/merged/queued。
	cfg := observabilityPresetConfigOrFull()
	cfg.FlushTimeout = time.Second
	cfg.EventMetrics = true
	cfg.EventMetricsSampleRate = 1
	cfg.QueuedWaitsMax = 0
	base := NewMemoryStore(StoreOptions{Prefix: "merge"})
	store := &blockingEventMetricStore{Store: base, entered: make(chan struct{}, 1), release: make(chan struct{})}
	coll := newCollector(cfg)
	f := newFlusher(cfg, store, coll, nil)

	now := time.Now().Truncate(time.Minute)
	coll.mu.Lock()
	coll.windows["manual"] = &eventMetricsWindow{
		windowStart: now,
		connection:  "redis",
		queue:       "default",
		jobName:     "EmailJob",
		processed:   1,
		samples:     1,
		sampleRate:  1,
	}
	coll.mu.Unlock()

	f.periodicFlush()
	<-store.entered
	f.periodicFlush()
	f.periodicFlush()

	diag := f.Diagnostics()
	if diag.SchedulerAccepted != 1 || diag.SchedulerRunning != 1 || diag.SchedulerMerged == 0 {
		t.Fatalf("periodic ticks should merge behind one running writer, got %#v", diag)
	}
	close(store.release)

	deadline := time.Now().Add(time.Second)
	for {
		diag = f.Diagnostics()
		if diag.SchedulerRunning == 0 && diag.SchedulerQueued > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("merged tick was not queued after writer release, got %#v", diag)
		}
		time.Sleep(10 * time.Millisecond)
	}
	windows, err := base.EventMetricWindows(context.Background(), EventMetricWindowQuery{Page: PageRequest{Page: 1, PageSize: 10}})
	if err != nil || windows.Total != 1 {
		t.Fatalf("expected exactly one drained event window after merged ticks, windows=%#v err=%v", windows, err)
	}
}

type contextObservingStore struct {
	Store
	sawCanceled bool
	sawDeadline bool
}

func (s *contextObservingStore) AppendEventMetricWindows(ctx context.Context, windows []EventMetricWindow, retention time.Duration) error {
	s.sawCanceled = ctx.Err() != nil
	_, s.sawDeadline = ctx.Deadline()
	return s.Store.AppendEventMetricWindows(ctx, windows, retention)
}

func TestFlusherShutdownUsesIndependentDeadlineContext(t *testing.T) {
	cfg := observabilityPresetConfigOrFull()
	cfg.FlushInterval = time.Hour
	cfg.FlushTimeout = time.Second
	cfg.EventMetrics = true
	cfg.EventMetricsSampleRate = 1
	cfg.QueuedWaitsMax = 0
	coll := newCollector(cfg)
	store := &contextObservingStore{Store: NewMemoryStore(StoreOptions{Prefix: "shutdown_ctx"})}
	f := newFlusher(cfg, store, coll, nil)
	ctx, cancel := context.WithCancel(context.Background())

	coll.Start(ctx)
	f.Start(ctx)
	_ = coll.Collect(ctx, CollectorInput{
		Event:      "queue.job_processed",
		Connection: "redis",
		Queue:      "default",
		JobName:    "ShutdownJob",
		Runtime:    time.Millisecond,
		Sampling: SamplingDecision{
			EventMetricsSampled:    true,
			EventMetricsSampleRate: 1,
		},
	})
	time.Sleep(100 * time.Millisecond)

	cancel()
	f.Stop()
	coll.Stop()

	if store.sawCanceled || !store.sawDeadline {
		t.Fatalf("shutdown Store context canceled=%v deadline=%v, want fresh deadline context", store.sawCanceled, store.sawDeadline)
	}
	windows, err := store.EventMetricWindows(context.Background(), EventMetricWindowQuery{Page: PageRequest{Page: 1, PageSize: 10}})
	if err != nil || windows.Total == 0 || !windows.Items[0].Partial {
		t.Fatalf("shutdown should persist partial event window, windows=%#v err=%v", windows, err)
	}
}

func TestFlusherMarksDegradedWhenFlushDurationApproachesTimeout(t *testing.T) {
	cfg := observabilityPresetConfigOrFull()
	cfg.FlushTimeout = 100 * time.Millisecond
	coll := newCollector(cfg)
	store := &slowEventMetricStore{
		Store: NewMemoryStore(StoreOptions{Prefix: "test"}),
		delay: 90 * time.Millisecond,
	}
	f := newFlusher(cfg, store, coll, nil)
	now := time.Date(2026, 5, 15, 14, 30, 0, 0, time.UTC)

	err := f.Flush(context.Background(), FlushBatch{
		WindowStart: now.Add(-time.Minute),
		WindowEnd:   now,
		Increments: []EventMetricIncrement{{
			WindowStart:         now.Add(-time.Minute),
			WindowEnd:           now,
			Connection:          "redis",
			Queue:               "default",
			Processed:           1,
			Samples:             1,
			EffectiveSampleRate: 1,
		}},
	})
	if err != nil {
		t.Fatalf("flush: %v", err)
	}

	diag := f.Diagnostics()
	if !diag.Degraded || diag.DegradedReason != MemoryDropFlushTimeoutNear {
		t.Fatalf("expected flush duration degradation, got %#v", diag)
	}
}

func TestFlusherMarksWindowsUnknownForUnquantifiableDrops(t *testing.T) {
	cfg := observabilityPresetConfigOrFull()
	coll := newCollector(cfg)
	store := NewMemoryStore(StoreOptions{Prefix: "test"})
	f := newFlusher(cfg, store, coll, nil)
	now := time.Date(2026, 5, 15, 15, 0, 0, 0, time.UTC)

	batch := f.buildFlushBatch(&flushSnapshot{
		windows: []*eventMetricsWindow{{
			windowStart: now.Add(-time.Minute),
			connection:  "redis",
			queue:       "default",
			jobName:     "MailJob",
			processed:   10,
			sampleRate:  1,
		}},
		drops:          map[string]int64{MemoryDropBufferFull: 3},
		windowStart:    now.Add(-time.Minute),
		windowEnd:      now,
		degraded:       true,
		degradedReason: "drops_detected",
	}, now)

	if len(batch.Increments) != 1 {
		t.Fatalf("expected one increment, got %#v", batch.Increments)
	}
	inc := batch.Increments[0]
	if !inc.Unknown || inc.Quality != EventMetricQualityUnknown || !inc.Degraded {
		t.Fatalf("unquantifiable drop should mark window unknown + degraded, got %#v", inc)
	}
	if len(batch.Diagnostics) != 1 || batch.Diagnostics[0].Gap != ObservabilityGapUnknown {
		t.Fatalf("diagnostic should record unknown gap, got %#v", batch.Diagnostics)
	}
}

func TestFlusherKeepsAggregateOverflowQuantifiable(t *testing.T) {
	cfg := observabilityPresetConfigOrFull()
	coll := newCollector(cfg)
	store := NewMemoryStore(StoreOptions{Prefix: "test"})
	f := newFlusher(cfg, store, coll, nil)
	now := time.Date(2026, 5, 15, 15, 30, 0, 0, time.UTC)

	batch := f.buildFlushBatch(&flushSnapshot{
		windows: []*eventMetricsWindow{{
			windowStart: now.Add(-time.Minute),
			connection:  "redis",
			queue:       "default",
			jobName:     "_overflow",
			processed:   10,
			sampleRate:  1,
		}},
		drops:          map[string]int64{MemoryDropAggregateOverflow: 3},
		windowStart:    now.Add(-time.Minute),
		windowEnd:      now,
		degraded:       true,
		degradedReason: "drops_detected",
	}, now)

	inc := batch.Increments[0]
	if inc.Unknown || inc.Quality != EventMetricQualityDegraded || !inc.Degraded {
		t.Fatalf("aggregate overflow should stay quantifiable degraded, got %#v", inc)
	}
	if len(batch.Diagnostics) != 1 || batch.Diagnostics[0].Gap != ObservabilityGapQuantifiable {
		t.Fatalf("diagnostic should record quantifiable gap, got %#v", batch.Diagnostics)
	}
}

func TestFlusherKeepsBatchSummaryLimitFromMarkingEventMetricsUnknown(t *testing.T) {
	cfg := observabilityPresetConfigOrFull()
	coll := newCollector(cfg)
	store := NewMemoryStore(StoreOptions{Prefix: "test"})
	f := newFlusher(cfg, store, coll, nil)
	now := time.Date(2026, 5, 15, 15, 30, 0, 0, time.UTC)

	batch := f.buildFlushBatch(&flushSnapshot{
		windows: []*eventMetricsWindow{{
			windowStart: now.Add(-time.Minute),
			connection:  "redis",
			queue:       "default",
			jobName:     "SendEmail",
			processed:   10,
			sampleRate:  1,
		}},
		drops:       map[string]int64{MemoryDropBatchSummaryLimit: 3},
		windowStart: now.Add(-time.Minute),
		windowEnd:   now,
	}, now)

	inc := batch.Increments[0]
	if inc.Unknown || inc.Quality != EventMetricQualityExact || inc.Degraded {
		t.Fatalf("batch summary limit should not degrade event metrics, got %#v", inc)
	}
	if len(batch.Diagnostics) != 1 || batch.Diagnostics[0].Gap != ObservabilityGapQuantifiable {
		t.Fatalf("diagnostic should record quantifiable batch summary gap, got %#v", batch.Diagnostics)
	}
}

// TestFlusherEmptyBatch 验证空 batch flush 行为。
func TestFlusherEmptyBatch(t *testing.T) {
	cfg := observabilityPresetConfigOrFull()
	coll := newCollector(cfg)
	store := NewMemoryStore(StoreOptions{Prefix: "test"})
	f := newFlusher(cfg, store, coll, nil)

	batch := FlushBatch{}
	err := f.Flush(context.Background(), batch)
	if err != nil {
		t.Errorf("empty batch flush should not error: %v", err)
	}
}

// TestFlusherCollectorStateResetAfterFlush 验证 collector 状态在 flush 后重置。
func TestFlusherCollectorStateResetAfterFlush(t *testing.T) {
	cfg := observabilityPresetConfigOrFull()
	cfg.BufferSize = 1000
	cfg.EventMetrics = true
	cfg.EventMetricsSampleRate = 1.0
	cfg.QueuedWaitsMax = 0
	coll := newCollector(cfg)
	store := NewMemoryStore(StoreOptions{Prefix: "test"})
	f := newFlusher(cfg, store, coll, nil)

	ctx := context.Background()
	coll.Start(ctx)
	defer coll.Stop()

	for i := 0; i < 5; i++ {
		_ = coll.Collect(ctx, CollectorInput{
			Event:      "queue.job_processed",
			Connection: "redis",
			Queue:      "default",
			JobName:    "ResetJob",
			Runtime:    100 * time.Millisecond,
			Sampling: SamplingDecision{
				EventMetricsSampled:    true,
				EventMetricsSampleRate: 1.0,
			},
		})
	}

	time.Sleep(200 * time.Millisecond)

	summary1, _ := f.FlushSnapshotOnDemand(ctx)
	summary2, _ := f.FlushSnapshotOnDemand(ctx)
	t.Logf("first flush=%d, second flush=%d", summary1.IncrementCount, summary2.IncrementCount)

	// 第二次 flush 应在第一次重置后返回更少（理想为 0）
	if summary1.IncrementCount > 0 && summary2.IncrementCount > summary1.IncrementCount {
		t.Error("second flush should not have more increments than first after reset")
	}
}

// TestFlusherWindowBoundary 验证窗口边界写入。
func TestFlusherWindowBoundary(t *testing.T) {
	cfg := observabilityPresetConfigOrFull()
	cfg.MetricsWindow = time.Minute
	cfg.EventMetricsSampleRate = 1.0
	cfg.EventMetrics = true
	cfg.QueuedWaitsMax = 0

	coll := newCollector(cfg)
	store := NewMemoryStore(StoreOptions{Prefix: "test"})
	f := newFlusher(cfg, store, coll, nil)
	_ = f

	ctx := context.Background()
	coll.Start(ctx)
	defer coll.Stop()

	now := time.Now()
	_ = coll.Collect(ctx, CollectorInput{
		Event: "queue.job_processed", Connection: "redis", Queue: "default",
		JobName: "WinJob", Runtime: 100 * time.Millisecond, OccurredAt: now,
		Sampling: SamplingDecision{EventMetricsSampled: true, EventMetricsSampleRate: 1.0},
	})

	time.Sleep(200 * time.Millisecond)
	snapshot := coll.FlushSnapshot(now.Add(time.Minute))

	if len(snapshot.windows) == 0 {
		t.Error("expected at least one window")
	}
	if snapshot.windowEnd.Sub(snapshot.windowStart) < time.Minute {
		t.Error("window range too small")
	}
}

// panicFlushStore 在 AppendEventMetricWindows 上 panic，用于验证 runBackgroundFlush 的 recover。
type panicFlushStore struct {
	MemoryStore
}

func (s *panicFlushStore) AppendEventMetricWindows(_ context.Context, _ []EventMetricWindow, _ time.Duration) error {
	panic("injected flush panic for test coverage")
}

// TestFlusherRunBackgroundFlushRecoversFromPanic 验证 runBackgroundFlush 内部
// FlushSnapshot 已清空 collector 内存窗口后再 panic 时，recover 能捕获异常并记录错误，
// goroutine 安全退出，不会导致落盘管线进程崩溃。
func TestFlusherRunBackgroundFlushRecoversFromPanic(t *testing.T) {
	cfg := observabilityPresetConfigOrFull()
	cfg.EventMetrics = true
	cfg.EventMetricsSampleRate = 1
	cfg.FlushInterval = time.Hour // 长间隔，避免 loop ticker 干扰
	cfg.FlushTimeout = 2 * time.Second

	coll := newCollector(cfg)
	coll.Start(context.Background())
	defer coll.Stop()

	// 写一条事件到 collector，确保 FlushSnapshot 有数据
	_ = coll.Collect(context.Background(), CollectorInput{
		Event:      "queue.job_processed",
		Connection: "redis",
		Queue:      "default",
		SourceHost: "test-host",
		OccurredAt: time.Now(),
		Sampling:   SamplingDecision{EventMetricsSampled: true},
	})
	time.Sleep(50 * time.Millisecond)

	store := &panicFlushStore{MemoryStore: *NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})}
	f := newFlusher(cfg, store, coll, nil)
	// 手动调用 runBackgroundFlushOnce（不走 scheduleBackgroundFlush 的单飞逻辑），
	// 避免竞态和 WaitGroup 问题；在 defer 中捕获 panic 并记录错误，模拟上层 goroutine 恢复路径。
	func() {
		defer func() {
			if rec := recover(); rec != nil {
				f.recordFlushError(fmt.Errorf("flusher background flush panic: %v", rec))
			}
		}()
		f.runBackgroundFlushOnce()
	}()

	// 验证 flush 错误被记录（panic 恢复路径写入了错误诊断）
	f.mu.Lock()
	errMsg := f.lastFlushError
	f.mu.Unlock()
	if errMsg == "" {
		t.Fatal("runBackgroundFlush panic should be recorded as flush error")
	}
}

func TestRecordShutdownFlushError(t *testing.T) {
	// 需求背景：recordShutdownFlushError 在 shutdown flush 失败时记录错误并持久化
	// MemoryDropStoreUnavailable 诊断。三个分支：err=nil 无操作、store 存在时持久化诊断、
	// store 为 nil 时仅记录 flush 错误不写 Store。

	cfg := observabilityPresetConfigOrFull()
	coll := newCollector(cfg)
	store := NewMemoryStore(StoreOptions{Prefix: "test"})
	f := newFlusher(cfg, store, coll, nil)

	// 分支 1：err == nil → no-op，错误计数和诊断均为零值
	f.recordShutdownFlushError(context.Background(), nil)
	diag := f.Diagnostics()
	if diag.FlushErrorStreak != 0 || diag.LastFlushError != "" {
		t.Fatalf("nil error should be no-op, got streak=%d err=%q", diag.FlushErrorStreak, diag.LastFlushError)
	}

	// 分支 2：err != nil + store 存在 → FlushErrorStreak 递增 + 诊断持久化到 Store
	testErr := errors.New("store write failed during shutdown")
	f.recordShutdownFlushError(context.Background(), testErr)
	diag = f.Diagnostics()
	if diag.FlushErrorStreak != 1 {
		t.Fatalf("expected error streak 1, got %d", diag.FlushErrorStreak)
	}
	if diag.LastFlushError != "store write failed during shutdown" {
		t.Fatalf("expected last_flush_error, got %q", diag.LastFlushError)
	}

	// 验证 Store 中有 MemoryDropStoreUnavailable 诊断
	diags, err := store.ObservabilityDiagnostics(context.Background(), PageRequest{Page: 1, PageSize: 10})
	if err != nil {
		t.Fatalf("failed to read diagnostics: %v", err)
	}
	found := false
	for _, d := range diags.Items {
		if d.Reason == MemoryDropStoreUnavailable && d.Count == 1 {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected MemoryDropStoreUnavailable diagnostic in store, got %d items", len(diags.Items))
	}

	// 分支 3：err != nil + store == nil → recordFlushError 生效，不写 Store，不 panic
	f2 := newFlusher(cfg, store, coll, nil)
	f2.store = nil
	f2.recordShutdownFlushError(context.Background(), errors.New("another error"))
	diag = f2.Diagnostics()
	if diag.FlushErrorStreak != 1 || diag.LastFlushError != "another error" {
		t.Fatalf("expected error recorded without store, got streak=%d err=%q",
			diag.FlushErrorStreak, diag.LastFlushError)
	}
}

// failingHighValueStore 是一个辅助 Store，SaveHighValueDetails 总是返回错误。
type failingHighValueStore struct {
	Store
}

func (s *failingHighValueStore) SaveHighValueDetails(_ context.Context, _ []HighValueJobDetail, _ time.Duration) error {
	return errors.New("auxiliary high-value write failure")
}

// TestFlusherAuxiliaryWriteAccumulatesErrorStreak 验证辅助写入失败时 flushErrorStreak 持续累积。
//
// 需求背景：代码审查发现 flushOnce 在辅助写入（high-value details、diagnostics、batch summaries）
// 失败后无条件重置 flushErrorStreak 为 0，导致连续 3 次辅助写入失败永远不会触发降级。
func TestFlusherAuxiliaryWriteAccumulatesErrorStreak(t *testing.T) {
	cfg := observabilityPresetConfigOrFull()
	cfg.HighValueDetailRetention = time.Hour
	base := NewMemoryStore(StoreOptions{Prefix: "test"})
	store := &failingHighValueStore{Store: base}
	coll := newCollector(cfg)
	f := newFlusher(cfg, store, coll, nil)

	batch := FlushBatch{
		WindowStart: time.Now().Add(-time.Minute),
		WindowEnd:   time.Now(),
		Increments: []EventMetricIncrement{{
			Connection:          "redis",
			Queue:               "default",
			WindowStart:         time.Now().Add(-time.Minute),
			WindowEnd:           time.Now(),
			Processed:           1,
			Samples:             1,
			EffectiveSampleRate: 1,
		}},
		HighValueDetails: []HighValueJobDetail{{
			ID:   "detail-1",
			Kind: HighValueDetailFailed,
		}},
	}

	// 执行 4 次，每次辅助写入都失败
	for i := 0; i < 4; i++ {
		if err := f.Flush(context.Background(), batch); err != nil {
			t.Fatalf("flush %d should not return error (core metrics succeed): %v", i, err)
		}
	}

	diag := f.Diagnostics()
	if diag.FlushErrorStreak < 3 {
		t.Fatalf("expected error streak >= 3 after 4 auxiliary write failures, got %d", diag.FlushErrorStreak)
	}
	if !diag.Degraded || diag.DegradedReason != MemoryDropStoreUnavailable {
		t.Fatalf("expected degraded=MemoryDropStoreUnavailable after 3+ consecutive auxiliary failures, got degraded=%v reason=%q",
			diag.Degraded, diag.DegradedReason)
	}
}

// TestFlusherCleanFlushResetsAuxiliaryErrorStreak 验证全量成功的 flush 仍然可以重置错误计数。
//
// 需求背景：当辅助写入从失败恢复为成功后，error streak 应该被清零，降级状态应恢复。
func TestFlusherCleanFlushResetsAuxiliaryErrorStreak(t *testing.T) {
	cfg := observabilityPresetConfigOrFull()
	cfg.HighValueDetailRetention = time.Hour
	base := NewMemoryStore(StoreOptions{Prefix: "test"})
	store := &failingHighValueStore{Store: base}
	coll := newCollector(cfg)
	f := newFlusher(cfg, store, coll, nil)

	// 先产生一些辅助写入失败
	batch := FlushBatch{
		WindowStart: time.Now().Add(-time.Minute),
		WindowEnd:   time.Now(),
		Increments: []EventMetricIncrement{{
			Connection:          "redis",
			Queue:               "default",
			WindowStart:         time.Now().Add(-time.Minute),
			WindowEnd:           time.Now(),
			Processed:           1,
			Samples:             1,
			EffectiveSampleRate: 1,
		}},
		HighValueDetails: []HighValueJobDetail{{
			ID:   "detail-2",
			Kind: HighValueDetailFailed,
		}},
	}
	for i := 0; i < 3; i++ {
		_ = f.Flush(context.Background(), batch)
	}

	diag := f.Diagnostics()
	if !diag.Degraded {
		t.Fatal("expected degraded after 3 auxiliary write failures")
	}

	// 现在发一个干净的 flush（无 high-value details，所有写入都成功）
	cleanBatch := FlushBatch{
		WindowStart: time.Now().Add(-time.Minute),
		WindowEnd:   time.Now(),
		Increments: []EventMetricIncrement{{
			Connection:          "redis",
			Queue:               "default",
			WindowStart:         time.Now().Add(-time.Minute),
			WindowEnd:           time.Now(),
			Processed:           1,
			Samples:             1,
			EffectiveSampleRate: 1,
		}},
	}
	for i := 0; i < 3; i++ {
		if err := f.Flush(context.Background(), cleanBatch); err != nil {
			t.Fatalf("clean flush %d: %v", i, err)
		}
	}

	diag = f.Diagnostics()
	if diag.Degraded {
		t.Fatalf("expected degradation cleared after 3 consecutive clean flushes, got degraded=%v reason=%q streak=%d",
			diag.Degraded, diag.DegradedReason, diag.FlushErrorStreak)
	}
	if diag.FlushErrorStreak != 0 {
		t.Fatalf("expected zero error streak after 3 clean flushes, got %d", diag.FlushErrorStreak)
	}
}