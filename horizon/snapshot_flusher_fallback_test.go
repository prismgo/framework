package horizon

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	horizoncmd "github.com/prismgo/framework/horizon/cmd"
)

func TestSnapshotUsesTemporaryFlusherWhenRuntimeFlusherMissing(t *testing.T) {
	// 需求背景：命令执行时可能只有 collector 和 Store 已解析，后台 flusher 尚未启动。
	// 此时 horizon:snapshot 仍必须创建临时 flusher，并走 FlushSnapshotOnDemand 的同一条 writer 路径，
	// 而不是恢复旧的 runtimeCommandAdapter 直接 FlushSnapshot 后手写 MetricsSnapshot。
	ctx := context.Background()
	now := time.Date(2026, 5, 15, 11, 0, 0, 0, time.UTC)
	store := NewMemoryStore(StoreOptions{Prefix: "snapshot_flusher_temp"})
	obs := observabilityPresetConfigOrFull()
	obs.FlushTimeout = time.Second
	obs.EventMetrics = true
	obs.EventMetricsSampleRate = 1
	obs.QueuedWaitsMax = 0
	manager, _ := NewManager(Config{Store: "memory", Observability: obs}, WithStoreFactory(staticStoreResolver{store: store}))
	coll := manager.Collector()
	coll.Start(ctx)
	defer coll.Stop()
	_ = coll.Collect(ctx, CollectorInput{
		Event:      "queue.job_processed",
		Connection: "redis",
		Queue:      "default",
		JobName:    "EmailJob",
		Runtime:    time.Millisecond,
		OccurredAt: now,
		Sampling: SamplingDecision{
			EventMetricsSampled:    true,
			EventMetricsSampleRate: 1,
		},
	})
	time.Sleep(100 * time.Millisecond)

	runtime := &runtimeCommandAdapter{manager: manager, store: store}
	summary, err := runtime.Snapshot(ctx, now)
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if summary.FlushStatus != FlushSchedulingFlushed || summary.MetricsStatus != horizoncmd.SnapshotStatusEnabled {
		t.Fatalf("snapshot summary should report temporary flusher result, got %#v", summary)
	}
	windows, err := store.EventMetricWindows(ctx, EventMetricWindowQuery{Page: PageRequest{Page: 1, PageSize: 10}})
	if err != nil || windows.Total != 1 {
		t.Fatalf("temporary flusher should write append-only event windows, windows=%#v err=%v", windows, err)
	}
}

func TestSnapshotErrorsWhenCollectorOrStoreMissing(t *testing.T) {
	// 错误边界说明：缺少 collector 或 Store 时，snapshot 不能回退到旧同步写入面。
	// 明确错误比静默写空 read model 更安全，因为 collector 中的 append-only facts 无法被可靠持久化。
	ctx := context.Background()
	store := NewMemoryStore(StoreOptions{Prefix: "snapshot_flusher_errors"})
	manager, _ := NewManager(Config{Store: "memory", Observability: observabilityPresetConfigOrFull()}, WithStoreFactory(staticStoreResolver{store: store}))
	manager.coll = nil
	runtime := &runtimeCommandAdapter{manager: manager, store: store}
	if _, err := runtime.Snapshot(ctx, time.Now()); err == nil || !strings.Contains(err.Error(), "collector") {
		t.Fatalf("snapshot without collector error = %v, want explicit collector error", err)
	}

	manager, _ = NewManager(Config{Store: "memory", Observability: observabilityPresetConfigOrFull()})
	runtime = &runtimeCommandAdapter{manager: manager}
	if _, err := runtime.Snapshot(ctx, time.Now()); !errors.Is(err, ErrStoreNotConfigured) {
		t.Fatalf("snapshot without Store error = %v, want %v", err, ErrStoreNotConfigured)
	}
}
