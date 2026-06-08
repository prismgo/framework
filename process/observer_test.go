package process

import (
	"context"
	"runtime"
	"testing"
	"time"
)

// TestObserverReturnsFieldLevelUnavailableShapeWithoutFakeZeroes 固定无效 PID 的降级契约。
// 需求背景：process observability contract 要求 CPU、内存和 goroutine 等字段不可用时必须返回 nil value 和稳定 reason，
// 不能用 0 伪装缺失数据，否则 Dashboard 会误导用户。
func TestObserverReturnsFieldLevelUnavailableShapeWithoutFakeZeroes(t *testing.T) {
	observer := NewObserver(ObserverOptions{SampleWindow: time.Millisecond})
	snapshots, err := observer.Observe(context.Background(), []int{-99})
	if err != nil {
		t.Fatalf("observe invalid pid: %v", err)
	}
	snapshot, ok := snapshots[-99]
	if !ok {
		t.Fatalf("missing snapshot for requested pid: %#v", snapshots)
	}
	for name, metric := range map[string]Metric{
		"cpu":            snapshot.CPUPercent,
		"memory_rss":     snapshot.MemoryRSSBytes,
		"memory_percent": snapshot.MemoryPercent,
		"goroutines":     snapshot.GoroutineCount,
	} {
		if metric.Status == StatusAvailable {
			t.Fatalf("%s should not be available for invalid pid: %#v", name, metric)
		}
		if metric.Value != nil {
			t.Fatalf("%s unavailable value must be nil, got %#v", name, metric.Value)
		}
		if metric.Reason == "" {
			t.Fatalf("%s unavailable metric must include a stable reason", name)
		}
	}
	if snapshot.SampleWindowMS < 0 {
		t.Fatalf("sample window must be stable, got %d", snapshot.SampleWindowMS)
	}
}

// TestSelfSnapshotReportsLowCostRuntimeFields 验证 heartbeat 可使用的低成本自省字段。
// 设计思路：当前 Go 进程的 goroutine 数来自 runtime，不需要 OS 级短窗口采样；RSS 字段按平台能力可用或降级。
func TestSelfSnapshotReportsLowCostRuntimeFields(t *testing.T) {
	snapshot := SelfSnapshot()
	if snapshot.PID <= 0 {
		t.Fatalf("self pid must be set: %#v", snapshot)
	}
	if snapshot.GoroutineCount.Status != StatusAvailable || snapshot.GoroutineCount.Unit != UnitCount || snapshot.GoroutineCount.Value == nil {
		t.Fatalf("goroutine metric = %#v, want available count", snapshot.GoroutineCount)
	}
	if got, ok := snapshot.GoroutineCount.Value.(int); !ok || got < runtime.NumGoroutine() {
		t.Fatalf("goroutine value = %#v, want at least current runtime count", snapshot.GoroutineCount.Value)
	}
	if snapshot.MemoryRSSBytes.Status == StatusAvailable && snapshot.MemoryRSSBytes.Unit != UnitBytes {
		t.Fatalf("rss unit = %q, want bytes", snapshot.MemoryRSSBytes.Unit)
	}
}
