package process

import (
	"context"
	"testing"
	"time"
)

// TestNewObserverUsesDefaultSampleWindow 验证零值配置会回退到默认短窗口，避免请求路径出现无界等待。
func TestNewObserverUsesDefaultSampleWindow(t *testing.T) {
	instance, ok := NewObserver(ObserverOptions{}).(*observer)
	if !ok {
		t.Fatalf("observer type = %T, want *observer", NewObserver(ObserverOptions{}))
	}
	if instance.options.SampleWindow != 100*time.Millisecond {
		t.Fatalf("sample window = %v, want 100ms", instance.options.SampleWindow)
	}
	if instance.sampler == nil {
		t.Fatalf("sampler should be initialized")
	}
}

// TestObserverNilReceiverSelfHeals 验证 nil receiver 调用会自动补齐默认配置，而不是 panic。
func TestObserverNilReceiverSelfHeals(t *testing.T) {
	var instance *observer
	snapshots, err := instance.Observe(context.Background(), []int{-7})
	if err != nil {
		t.Fatalf("observe with nil receiver: %v", err)
	}
	snapshot, ok := snapshots[-7]
	if !ok {
		t.Fatalf("missing snapshot for invalid pid: %#v", snapshots)
	}
	if snapshot.SampleWindowMS != 100 {
		t.Fatalf("sample window = %d, want 100", snapshot.SampleWindowMS)
	}
}

// TestNormalizePIDsKeepsFirstOccurrenceAndInvalidValues 验证 PID 去重时保留首次出现值，并保留非正数给上层表达 unavailable。
func TestNormalizePIDsKeepsFirstOccurrenceAndInvalidValues(t *testing.T) {
	got := normalizePIDs([]int{5, 0, 5, -2, 0, 7, -2})
	want := []int{5, 0, -2, 7}
	if len(got) != len(want) {
		t.Fatalf("normalized len = %d, want %d (%v)", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("normalized[%d] = %d, want %d (full=%v)", i, got[i], want[i], got)
		}
	}
}

// TestMetricHelpersReturnStableStatuses 验证字段辅助构造器返回稳定状态，避免调用方把缺失值误判为 0。
func TestMetricHelpersReturnStableStatuses(t *testing.T) {
	availableMetric := available(12.5, UnitPercent)
	if availableMetric.Status != StatusAvailable || availableMetric.Value != 12.5 || availableMetric.Reason != "" {
		t.Fatalf("available metric = %#v", availableMetric)
	}
	unavailableMetric := unavailable(UnitBytes, "missing")
	if unavailableMetric.Status != StatusUnavailable || unavailableMetric.Value != nil || unavailableMetric.Reason != "missing" {
		t.Fatalf("unavailable metric = %#v", unavailableMetric)
	}
	unsupportedMetric := unsupported(UnitCount, "unsupported")
	if unsupportedMetric.Status != StatusUnsupported || unsupportedMetric.Value != nil || unsupportedMetric.Reason != "unsupported" {
		t.Fatalf("unsupported metric = %#v", unsupportedMetric)
	}
}
