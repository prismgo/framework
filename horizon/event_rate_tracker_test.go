package horizon

import (
	"testing"
	"time"
)

func TestEventRateTrackerExpiresSlotsOutsideWindow(t *testing.T) {
	var tracker eventRateTracker
	start := time.Date(2026, 5, 16, 10, 0, 0, 0, time.UTC)

	for i := 0; i < 10; i++ {
		tracker.recordAt(start)
	}
	for i := 0; i < 5; i++ {
		tracker.recordAt(start.Add(59 * time.Second))
	}

	if total := tracker.totalAt(start.Add(59 * time.Second)); total != 15 {
		t.Fatalf("expected all events inside the window, got %d", total)
	}
	if total := tracker.totalAt(start.Add(60 * time.Second)); total != 5 {
		t.Fatalf("expected first second to expire at the next window, got %d", total)
	}
	if total := tracker.totalAt(start.Add(2 * time.Minute)); total != 0 {
		t.Fatalf("expected long idle window to expire all slots, got %d", total)
	}
}

func TestEventRateTrackerHandlesNonMonotonicInputConservatively(t *testing.T) {
	var tracker eventRateTracker
	start := time.Date(2026, 5, 16, 10, 0, 0, 0, time.UTC)

	for i := 0; i < 100; i++ {
		tracker.recordAt(start)
	}
	if count := tracker.recordAt(start.Add(-2 * time.Minute)); count != 0 {
		t.Fatalf("old non-monotonic input should not revive an expired slot, got %d", count)
	}
	if total := tracker.totalAt(start.Add(2 * time.Minute)); total != 0 {
		t.Fatalf("historical traffic should not pollute future pressure, got %d", total)
	}

	for i := 0; i < 3; i++ {
		tracker.recordAt(start.Add(2 * time.Minute))
	}
	if total := tracker.totalAt(start.Add(2 * time.Minute)); total != 3 {
		t.Fatalf("tracker should recover after idle, got %d", total)
	}
}

func TestCollectorSamplingPressureUsesCurrentWindowRate(t *testing.T) {
	cfg := observabilityPresetConfigOrFull()
	cfg.MaxEventsPerSecond = 1000
	cfg.EventMetricsSampleRate = 1
	cfg.DynamicSamplingEnabled = true
	cfg.MinSampleRate = 0.1
	coll := newCollector(cfg)
	start := time.Date(2026, 5, 16, 10, 0, 0, 0, time.UTC)

	coll.mu.Lock()
	for i := 0; i < 1000; i++ {
		coll.rateTracker.recordAt(start)
	}
	pressure := coll.samplingPressureAt(start)
	if pressure.EventRate == 0 {
		coll.mu.Unlock()
		t.Fatalf("expected high event rate before the window expires: %#v", pressure)
	}
	pressure = coll.samplingPressureAt(start.Add(2 * time.Minute))
	coll.mu.Unlock()

	if pressure.EventRate != 0 {
		t.Fatalf("expected expired event rate to be ignored, got %#v", pressure)
	}
	if result := EvaluateSamplingPolicy(cfg, pressure); result.State != SamplingStateNormal {
		t.Fatalf("expired traffic should not keep dynamic sampling pressured, got %#v", result)
	}
}
