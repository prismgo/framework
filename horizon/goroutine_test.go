package horizon

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRestartPolicyAppliesCoolingAfterTenConsecutivePanics(t *testing.T) {
	policy := defaultGoroutineRestartPolicy()

	for consecutive := 1; consecutive <= 9; consecutive++ {
		delay, cooling := policy.delayAfterPanic(consecutive)
		if cooling {
			t.Fatalf("panic %d should use short restart delay, got cooling delay %s", consecutive, delay)
		}
		if delay != 10*time.Millisecond {
			t.Fatalf("panic %d delay = %s, want 10ms", consecutive, delay)
		}
	}

	cases := []struct {
		consecutive int
		want        time.Duration
	}{
		{consecutive: 10, want: time.Minute},
		{consecutive: 11, want: 5 * time.Minute},
		{consecutive: 12, want: 10 * time.Minute},
		{consecutive: 13, want: 10 * time.Minute},
	}
	for _, tc := range cases {
		delay, cooling := policy.delayAfterPanic(tc.consecutive)
		if !cooling {
			t.Fatalf("panic %d should enter cooling delay", tc.consecutive)
		}
		if delay != tc.want {
			t.Fatalf("panic %d delay = %s, want %s", tc.consecutive, delay, tc.want)
		}
	}
}

func TestRestartingTrackedGoroutineCoolingWaitStopsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	var attempts atomic.Int64

	wg.Add(1)
	startRestartingTrackedGoroutineWithPolicy(ctx, &wg, "test", nil, nil, func() {
		attempts.Add(1)
		panic("forced restart")
	}, goroutineRestartPolicy{
		panicThreshold: 1,
		shortDelay:     time.Millisecond,
		coolingDelays:  []time.Duration{time.Hour},
	})

	deadline := time.Now().Add(time.Second)
	for attempts.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for first panic attempt")
		}
		time.Sleep(time.Millisecond)
	}

	cancel()
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("cooling wait should stop promptly after context cancellation")
	}
	if got := attempts.Load(); got != 1 {
		t.Fatalf("cooling policy should not retry before cancellation, got attempts=%d", got)
	}
}

func TestRestartingTrackedGoroutineRetriesAfterCoolingDelay(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	var attempts atomic.Int64

	wg.Add(1)
	startRestartingTrackedGoroutineWithPolicy(ctx, &wg, "test", nil, nil, func() {
		if attempts.Add(1) <= 2 {
			panic("forced restart")
		}
	}, goroutineRestartPolicy{
		panicThreshold: 1,
		shortDelay:     time.Millisecond,
		coolingDelays:  []time.Duration{5 * time.Millisecond},
	})

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("restart loop should retry after cooling delay and exit after run returns")
	}
	if got := attempts.Load(); got != 3 {
		t.Fatalf("expected two panics followed by a successful run, got attempts=%d", got)
	}
}
