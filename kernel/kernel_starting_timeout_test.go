package kernel

import (
	"context"
	"testing"
	"time"
)

// TestRunStartingCallbacksRespectsContextTimeout verifies that when a goroutine is
// waiting on the starting wait channel and the context is cancelled, it returns
// promptly instead of blocking forever.
func TestRunStartingCallbacksRespectsContextTimeout(t *testing.T) {
	useKernelTestContainer(t)

	k := New("test")

	// Simulate: goroutine A enters runStartingCallbacks and starts executing callbacks.
	// We register a callback that blocks until we release it, so goroutine B will wait.
	blocker := make(chan struct{})
	_ = k.Starting(func(kernel *Kernel) error {
		<-blocker
		return nil
	})

	errCh := make(chan error, 1)

	// Goroutine A: starts running callbacks (will block on the blocker channel).
	go func() {
		ctx := context.Background()
		// This will block until blocker is closed or context expires.
		_ = k.runStartingCallbacks(ctx)
	}()

	// Give goroutine A time to enter the callback and start blocking.
	time.Sleep(50 * time.Millisecond)

	// Goroutine B: tries to run callbacks but must wait because A is running.
	// Use a context with a short timeout — it should return context.DeadlineExceeded.
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		errCh <- k.runStartingCallbacks(ctx)
	}()

	// Wait for goroutine B to finish (should return quickly due to context timeout).
	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("expected context error from waiting goroutine, got nil")
		}
		if err != context.DeadlineExceeded {
			t.Fatalf("expected context.DeadlineExceeded, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("goroutine B did not return within timeout")
	}

	// Clean up: release goroutine A.
	close(blocker)
}

// TestRunStartingCallbacksContextAlreadyCancelled verifies that an already-cancelled
// context returns immediately without waiting.
func TestRunStartingCallbacksContextAlreadyCancelled(t *testing.T) {
	useKernelTestContainer(t)

	k := New("test")

	blocker := make(chan struct{})
	_ = k.Starting(func(kernel *Kernel) error {
		<-blocker
		return nil
	})

	// Goroutine A: starts running callbacks (will block).
	go func() {
		_ = k.runStartingCallbacks(context.Background())
	}()

	time.Sleep(50 * time.Millisecond)

	// Goroutine B: uses an already-cancelled context.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := k.runStartingCallbacks(ctx)
	if err != context.Canceled {
		t.Fatalf("expected context.Canceled, got %v", err)
	}

	// Clean up: release goroutine A.
	close(blocker)
}
