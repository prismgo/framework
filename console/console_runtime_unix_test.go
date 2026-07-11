//go:build !windows

package console

import (
	"context"
	"io"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/spf13/cobra"
)

type stubCaller struct {
	called       string
	silentCalled string
	inputCount   int
}

func (c *stubCaller) Call(_ context.Context, signature string, input ...CallInput) error {
	c.called = signature
	c.inputCount = len(input)
	return nil
}

func (c *stubCaller) CallSilently(_ context.Context, signature string, input ...CallInput) error {
	c.silentCalled = signature
	c.inputCount = len(input)
	return nil
}

func TestCommandContextRoundTripAndCaller(t *testing.T) {
	caller := &stubCaller{}
	commandCtx := NewCommandContext(context.Background(), nil, Definition{Name: "sample:run"}, nil, NewIO(strings.NewReader(""), io.Discard, io.Discard), caller, &cobra.Command{Use: "sample:run"})
	ctx := WithContext(context.Background(), commandCtx)

	fromContext, ok := FromContext(ctx)
	if !ok || fromContext.CommandName() != "sample:run" {
		t.Fatalf("FromContext returned %+v, want sample:run", fromContext)
	}
	cobraCmd := &cobra.Command{Use: "sample:run"}
	cobraCmd.SetContext(ctx)
	fromCommand, ok := FromCommand(cobraCmd)
	if !ok || fromCommand.CommandName() != "sample:run" {
		t.Fatalf("FromCommand returned %+v, want sample:run", fromCommand)
	}
	must, err := MustFromCommand(cobraCmd)
	if err != nil || must.CommandName() != "sample:run" {
		t.Fatalf("MustFromCommand returned %+v, %v", must, err)
	}
	if err := commandCtx.Call("child:run"); err != nil {
		t.Fatalf("Call returned error: %v", err)
	}
	if err := commandCtx.CallSilently("child:silent"); err != nil {
		t.Fatalf("CallSilently returned error: %v", err)
	}
	if caller.called != "child:run" || caller.silentCalled != "child:silent" {
		t.Fatalf("caller = %+v, want child:run / child:silent", caller)
	}
	if err := commandCtx.Call("child:with", CallInput{Arguments: map[string]any{"name": "a"}}); err != nil {
		t.Fatalf("Call with input returned error: %v", err)
	}
	if err := commandCtx.CallSilently("child:silent-with", CallInput{}); err != nil {
		t.Fatalf("CallSilently with input returned error: %v", err)
	}
	if caller.called != "child:with" || caller.silentCalled != "child:silent-with" || caller.inputCount != 1 {
		t.Fatalf("structured caller = %+v", caller)
	}
	if commandCtx.Definition().Name != "sample:run" || commandCtx.IO() == nil {
		t.Fatalf("unexpected command context definition/io")
	}
	if err := commandCtx.Fail("manual"); err == nil {
		t.Fatal("Fail returned nil")
	}
	release, err := commandCtx.Trap([]os.Signal{syscall.SIGUSR1}, func(os.Signal) {})
	if err != nil {
		t.Fatalf("Trap returned error: %v", err)
	}
	release()
	ReleaseTraps(commandCtx)
}

func TestCommandContextTrapRecoversCallbackPanicAndReleaseSafe(t *testing.T) {
	reports := captureConsoleReports(t)
	commandCtx := NewCommandContext(context.Background(), nil, Definition{Name: "trap:panic"}, nil, NewIO(strings.NewReader(""), io.Discard, io.Discard), nil, &cobra.Command{Use: "trap:panic"})
	var calls atomic.Int32

	release, err := commandCtx.Trap([]os.Signal{syscall.SIGUSR2}, func(os.Signal) {
		calls.Add(1)
		panic("trap callback failed")
	})
	if err != nil {
		t.Fatalf("Trap returned error: %v", err)
	}
	defer ReleaseTraps(commandCtx)

	if err := syscall.Kill(os.Getpid(), syscall.SIGUSR2); err != nil {
		t.Fatalf("send signal: %v", err)
	}
	report := waitConsoleReport(t, reports)
	if report.err == nil || report.err.Error() != "trap callback failed" {
		t.Fatalf("reported err = %v, want trap callback failed", report.err)
	}
	if report.fields["component"] != "console" {
		t.Fatalf("component = %v, want console", report.fields["component"])
	}
	if report.fields["routine"] != "command.trap" {
		t.Fatalf("routine = %v, want command.trap", report.fields["routine"])
	}
	if report.fields["command"] != "trap:panic" {
		t.Fatalf("command = %v, want trap:panic", report.fields["command"])
	}

	if err := syscall.Kill(os.Getpid(), syscall.SIGUSR2); err != nil {
		t.Fatalf("send second signal: %v", err)
	}
	time.Sleep(50 * time.Millisecond)
	if got := calls.Load(); got != 1 {
		t.Fatalf("callback calls after panic = %d, want 1", got)
	}
	release()
	release()
	ReleaseTraps(commandCtx)
}

func TestCommandContextTrapReleaseIsIdempotent(t *testing.T) {
	commandCtx := NewCommandContext(context.Background(), nil, Definition{Name: "trap:release"}, nil, NewIO(strings.NewReader(""), io.Discard, io.Discard), nil, &cobra.Command{Use: "trap:release"})
	callbacks := make(chan os.Signal, 1)

	release, err := commandCtx.Trap([]os.Signal{syscall.SIGUSR1}, func(sig os.Signal) {
		callbacks <- sig
	})
	if err != nil {
		t.Fatalf("Trap returned error: %v", err)
	}
	if err := syscall.Kill(os.Getpid(), syscall.SIGUSR1); err != nil {
		t.Fatalf("send signal: %v", err)
	}
	if got := waitTrapCallback(t, callbacks); got != syscall.SIGUSR1 {
		t.Fatalf("callback signal = %v, want %v", got, syscall.SIGUSR1)
	}

	release()
	release()
	ReleaseTraps(commandCtx)
	ReleaseTraps(commandCtx)
}

func TestCommandContextTrapExitsOnContextCancel(t *testing.T) {
	captureConsoleReports(t)
	ctx, cancel := context.WithCancel(context.Background())
	commandCtx := NewCommandContext(ctx, nil, Definition{Name: "trap:ctx"}, nil, NewIO(strings.NewReader(""), io.Discard, io.Discard), nil, &cobra.Command{Use: "trap:ctx"})

	initialGoroutines := runtime.NumGoroutine()

	_, err := commandCtx.Trap([]os.Signal{syscall.SIGUSR1}, func(os.Signal) {})
	if err != nil {
		t.Fatalf("Trap returned error: %v", err)
	}

	immediateGoroutines := runtime.NumGoroutine()

	checkForTrapGoroutine := func() bool {
		buf := make([]byte, 32768)
		n := runtime.Stack(buf, true)
		stack := string(buf[:n])
		return strings.Contains(stack, "Trap.func") || strings.Contains(stack, "routine.(*builder).Go.gowrap")
	}

	var afterTrapGoroutines int
	var goroutineStarted bool
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		afterTrapGoroutines = runtime.NumGoroutine()

		if afterTrapGoroutines > initialGoroutines {
			goroutineStarted = true
			break
		}

		if checkForTrapGoroutine() {
			goroutineStarted = true
			break
		}

		time.Sleep(1 * time.Millisecond)
	}

	if !goroutineStarted {
		buf := make([]byte, 32768)
		n := runtime.Stack(buf, true)
		t.Logf("goroutine dump:\n%s", string(buf[:n]))
		t.Fatalf("trap goroutine did not start: initial=%d, immediate=%d, after=%d", initialGoroutines, immediateGoroutines, afterTrapGoroutines)
	}

	cancel()

	time.Sleep(100 * time.Millisecond)
	afterCancelGoroutines := runtime.NumGoroutine()

	if afterCancelGoroutines >= afterTrapGoroutines {
		t.Fatalf("trap goroutine did not exit after context cancel: afterTrap=%d, afterCancel=%d", afterTrapGoroutines, afterCancelGoroutines)
	}
}

func TestTrapConcurrentSafety(t *testing.T) {
	captureConsoleReports(t)
	commandCtx := NewCommandContext(context.Background(), nil, Definition{Name: "trap:concurrent"}, nil, NewIO(strings.NewReader(""), io.Discard, io.Discard), nil, &cobra.Command{Use: "trap:concurrent"})

	var wg sync.WaitGroup
	const goroutineCount = 10

	for i := 0; i < goroutineCount; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			release, err := commandCtx.Trap([]os.Signal{syscall.SIGUSR1}, func(os.Signal) {})
			if err != nil {
				t.Errorf("goroutine %d: Trap returned error: %v", id, err)
				return
			}
			time.Sleep(10 * time.Millisecond)
			release()
		}(i)
	}

	wg.Wait()
	ReleaseTraps(commandCtx)
}

func TestCommandContextTrapClosesSignalChannelOnRelease(t *testing.T) {
	captureConsoleReports(t)
	commandCtx := NewCommandContext(context.Background(), nil, Definition{Name: "trap:channel"}, nil, NewIO(strings.NewReader(""), io.Discard, io.Discard), nil, &cobra.Command{Use: "trap:channel"})

	release, err := commandCtx.Trap([]os.Signal{syscall.SIGUSR1}, func(os.Signal) {})
	if err != nil {
		t.Fatalf("Trap returned error: %v", err)
	}

	initialGoroutines := runtime.NumGoroutine()

	for i := 0; i < 5; i++ {
		_, err := commandCtx.Trap([]os.Signal{syscall.SIGUSR2}, func(os.Signal) {})
		if err != nil {
			t.Fatalf("Trap %d returned error: %v", i, err)
		}
	}

	var afterTrapsGoroutines int
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		afterTrapsGoroutines = runtime.NumGoroutine()
		if afterTrapsGoroutines > initialGoroutines {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if afterTrapsGoroutines <= initialGoroutines {
		t.Fatalf("trap goroutines did not start: initial=%d, after=%d", initialGoroutines, afterTrapsGoroutines)
	}

	release()
	ReleaseTraps(commandCtx)

	time.Sleep(100 * time.Millisecond)
	afterReleaseGoroutines := runtime.NumGoroutine()

	if afterReleaseGoroutines >= afterTrapsGoroutines {
		t.Fatalf("trap goroutines did not exit after release: afterTraps=%d, afterRelease=%d", afterTrapsGoroutines, afterReleaseGoroutines)
	}

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("release panicked: %v", r)
		}
	}()
	release()
}
