package console

import (
	"bytes"
	"context"
	"io"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/mgutz/ansi"
	"github.com/prismgo/framework/container"
	goexception "github.com/prismgo/framework/exception"
	"github.com/prismgo/framework/logger"
	"github.com/spf13/cobra"
)

type capturedException struct {
	err    error
	fields map[string]any
}

func captureConsoleReports(t *testing.T) <-chan capturedException {
	t.Helper()
	registry := container.NewContainer()
	container.SetProvider(func() *container.Container { return registry })
	t.Cleanup(func() { container.SetProvider(nil) })

	reports := make(chan capturedException, 4)
	handler := goexception.New(goexception.WithPanicStack(false))
	handler.Reporters = append(handler.Reporters, func(_ any, err error, fields map[string]any) {
		var copied map[string]any
		if fields != nil {
			copied = make(map[string]any, len(fields))
			for key, value := range fields {
				copied[key] = value
			}
		}
		reports <- capturedException{err: err, fields: copied}
	})
	if err := registry.Instance("exception.handler", handler, container.WithCloseGroup(container.CloseGroupReporting)); err != nil {
		t.Fatalf("bind exception handler: %v", err)
	}
	manager, err := logger.NewManager(logger.Config{
		Default:  "null",
		Channels: map[string]logger.ChannelOptions{"null": {Driver: "null", Level: "debug"}},
	})
	if err != nil {
		t.Fatalf("new logger manager: %v", err)
	}
	if err := registry.Instance("logger.manager", manager, container.WithCloseGroup(container.CloseGroupReporting)); err != nil {
		t.Fatalf("bind logger manager: %v", err)
	}
	return reports
}

func waitConsoleReport(t *testing.T, reports <-chan capturedException) capturedException {
	t.Helper()
	select {
	case report := <-reports:
		return report
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for exception report")
		return capturedException{}
	}
}

func waitTrapCallback(t *testing.T, callbacks <-chan os.Signal) os.Signal {
	t.Helper()
	select {
	case sig := <-callbacks:
		return sig
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for trap callback")
		return nil
	}
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
	captureConsoleReports(t) // 设置 exception handler
	ctx, cancel := context.WithCancel(context.Background())
	commandCtx := NewCommandContext(ctx, nil, Definition{Name: "trap:ctx"}, nil, NewIO(strings.NewReader(""), io.Discard, io.Discard), nil, &cobra.Command{Use: "trap:ctx"})

	// 记录 goroutine 数量
	initialGoroutines := runtime.NumGoroutine()

	// 创建 trap，不保存 release
	_, err := commandCtx.Trap([]os.Signal{syscall.SIGUSR1}, func(os.Signal) {})
	if err != nil {
		t.Fatalf("Trap returned error: %v", err)
	}

	// 等待 goroutine 启动
	time.Sleep(50 * time.Millisecond)
	afterTrapGoroutines := runtime.NumGoroutine()

	if afterTrapGoroutines <= initialGoroutines {
		t.Fatalf("trap goroutine did not start: initial=%d, after=%d", initialGoroutines, afterTrapGoroutines)
	}

	// 取消 context，goroutine 应该通过 ctx.Done() 退出
	cancel()

	// 等待 goroutine 退出
	time.Sleep(100 * time.Millisecond)
	afterCancelGoroutines := runtime.NumGoroutine()

	// goroutine 数量应该恢复到接近初始值
	if afterCancelGoroutines >= afterTrapGoroutines {
		t.Fatalf("trap goroutine did not exit after context cancel: afterTrap=%d, afterCancel=%d", afterTrapGoroutines, afterCancelGoroutines)
	}
}

func TestConsoleFailErrorBranches(t *testing.T) {
	base := io.ErrUnexpectedEOF
	err := Fail("wrapped", base)
	failed, ok := IsManualFailure(err)
	if !ok || failed.Error() != "wrapped unexpected EOF" || failed.Unwrap() == nil {
		t.Fatalf("manual failure = %#v ok=%v", failed, ok)
	}
	if Fail().Error() != "command failed" {
		t.Fatalf("empty Fail = %q", Fail().Error())
	}
	if (&ManuallyFailedError{Err: base}).Error() != base.Error() {
		t.Fatalf("wrapped-only manual failure mismatch")
	}
}

func TestFailWithOnlyNil(t *testing.T) {
	err := Fail(nil)
	if err == nil || err.Error() != "command failed" {
		t.Fatalf("Fail(nil) = %v, want 'command failed'", err)
	}
	failed, ok := IsManualFailure(err)
	if !ok {
		t.Fatal("IsManualFailure returned false for Fail(nil)")
	}
	if failed.Message != "" {
		t.Fatalf("Fail(nil) message = %q, want empty", failed.Message)
	}
	if failed.Unwrap() != nil {
		t.Fatal("Fail(nil) should not wrap any error")
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

	// 记录当前 goroutine 数量
	initialGoroutines := runtime.NumGoroutine()

	// 创建多个 trap
	for i := 0; i < 5; i++ {
		_, err := commandCtx.Trap([]os.Signal{syscall.SIGUSR2}, func(os.Signal) {})
		if err != nil {
			t.Fatalf("Trap %d returned error: %v", i, err)
		}
	}

	time.Sleep(50 * time.Millisecond)
	afterTrapsGoroutines := runtime.NumGoroutine()

	if afterTrapsGoroutines <= initialGoroutines {
		t.Fatalf("trap goroutines did not start: initial=%d, after=%d", initialGoroutines, afterTrapsGoroutines)
	}

	// 释放所有 trap
	release()
	ReleaseTraps(commandCtx)

	// 等待 goroutine 退出
	time.Sleep(100 * time.Millisecond)
	afterReleaseGoroutines := runtime.NumGoroutine()

	// goroutine 数量应该恢复到接近初始值
	if afterReleaseGoroutines >= afterTrapsGoroutines {
		t.Fatalf("trap goroutines did not exit after release: afterTraps=%d, afterRelease=%d", afterTrapsGoroutines, afterReleaseGoroutines)
	}

	// 验证 channel 被关闭：尝试向 channel 发送信号应该不会 panic
	// 但由于 channel 是私有的，我们无法直接访问
	// 所以通过验证多次 release 不会 panic 来间接验证
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("release panicked: %v", r)
		}
	}()
	release() // 重复 release 不应该 panic
}

func TestDefinitionUsageAndNormalizeAndGlobalConsoleOutput(t *testing.T) {
	defaultValue := "default"
	definition, err := NormalizeDefinition(Definition{
		Name:      "report:send",
		Arguments: []Argument{{Name: "tenant", Required: true}, {Name: "user", DefaultValue: &defaultValue}, {Name: "tags", IsArray: true}},
		Options:   []Option{{Name: "queue", ValueMode: OptionValueRequired}, {Name: "force", ValueMode: OptionValueNone}, {Name: "tag", ValueMode: OptionValueRequired, IsArray: true}},
		Aliases:   []string{"report", "report"},
		Examples:  []string{"go run ./ report:send"},
	})
	if err != nil {
		t.Fatalf("Normalize returned error: %v", err)
	}
	if usage := DefinitionUsage(definition); usage != "report:send <tenant> [user] [tags...]" {
		t.Fatalf("Usage = %q, want report:send <tenant> [user] [tags...]", usage)
	}

	buffer := captureConsoleOutput(t, func() {
		Line("plain")
		Info("info")
		Comment("comment")
		Question("question")
		Success("ok")
		Error("bad")
		Warn("warn")
		Alert("alert")
		ExitIf(nil)
	})
	if !strings.Contains(buffer, "plain") || !strings.Contains(buffer, "info") || !strings.Contains(buffer, "comment") || !strings.Contains(buffer, "question") || !strings.Contains(buffer, "ok") || !strings.Contains(buffer, "bad") || !strings.Contains(buffer, "warn") || !strings.Contains(buffer, "alert") {
		t.Fatalf("expected console output to contain messages, got %q", buffer)
	}
}

func TestParseSignatureOptionalArrayAndOptionInt(t *testing.T) {
	definition, err := ParseSignature("sample:run {users?*} {--take=10}")
	if err != nil {
		t.Fatalf("ParseSignature returned error: %v", err)
	}
	if !definition.Arguments[0].IsArray || definition.Arguments[0].Required {
		t.Fatalf("unexpected optional array argument: %+v", definition.Arguments[0])
	}
	cmd := &cobra.Command{Use: "sample:run"}
	if err := BindDefinitionFlags(cmd, definition); err != nil {
		t.Fatalf("BindDefinitionFlags returned error: %v", err)
	}
	input := NewInput(definition, cmd, nil)
	if got, err := input.OptionInt("take"); err != nil || got != 10 {
		t.Fatalf("OptionInt(take) = %d, err=%v, want 10, nil", got, err)
	}
}

func TestGlobalConsoleOutputStylesMatchIO(t *testing.T) {
	t.Setenv("FORCE_COLOR", "1")
	t.Setenv("NO_COLOR", "")
	buffer := captureConsoleOutput(t, func() {
		Line("plain")
		Line("unknown", "missing")
		Line("warning", "warning")
		Info("info")
		Comment("comment")
		Question("question")
		Success("success")
		Warn("warn")
		Error("error")
	})

	want := "plain\n" +
		"unknown\n" +
		ansi.Color("warning", "yellow") + "\n" +
		ansi.Color("info", "green") + "\n" +
		ansi.Color("comment", "yellow") + "\n" +
		ansi.Color("question", "black:cyan") + "\n" +
		ansi.Color("success", "white:green") + "\n" +
		ansi.Color("warn", "yellow") + "\n" +
		ansi.Color("error", "white:red") + "\n"
	if buffer != want {
		t.Fatalf("console output = %q, want %q", buffer, want)
	}
}

func TestExitWritesErrorStyleBeforeExiting(t *testing.T) {
	cmd := exec.Command(os.Args[0], "-test.run=TestConsoleExitHelperProcess", "--")
	cmd.Env = append(os.Environ(), "PRISMGO_TEST_CONSOLE_EXIT=1", "FORCE_COLOR=1", "NO_COLOR=")

	output, err := cmd.Output()
	if err == nil {
		t.Fatal("expected helper process to exit non-zero")
	}
	if exitErr, ok := err.(*exec.ExitError); !ok || exitErr.ExitCode() != 1 {
		t.Fatalf("Exit returned %T %v, want exit code 1", err, err)
	}
	want := ansi.Color("fatal", "white:red") + "\n"
	if string(output) != want {
		t.Fatalf("Exit output = %q, want %q", string(output), want)
	}
}

func TestConsoleExitHelperProcess(t *testing.T) {
	if os.Getenv("PRISMGO_TEST_CONSOLE_EXIT") != "1" {
		return
	}
	Exit("fatal")
}

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

func captureConsoleOutput(t *testing.T, run func()) string {
	t.Helper()
	originalStdout := os.Stdout
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe failed: %v", err)
	}
	os.Stdout = writer
	defer func() {
		os.Stdout = originalStdout
	}()

	outputCh := make(chan string, 1)
	go func() {
		buffer := &bytes.Buffer{}
		_, _ = io.Copy(buffer, reader)
		outputCh <- buffer.String()
	}()

	run()
	_ = writer.Close()
	return <-outputCh
}
