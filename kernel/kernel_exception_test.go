// Package kernel CLI 层异常处理集成测试。
//
// 测试范围：
//   - 命令返回 error 时通过 prismgo/exception 上报
//   - 命令 panic 时恢复并上报
//   - 成功命令不触发上报
//   - acquireIsolationLock 直接失败不上报（在 Run() 之前 return，代码审查保证）
package kernel

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/prismgo/framework/console"
	goexception "github.com/prismgo/framework/exception"
)

// TestExecuteCommandErrorIsReported 验证命令返回 error 时通过 exception.Report 上报。
func TestExecuteCommandErrorIsReported(t *testing.T) {
	var reported struct {
		err    error
		fields map[string]any
	}
	handler := goexception.New(
		goexception.WithPanicStack(false),
		goexception.WithReporter(func(_ any, err error, fields map[string]any) {
			reported.err = err
			reported.fields = fields
		}),
	)
	useExceptionHandlerForKernelTest(t, handler)

	k := New("test")
	runErr := errors.New("command failed")
	cmd := &exceptionErrorCommand{err: runErr}
	k.Register(cmd)

	err := k.Call(context.Background(), "exception:error")
	if !errors.Is(err, runErr) {
		t.Fatalf("expected error %v, got %v", runErr, err)
	}

	if reported.err == nil {
		t.Fatal("expected reporter to be called, but it wasn't")
	}
	if reported.fields["command"] != "exception:error" {
		t.Errorf("command = %q, want exception:error", reported.fields["command"])
	}
	if reported.fields["status"] != 500 {
		t.Errorf("status = %v, want 500", reported.fields["status"])
	}
	if reported.fields["component"] != "cli" {
		t.Errorf("component = %q, want cli", reported.fields["component"])
	}
	if _, ok := reported.fields["duration_ms"]; !ok {
		t.Error("fields missing duration_ms")
	}
	if input, _ := reported.fields["input"].(string); !strings.Contains(input, "exception:error") {
		t.Errorf("input = %q, should contain command name", input)
	}
}

// TestExecuteCommandPanicIsRecoveredAndReported 验证命令 panic 时恢复并上报。
func TestExecuteCommandPanicIsRecoveredAndReported(t *testing.T) {
	var reported struct {
		err    error
		fields map[string]any
	}
	handler := goexception.New(
		goexception.WithPanicStack(false),
		goexception.WithReporter(func(_ any, err error, fields map[string]any) {
			reported.err = err
			reported.fields = fields
		}),
	)
	useExceptionHandlerForKernelTest(t, handler)

	k := New("test")
	k.Register(&exceptionPanicCommand{})

	err := k.Call(context.Background(), "exception:panic")
	if err == nil {
		t.Fatal("expected error from panic recovery, got nil")
	}
	if !strings.Contains(err.Error(), "panic recovered") {
		t.Fatalf("expected panic recovered error, got %v", err)
	}

	if reported.err == nil {
		t.Fatal("expected reporter to be called for panic, but it wasn't")
	}
	if reported.fields["command"] != "exception:panic" {
		t.Errorf("command = %q, want exception:panic", reported.fields["command"])
	}
	if reported.fields["status"] != 500 {
		t.Errorf("status = %v, want 500", reported.fields["status"])
	}
	if reported.fields["component"] != "cli" {
		t.Errorf("component = %q, want cli", reported.fields["component"])
	}
	if msg, _ := reported.fields["message"].(string); msg != "command panic" {
		t.Errorf("message = %q, want command panic", msg)
	}
}

// TestSuccessfulCommandIsNotReported 验证成功命令不触发 Reporter。
func TestSuccessfulCommandIsNotReported(t *testing.T) {
	reported := false
	handler := goexception.New(
		goexception.WithPanicStack(false),
		goexception.WithReporter(func(_ any, _ error, _ map[string]any) {
			reported = true
		}),
	)
	useExceptionHandlerForKernelTest(t, handler)

	k := New("test")
	k.Register(&exceptionSuccessCommand{})

	err := k.Call(context.Background(), "exception:success")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if reported {
		t.Error("reporter should NOT be called for successful command")
	}
}

// ---- test commands ----

type exceptionErrorCommand struct{ err error }

func (c *exceptionErrorCommand) Definition() *console.Definition {
	return console.MustDefinition("exception:error", "exception:error")
}
func (c *exceptionErrorCommand) Handle(_ console.CommandContext) error { return c.err }

type exceptionPanicCommand struct{}

func (c *exceptionPanicCommand) Definition() *console.Definition {
	return console.MustDefinition("exception:panic", "exception:panic")
}
func (c *exceptionPanicCommand) Handle(_ console.CommandContext) error {
	panic("test panic")
}

type exceptionSuccessCommand struct{}

func (c *exceptionSuccessCommand) Definition() *console.Definition {
	return console.MustDefinition("exception:success", "exception:success")
}
func (c *exceptionSuccessCommand) Handle(_ console.CommandContext) error { return nil }

func useExceptionHandlerForKernelTest(t *testing.T, handler *goexception.Handler) {
	t.Helper()
	registry := useKernelTestContainer(t)
	// 测试装配说明：exception.Report 通过 facade 从当前 Application 容器解析处理器；
	// 这里直接写入容器实例，覆盖旧 Use fallback 测试路径。
	if err := registry.Instance("exception.handler", handler); err != nil {
		t.Fatalf("bind exception handler: %v", err)
	}
}
