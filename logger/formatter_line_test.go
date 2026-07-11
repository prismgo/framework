package logger

import (
	"errors"
	"strings"
	"testing"

	"github.com/prismgo/framework/internal/stackx"
)

// mockErrorWithStack 模拟实现了StackTrace()方法的error
type mockErrorWithStack struct {
	err   error
	stack *stackx.StackTrace
}

func (e *mockErrorWithStack) Error() string {
	return e.err.Error()
}

func (e *mockErrorWithStack) StackTrace() *stackx.StackTrace {
	return e.stack
}

func TestExtractStackTrace_PreferErrorStackTrace(t *testing.T) {
	// 测试：当error实现了StackTrace()方法时，优先使用error自带的结构化堆栈
	f := &LineFormatter{
		includeStacktraces: true,
	}

	// 捕获当前堆栈
	stack := stackx.Capture(0)
	err := &mockErrorWithStack{
		err:   errors.New("test error"),
		stack: stack,
	}

	result := f.extractStackTrace(err)

	// 验证包含当前测试函数名
	if !strings.Contains(result, "TestExtractStackTrace_PreferErrorStackTrace") {
		t.Errorf("extractStackTrace() should contain current function name, got %v", result)
	}
}

func TestExtractStackTrace_FallbackToCapture(t *testing.T) {
	// 测试：当error没有实现StackTrace()方法时，回退到stackx.CaptureBytes()
	f := &LineFormatter{
		includeStacktraces: true,
	}

	err := errors.New("test error")
	result := f.extractStackTrace(err)

	// 回退到stackx.CaptureBytes()时，应该包含当前函数的堆栈
	if !strings.Contains(result, "TestExtractStackTrace_FallbackToCapture") {
		t.Errorf("extractStackTrace() should contain current function name, got %v", result)
	}
}

func TestExtractStackTrace_NilStackTrace(t *testing.T) {
	// 测试：当error的StackTrace()返回nil时，回退到stackx.CaptureBytes()
	f := &LineFormatter{
		includeStacktraces: true,
	}

	err := &mockErrorWithStack{
		err:   errors.New("test error"),
		stack: nil,
	}

	result := f.extractStackTrace(err)

	// 应该回退到stackx.CaptureBytes()
	if !strings.Contains(result, "TestExtractStackTrace_NilStackTrace") {
		t.Errorf("extractStackTrace() should fallback to capture when stack is nil, got %v", result)
	}
}

func TestExtractStackTrace_FilterApplied(t *testing.T) {
	// 测试：验证结构化堆栈应用了DefaultFilter过滤规则
	f := &LineFormatter{
		includeStacktraces: true,
	}

	stack := stackx.Capture(0)
	err := &mockErrorWithStack{
		err:   errors.New("test error"),
		stack: stack,
	}

	result := f.extractStackTrace(err)

	// 验证不包含runtime帧
	if strings.Contains(result, "runtime.") {
		t.Errorf("extractStackTrace() should filter runtime frames, got %v", result)
	}

	// 验证不包含stackx内部帧
	if strings.Contains(result, "internal/stackx") {
		t.Errorf("extractStackTrace() should filter stackx frames, got %v", result)
	}
}

func TestFormatContext_ExcludeStackField(t *testing.T) {
	// 测试：formatContext应该排除stack字段，避免JSON中重复输出堆栈
	f := &LineFormatter{
		ignoreEmptyContextAndExtra: false,
	}

	data := map[string]any{
		"command": "migrate:install",
		"status":  500,
		"stack":   "goroutine 1 [running]:\nmain.main()",
	}

	result, err := f.formatContext(data)
	if err != nil {
		t.Fatalf("formatContext() error = %v", err)
	}

	// 应该包含command和status
	if !strings.Contains(result, "command") {
		t.Errorf("formatContext() should contain 'command', got %v", result)
	}
	if !strings.Contains(result, "status") {
		t.Errorf("formatContext() should contain 'status', got %v", result)
	}

	// 不应该包含stack字段
	if strings.Contains(result, "stack") {
		t.Errorf("formatContext() should exclude 'stack' field, got %v", result)
	}
}
