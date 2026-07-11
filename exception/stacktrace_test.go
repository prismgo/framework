package exception

import (
	"errors"
	"strings"
	"testing"

	"github.com/prismgo/framework/internal/stackx"
)

// TestStackTraceError_WrapError 测试包装 error 并携带堆栈信息
func TestStackTraceError_WrapError(t *testing.T) {
	originalErr := errors.New("original error")
	stack := stackx.Capture(0)

	wrappedErr := WithStackTrace(originalErr, stack)

	// 验证 Error() 返回原始错误消息
	if wrappedErr.Error() != originalErr.Error() {
		t.Errorf("Error() = %v, want %v", wrappedErr.Error(), originalErr.Error())
	}

	// 验证实现了 stackTracer 接口
	type stackTracer interface {
		StackTrace() *stackx.StackTrace
	}
	if tracer, ok := wrappedErr.(stackTracer); !ok {
		t.Error("wrapped error should implement stackTracer interface")
	} else if tracer.StackTrace() != stack {
		t.Errorf("StackTrace() should return the provided stack")
	}
}

// TestStackTraceError_Unwrap 测试 Unwrap 支持 errors.Is/As
func TestStackTraceError_Unwrap(t *testing.T) {
	originalErr := errors.New("original error")
	stack := stackx.Capture(0)

	wrappedErr := WithStackTrace(originalErr, stack)

	// 验证 errors.Is 可以识别原始错误
	if !errors.Is(wrappedErr, originalErr) {
		t.Error("errors.Is should recognize original error")
	}

	// 验证 errors.Unwrap 返回原始错误
	unwrapped := errors.Unwrap(wrappedErr)
	if unwrapped != originalErr {
		t.Errorf("Unwrap() = %v, want %v", unwrapped, originalErr)
	}
}

// TestStackTraceError_NilError 测试包装 nil error
func TestStackTraceError_NilError(t *testing.T) {
	stack := stackx.Capture(0)

	wrappedErr := WithStackTrace(nil, stack)

	if wrappedErr != nil {
		t.Errorf("WithStackTrace(nil) should return nil, got %v", wrappedErr)
	}
}

// TestStackTraceError_NilStack 测试空堆栈
func TestStackTraceError_NilStack(t *testing.T) {
	originalErr := errors.New("original error")

	wrappedErr := WithStackTrace(originalErr, nil)

	// 即使堆栈为空，也应该包装
	if wrappedErr == nil {
		t.Error("WithStackTrace should wrap error even with nil stack")
	}

	type stackTracer interface {
		StackTrace() *stackx.StackTrace
	}
	if tracer, ok := wrappedErr.(stackTracer); !ok {
		t.Error("wrapped error should implement stackTracer interface")
	} else if tracer.StackTrace() != nil {
		t.Errorf("StackTrace() should be nil, got %v", tracer.StackTrace())
	}
}

// TestStackTraceError_AlreadyHasStack 测试已经携带堆栈的 error 不重复包装
func TestStackTraceError_AlreadyHasStack(t *testing.T) {
	originalErr := errors.New("original error")
	stack1 := stackx.Capture(0)
	stack2 := stackx.Capture(0)

	wrappedErr1 := WithStackTrace(originalErr, stack1)
	wrappedErr2 := WithStackTrace(wrappedErr1, stack2)

	// 应该返回同一个对象，不重复包装
	if wrappedErr1 != wrappedErr2 {
		t.Error("WithStackTrace should not re-wrap error that already has stack")
	}

	// 堆栈应该保持为 stack1
	type stackTracer interface {
		StackTrace() *stackx.StackTrace
	}
	if tracer, ok := wrappedErr2.(stackTracer); !ok {
		t.Error("wrapped error should implement stackTracer interface")
	} else if tracer.StackTrace() != stack1 {
		t.Errorf("StackTrace should be stack1")
	}
}

// TestWrap 测试 Wrap 函数捕获堆栈并添加消息
func TestWrap(t *testing.T) {
	originalErr := errors.New("original error")

	wrappedErr := Wrap(originalErr, "context message")

	// 验证 Error() 包含消息
	if !strings.Contains(wrappedErr.Error(), "context message") {
		t.Errorf("Error() should contain context message, got %v", wrappedErr.Error())
	}
	if !strings.Contains(wrappedErr.Error(), "original error") {
		t.Errorf("Error() should contain original error, got %v", wrappedErr.Error())
	}

	// 验证实现了 stackTracer 接口
	type stackTracer interface {
		StackTrace() *stackx.StackTrace
	}
	if tracer, ok := wrappedErr.(stackTracer); !ok {
		t.Error("wrapped error should implement stackTracer interface")
	} else if tracer.StackTrace() == nil {
		t.Error("StackTrace() should not be nil")
	}

	// 验证 errors.Is 可以识别原始错误
	if !errors.Is(wrappedErr, originalErr) {
		t.Error("errors.Is should recognize original error")
	}
}

// TestWrap_NilError 测试 Wrap nil error
func TestWrap_NilError(t *testing.T) {
	wrappedErr := Wrap(nil, "context message")

	if wrappedErr != nil {
		t.Errorf("Wrap(nil) should return nil, got %v", wrappedErr)
	}
}

// TestWrap_AlreadyHasStack 测试 Wrap 已经携带堆栈的 error 只添加消息
func TestWrap_AlreadyHasStack(t *testing.T) {
	originalErr := errors.New("original error")
	stack := stackx.Capture(0)
	wrappedWithStack := WithStackTrace(originalErr, stack)

	wrappedErr := Wrap(wrappedWithStack, "context message")

	// 验证 Error() 包含消息
	if !strings.Contains(wrappedErr.Error(), "context message") {
		t.Errorf("Error() should contain context message, got %v", wrappedErr.Error())
	}

	// 验证仍然携带堆栈
	type stackTracer interface {
		StackTrace() *stackx.StackTrace
	}
	if tracer, ok := wrappedErr.(stackTracer); !ok {
		t.Error("wrapped error should still implement stackTracer interface")
	} else if tracer.StackTrace() != stack {
		t.Error("StackTrace should be preserved")
	}
}

// TestWithStack 测试 WithStack 函数仅附加堆栈
func TestWithStack(t *testing.T) {
	originalErr := errors.New("original error")

	wrappedErr := WithStack(originalErr)

	// 验证 Error() 返回原始错误消息
	if wrappedErr.Error() != originalErr.Error() {
		t.Errorf("Error() = %v, want %v", wrappedErr.Error(), originalErr.Error())
	}

	// 验证实现了 stackTracer 接口
	type stackTracer interface {
		StackTrace() *stackx.StackTrace
	}
	if tracer, ok := wrappedErr.(stackTracer); !ok {
		t.Error("wrapped error should implement stackTracer interface")
	} else if tracer.StackTrace() == nil {
		t.Error("StackTrace() should not be nil")
	}
}

// TestWithStack_NilError 测试 WithStack nil error
func TestWithStack_NilError(t *testing.T) {
	wrappedErr := WithStack(nil)

	if wrappedErr != nil {
		t.Errorf("WithStack(nil) should return nil, got %v", wrappedErr)
	}
}

// TestWithStack_AlreadyHasStack 测试 WithStack 已经携带堆栈的 error 不重复包装
func TestWithStack_AlreadyHasStack(t *testing.T) {
	originalErr := errors.New("original error")
	stack := stackx.Capture(0)
	wrappedWithStack := WithStackTrace(originalErr, stack)

	wrappedErr := WithStack(wrappedWithStack)

	// 应该返回同一个对象，不重复包装
	if wrappedWithStack != wrappedErr {
		t.Error("WithStack should not re-wrap error that already has stack")
	}
}

// TestHasStackTrace 测试 hasStackTrace 函数
func TestHasStackTrace(t *testing.T) {
	// 测试普通 error
	plainErr := errors.New("plain error")
	if hasStackTrace(plainErr) {
		t.Error("hasStackTrace should return false for plain error")
	}

	// 测试携带堆栈的 error
	stack := stackx.Capture(0)
	wrappedErr := WithStackTrace(plainErr, stack)
	if !hasStackTrace(wrappedErr) {
		t.Error("hasStackTrace should return true for error with stack")
	}

	// 测试 Wrap 包装的 error
	wrapErr := Wrap(plainErr, "context")
	if !hasStackTrace(wrapErr) {
		t.Error("hasStackTrace should return true for Wrap error")
	}

	// 测试 WithStack 包装的 error
	withStackErr := WithStack(plainErr)
	if !hasStackTrace(withStackErr) {
		t.Error("hasStackTrace should return true for WithStack error")
	}
}
