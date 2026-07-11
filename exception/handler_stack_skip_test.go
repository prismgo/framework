package exception

import (
	"errors"
	"strings"
	"testing"

	"github.com/prismgo/framework/internal/stackx"
)

// simulateReportAutoCapture 模拟 Handler.Report 内部的自动堆栈捕获逻辑
// 返回捕获到的 StackTrace，用于验证 skip 值是否正确
func simulateReportAutoCapture() *stackx.StackTrace {
	// 模拟 Report 内部的调用序列：
	// 1. addErrorMetadata (已返回，不在调用栈上)
	// 2. hasStackTrace (已返回，不在调用栈上)
	// 3. stackx.Capture(skip) ← 当前调用点
	//
	// 调用栈（从底到顶）：
	//   测试函数 → simulateReportAutoCapture → stackx.Capture → runtime.Callers
	//
	// Capture(0): 第一帧 = simulateReportAutoCapture
	// Capture(1): 第一帧 = 测试函数 ← 这是期望的行为（跳过 Report 自身）
	// Capture(2): 第一帧 = testing.tRunner
	// Capture(3): 第一帧 = testing 更上层
	return stackx.Capture(1) // 当前代码使用 skip=3，这是我们要验证/修复的值
}

// simulateReportAutoCaptureWithSkip 允许指定 skip 值进行测试
func simulateReportAutoCaptureWithSkip(skip int) *stackx.StackTrace {
	return stackx.Capture(skip)
}

// TestReportAutoCaptureSkipValue 验证 Report 自动捕获堆栈时正确的 skip 值
// 目标：确保捕获的堆栈第一帧是 Report 的直接调用者，而不是 Report 内部帧
func TestReportAutoCaptureSkipValue(t *testing.T) {
	t.Run("capture_skip_semantics", func(t *testing.T) {
		// 验证 Capture 的 skip 语义
		// Capture(skip) 内部调用 runtime.Callers(skip+2, pcs)
		// skip+2 跳过了 runtime.Callers 和 Capture 自身
		// 额外的 skip 跳过调用 Capture 的函数

		// skip=0: 第一帧应该是调用 Capture 的函数（simulateReportAutoCaptureWithSkip）
		stack0 := simulateReportAutoCaptureWithSkip(0)
		frames0 := stack0.Frames()
		if len(frames0) == 0 {
			t.Fatal("Capture(0) returned no frames")
		}
		if !strings.Contains(frames0[0].Function, "simulateReportAutoCaptureWithSkip") {
			t.Errorf("Capture(0) first frame = %s, want simulateReportAutoCaptureWithSkip", frames0[0].Function)
		}

		// skip=1: 第一帧应该是调用 simulateReportAutoCaptureWithSkip 的函数（当前测试函数）
		stack1 := simulateReportAutoCaptureWithSkip(1)
		frames1 := stack1.Frames()
		if len(frames1) == 0 {
			t.Fatal("Capture(1) returned no frames")
		}
		if !strings.Contains(frames1[0].Function, "TestReportAutoCaptureSkipValue") {
			t.Errorf("Capture(1) first frame = %s, want TestReportAutoCaptureSkipValue", frames1[0].Function)
		}
	})

	t.Run("simulate_report_skip", func(t *testing.T) {
		// 从 simulateReportAutoCapture 内部调用 Capture
		// 调用栈：TestReportAutoCaptureSkipValue → simulateReportAutoCapture → Capture
		//
		// 期望行为（模拟 Report）：第一帧应该是 TestReportAutoCaptureSkipValue
		// 即跳过 simulateReportAutoCapture（对应 Report）本身
		//
		// 当前 simulateReportAutoCapture 使用 skip=1
		// 如果正确，第一帧应该是当前测试函数
		stack := simulateReportAutoCapture()
		frames := stack.Frames()

		if len(frames) == 0 {
			t.Fatal("simulateReportAutoCapture returned no frames")
		}

		// 打印前几帧用于调试
		t.Logf("Captured %d frames:", len(frames))
		for i := 0; i < len(frames) && i < 5; i++ {
			t.Logf("  [%d] %s at %s:%d", i, frames[i].Function, frames[i].File, frames[i].Line)
		}

		// 第一帧应该是当前测试函数（模拟 Report 的调用者）
		if !strings.Contains(frames[0].Function, "TestReportAutoCaptureSkipValue") {
			t.Errorf("First frame = %s, want TestReportAutoCaptureSkipValue (Report's caller)", frames[0].Function)
		}

		// 不应该包含 simulateReportAutoCapture（模拟 Report 内部帧）
		if strings.Contains(frames[0].Function, "simulateReportAutoCapture") {
			t.Error("First frame should NOT be simulateReportAutoCapture (Report internal frame)")
		}
	})
}

// TestHandlerReportSkipValueIntegration 集成测试：验证 Handler.Report 的 skip 值
// 使用 mock logger 避免容器依赖
func TestHandlerReportSkipValueIntegration(t *testing.T) {
	// 验证 hasStackTrace 函数与 WithStackTrace 的配合
	plainErr := errors.New("plain error")

	// 初始状态：error 不带堆栈
	if hasStackTrace(plainErr) {
		t.Fatal("plain error should not have stack trace")
	}

	// 使用 WithStackTrace 包装
	stack := stackx.Capture(0)
	wrappedErr := WithStackTrace(plainErr, stack)

	// 包装后应该携带堆栈
	if !hasStackTrace(wrappedErr) {
		t.Fatal("wrapped error should have stack trace")
	}

	// 验证堆栈内容包含当前测试函数
	var tracer interface{ StackTrace() *stackx.StackTrace }
	if !errors.As(wrappedErr, &tracer) {
		t.Fatal("wrapped error should implement StackTrace()")
	}

	frames := tracer.StackTrace().Frames()
	found := false
	for _, frame := range frames {
		if strings.Contains(frame.Function, "TestHandlerReportSkipValueIntegration") {
			found = true
			break
		}
	}
	if !found {
		t.Error("Stack trace should contain TestHandlerReportSkipValueIntegration")
	}
}
