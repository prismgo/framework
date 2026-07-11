package stackx

import (
	"strings"
	"testing"
)

// TestCapture 测试结构化堆栈采集
func TestCapture(t *testing.T) {
	st := Capture(0)
	if st == nil {
		t.Fatal("Capture returned nil")
	}
	
	frames := st.Frames()
	if len(frames) == 0 {
		t.Fatal("Frames returned empty array")
	}
	
	// 验证第一帧包含当前测试函数
	found := false
	for _, frame := range frames {
		if strings.Contains(frame.Function, "TestCapture") {
			found = true
			break
		}
	}
	if !found {
		t.Error("Expected to find TestCapture in frames")
	}
}

// TestCaptureWithSkip 测试 skip 参数
func TestCaptureWithSkip(t *testing.T) {
	st := Capture(1)
	frames := st.Frames()
	
	// skip=1 应该跳过调用 Capture 的函数
	// 第一帧应该是 runtime.goexit 或其他上层函数
	if len(frames) == 0 {
		t.Fatal("Frames returned empty array")
	}
}

// TestStackTraceFormat 测试格式化输出
func TestStackTraceFormat(t *testing.T) {
	st := Capture(0)
	formatted := st.Format()
	
	if formatted == "" {
		t.Fatal("Format returned empty string")
	}
	
	// 验证格式包含函数名和文件路径
	if !strings.Contains(formatted, "TestStackTraceFormat") {
		t.Error("Expected formatted output to contain function name")
	}
	if !strings.Contains(formatted, ".go:") {
		t.Error("Expected formatted output to contain file path")
	}
}

// TestStackTraceFilter 测试过滤功能
func TestStackTraceFilter(t *testing.T) {
	st := Capture(0)
	
	// 过滤掉所有 runtime 帧
	filtered := st.Filter(func(frame StackFrame) bool {
		return !strings.HasPrefix(frame.Function, "runtime.")
	})
	
	frames := filtered.Frames()
	for _, frame := range frames {
		if strings.HasPrefix(frame.Function, "runtime.") {
			t.Errorf("Filter failed: found runtime frame %s", frame.Function)
		}
	}
}

// TestDefaultFilter 测试默认过滤规则
func TestDefaultFilter(t *testing.T) {
	filter := DefaultFilter()
	
	// 测试 runtime 帧被过滤
	runtimeFrame := StackFrame{Function: "runtime.goexit", File: "/usr/local/go/src/runtime/asm_amd64.s"}
	if filter(runtimeFrame) {
		t.Error("DefaultFilter should filter runtime frames")
	}
	
	// 测试 stackx 帧被过滤
	stackxFrame := StackFrame{Function: "stackx.Capture", File: "/www/code/framework/internal/stackx/stackx.go"}
	if filter(stackxFrame) {
		t.Error("DefaultFilter should filter stackx frames")
	}
	
	// 测试 exception.Report 帧被过滤
	reportFrame := StackFrame{Function: "exception.(*Handler).Report", File: "/www/code/framework/exception/handler.go"}
	if filter(reportFrame) {
		t.Error("DefaultFilter should filter exception.Report frames")
	}
	
	// 测试业务帧被保留
	bizFrame := StackFrame{Function: "main.main", File: "/app/main.go"}
	if !filter(bizFrame) {
		t.Error("DefaultFilter should preserve business frames")
	}
	
	// 测试框架业务帧被保留
	frameworkBizFrame := StackFrame{Function: "kernel.Kernel.Run", File: "/www/code/framework/kernel/kernel.go"}
	if !filter(frameworkBizFrame) {
		t.Error("DefaultFilter should preserve framework business frames")
	}
}

// TestCaptureBytes 测试向后兼容的 CaptureBytes
func TestCaptureBytes(t *testing.T) {
	stack := CaptureBytes()
	if len(stack) == 0 {
		t.Fatal("CaptureBytes returned empty")
	}
	
	// 验证包含当前测试函数
	if !strings.Contains(string(stack), "TestCaptureBytes") {
		t.Error("Expected stack to contain TestCaptureBytes")
	}
}

// TestTruncate 测试堆栈截断
func TestTruncate(t *testing.T) {
	// 测试小于限制的堆栈
	smallStack := []byte("small stack")
	result := truncate(smallStack)
	if string(result) != string(smallStack) {
		t.Errorf("truncate modified small stack unexpectedly")
	}
	
	// 测试超过限制的堆栈
	largeStack := make([]byte, maxStackSize+100)
	for i := range largeStack {
		largeStack[i] = 'a'
	}
	result = truncate(largeStack)
	if len(result) > maxStackSize+len(truncationSuffix) {
		t.Errorf("truncate failed to limit stack size: got %d, max %d", len(result), maxStackSize+len(truncationSuffix))
	}
	if !strings.Contains(string(result), truncationSuffix) {
		t.Error("truncate should add truncation suffix")
	}
}

// TestStackFrameString 测试 StackFrame 字符串表示
func TestStackFrameString(t *testing.T) {
	frame := StackFrame{
		Function: "main.main",
		File:     "/app/main.go",
		Line:     10,
	}
	
	str := frame.String()
	expected := "main.main\n\t/app/main.go:10"
	if str != expected {
		t.Errorf("StackFrame.String() = %q, want %q", str, expected)
	}
}

// TestNilStackTrace 测试 nil StackTrace
func TestNilStackTrace(t *testing.T) {
	var st *StackTrace
	
	if frames := st.Frames(); frames != nil {
		t.Error("nil StackTrace.Frames() should return nil")
	}
	
	if formatted := st.Format(); formatted != "" {
		t.Error("nil StackTrace.Format() should return empty string")
	}
	
	if filtered := st.Filter(nil); filtered != nil {
		t.Error("nil StackTrace.Filter() should return nil")
	}
}

// TestEmptyStackTrace 测试空 StackTrace
func TestEmptyStackTrace(t *testing.T) {
	st := &StackTrace{}
	
	if frames := st.Frames(); len(frames) != 0 {
		t.Errorf("empty StackTrace.Frames() should return empty array, got %d frames", len(frames))
	}
	
	if formatted := st.Format(); formatted != "" {
		t.Error("empty StackTrace.Format() should return empty string")
	}
}
