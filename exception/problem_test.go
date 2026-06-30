package exception

import (
	"strings"
	"testing"
)

// TestFirstTraceLocation_SkipsStackxFrame 验证 firstTraceLocation 跳过 stackx.Capture 帧。
func TestFirstTraceLocation_SkipsStackxFrame(t *testing.T) {
	// 模拟包含 stackx.Capture 帧的堆栈（实际格式）
	trace := []string{
		"goroutine 1 [running]:",
		"github.com/prismgo/framework/internal/stackx.Capture()",
		"/www/code/framework/internal/stackx/stackx.go:20 +0x45",
		"github.com/prismgo/framework/exception.(*Handler).Report()",
		"/www/code/framework/exception/handler.go:152 +0x123",
		"main.main()",
		"/www/code/app/main.go:10 +0x25",
	}

	file, line := firstTraceLocation(trace)

	// 应该跳过 stackx.go，返回 handler.go
	if !strings.Contains(file, "handler.go") {
		t.Errorf("expected handler.go, got %s", file)
	}
	if line != 152 {
		t.Errorf("expected line 152, got %d", line)
	}
}

// TestFirstTraceLocation_SkipsDebugStack 验证 firstTraceLocation 跳过 runtime/debug.Stack 帧。
func TestFirstTraceLocation_SkipsDebugStack(t *testing.T) {
	trace := []string{
		"goroutine 1 [running]:",
		"runtime/debug.Stack()",
		"/usr/local/go/src/runtime/debug/stack.go:24 +0x5e",
		"github.com/prismgo/framework/exception.(*Handler).Report()",
		"/www/code/framework/exception/handler.go:152 +0x123",
	}

	file, line := firstTraceLocation(trace)

	if !strings.Contains(file, "handler.go") {
		t.Errorf("expected handler.go, got %s", file)
	}
	if line != 152 {
		t.Errorf("expected line 152, got %d", line)
	}
}
