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

// TestStackTraceFormatDefaultLimit 测试 Format() 默认 4096 字节限制
// 验证帧对齐截断：内容在完整的帧边界处截断，输出精确匹配计算值
func TestStackTraceFormatDefaultLimit(t *testing.T) {
	const defaultMaxBytes = 4096

	// 构造大量帧以超过 4096 字节限制
	frames := make([]StackFrame, 0, 100)
	for i := range 100 {
		frames = append(frames, StackFrame{
			Function: "main.someVeryLongFunctionNameForTestingPurposes",
			File:     "/very/long/path/to/some/file/for/testing/purposes.go",
			Line:     i + 1,
		})
	}
	st := NewStackTraceFromFrames(frames)

	formatted := st.Format()

	// 验证截断标记存在
	truncationMarker := "\n... (truncated)"
	if !strings.Contains(formatted, "... (truncated)") {
		t.Error("Format() should truncate when content exceeds 4096 bytes")
	}

	// 验证总长度不超过 4096 + 截断标记长度
	maxExpected := defaultMaxBytes + len(truncationMarker)
	if len(formatted) > maxExpected {
		t.Errorf("Format() returned %d bytes, expected <= %d", len(formatted), maxExpected)
	}

	// 验证帧对齐截断的精确长度
	// 计算在 4096 字节限制内能完整放入的最大帧数
	total := 0
	frameCount := 0
	for i, frame := range frames {
		frameStr := frame.String()
		needed := len(frameStr)
		if i > 0 {
			needed++ // 帧间换行符
		}
		if total+needed > defaultMaxBytes {
			break
		}
		total += needed
		frameCount++
	}
	expectedLen := total + len(truncationMarker)
	if len(formatted) != expectedLen {
		t.Errorf("Format() truncated length = %d, want exactly %d (%d complete frames + marker)",
			len(formatted), expectedLen, frameCount)
	}
}

// TestStackTraceFormatNoTruncationWhenSmall 测试内容小于限制时不截断
func TestStackTraceFormatNoTruncationWhenSmall(t *testing.T) {
	frames := []StackFrame{
		{Function: "main.main", File: "/app/main.go", Line: 10},
		{Function: "main.run", File: "/app/run.go", Line: 20},
	}
	st := NewStackTraceFromFrames(frames)

	formatted := st.Format()

	// 小内容不应包含截断标记
	if strings.Contains(formatted, "... (truncated)") {
		t.Error("Format() should not truncate small content")
	}

	// 验证内容完整
	if !strings.Contains(formatted, "main.main") {
		t.Error("Format() should contain all frames")
	}
	if !strings.Contains(formatted, "main.run") {
		t.Error("Format() should contain all frames")
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

// TestStackTraceFilterDefault 测试 Filter(nil) 调用使用 DefaultFilter
func TestStackTraceFilterDefault(t *testing.T) {
	st := Capture(0)

	// 传入 nil 应自动使用 DefaultFilter
	filtered := st.Filter(nil)

	frames := filtered.Frames()
	if len(frames) == 0 {
		t.Fatal("Filter(nil) returned no frames")
	}

	// 验证 DefaultFilter 生效：不应包含 runtime 帧
	for _, frame := range frames {
		if strings.HasPrefix(frame.Function, "runtime.") {
			t.Errorf("DefaultFilter should filter runtime frames, but found: %s", frame.Function)
		}
	}

	// 验证 DefaultFilter 生效：不应包含 stackx 基础设施帧
	for _, frame := range frames {
		if strings.Contains(frame.File, "/internal/stackx/") && !strings.HasSuffix(frame.File, "_test.go") {
			t.Errorf("DefaultFilter should filter stackx infrastructure frames, but found: %s in %s", frame.Function, frame.File)
		}
	}
}

// TestStackTraceFilterDoesNotMutateOriginal 验证 Filter 不修改原始 StackTrace
func TestStackTraceFilterDoesNotMutateOriginal(t *testing.T) {
	st := Capture(0)
	originalCount := len(st.Frames())

	// 使用一个过滤掉所有帧的函数
	_ = st.Filter(func(frame StackFrame) bool {
		return false
	})

	// 原始 StackTrace 应保持不变
	if len(st.Frames()) != originalCount {
		t.Errorf("Filter() mutated original StackTrace: got %d frames, want %d", len(st.Frames()), originalCount)
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
	reportFrame := StackFrame{Function: "github.com/prismgo/framework/exception.(*Handler).Report", File: "/www/code/framework/exception/handler.go"}
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

	// 测试 logger 帧被过滤
	loggerFrame := StackFrame{Function: "github.com/prismgo/framework/logger.(*Logger).Error", File: "/www/code/framework/logger/logger.go"}
	if filter(loggerFrame) {
		t.Error("DefaultFilter should filter logger frames")
	}

	// 测试 sirupsen/logrus 帧被过滤
	logrusFrame := StackFrame{Function: "github.com/sirupsen/logrus.(*Entry).Error", File: "/home/jqh/go/pkg/mod/github.com/sirupsen/logrus@v1.9.4/entry.go"}
	if filter(logrusFrame) {
		t.Error("DefaultFilter should filter sirupsen/logrus frames")
	}

	// 测试用户自定义的 logger 包不被误过滤
	userLoggerFrame := StackFrame{Function: "myapp/internal/logger.Handler", File: "/app/internal/logger/handler.go"}
	if !filter(userLoggerFrame) {
		t.Error("DefaultFilter should not filter user-defined logger frames")
	}

	// 测试用户自定义的 exception 包不被误过滤
	userExceptionFrame := StackFrame{Function: "myapp/internal/exception.Handle", File: "/app/internal/exception/handle.go"}
	if !filter(userExceptionFrame) {
		t.Error("DefaultFilter should not filter user-defined exception frames")
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

// TestFirstLocation 表驱动测试 FirstLocation 方法的不同场景
func TestFirstLocation(t *testing.T) {
	tests := []struct {
		name     string
		frames   []StackFrame
		wantFile string
		wantLine int
	}{
		{
			name: "skips assembly frames, returns first .go file",
			frames: []StackFrame{
				{Function: "runtime.goexit", File: "/usr/local/go/src/runtime/asm_amd64.s", Line: 1695},
				{Function: "main.handler", File: "/app/main.go", Line: 42},
				{Function: "main.process", File: "/app/process.go", Line: 100},
			},
			wantFile: "/app/main.go",
			wantLine: 42,
		},
		{
			name:   "returns empty for nil frames",
			frames: nil,
		},
		{
			name: "returns empty when no .go files present",
			frames: []StackFrame{
				{Function: "runtime.goexit", File: "/usr/local/go/src/runtime/asm_amd64.s", Line: 1695},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := NewStackTraceFromFrames(tt.frames)
			file, line := st.FirstLocation()
			if file != tt.wantFile || line != tt.wantLine {
				t.Errorf("FirstLocation() = (%q, %d), want (%q, %d)", file, line, tt.wantFile, tt.wantLine)
			}
		})
	}
}

// TestStackTraceFirstLocation_FrameworkCode 测试框架底层代码的堆栈帧不会被过滤
func TestStackTraceFirstLocation_FrameworkCode(t *testing.T) {
	// 模拟框架底层报错的堆栈（如 cache.RedisDriver.Get 出错）
	frames := []StackFrame{
		{Function: "github.com/prismgo/framework/cache.(*RedisDriver).Get", File: "/www/code/framework/cache/redis_driver.go", Line: 45},
		{Function: "github.com/prismgo/framework/cache.(*Manager).Get", File: "/www/code/framework/cache/manager.go", Line: 120},
		{Function: "main.handleRequest", File: "/app/main.go", Line: 30},
	}
	st := NewStackTraceFromFrames(frames)

	// 应用默认过滤器
	filtered := st.Filter(nil)

	// 验证第一帧是框架代码，不是用户代码
	file, line := filtered.FirstLocation()
	if file != "/www/code/framework/cache/redis_driver.go" {
		t.Errorf("FirstLocation() file = %q, want framework code", file)
	}
	if line != 45 {
		t.Errorf("FirstLocation() line = %d, want 45", line)
	}

	// 验证所有帧都被保留（框架代码不应被过滤）
	filteredFrames := filtered.Frames()
	if len(filteredFrames) != 3 {
		t.Errorf("Filter() removed framework frames, got %d frames, want 3", len(filteredFrames))
	}
}

// TestLines 表驱动测试 Lines 方法的不同场景
func TestLines(t *testing.T) {
	tests := []struct {
		name   string
		frames []StackFrame
		want   []string
	}{
		{
			name: "two frames produce four lines",
			frames: []StackFrame{
				{Function: "main.test", File: "/path/to/file.go", Line: 42},
				{Function: "main.caller", File: "/path/to/caller.go", Line: 10},
			},
			want: []string{"main.test", "/path/to/file.go:42", "main.caller", "/path/to/caller.go:10"},
		},
		{
			name:   "returns nil for empty stack",
			frames: nil,
			want:   nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := NewStackTraceFromFrames(tt.frames)
			got := st.Lines()
			if len(got) != len(tt.want) {
				t.Fatalf("Lines() returned %d lines, want %d\ngot:  %v\nwant: %v", len(got), len(tt.want), got, tt.want)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("Lines()[%d] = %q, want %q", i, got[i], tt.want[i])
				}
			}
		})
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
		t.Error("nil StackTrace.Filter(nil) should return nil")
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
