package exception

import (
	"bytes"
	"context"
	"errors"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/prismgo/framework/logger"
)

// TestStackTraceAccuracyForPanicAndError 验证 panic 和普通 error 的堆栈捕获精度。
// 核心验证目标：日志中的 [stacktrace] 段落精确包含错误发生位置的文件和行号。
func TestStackTraceAccuracyForPanicAndError(t *testing.T) {
	t.Run("panic_stack_trace_accuracy", func(t *testing.T) {
		testPanicStackTraceAccuracy(t)
	})

	t.Run("error_stack_trace_accuracy", func(t *testing.T) {
		testErrorStackTraceAccuracy(t)
	})

	t.Run("wrapped_error_stack_trace_accuracy", func(t *testing.T) {
		testWrappedErrorStackTraceAccuracy(t)
	})
}

// testPanicStackTraceAccuracy 验证 panic 恢复后的堆栈捕获精度。
// panic 场景：堆栈应包含 panic 发生位置和 Report 调用位置。
func testPanicStackTraceAccuracy(t *testing.T) {
	registry := useExceptionTestContainer(t)
	buf := new(bytes.Buffer)

	logger.Extend("panic-stack-buffer-v2", func(logger.ChannelOptions) (logger.Driver, error) {
		return exceptionLogBufferDriver{Buffer: buf}, nil
	})

	manager, err := logger.NewManager(logger.Config{
		Default: "panic-test",
		Channels: map[string]logger.ChannelOptions{
			"panic-test": {Driver: "panic-stack-buffer-v2", Formatter: "line", Level: "error"},
		},
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	if err := registry.Instance("logger.manager", manager); err != nil {
		t.Fatalf("bind logger manager: %v", err)
	}
	t.Cleanup(func() {
		_ = manager.Close()
	})

	// 触发 panic 并恢复
	func() {
		defer func() {
			if r := recover(); r != nil {
				capturedErr := errors.New("panic recovered")
				h := New()
				h.Report(context.Background(), capturedErr, map[string]any{
					"status":  500,
					"message": "panic occurred",
				})
			}
		}()
		triggerPanicV2()
	}()

	logOutput := buf.String()
	if logOutput == "" {
		t.Fatal("expected log output, got empty")
	}

	stackSection := extractStackSection(logOutput)
	t.Logf("[panic] Stack trace section:\n%s", stackSection)

	// panic 场景验证：堆栈应包含 triggerPanicV2 函数（panic 发生位置）
	if !strings.Contains(stackSection, "triggerPanicV2") {
		t.Error("stack trace should contain triggerPanicV2 (panic location)")
	}

	// 堆栈应包含当前测试函数（Report 调用位置）
	if !strings.Contains(stackSection, "testPanicStackTraceAccuracy") {
		t.Error("stack trace should contain testPanicStackTraceAccuracy (Report caller)")
	}

	// 验证堆栈帧格式正确（函数名\n\t文件:行号）
	// 函数名包含 / （如 github.com/prismgo/framework/exception.xxx）
	framePattern := regexp.MustCompile(`(?m)^[\w\.\(\)\*\/]+\n\t[^\n]+:\d+$`)
	matches := framePattern.FindAllString(stackSection, -1)
	if len(matches) == 0 {
		t.Error("no valid stack frames found in panic stack trace")
	}
	t.Logf("[panic] Found %d valid stack frames", len(matches))
}

// testErrorStackTraceAccuracy 验证普通 error 的堆栈捕获精度。
// 普通 error 没有自带堆栈，Report 在调用点自动捕获，
// 堆栈第一帧应指向 Report 的调用者（而非 Report 内部）。
func testErrorStackTraceAccuracy(t *testing.T) {
	registry := useExceptionTestContainer(t)
	buf := new(bytes.Buffer)

	logger.Extend("error-stack-buffer-v2", func(logger.ChannelOptions) (logger.Driver, error) {
		return exceptionLogBufferDriver{Buffer: buf}, nil
	})

	manager, err := logger.NewManager(logger.Config{
		Default: "error-test",
		Channels: map[string]logger.ChannelOptions{
			"error-test": {Driver: "error-stack-buffer-v2", Formatter: "line", Level: "error"},
		},
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	if err := registry.Instance("logger.manager", manager); err != nil {
		t.Fatalf("bind logger manager: %v", err)
	}
	t.Cleanup(func() {
		_ = manager.Close()
	})

	// 创建普通 error 并上报
	// 堆栈将在 Report 调用时自动捕获，指向此处
	plainErr := errors.New("plain error for stack verification")
	h := New()
	// 记录 Report 调用的行号——这是堆栈应该指向的位置
	_, reportFile, reportLine, _ := runtime.Caller(0)
	h.Report(context.Background(), plainErr, map[string]any{
		"status":  500,
		"message": "error occurred",
	})

	logOutput := buf.String()
	if logOutput == "" {
		t.Fatal("expected log output, got empty")
	}

	stackSection := extractStackSection(logOutput)
	t.Logf("[error] Stack trace section:\n%s", stackSection)

	// 验证堆栈包含 Report 调用位置的文件和行号
	verifyStackTraceContainsLocation(t, stackSection, reportFile, reportLine, "error (Report caller)")
}

// testWrappedErrorStackTraceAccuracy 验证 Wrap() 包装 error 的堆栈捕获精度。
// Wrap() 在包装时捕获堆栈，堆栈第一帧应指向 Wrap 调用位置。
func testWrappedErrorStackTraceAccuracy(t *testing.T) {
	registry := useExceptionTestContainer(t)
	buf := new(bytes.Buffer)

	logger.Extend("wrapped-error-stack-buffer-v2", func(logger.ChannelOptions) (logger.Driver, error) {
		return exceptionLogBufferDriver{Buffer: buf}, nil
	})

	manager, err := logger.NewManager(logger.Config{
		Default: "wrapped-test",
		Channels: map[string]logger.ChannelOptions{
			"wrapped-test": {Driver: "wrapped-error-stack-buffer-v2", Formatter: "line", Level: "error"},
		},
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	if err := registry.Instance("logger.manager", manager); err != nil {
		t.Fatalf("bind logger manager: %v", err)
	}
	t.Cleanup(func() {
		_ = manager.Close()
	})

	// Wrap() 在调用时捕获堆栈
	baseErr := errors.New("base error")
	_, wrapFile, wrapLine, _ := runtime.Caller(0)
	wrappedErr := Wrap(baseErr, "wrapped context")
	h := New()
	h.Report(context.Background(), wrappedErr, map[string]any{
		"status":  500,
		"message": "wrapped error occurred",
	})

	logOutput := buf.String()
	if logOutput == "" {
		t.Fatal("expected log output, got empty")
	}

	stackSection := extractStackSection(logOutput)
	t.Logf("[wrapped error] Stack trace section:\n%s", stackSection)

	// Wrap 的堆栈应指向 Wrap 调用位置
	verifyStackTraceContainsLocation(t, stackSection, wrapFile, wrapLine, "wrapped error (Wrap caller)")
}

// triggerPanicV2 触发 panic 的辅助函数
func triggerPanicV2() {
	panic("test panic for stack trace verification")
}

// verifyStackTraceContainsLocation 验证堆栈中包含指定文件和行号（±5 行容差）。
func verifyStackTraceContainsLocation(t *testing.T, stackSection, expectedFile string, expectedLine int, scenario string) {
	t.Helper()

	lines := strings.Split(stackSection, "\n")
	found := false

	for i := 0; i < len(lines); i++ {
		line := lines[i]
		if !strings.HasPrefix(line, "\t") {
			continue
		}
		filePath, lineNum, ok := parseStackFrameLine(line)
		if !ok {
			continue
		}
		if strings.HasSuffix(filePath, expectedFile) || filePath == expectedFile {
			if abs(lineNum-expectedLine) <= 5 {
				found = true
				t.Logf("[%s] Found expected location: %s:%d (expected: %d)",
					scenario, filePath, lineNum, expectedLine)
				break
			}
			t.Logf("[%s] Found file but line mismatch: got %d, expected %d (±5)",
				scenario, lineNum, expectedLine)
		}
	}

	if !found {
		t.Errorf("[%s] stack trace does not contain expected location %s:%d",
			scenario, expectedFile, expectedLine)
	}
}

// parseStackFrameLine 解析堆栈帧的文件行号行（格式：\t/path/to/file.go:123）
func parseStackFrameLine(line string) (file string, lineNum int, ok bool) {
	if !strings.HasPrefix(line, "\t") {
		return "", 0, false
	}
	content := line[1:]
	lastColon := strings.LastIndex(content, ":")
	if lastColon < 0 {
		return "", 0, false
	}
	filePath := content[:lastColon]
	num, err := strconv.Atoi(content[lastColon+1:])
	if err != nil {
		return "", 0, false
	}
	return filePath, num, true
}

// extractStackSection 从日志输出中提取 [stacktrace] 段落
func extractStackSection(logOutput string) string {
	idx := strings.Index(logOutput, "[stacktrace]")
	if idx == -1 {
		return ""
	}
	return logOutput[idx:]
}

func abs(n int) int {
	if n < 0 {
		return -n
	}
	return n
}

// TestStackTraceContainsCurrentTestFunction 验证堆栈包含当前测试函数名和文件名
func TestStackTraceContainsCurrentTestFunction(t *testing.T) {
	registry := useExceptionTestContainer(t)
	buf := new(bytes.Buffer)

	logger.Extend("current-func-buffer-v2", func(logger.ChannelOptions) (logger.Driver, error) {
		return exceptionLogBufferDriver{Buffer: buf}, nil
	})

	manager, err := logger.NewManager(logger.Config{
		Default: "func-test",
		Channels: map[string]logger.ChannelOptions{
			"func-test": {Driver: "current-func-buffer-v2", Formatter: "line", Level: "error"},
		},
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	if err := registry.Instance("logger.manager", manager); err != nil {
		t.Fatalf("bind logger manager: %v", err)
	}
	t.Cleanup(func() {
		_ = manager.Close()
	})

	plainErr := errors.New("error in current test function")
	h := New()
	h.Report(context.Background(), plainErr, map[string]any{
		"status":  500,
		"message": "test error",
	})

	logOutput := buf.String()
	stackSection := extractStackSection(logOutput)

	if !strings.Contains(stackSection, "TestStackTraceContainsCurrentTestFunction") {
		t.Errorf("stack trace should contain current test function name")
		t.Errorf("Stack trace:\n%s", stackSection)
	}

	_, currentFile, _, _ := runtime.Caller(0)
	if !strings.Contains(stackSection, currentFile) {
		t.Errorf("stack trace should contain current file: %s", currentFile)
		t.Errorf("Stack trace:\n%s", stackSection)
	}
}

// TestStackTraceFilterRemovesFrameworkFrames 验证 DefaultFilter 正确过滤框架内部帧
func TestStackTraceFilterRemovesFrameworkFrames(t *testing.T) {
	registry := useExceptionTestContainer(t)
	buf := new(bytes.Buffer)

	logger.Extend("filter-test-buffer-v2", func(logger.ChannelOptions) (logger.Driver, error) {
		return exceptionLogBufferDriver{Buffer: buf}, nil
	})

	manager, err := logger.NewManager(logger.Config{
		Default: "filter-test",
		Channels: map[string]logger.ChannelOptions{
			"filter-test": {Driver: "filter-test-buffer-v2", Formatter: "line", Level: "error"},
		},
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	if err := registry.Instance("logger.manager", manager); err != nil {
		t.Fatalf("bind logger manager: %v", err)
	}
	t.Cleanup(func() {
		_ = manager.Close()
	})

	plainErr := errors.New("test error for filter verification")
	h := New()
	h.Report(context.Background(), plainErr, map[string]any{
		"status":  500,
		"message": "filter test",
	})

	logOutput := buf.String()
	stackSection := extractStackSection(logOutput)

	// 堆栈不应包含 exception.(*Handler).Report（被 DefaultFilter 过滤）
	if strings.Contains(stackSection, "exception.(*Handler).Report") {
		t.Error("stack trace should NOT contain exception.(*Handler).Report (should be filtered)")
		t.Errorf("Stack trace:\n%s", stackSection)
	}

	// 堆栈不应包含 internal/stackx（被 DefaultFilter 过滤）
	if strings.Contains(stackSection, "internal/stackx") {
		t.Error("stack trace should NOT contain internal/stackx (should be filtered)")
		t.Errorf("Stack trace:\n%s", stackSection)
	}

	// 堆栈应包含业务代码（当前测试函数）
	if !strings.Contains(stackSection, "TestStackTraceFilterRemovesFrameworkFrames") {
		t.Error("stack trace should contain business code (current test function)")
		t.Errorf("Stack trace:\n%s", stackSection)
	}
}

// TestStackTraceMultipleFrames 验证 Wrap 包装的 error 保留完整调用链帧。
// 普通 error 的堆栈仅在 Report 调用时捕获，不含深层调用链；
// 只有用 Wrap/WithStack 在每层显式捕获的 error 才保留完整调用链。
func TestStackTraceMultipleFrames(t *testing.T) {
	registry := useExceptionTestContainer(t)
	buf := new(bytes.Buffer)

	logger.Extend("multi-frame-buffer-v2", func(logger.ChannelOptions) (logger.Driver, error) {
		return exceptionLogBufferDriver{Buffer: buf}, nil
	})

	manager, err := logger.NewManager(logger.Config{
		Default: "multi-frame",
		Channels: map[string]logger.ChannelOptions{
			"multi-frame": {Driver: "multi-frame-buffer-v2", Formatter: "line", Level: "error"},
		},
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	if err := registry.Instance("logger.manager", manager); err != nil {
		t.Fatalf("bind logger manager: %v", err)
	}
	t.Cleanup(func() {
		_ = manager.Close()
	})

	// 使用 Wrap 在每层包装，保留完整调用链
	err = deepWrapStack1()
	h := New()
	h.Report(context.Background(), err, map[string]any{
		"status":  500,
		"message": "multi-frame test",
	})

	logOutput := buf.String()
	stackSection := extractStackSection(logOutput)

	// Wrap 在每层捕获堆栈，第一帧应指向最内层 Wrap 调用
	if !strings.Contains(stackSection, "deepWrapStack3") {
		t.Errorf("stack trace should contain deepWrapStack3 (innermost Wrap)")
	}
	if !strings.Contains(stackSection, "TestStackTraceMultipleFrames") {
		t.Errorf("stack trace should contain TestStackTraceMultipleFrames")
	}

	t.Logf("Multi-frame stack trace:\n%s", stackSection)
}

func deepWrapStack1() error {
	err := deepWrapStack2()
	return Wrap(err, "layer 1")
}

func deepWrapStack2() error {
	err := deepWrapStack3()
	return Wrap(err, "layer 2")
}

func deepWrapStack3() error {
	return Wrap(errors.New("root cause"), "layer 3")
}

// TestStackTraceRegexPattern 使用正则表达式验证堆栈帧格式
func TestStackTraceRegexPattern(t *testing.T) {
	registry := useExceptionTestContainer(t)
	buf := new(bytes.Buffer)

	logger.Extend("regex-buffer-v2", func(logger.ChannelOptions) (logger.Driver, error) {
		return exceptionLogBufferDriver{Buffer: buf}, nil
	})

	manager, err := logger.NewManager(logger.Config{
		Default: "regex-test",
		Channels: map[string]logger.ChannelOptions{
			"regex-test": {Driver: "regex-buffer-v2", Formatter: "line", Level: "error"},
		},
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	if err := registry.Instance("logger.manager", manager); err != nil {
		t.Fatalf("bind logger manager: %v", err)
	}
	t.Cleanup(func() {
		_ = manager.Close()
	})

	plainErr := errors.New("regex test error")
	h := New()
	h.Report(context.Background(), plainErr, map[string]any{
		"status":  500,
		"message": "regex test",
	})

	logOutput := buf.String()
	stackSection := extractStackSection(logOutput)

	// 正则匹配堆栈帧格式：函数名\n\t文件:行号
	// 函数名包含 / （如 github.com/prismgo/framework/exception.xxx）
	framePattern := regexp.MustCompile(`(?m)^[\w\.\(\)\*\/]+\n\t[^\n]+:\d+$`)
	matches := framePattern.FindAllString(stackSection, -1)

	if len(matches) == 0 {
		t.Error("no valid stack frames found matching expected pattern")
		t.Errorf("Stack trace:\n%s", stackSection)
		return
	}

	t.Logf("Found %d valid stack frames", len(matches))
	for i, match := range matches {
		t.Logf("Frame %d:\n%s", i, match)
	}
}
