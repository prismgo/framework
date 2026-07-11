package logger

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/prismgo/framework/internal/stackx"
)

// stubStackError 用固定堆栈内容验证 line formatter 的多行 trace 输出，
// 避免依赖 runtime/debug.Stack 的运行时细节导致测试脆弱。
// 适配新的结构化堆栈接口，返回 *stackx.StackTrace
type stubStackError struct {
	message string
	stack   *stackx.StackTrace
}

func (e stubStackError) Error() string {
	return e.message
}

func (e stubStackError) StackTrace() *stackx.StackTrace {
	return e.stack
}

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

// =============================================================================
// LineFormatter 核心行为测试（恢复自旧版本）
// =============================================================================

// TestLineFormatterFormatsLaravelLine 验证 line formatter 的基础输出严格对齐 Laravel 默认行格式。
func TestLineFormatterFormatsLaravelLine(t *testing.T) {
	formatter, err := newLineFormatter(map[string]any{"channel": "local"})
	if err != nil {
		t.Fatalf("newLineFormatter: %v", err)
	}
	entry := &logrus.Entry{
		Time:    time.Date(2026, 5, 29, 10, 30, 45, 0, time.UTC),
		Level:   logrus.InfoLevel,
		Message: "hello world",
		Data:    logrus.Fields{},
	}

	out, err := formatter.Format(entry)
	if err != nil {
		t.Fatalf("format: %v", err)
	}
	if got, want := string(out), "[2026-05-29 10:30:45] local.INFO: hello world\n"; got != want {
		t.Fatalf("unexpected line output\nwant: %q\n got: %q", want, got)
	}
}

// TestLineFormatterOmitsChannelPrefixWhenChannelEmpty 验证空 channel 时不会输出多余的前导点。
func TestLineFormatterOmitsChannelPrefixWhenChannelEmpty(t *testing.T) {
	formatter, err := newLineFormatter(nil)
	if err != nil {
		t.Fatalf("newLineFormatter: %v", err)
	}
	entry := &logrus.Entry{
		Time:    time.Date(2026, 5, 29, 10, 30, 45, 0, time.UTC),
		Level:   logrus.InfoLevel,
		Message: "hello world",
		Data:    logrus.Fields{},
	}

	out, err := formatter.Format(entry)
	if err != nil {
		t.Fatalf("format: %v", err)
	}
	if got, want := string(out), "[2026-05-29 10:30:45] INFO: hello world\n"; got != want {
		t.Fatalf("unexpected empty-channel output\nwant: %q\n got: %q", want, got)
	}
}

// TestLineFormatterRendersContextAsJSON 验证结构化字段会进入 context JSON，且空 extra 不会残留尾巴。
func TestLineFormatterRendersContextAsJSON(t *testing.T) {
	formatter, err := newLineFormatter(map[string]any{"channel": "orders"})
	if err != nil {
		t.Fatalf("newLineFormatter: %v", err)
	}
	entry := &logrus.Entry{
		Time:    time.Date(2026, 5, 29, 10, 30, 45, 0, time.UTC),
		Level:   logrus.WarnLevel,
		Message: "job slow",
		Data:    logrus.Fields{"tenant_id": 7, "user_id": 9},
	}

	out, err := formatter.Format(entry)
	if err != nil {
		t.Fatalf("format: %v", err)
	}
	got := string(out)
	if !strings.Contains(got, "[2026-05-29 10:30:45] orders.WARNING: job slow ") {
		t.Fatalf("missing header: %q", got)
	}
	if !strings.Contains(got, `{"tenant_id":7,"user_id":9}`) {
		t.Fatalf("context json missing: %q", got)
	}
	if strings.Contains(got, " {}") {
		t.Fatalf("empty extra should be omitted: %q", got)
	}
}

// TestLineFormatterOmitsEmptyContextAndExtra 验证空 context/extra 时不会输出 Laravel 禁止的尾部空对象。
func TestLineFormatterOmitsEmptyContextAndExtra(t *testing.T) {
	formatter, err := newLineFormatter(map[string]any{"channel": "app"})
	if err != nil {
		t.Fatalf("newLineFormatter: %v", err)
	}
	entry := &logrus.Entry{
		Time:    time.Date(2026, 5, 29, 10, 30, 45, 0, time.UTC),
		Level:   logrus.DebugLevel,
		Message: "debug line",
	}

	out, err := formatter.Format(entry)
	if err != nil {
		t.Fatalf("format: %v", err)
	}
	if got := string(out); strings.Contains(got, "{}") {
		t.Fatalf("empty context/extra should not appear: %q", got)
	}
}

// TestLineFormatterPreservesInlineLineBreaks 验证与 Laravel 一致地保留 message 内部换行。
func TestLineFormatterPreservesInlineLineBreaks(t *testing.T) {
	formatter, err := newLineFormatter(map[string]any{"channel": "app"})
	if err != nil {
		t.Fatalf("newLineFormatter: %v", err)
	}
	entry := &logrus.Entry{
		Time:    time.Date(2026, 5, 29, 10, 30, 45, 0, time.UTC),
		Level:   logrus.ErrorLevel,
		Message: "boom line1\nline2",
	}

	out, err := formatter.Format(entry)
	if err != nil {
		t.Fatalf("format: %v", err)
	}
	if got := string(out); !strings.Contains(got, "boom line1\nline2") {
		t.Fatalf("inline line breaks should be preserved: %q", got)
	}
}

// TestLineFormatterAppendsStackTrace 验证 error 会触发 [stacktrace] 多行段落，
// 同时保留标准 error 字段，避免错误消息在 line formatter 下丢失。
func TestLineFormatterAppendsStackTrace(t *testing.T) {
	formatter, err := newLineFormatter(map[string]any{"channel": "worker"})
	if err != nil {
		t.Fatalf("newLineFormatter: %v", err)
	}

	// 使用 NewStackTraceFromFrames 构造固定堆栈，避免依赖运行时细节
	testStack := stackx.NewStackTraceFromFrames([]stackx.StackFrame{
		{Function: "main.handler", File: "/app/main.go", Line: 10},
		{Function: "main.main", File: "/app/main.go", Line: 5},
	})

	entry := &logrus.Entry{
		Time:    time.Date(2026, 5, 29, 10, 30, 45, 0, time.UTC),
		Level:   logrus.ErrorLevel,
		Message: "job failed",
		Data: logrus.Fields{
			"tenant_id": 7,
			logrus.ErrorKey: stubStackError{
				message: "bad",
				stack:   testStack,
			},
		},
	}

	out, err := formatter.Format(entry)
	if err != nil {
		t.Fatalf("format: %v", err)
	}
	got := string(out)

	// context JSON 应包含 error 消息
	if !strings.Contains(got, `{"error":"bad","tenant_id":7}`) && !strings.Contains(got, `{"tenant_id":7,"error":"bad"}`) {
		t.Fatalf("context json should keep error message: %q", got)
	}

	// stacktrace 段落应包含格式化的堆栈
	if !strings.Contains(got, "[stacktrace]") {
		t.Fatalf("stacktrace header missing: %q", got)
	}
	if !strings.Contains(got, "main.handler") {
		t.Fatalf("stacktrace should contain function name: %q", got)
	}
	if !strings.Contains(got, "/app/main.go:10") {
		t.Fatalf("stacktrace should contain file location: %q", got)
	}
}

// TestLineFormatterFallsBackToCaptureBytes 验证普通 error 也会走 CaptureBytes 回退逻辑。
func TestLineFormatterFallsBackToCaptureBytes(t *testing.T) {
	formatter, err := newLineFormatter(map[string]any{"channel": "worker"})
	if err != nil {
		t.Fatalf("newLineFormatter: %v", err)
	}
	entry := &logrus.Entry{
		Time:    time.Date(2026, 5, 29, 10, 30, 45, 0, time.UTC),
		Level:   logrus.ErrorLevel,
		Message: "job failed",
		Data: logrus.Fields{
			logrus.ErrorKey: errors.New("bad"),
		},
	}

	out, err := formatter.Format(entry)
	if err != nil {
		t.Fatalf("format: %v", err)
	}
	got := string(out)
	if !strings.Contains(got, "[stacktrace]\n") {
		t.Fatalf("stacktrace header missing: %q", got)
	}
	// CaptureBytes 使用 debug.Stack()，应包含 runtime/debug.Stack
	if !strings.Contains(got, "runtime/debug.Stack") {
		t.Fatalf("expected debug.Stack fallback trace: %q", got)
	}
}

// =============================================================================
// 工厂函数测试（恢复自旧版本）
// =============================================================================

// TestDefaultFormatterReturnsLineFormatter 验证 defaultFormatter 始终返回有效的 LineFormatter。
func TestDefaultFormatterReturnsLineFormatter(t *testing.T) {
	f := defaultFormatter()
	if f == nil {
		t.Fatal("defaultFormatter returned nil")
	}
	if _, ok := f.(*LineFormatter); !ok {
		t.Fatalf("expected *LineFormatter, got %T", f)
	}
}

// TestBuildFormatterDefaultsToLine 验证空 formatter 名称时默认切换为 line。
func TestBuildFormatterDefaultsToLine(t *testing.T) {
	formatter, err := buildFormatter("", map[string]any{"channel": "local"})
	if err != nil {
		t.Fatalf("buildFormatter: %v", err)
	}
	if _, ok := formatter.(*LineFormatter); !ok {
		t.Fatalf("expected *LineFormatter, got %T", formatter)
	}
}

// TestNewChannelInjectsFormatterChannel 验证通道名会自动注入 line formatter，确保输出中包含 channel 名。
func TestNewChannelInjectsFormatterChannel(t *testing.T) {
	channel, err := newChannel("payments", ChannelOptions{Driver: "null", Level: "info", Formatter: "line"}, nil)
	if err != nil {
		t.Fatalf("newChannel: %v", err)
	}
	formatter, ok := channel.logger.Formatter.(*LineFormatter)
	if !ok {
		t.Fatalf("expected *LineFormatter, got %T", channel.logger.Formatter)
	}
	if formatter.channelName != "payments" {
		t.Fatalf("unexpected channel name: %q", formatter.channelName)
	}
}

// TestNewChannelDoesNotMutateFormatterParams 验证 newChannel 注入 channel 参数时不会污染调用方传入的 map。
func TestNewChannelDoesNotMutateFormatterParams(t *testing.T) {
	params := map[string]any{"keep": "value"}
	_, err := newChannel("payments", ChannelOptions{
		Driver:          "null",
		Level:           "info",
		Formatter:       "line",
		FormatterParams: params,
	}, nil)
	if err != nil {
		t.Fatalf("newChannel: %v", err)
	}
	if got := params["keep"]; got != "value" {
		t.Fatalf("formatter params changed unexpectedly: %#v", params)
	}
	if _, ok := params["channel"]; ok {
		t.Fatalf("formatter params should not be mutated: %#v", params)
	}
}

// =============================================================================
// 新增测试：结构化堆栈相关
// =============================================================================

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
