package logger

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
)

// stubStackError 用固定堆栈内容验证 line formatter 的多行 trace 输出，
// 避免依赖 runtime/debug.Stack 的运行时细节导致测试脆弱。
type stubStackError struct {
	message string
	stack   string
}

func (e stubStackError) Error() string {
	return e.message
}

func (e stubStackError) StackTrace() string {
	return e.stack
}

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
	entry := &logrus.Entry{
		Time:    time.Date(2026, 5, 29, 10, 30, 45, 0, time.UTC),
		Level:   logrus.ErrorLevel,
		Message: "job failed",
		Data: logrus.Fields{
			"tenant_id": 7,
			logrus.ErrorKey: stubStackError{
				message: "bad",
				stack:   "trace line 1\ntrace line 2",
			},
		},
	}

	out, err := formatter.Format(entry)
	if err != nil {
		t.Fatalf("format: %v", err)
	}
	got := string(out)
	if !strings.Contains(got, `{"error":"bad","tenant_id":7}`) && !strings.Contains(got, `{"tenant_id":7,"error":"bad"}`) {
		t.Fatalf("context json should keep error message: %q", got)
	}
	if !strings.Contains(got, "[stacktrace]\ntrace line 1\ntrace line 2\n") {
		t.Fatalf("stacktrace block missing: %q", got)
	}
}

// TestLineFormatterFallsBackToDebugStack 验证普通 error 也会走 Go 原生堆栈回退逻辑。
func TestLineFormatterFallsBackToDebugStack(t *testing.T) {
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
	if !strings.Contains(got, "runtime/debug.Stack") {
		t.Fatalf("expected debug.Stack fallback trace: %q", got)
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
