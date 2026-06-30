package logger

import (
	"encoding/json"
	"strings"

	"github.com/sirupsen/logrus"

	"github.com/prismgo/framework/internal/stackx"
)

const simpleLineFormat = "[%datetime%] %channel%.%level_name%: %message% %context% %extra%\n"

// stackTracer 抽象携带堆栈字符串的 error，
// 兼容业务侧未来接入自定义错误类型或第三方错误包。
type stackTracer interface {
	StackTrace() string
}

// LineFormatter 对齐 Laravel 13 默认 LineFormatter 行格式。
// 设计思路：保留 Laravel 的固定格式与多行 trace 语义，
// 但 trace 内容采用 Go 原生栈，避免伪造 PHP 风格导致信息失真。
type LineFormatter struct {
	format                     string
	dateFormat                 string
	channelName                string
	allowInlineLineBreaks      bool
	ignoreEmptyContextAndExtra bool
	includeStacktraces         bool
}

// newLineFormatter 构造 Laravel 风格的 line formatter。
// 参数约定：channel 由 newChannel 自动注入，用于输出 `%channel%`。
func newLineFormatter(params map[string]any) (Formatter, error) {
	formatter := &LineFormatter{
		format:                     simpleLineFormat,
		dateFormat:                 "2006-01-02 15:04:05",
		allowInlineLineBreaks:      true,
		ignoreEmptyContextAndExtra: true,
		includeStacktraces:         true,
	}
	if name, ok := params["channel"].(string); ok {
		formatter.channelName = name
	}
	return formatter, nil
}

// Format 实现 logrus.Formatter，输出 Laravel 默认日志行，
// 并在存在 error 时追加独立的 [stacktrace] 多行段落。
func (f *LineFormatter) Format(entry *logrus.Entry) ([]byte, error) {
	line, err := f.buildLine(entry)
	if err != nil {
		return nil, err
	}
	stackTrace := f.formatStackTraces(entry)
	return []byte(line + stackTrace), nil
}

// buildLine 按 Laravel 默认 SIMPLE_FORMAT 组装主日志行。
// 这里不走通用占位符解释器，直接显式拼接固定格式，避免为了未使用的可配置能力增加复杂度。
func (f *LineFormatter) buildLine(entry *logrus.Entry) (string, error) {
	message := entry.Message
	if !f.allowInlineLineBreaks {
		message = strings.ReplaceAll(message, "\n", " ")
	}
	levelLabel := strings.ToUpper(entry.Level.String()) + ":"
	if f.channelName != "" {
		levelLabel = f.channelName + "." + levelLabel
	}
	parts := []string{
		"[" + entry.Time.Format(f.dateFormat) + "]",
		levelLabel,
		message,
	}
	contextJSON, err := f.formatContext(entry.Data)
	if err != nil {
		return "", err
	}
	if contextJSON != "" {
		parts = append(parts, contextJSON)
	}
	return strings.Join(parts, " ") + "\n", nil
}

// formatContext 把 logrus 字段编码为 Laravel line formatter 的 context JSON。
// 设计约束：当前实现把全部 entry.Data 视为 context；error 字段保留标准错误消息，
// 这样既能兼容 WithError 语义，又能在 stacktrace 之外保留可直接检索的错误文本。
func (f *LineFormatter) formatContext(data logrus.Fields) (string, error) {
	if len(data) == 0 {
		return "", nil
	}
	context := make(map[string]any, len(data))
	for key, value := range data {
		if key == logrus.ErrorKey {
			if err, ok := value.(error); ok && err != nil {
				context[key] = err.Error()
			}
			continue
		}
		context[key] = value
	}
	if len(context) == 0 && f.ignoreEmptyContextAndExtra {
		return "", nil
	}
	encoded, err := json.Marshal(context)
	if err != nil {
		return "", err
	}
	if string(encoded) == "{}" && f.ignoreEmptyContextAndExtra {
		return "", nil
	}
	return string(encoded), nil
}

// formatStackTraces 在存在 error 且开启 includeStacktraces 时输出独立的多行 trace 段落。
// 输出头部固定为 Laravel 风格的 [stacktrace]，但具体内容保留 Go 原生栈格式。
func (f *LineFormatter) formatStackTraces(entry *logrus.Entry) string {
	if !f.includeStacktraces {
		return ""
	}
	errValue, ok := entry.Data[logrus.ErrorKey]
	if !ok {
		return ""
	}
	err, ok := errValue.(error)
	if !ok || err == nil {
		return ""
	}
	trace := f.extractStackTrace(err)
	if trace == "" {
		return ""
	}
	return "[stacktrace]\n" + trace + "\n"
}

// extractStackTrace 优先读取 error 自带的堆栈字符串，
// 否则回退到 debug.Stack 采集当前日志调用位置的 Go 原生栈。
func (f *LineFormatter) extractStackTrace(err error) string {
	if traced, ok := err.(stackTracer); ok {
		trace := strings.TrimSpace(traced.StackTrace())
		if trace != "" {
			return trace
		}
	}
	return strings.TrimSpace(string(stackx.Capture()))
}
