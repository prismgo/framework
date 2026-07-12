package exception

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/prismgo/framework/internal/stackx"
)

// Problem is the framework-owned public error response shape.
// It intentionally does not include application business codes.
type Problem struct {
	Type      string         `json:"type"`
	Title     string         `json:"title"`
	Status    int            `json:"status"`
	Detail    string         `json:"detail,omitempty"`
	Instance  string         `json:"instance,omitempty"`
	Code      int            `json:"code,omitempty"`
	Message   string         `json:"message,omitempty"`
	RequestID string         `json:"request_id,omitempty"`
	Errors    map[string]any `json:"errors,omitempty"`
	Exception string         `json:"exception,omitempty"`
	File      string         `json:"file,omitempty"`
	Line      int            `json:"line,omitempty"`
	// Trace 包含堆栈跟踪信息，会被截断至 4KB 左右（详见 internal/stackx）。
	Trace []string `json:"trace,omitempty"`

	// basePath 是项目根目录路径，用于在调试响应中裁剪绝对路径避免泄露目录结构。
	// 由 Handler.renderResponse 在调用 WithDebug 前设置。
	basePath string `json:"-"`
}

// HTTPError is the minimal contract for errors that can expose a safe HTTP
// status and public message without coupling the framework to business codes.
type HTTPError interface {
	error
	StatusCode() int
	PublicMessage() string
}

func problemForError(err error, requestID string) Problem {
	status := http.StatusInternalServerError
	detail := ""

	var httpErr HTTPError
	switch {
	case errors.As(err, &httpErr):
		status = normalizeStatus(httpErr.StatusCode())
		if status < http.StatusInternalServerError {
			detail = strings.TrimSpace(httpErr.PublicMessage())
		}
	case errors.Is(err, context.Canceled):
		status = http.StatusRequestTimeout
	case errors.Is(err, context.DeadlineExceeded):
		status = http.StatusGatewayTimeout
	case errors.Is(err, http.ErrNoCookie):
		status = http.StatusBadRequest
		detail = http.StatusText(status)
	}

	title := statusTitle(status)
	if detail == "" {
		detail = defaultDetail(status)
	}

	problem := Problem{
		Type:      defaultTypeForStatus(status),
		Title:     title,
		Status:    status,
		Detail:    detail,
		RequestID: requestID,
	}
	if fields := publicFieldErrors(err); len(fields) > 0 {
		problem.Errors = fields
	}
	return problem
}

// WithDebug 补充仅在 app.debug=true 时返回给客户端的调试字段。
// 业务 HTTPError 的公开消息仍按原合同渲染，不暴露内部 cause 或包装链。
func (p Problem) WithDebug(err error) Problem {
	if err == nil || p.Status < http.StatusInternalServerError {
		return p
	}
	var httpErr HTTPError
	if errors.As(err, &httpErr) {
		return p
	}
	p.Detail = err.Error()
	p.Message = err.Error()
	p.Exception = fmt.Sprintf("%T", err)

	// 优先从 error 自身读取结构化堆栈（通过 stackTracer 接口），
	// 这样能反映错误源头的调用位置而非 WithDebug 渲染位置。
	st := captureStackForDebug(err)
	p.File, p.Line = st.FirstLocation()
	p.Trace = st.Lines()

	// 裁剪文件路径中的项目根目录前缀，避免泄露服务器目录结构
	if p.basePath != "" && strings.HasPrefix(p.File, p.basePath) {
		p.File = strings.TrimPrefix(p.File, p.basePath)
		p.File = strings.TrimPrefix(p.File, "/")
	}
	if p.basePath != "" {
		for i, line := range p.Trace {
			if strings.HasPrefix(line, p.basePath) {
				p.Trace[i] = strings.TrimPrefix(line, p.basePath)
				p.Trace[i] = strings.TrimPrefix(p.Trace[i], "/")
			}
		}
	}

	return p
}

// captureStackForDebug 返回用于调试展示的堆栈。
// 优先从 error 自身的 stackTracer 接口读取，否则采集渲染位置的堆栈。
func captureStackForDebug(err error) *stackx.StackTrace {
	var tracer interface{ StackTrace() *stackx.StackTrace }
	if errors.As(err, &tracer) && tracer != nil {
		if stack := tracer.StackTrace(); stack != nil {
			return stack.Filter(nil)
		}
	}
	return stackx.Capture(0).Filter(nil)
}

func normalizeStatus(status int) int {
	if status >= 100 && status <= 599 {
		return status
	}
	return http.StatusInternalServerError
}

func statusTitle(status int) string {
	if title := http.StatusText(status); title != "" {
		return title
	}
	return "Error"
}

func defaultDetail(status int) string {
	if status >= http.StatusInternalServerError {
		return "Internal Server Error"
	}
	return statusTitle(status)
}

func defaultTypeForStatus(status int) string {
	switch status {
	case http.StatusBadRequest:
		return "bad_request"
	case http.StatusUnauthorized:
		return "unauthorized"
	case http.StatusForbidden:
		return "forbidden"
	case http.StatusNotFound:
		return "not_found"
	case http.StatusConflict:
		return "conflict"
	case http.StatusTooManyRequests:
		return "too_many_requests"
	case http.StatusUnprocessableEntity:
		return "unprocessable_entity"
	case http.StatusInternalServerError:
		return "internal_error"
	default:
		if status >= http.StatusInternalServerError {
			return "internal_error"
		}
		return fmt.Sprintf("http.%d", status)
	}
}

type fieldErrorProvider interface {
	PublicFields() map[string]any
}

func publicFieldErrors(err error) map[string]any {
	var provider fieldErrorProvider
	if errors.As(err, &provider) && provider != nil {
		return provider.PublicFields()
	}
	return nil
}
