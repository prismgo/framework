package exception

import (
	"errors"

	"github.com/prismgo/framework/internal/stackx"
)

// stackTraceError 包装 error 并携带结构化堆栈信息
type stackTraceError struct {
	err   error
	stack *stackx.StackTrace
}

// Error 返回原始错误消息
func (e *stackTraceError) Error() string {
	return e.err.Error()
}

// Unwrap 返回原始错误，支持 errors.Is/As 链式查找
func (e *stackTraceError) Unwrap() error {
	return e.err
}

// StackTrace 返回结构化堆栈
func (e *stackTraceError) StackTrace() *stackx.StackTrace {
	return e.stack
}

// WithStackTrace 使用已有的结构化堆栈包装错误
// 如果 error 为 nil，返回 nil
// 如果 error 已经携带堆栈，不重复包装，直接返回原 error
func WithStackTrace(err error, stack *stackx.StackTrace) error {
	if err == nil {
		return nil
	}

	// 如果 error 已经携带堆栈，不重复包装
	if _, ok := err.(interface{ StackTrace() *stackx.StackTrace }); ok {
		return err
	}

	return &stackTraceError{
		err:   err,
		stack: stack,
	}
}

// Wrap 包装错误并捕获当前调用位置的堆栈
// 用于在关键边界显式捕获错误堆栈
// 示例：return exception.Wrap(err, "database query failed")
func Wrap(err error, message string) error {
	if err == nil {
		return nil
	}

	// 如果 error 已经携带堆栈，只添加消息包装
	if _, ok := err.(interface{ StackTrace() *stackx.StackTrace }); ok {
		return &wrapError{
			err: err,
			msg: message,
		}
	}

	// skip=1: 跳过 Wrap 函数本身
	stack := stackx.Capture(1)
	return &stackTraceError{
		err: &wrapError{
			err: err,
			msg: message,
		},
		stack: stack,
	}
}

// WithStack 仅为错误附加堆栈，不添加消息
// 等价于 Wrap(err, "")
func WithStack(err error) error {
	if err == nil {
		return nil
	}

	// 如果 error 已经携带堆栈，不重复包装
	if _, ok := err.(interface{ StackTrace() *stackx.StackTrace }); ok {
		return err
	}

	// skip=1: 跳过 WithStack 函数本身
	stack := stackx.Capture(1)
	return &stackTraceError{
		err:   err,
		stack: stack,
	}
}

// wrapError 简单的消息包装错误
type wrapError struct {
	err error
	msg string
}

func (e *wrapError) Error() string {
	if e.msg == "" {
		return e.err.Error()
	}
	return e.msg + ": " + e.err.Error()
}

func (e *wrapError) Unwrap() error {
	return e.err
}

// StackTrace 透传内部 error 的堆栈信息
func (e *wrapError) StackTrace() *stackx.StackTrace {
	var tracer interface{ StackTrace() *stackx.StackTrace }
	if errors.As(e.err, &tracer) {
		return tracer.StackTrace()
	}
	return nil
}

// hasStackTrace 检查 error 是否已携带结构化堆栈
func hasStackTrace(err error) bool {
	var tracer interface{ StackTrace() *stackx.StackTrace }
	return errors.As(err, &tracer)
}
