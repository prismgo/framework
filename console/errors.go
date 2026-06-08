package console

import (
	"errors"
	"fmt"
	"strings"
)

// ManuallyFailedError 表示命令通过 ctx.Fail/console.Fail 主动失败。
type ManuallyFailedError struct {
	Message string
	Err     error
}

func (e *ManuallyFailedError) Error() string {
	if e == nil {
		return ""
	}
	if strings.TrimSpace(e.Message) != "" {
		return e.Message
	}
	if e.Err != nil {
		return e.Err.Error()
	}
	return "command failed"
}

func (e *ManuallyFailedError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// Fail 构造可被 Kernel 识别的主动失败错误。
func Fail(messageOrErr ...any) error {
	if len(messageOrErr) == 0 {
		return &ManuallyFailedError{Message: "command failed"}
	}
	var parts []string
	var wrapped error
	for _, value := range messageOrErr {
		switch typed := value.(type) {
		case nil:
			continue
		case error:
			if wrapped == nil {
				wrapped = typed
			} else {
				wrapped = errors.Join(wrapped, typed)
			}
			parts = append(parts, typed.Error())
		case string:
			parts = append(parts, typed)
		default:
			parts = append(parts, fmt.Sprint(typed))
		}
	}
	message := strings.TrimSpace(strings.Join(parts, " "))
	if message == "" && wrapped != nil {
		message = wrapped.Error()
	}
	return &ManuallyFailedError{Message: message, Err: wrapped}
}

// IsManualFailure 判断 err 是否为 ctx.Fail/console.Fail 产生的主动失败。
func IsManualFailure(err error) (*ManuallyFailedError, bool) {
	var failed *ManuallyFailedError
	if errors.As(err, &failed) {
		return failed, true
	}
	return nil, false
}
