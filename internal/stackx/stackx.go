// Package stackx 提供堆栈采集与安全截断能力。
//
// 设计原因：框架多处需要采集 panic 堆栈用于日志和上报，但原始堆栈可能非常大
// （深层递归、复杂调用链），直接写入日志会导致日志爆炸。stackx 统一提供
// 截断保护，确保堆栈大小可控。
package stackx

import (
	"runtime"
	"runtime/debug"
	"strings"
)

const maxStackSize = 4 * 1024 // 4KB

const truncationSuffix = "\n... stack trace truncated ..."

// Capture 采集当前调用位置的结构化堆栈。
// skip 参数表示跳过的帧数（0 表示从调用者开始）。
// 返回的 StackTrace 支持延迟解析和智能过滤。
func Capture(skip int) *StackTrace {
	const maxDepth = 64
	pcs := make([]uintptr, maxDepth)
	
	// skip + 2: 跳过 runtime.Callers 和 Capture 本身
	n := runtime.Callers(skip+2, pcs)
	if n == 0 {
		return &StackTrace{}
	}
	
	return &StackTrace{
		pcs: pcs[:n],
	}
}

// CaptureBytes 采集当前调用位置的堆栈并截断到安全大小。
// Deprecated: 使用 Capture(skip int) *StackTrace 代替，支持结构化堆栈和智能过滤。
func CaptureBytes() []byte {
	return truncate(debug.Stack())
}

// DefaultFilter 返回默认的堆栈帧过滤函数。
// 过滤规则：
// - 过滤 runtime.* 帧
// - 过滤 internal/stackx.* 帧
// - 过滤 exception.Report 相关帧
// 保留框架业务代码帧和所有业务代码帧。
func DefaultFilter() func(StackFrame) bool {
	return func(frame StackFrame) bool {
		// 过滤 runtime 帧
		if strings.HasPrefix(frame.Function, "runtime.") {
			return false
		}
		
		// 过滤 stackx 自身帧
		if strings.Contains(frame.File, "internal/stackx") {
			return false
		}
		
		// 过滤 exception.Report 相关帧
		if strings.Contains(frame.Function, "exception.(*Handler).Report") ||
		   strings.Contains(frame.Function, "exception.Report") {
			return false
		}
		
		return true
	}
}

// truncate 限制堆栈信息大小，防止日志爆炸。
// 超过 maxStackSize 的堆栈会被截断，并添加截断提示。
// 截断点会向前回退到最近的换行符，避免截断到行中间。
func truncate(stack []byte) []byte {
	if len(stack) <= maxStackSize {
		return stack
	}
	// 向前回退到最近的换行符
	cut := maxStackSize
	for cut > 0 && stack[cut-1] != '\n' {
		cut--
	}
	suffix := []byte(truncationSuffix)
	truncated := make([]byte, cut+len(suffix))
	copy(truncated, stack[:cut])
	copy(truncated[cut:], suffix)
	return truncated
}
