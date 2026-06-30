// Package stackx 提供堆栈采集与安全截断能力。
//
// 设计原因：框架多处需要采集 panic 堆栈用于日志和上报，但原始堆栈可能非常大
// （深层递归、复杂调用链），直接写入日志会导致日志爆炸。stackx 统一提供
// 截断保护，确保堆栈大小可控。
package stackx

import (
	"runtime/debug"
)

const maxStackSize = 4 * 1024 // 4KB

const truncationSuffix = "\n... stack trace truncated ..."

// Capture 采集当前调用位置的堆栈并截断到安全大小。
//
// 返回值保证不超过 maxStackSize + len(truncationSuffix)。
// 超过限制时，截断后的堆栈末尾会附加截断提示。
// 截断点会对齐到最近的换行符，确保输出可读性。
func Capture() []byte {
	return truncate(debug.Stack())
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
