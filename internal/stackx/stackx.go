// Package stackx 提供堆栈采集与安全截断能力。
//
// 设计原因：框架多处需要采集 panic 堆栈用于日志和上报，但原始堆栈可能非常大
// （深层递归、复杂调用链），直接写入日志会导致日志爆炸。stackx 统一提供
// 截断保护，确保堆栈大小可控。
package stackx

import (
	"runtime"
	"strings"
)

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

// defaultFilterFunc 是 DefaultFilter 返回的共享过滤函数，避免闭包重复分配。
// 过滤规则：
// - 过滤 runtime.* 帧
// - 过滤 testing.* 帧（测试框架基础设施）
// - 过滤 internal/stackx.* 帧
// - 过滤 exception.Report 相关帧
// - 过滤 logger.* 帧（日志记录基础设施）
// - 过滤 sirupsen/logrus.* 帧（第三方日志库）
// 保留框架业务代码帧和所有业务代码帧。
var defaultFilterFunc = func(frame StackFrame) bool {
	// 过滤 runtime 帧
	if strings.HasPrefix(frame.Function, "runtime.") {
		return false
	}

	// 过滤 testing 帧（测试框架基础设施），但保留测试函数本身
	// 只过滤 testing.tRunner, testing.(*T).Run 等基础设施代码
	if frame.Function == "testing.tRunner" ||
		strings.HasPrefix(frame.Function, "testing.(*T).") {
		return false
	}

	// 过滤 stackx 自身帧，但保留测试函数（通过检查文件路径是否为 _test.go）
	if strings.Contains(frame.File, "/internal/stackx/") && !strings.HasSuffix(frame.File, "_test.go") {
		return false
	}

	// 过滤 exception.Report 相关帧（合并检查以提高效率）
	if strings.HasPrefix(frame.Function, "github.com/prismgo/framework/exception.") &&
		strings.Contains(frame.Function, ".Report") {
		return false
	}

	// 过滤 logger 包的帧（日志记录基础设施），但保留测试函数（通过检查文件路径是否为 _test.go）
	// 合并 logger 和 logrus 的检查
	if !strings.HasSuffix(frame.File, "_test.go") &&
		(strings.HasPrefix(frame.Function, "github.com/prismgo/framework/logger.") || strings.HasPrefix(frame.Function, "github.com/sirupsen/logrus")) {
		return false
	}

	return true
}

// CaptureBytes 采集当前调用位置的堆栈并截断到安全大小。
// Deprecated: 使用 Capture(0).Filter(nil).Format() 代替。
func CaptureBytes() []byte {
	return []byte(Capture(0).Filter(nil).Format())
}

// DefaultFilter 返回默认的堆栈帧过滤函数（共享实例）。
// 多次调用返回同一个函数，避免闭包重复分配。
func DefaultFilter() func(StackFrame) bool {
	return defaultFilterFunc
}
