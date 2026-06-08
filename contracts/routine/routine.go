// Package routine 定义 Prismgo 安全协程执行器的公共 contract。
package routine

import "context"

// Task 是 routine 执行的任务函数。
//
// 返回非 nil error 时，默认实现会通过 exception handler 上报异常。
type Task func(context.Context) error

// Builder 以链式调用配置一次安全协程执行。
type Builder interface {
	// Name 设置协程语义名称，例如 "supervisor.loop"。
	Name(name string) Builder

	// Component 设置异常来源组件，例如 "horizon"。
	//
	// 当 Fields 中也包含 component 时，Component 的值优先级最高。
	Component(component string) Builder

	// Fields 设置异常上报附加字段。
	Fields(fields map[string]any) Builder

	// OnPanic 设置 panic recover 后的回调。
	OnPanic(callback func(error)) Builder

	// OnError 设置任务返回 error 后的回调。
	OnError(callback func(error)) Builder

	// Go 启动 goroutine。
	Go()
}

// Runner 是安全协程执行器 contract。
type Runner interface {
	// Task 创建一次待启动的安全协程配置。
	Task(ctx context.Context, task Task) Builder

	// Go 使用默认配置立即启动安全协程。
	Go(ctx context.Context, task Task)
}
