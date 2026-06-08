// Package event 提供 Laravel 风格的事件订阅与派发能力。
//
// 设计目标：
//   - 零业务耦合，可跨项目复用；仅依赖标准库与 logrus。
//   - 同步派发为主，单个监听器可声明 Async() 切换为 goroutine 异步执行。
//   - 监听器之间互相隔离：任一监听器 panic 或返回 error 都不会影响其余监听器。
//   - 支持精确事件名匹配，以及 "*"（全匹配）与 "<前缀>.*"（前缀匹配）两种通配符。
//
// 典型用法：
//
//	bus := event.New()
//	bus.ListenFunc("prismgo.created", func(ctx context.Context, ev event.Event) error {
//	    log.Println("prismgo created:", ev)
//	    return nil
//	})
//	bus.Dispatch(ctx, MyEvent{ID: 123})
package event

import (
	"context"

	eventcontract "github.com/prismgo/framework/contracts/event"
)

// Event 是任何事件的最小契约：暴露稳定的事件名，供订阅匹配使用。
// 业务事件类型实现该接口即可被 Dispatcher 识别。
type Event = eventcontract.Event

// Listener 处理事件；返回 error 时只记录日志，不会中断其他监听器。
type Listener = eventcontract.Listener

// ListenerFunc 把普通函数适配为 Listener，便于业务侧用闭包形式注册。
type ListenerFunc func(ctx context.Context, ev Event) error

// Handle 实现 Listener 接口。
func (f ListenerFunc) Handle(ctx context.Context, ev Event) error {
	return f(ctx, ev)
}

// AsyncListener 是可选的标记接口：实现并返回 true 时，Dispatcher 会在独立 goroutine
// 中执行该监听器，panic 与 error 由 Dispatcher 捕获并写日志。适用于通知、推送等
// 不希望阻塞业务主流程的副作用。
type AsyncListener interface {
	Listener
	Async() bool
}

// Async 是一个便捷适配：把任意 ListenerFunc 包装成异步监听器。
//
//	bus.Listen("user.registered", event.Async(func(ctx context.Context, ev event.Event) error {
//	    return sendWelcomeEmail(ctx, ev)
//	}))
func Async(fn ListenerFunc) Listener {
	return asyncFunc(fn)
}

type asyncFunc ListenerFunc

func (a asyncFunc) Handle(ctx context.Context, ev Event) error {
	return a(ctx, ev)
}

func (a asyncFunc) Async() bool { return true }

// isAsync 判定监听器是否声明为异步执行。
func isAsync(l Listener) bool {
	a, ok := l.(AsyncListener)
	return ok && a.Async()
}
