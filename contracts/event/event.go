// Package event 定义事件总线的公共契约。
//
// 本包只包含接口声明；具体的事件派发、监听器注册、通配符匹配和生命周期事件
// 由 prismgo/event 实现包提供。
package event

import "context"

// Event 是任何事件都必须实现的基础接口。
//
// 用途：暴露稳定的事件名称，供监听器匹配、日志标识和监控聚合使用。
// 使用方式：业务事件结构体实现 Name() 方法即可被 Dispatcher 识别和分发。
//
//	type PrismgoCreated struct { ID uint }
//	func (e PrismgoCreated) Name() string { return "prismgo.created" }
type Event interface {
	// Name 返回事件的稳定名称，如 "prismgo.created"。
	// 该名称用于监听器注册匹配和事件路由。
	Name() string
}

// Listener 是事件监听器的基本契约。
//
// 用途：所有监听器必须实现此接口，框架通过 Handle 方法执行监听逻辑。
// 返回 error 时框架记录日志但不中断其他监听器的执行。
//
// 使用方式：
//
//	type AuditListener struct { svc *AuditService }
//	func (l *AuditListener) Handle(ctx context.Context, ev event.Event) error {
//	    return l.svc.Record(ctx, ev)
//	}
type Listener interface {
	// Handle 处理事件。
	//
	// 参数 ctx 是当前请求、命令或 worker 的上下文。
	// 参数 ev 是当前事件负载。
	// 返回非 nil error 时框架记录日志，但不影响其他监听器。
	Handle(ctx context.Context, ev Event) error
}

// ListenerFunc 将普通函数适配为 Listener 接口。
//
// 用途：允许业务代码以闭包形式注册监听器，无需定义独立结构体。
//
// 使用方式：
//
//	dispatcher.ListenFunc("prismgo.created", func(ctx context.Context, ev event.Event) error {
//	    log.Println("prismgo created:", ev.Name())
//	    return nil
//	})
type ListenerFunc func(ctx context.Context, ev Event) error

// Dispatcher 是事件总线的完整契约。
//
// 用途：提供事件的注册、查找、取消监听和分发能力。实现方负责决定同步、异步或
// 队列方式执行监听器。
//
// 使用方式：
//
//	bus := event.NewDispatcher()
//	bus.Listen("user.created", listener)
//	bus.Dispatch(ctx, UserCreatedEvent{ID: 123})
type Dispatcher interface {
	// Listen 为指定事件名注册监听器。
	//
	// 参数 eventName 支持精确匹配、"*" 全匹配和 "prefix.*" 前缀匹配三种模式。
	// 参数 l 是实现 Listener 的监听器，传入 nil 时忽略。
	Listen(eventName string, l Listener)

	// ListenFunc 为指定事件名注册函数式监听器。
	//
	// 参数 eventName 的匹配规则与 Listen 相同。
	// 参数 fn 是监听器函数，传入 nil 时忽略。
	ListenFunc(eventName string, fn func(context.Context, Event) error)

	// Subscribe 让订阅者一次性注册多个监听器。
	//
	// 参数 s 是实现 Subscriber 的对象，会通过 Subscribe 方法将多个监听器挂载到
	// 同一个 Dispatcher 上。传入 nil 时忽略。
	Subscribe(s Subscriber)

	// Forget 移除指定事件名的所有精确匹配监听器。
	//
	// 参数 eventName 必须是精确事件名；该方法不影响通配符监听器。
	Forget(eventName string)

	// Has 判断指定事件名是否存在精确匹配的监听器。
	//
	// 返回 true 表示至少存在一个监听器；该方法不检查通配符匹配。
	Has(eventName string) bool

	// Dispatch 向所有匹配的监听器分发事件。
	//
	// 参数 ctx 是当前调用链上下文，为 nil 时回退到 context.Background。
	// 参数 ev 是待分发的事件负载，为 nil 时忽略。
	// 同步监听器在当前 goroutine 执行；ShouldQueue 监听器投递到队列；
	// Async 监听器在独立 goroutine 中执行。
	Dispatch(ctx context.Context, ev Event)
}

// Subscriber 是批量注册监听器的契约。
//
// 用途：让一个对象可以一次性把多个监听器挂载到事件总线，避免散落的 Listen 调用。
//
// 使用方式：
//
//	type AuditSubscriber struct { svc *AuditService }
//	func (s *AuditSubscriber) Subscribe(d event.Dispatcher) {
//	    d.ListenFunc("prismgo.created", s.onPrismgoCreated)
//	    d.ListenFunc("prismgo.assigned", s.onPrismgoAssigned)
//	}
type Subscriber interface {
	// Subscribe 将当前订阅者关心的所有监听器注册到事件总线。
	Subscribe(dispatcher Dispatcher)
}

// ShouldQueue 标记监听器应通过队列异步执行。
//
// 用途：实现该接口的监听器在事件分发时不会被同步执行，而是序列化后投递到队列
// 由 worker 消费。
//
// 使用方式：
//
//	type SendEmailListener struct{}
//	func (l *SendEmailListener) Handle(ctx context.Context, ev event.Event) error { ... }
//	func (l *SendEmailListener) ShouldQueue() bool { return true }
type ShouldQueue interface {
	Listener
	// ShouldQueue 返回 true 时监听器将投递到队列执行。
	ShouldQueue() bool
}
