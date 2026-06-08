package event

import (
	"context"

	eventcontract "github.com/prismgo/framework/contracts/event"
	"github.com/prismgo/framework/facade"
)

const serviceKey = "event.dispatcher"

// Resolve 从当前 Application 容器解析 Dispatcher。
func Resolve() eventcontract.Dispatcher {
	return facade.Resolve[eventcontract.Dispatcher](serviceKey)
}

// Dispatch 通过全局 Dispatcher 派发事件。
func Dispatch(ctx context.Context, ev Event) {
	Resolve().Dispatch(ctx, ev)
}

// Listen 通过全局 Dispatcher 注册监听器。
func Listen(eventName string, l Listener) {
	Resolve().Listen(eventName, l)
}

// ListenFunc 通过全局 Dispatcher 注册函数式监听器。
func ListenFunc(eventName string, fn ListenerFunc) {
	Resolve().ListenFunc(eventName, fn)
}

// Subscribe 通过全局 Dispatcher 注册订阅者。
func Subscribe(s Subscriber) {
	Resolve().Subscribe(s)
}

// Forget 通过全局 Dispatcher 移除指定事件名的精确监听器。
func Forget(eventName string) {
	Resolve().Forget(eventName)
}

// Has 判断全局 Dispatcher 是否存在指定事件名的精确监听器。
func Has(eventName string) bool {
	return Resolve().Has(eventName)
}
