package event

import eventcontract "github.com/prismgo/framework/contracts/event"

// Subscriber 让一个对象一次性把它关心的所有事件挂载到 Dispatcher，
// 避免业务侧把多个 Listen 调用散落在不同位置。
//
// 典型实现：
//
//	type AuditSubscriber struct{ svc *AuditService }
//
//	func (s *AuditSubscriber) Subscribe(d event.Dispatcher) {
//	    d.ListenFunc("prismgo.created", s.onPrismgoCreated)
//	    d.ListenFunc("prismgo.assigned", s.onPrismgoAssigned)
//	}
//
//	bus.Subscribe(&AuditSubscriber{svc: auditService})
type Subscriber = eventcontract.Subscriber
