package event

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prismgo/framework/container"
	eventcontract "github.com/prismgo/framework/contracts/event"
	queuecontract "github.com/prismgo/framework/contracts/queue"
	"github.com/prismgo/framework/exception"
	"github.com/prismgo/framework/internal/stackx"
)

// ShouldQueue 标记监听器应通过 queue worker 异步执行。
type ShouldQueue = eventcontract.ShouldQueue

// QueueOptionsProvider 允许队列监听器声明连接、队列、延迟和重试策略。
type QueueOptionsProvider interface {
	QueueConnection() string
	QueueName() string
	QueueDelay() time.Duration
	QueueTries() int
	QueueBackoff() []time.Duration
	QueueTimeout() time.Duration
}

// QueuedListenerJobRegistry 是 queue.Registry 暴露给 event 的最小注册能力。
type QueuedListenerJobRegistry interface {
	RegisterJobType(queuecontract.Job)
}

// RegisterQueuedListenerJobs 注册 queued listener 内部 Job，供独立 worker 恢复 payload。
func RegisterQueuedListenerJobs(registry QueuedListenerJobRegistry) {
	if registry == nil {
		return
	}
	registry.RegisterJobType(&queuedListenerJob{})
}

// Queued 把普通 Listener 包装为 ShouldQueue 监听器。
func Queued(listener Listener) Listener {
	return queuedWrapper{Listener: listener}
}

type queuedWrapper struct{ Listener }

func (queuedWrapper) ShouldQueue() bool { return true }

var (
	eventFactoriesMu sync.RWMutex
	eventFactories   = map[string]func() Event{}
	queuedMu         sync.RWMutex
	queuedListeners  = map[string]Listener{}
	queuedSequence   atomic.Uint64
	dispatcherMu     sync.RWMutex
	queuedDispatcher queuecontract.Dispatcher
)

// UseQueuedDispatcher 设置 queued listener 使用的队列投递 contract。
//
// 需求背景：event 需要支持 Laravel 风格的 ShouldQueue 监听器，但不能直接依赖
// prismgo/queue 实现包。queue 包或测试可通过该入口注入实现了 contracts/queue.Dispatcher
// 的投递器；Application 场景优先从当前 Facade Registry 的 queue.dispatcher 解析。
//
// 参数说明：dispatcher 为队列投递 contract；传 nil 会清除进程级回退值。
func UseQueuedDispatcher(dispatcher queuecontract.Dispatcher) {
	dispatcherMu.Lock()
	queuedDispatcher = dispatcher
	dispatcherMu.Unlock()
}

// RegisterEvent 注册可被队列监听器反序列化的事件类型。
//
// 需求背景：queued listener 的 payload 只保存事件名和 JSON 内容，跨进程 worker
// 恢复时必须知道事件名对应的具体结构体。泛型注册让调用方只传事件类型，事件名
// 统一从事件自身 Name() 获取，避免 name 与 factory 重复配置后出现不一致。
//
// 设计约定：T 必须是指向具体事件结构体的指针类型，例如 *UserRegistered。
// 注册属于启动期配置，类型错误或空事件名直接 panic，便于尽早暴露编程错误。
func RegisterEvent[T Event]() {
	factory := typedEventFactory[T]()
	ev := factory()
	name := ev.Name()
	if name == "" {
		panic("event: registered event has empty name")
	}

	eventFactoriesMu.Lock()
	eventFactories[name] = factory
	eventFactoriesMu.Unlock()
}

// typedEventFactory 根据泛型事件类型创建队列恢复用 factory。
//
// 设计思路：队列 worker 恢复 payload 时需要拿到一个新的可写事件实例，因此这里只
// 接受指针事件类型，并通过 reflect.New(elemType) 创建同类新实例。这样业务侧只需要
// 在启动阶段写 event.RegisterEvent[*ConcreteEvent]()，不再手写重复的工厂函数。
func typedEventFactory[T Event]() func() Event {
	eventType := reflect.TypeFor[T]()
	if eventType.Kind() != reflect.Pointer {
		panic("event: RegisterEvent[T] requires T to be a pointer event type")
	}

	elemType := eventType.Elem()
	if elemType.Kind() == reflect.Pointer || elemType.Kind() == reflect.Interface {
		panic("event: RegisterEvent[T] requires T to point to a concrete event type")
	}

	return func() Event {
		ev, ok := reflect.New(elemType).Interface().(Event)
		if !ok {
			panic("event: registered type does not implement event.Event")
		}
		return ev
	}
}

func nextQueuedListenerID() string {
	return fmt.Sprintf("listener-%d", queuedSequence.Add(1))
}

func rememberQueuedListener(id string, listener Listener) {
	queuedMu.Lock()
	queuedListeners[id] = listener
	queuedMu.Unlock()
}

func queuedListener(id string) Listener {
	queuedMu.RLock()
	defer queuedMu.RUnlock()
	return queuedListeners[id]
}

func isQueued(listener Listener) bool {
	q, ok := listener.(ShouldQueue)
	return ok && q.ShouldQueue()
}

func dispatchQueuedListener(ctx context.Context, id string, listener Listener, ev Event) {
	payload, err := json.Marshal(ev)
	if err != nil {
		reportQueuedDispatchError(ctx, ev, err)
		return
	}
	job := &queuedListenerJob{ListenerID: id, EventName: ev.Name(), Payload: payload}
	dispatcher := resolveQueuedDispatcher()
	if dispatcher == nil {
		reportQueuedDispatchError(ctx, ev, fmt.Errorf("event queued listener dispatcher is not configured"))
		return
	}
	if _, err := dispatcher.DispatchJob(ctx, job, listenerQueueOptions(listener)); err != nil {
		reportQueuedDispatchError(ctx, ev, err)
	}
}

func resolveQueuedDispatcher() queuecontract.Dispatcher {
	dispatcher, err := container.Make[queuecontract.Dispatcher](queuecontract.DispatcherServiceKey)
	if err == nil && dispatcher != nil {
		return dispatcher
	}
	dispatcherMu.RLock()
	defer dispatcherMu.RUnlock()
	return queuedDispatcher
}

func listenerQueueOptions(listener Listener) queueListenerOptions {
	provider, ok := listener.(QueueOptionsProvider)
	if !ok {
		return queueListenerOptions{}
	}
	return queueListenerOptions{
		connection: provider.QueueConnection(),
		queue:      provider.QueueName(),
		delay:      provider.QueueDelay(),
		tries:      provider.QueueTries(),
		backoff:    provider.QueueBackoff(),
		timeout:    provider.QueueTimeout(),
	}
}

func reportQueuedDispatchError(ctx context.Context, ev Event, err error) {
	name := ""
	if ev != nil {
		name = ev.Name()
	}
	exception.Report(ctx, err, map[string]any{
		"component": "event",
		"subsystem": "queued_listener",
		"event":     name,
		"listener":  "queued",
		"operation": "dispatch",
		"status":    500,
	})
}

type queuedListenerJob struct {
	ListenerID string          `json:"listener_id"`
	EventName  string          `json:"event_name"`
	Payload    json.RawMessage `json:"payload"`
}

type queueListenerOptions struct {
	connection string
	queue      string
	delay      time.Duration
	tries      int
	backoff    []time.Duration
	timeout    time.Duration
}

func (o queueListenerOptions) QueueConnection() string { return o.connection }
func (o queueListenerOptions) QueueName() string       { return o.queue }
func (o queueListenerOptions) QueueDelay() time.Duration {
	return o.delay
}
func (o queueListenerOptions) QueueTries() int { return o.tries }
func (o queueListenerOptions) QueueBackoff() []time.Duration {
	return append([]time.Duration(nil), o.backoff...)
}
func (o queueListenerOptions) QueueTimeout() time.Duration     { return o.timeout }
func (o queueListenerOptions) QueueMaxExceptions() int         { return 0 }
func (o queueListenerOptions) QueueFailOnTimeout() bool        { return false }
func (o queueListenerOptions) QueueEncrypted() bool            { return false }
func (o queueListenerOptions) QueueRetryUntil() time.Time      { return time.Time{} }
func (o queueListenerOptions) QueueBatchID() string            { return "" }
func (o queueListenerOptions) QueueUniqueKey() string          { return "" }
func (o queueListenerOptions) QueueUniqueFor() time.Duration   { return 0 }
func (o queueListenerOptions) QueueUniqueUntil() bool          { return false }
func (o queueListenerOptions) QueueDebounceKey() string        { return "" }
func (o queueListenerOptions) QueueDebounceFor() time.Duration { return 0 }
func (o queueListenerOptions) QueueTags() []string             { return nil }
func (o queueListenerOptions) QueueSilenced() bool             { return false }

func (j *queuedListenerJob) Handle(ctx context.Context) (err error) {
	defer func() {
		if r := recover(); r != nil {
			// 捕获 panic 发生位置的结构化堆栈
			stack := stackx.Capture(0)
			panicErr := fmt.Errorf("event queued listener panic: %v", r)
			err = exception.WithStackTrace(panicErr, stack)
			exception.Report(ctx, err, map[string]any{
				"component":   "event",
				"subsystem":   "queued_listener",
				"event":       j.EventName,
				"listener_id": j.ListenerID,
				"operation":   "handle",
			})
		}
	}()

	listener := queuedListener(j.ListenerID)
	if listener == nil {
		return fmt.Errorf("event queued listener %s is not registered", j.ListenerID)
	}
	ev, err := restoreEvent(j.EventName, j.Payload)
	if err != nil {
		return err
	}
	return listener.Handle(ctx, ev)
}

func restoreEvent(name string, payload json.RawMessage) (Event, error) {
	eventFactoriesMu.RLock()
	factory := eventFactories[name]
	eventFactoriesMu.RUnlock()
	if factory == nil {
		return rawQueuedEvent{name: name, payload: payload}, nil
	}
	ev := factory()
	if ev == nil {
		return nil, fmt.Errorf("event factory for %s returned nil", name)
	}
	if err := json.Unmarshal(payload, ev); err != nil {
		return nil, err
	}
	return ev, nil
}

type rawQueuedEvent struct {
	name    string
	payload json.RawMessage
}

func (e rawQueuedEvent) Name() string { return e.name }
