package event

import (
	"context"
	"fmt"
	"runtime/debug"
	"strings"
	"sync"

	"github.com/prismgo/framework/exception"
	"github.com/prismgo/framework/routine"
)

// Dispatcher 是并发安全的事件总线，负责按事件名分发监听器。
type Dispatcher struct {
	mu       sync.RWMutex
	handlers map[string][]registeredListener
	wildcard []wildcardEntry
}

type registeredListener struct {
	id       string
	listener Listener
}

type wildcardEntry struct {
	pattern  string
	prefix   string
	listener registeredListener
}

// New 创建新的事件总线。
func New() *Dispatcher {
	return &Dispatcher{handlers: make(map[string][]registeredListener)}
}

// Listen 注册监听器。eventName 支持精确名称、"*" 和 "<prefix>.*"。
func (d *Dispatcher) Listen(eventName string, l Listener) {
	if l == nil || eventName == "" {
		return
	}
	registered := registeredListener{id: nextQueuedListenerID(), listener: l}
	if isQueued(l) {
		rememberQueuedListener(registered.id, l)
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	if isWildcard(eventName) {
		d.wildcard = append(d.wildcard, wildcardEntry{
			pattern:  eventName,
			prefix:   strings.TrimSuffix(eventName, "*"),
			listener: registered,
		})
		return
	}
	d.handlers[eventName] = append(d.handlers[eventName], registered)
}

// ListenFunc 注册函数式监听器。
func (d *Dispatcher) ListenFunc(eventName string, fn func(context.Context, Event) error) {
	if fn == nil {
		return
	}
	d.Listen(eventName, ListenerFunc(fn))
}

// Subscribe 让订阅者一次性把多个监听器挂载到总线。
func (d *Dispatcher) Subscribe(s Subscriber) {
	if s == nil {
		return
	}
	s.Subscribe(d)
}

// Forget 移除指定事件名的精确监听器，不影响通配符监听器。
func (d *Dispatcher) Forget(eventName string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.handlers, eventName)
}

// Has 判断指定事件名是否存在精确匹配监听器。
func (d *Dispatcher) Has(eventName string) bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return len(d.handlers[eventName]) > 0
}

// Dispatch 分发事件；同步、Async 和 ShouldQueue 监听器各自按声明执行。
func (d *Dispatcher) Dispatch(ctx context.Context, ev Event) {
	if d == nil || ev == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	for _, item := range d.collect(ev.Name()) {
		switch {
		case isQueued(item.listener):
			dispatchQueuedListener(ctx, item.id, item.listener, ev)
		case isAsync(item.listener):
			routine.Task(ctx, func(ctx context.Context) error {
				return item.listener.Handle(ctx, ev)
			}).
				Component("event").
				Name("listener.async").
				Fields(map[string]any{
					"event": ev.Name(),
					"async": true,
				}).
				Go()
		default:
			d.invokeListener(ctx, ev, item.listener, false)
		}
	}
}

func (d *Dispatcher) collect(name string) []registeredListener {
	d.mu.RLock()
	defer d.mu.RUnlock()

	exact := d.handlers[name]
	if len(d.wildcard) == 0 {
		out := make([]registeredListener, len(exact))
		copy(out, exact)
		return out
	}

	out := make([]registeredListener, 0, len(exact)+len(d.wildcard))
	out = append(out, exact...)
	for _, w := range d.wildcard {
		if matchWildcard(w, name) {
			out = append(out, w.listener)
		}
	}
	return out
}

func (d *Dispatcher) invokeListener(ctx context.Context, ev Event, l Listener, async bool) {
	defer func() {
		if r := recover(); r != nil {
			fields := syncListenerReportFields(ev, async)
			fields["stack"] = string(debug.Stack())
			exception.Report(ctx, fmt.Errorf("event listener panic: %v", r), fields)
		}
	}()

	if err := l.Handle(ctx, ev); err != nil {
		exception.Report(ctx, err, syncListenerReportFields(ev, async))
	}
}

func syncListenerReportFields(ev Event, async bool) map[string]any {
	name := ""
	if ev != nil {
		name = ev.Name()
	}
	return map[string]any{
		"component": "event",
		"subsystem": "dispatcher",
		"event":     name,
		"listener":  "sync",
		"async":     async,
		"status":    500,
	}
}

func isWildcard(name string) bool {
	return name == "*" || strings.HasSuffix(name, ".*")
}

func matchWildcard(w wildcardEntry, name string) bool {
	if w.pattern == "*" {
		return true
	}
	return strings.HasPrefix(name, w.prefix)
}
