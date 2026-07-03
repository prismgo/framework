package cookie

import (
	"net/http"
	"sync"
)

type queuedCookie struct {
	cookie Cookie
	order  int
}

// Queue 保存尚未写入响应的 cookie 操作。
//
// 用途：让 handler 可以像 Laravel Cookie facade 一样先排队，再由 middleware 或显式 Flush 统一写出。
// 设计原因：同一请求中可能多次设置同一 cookie，队列按 name/path/domain 去重并以后入值为准。
// 并发说明：Queue 内部使用互斥锁保护，允许同一请求内的辅助逻辑安全追加 cookie。
type Queue struct {
	mu      sync.Mutex
	items   map[Scope]queuedCookie
	order   int
	options []AttachOption
}

// NewQueue 创建空 cookie 队列。
//
// 参数 options 会在 Flush 时应用到每个 cookie，适合统一注入签名器、加密器或测试时钟。
func NewQueue(options ...AttachOption) *Queue {
	return &Queue{
		items:   make(map[Scope]queuedCookie),
		options: append([]AttachOption(nil), options...),
	}
}

// Queue 按 name/path/domain 作用域追加或替换 cookie。
//
// 参数 c 是待写出的 cookie 值对象；当作用域相同时，后加入的 cookie 覆盖先加入的 cookie。
func (q *Queue) Queue(c Cookie) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.ensure()
	q.order++
	q.items[scopeFor(c)] = queuedCookie{cookie: c, order: q.order}
}

// Make 创建 cookie 并立即加入队列。
func (q *Queue) Make(name string, value string, minutes int, options ...Option) Cookie {
	c := New(name, value, minutes, options...)
	q.Queue(c)
	return c
}

// Forever 创建长期 cookie 并立即加入队列。
func (q *Queue) Forever(name string, value string, options ...Option) Cookie {
	c := Forever(name, value, options...)
	q.Queue(c)
	return c
}

// Expire 创建过期 cookie 并立即加入队列。
//
// 参数 options 应包含待删除 cookie 原本使用的 Path/Domain，保证浏览器能匹配并清除。
func (q *Queue) Expire(name string, options ...Option) Cookie {
	c := Expire(name, options...)
	q.Queue(c)
	return c
}

// Forget 是 Expire 的语义化别名。
func (q *Queue) Forget(name string, options ...Option) Cookie {
	return q.Expire(name, options...)
}

// Queued 返回指定 name 和可选作用域下的队列项。
//
// 参数 scope 省略时使用默认 Path=/；返回值 ok=false 表示当前没有匹配项。
func (q *Queue) Queued(name string, scope ...Scope) (Cookie, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.items == nil {
		return Cookie{}, false
	}
	c, ok := q.items[scopeKey(name, scope...)]
	return c.cookie, ok
}

// HasQueued 判断指定 cookie 是否已排队。
func (q *Queue) HasQueued(name string, scope ...Scope) bool {
	_, ok := q.Queued(name, scope...)
	return ok
}

// Unqueue 移除指定 name 和可选作用域下的队列项。
func (q *Queue) Unqueue(name string, scope ...Scope) {
	q.mu.Lock()
	defer q.mu.Unlock()
	delete(q.items, scopeKey(name, scope...))
}

// Flush 将队列中的 cookie 写入响应，并在全部成功后清空队列。
//
// 参数 w 是目标响应写入器。若任意 cookie 写出失败，函数返回错误且保留队列，便于调用方排查。
func (q *Queue) Flush(w http.ResponseWriter) error {
	q.mu.Lock()
	items := q.orderedLocked()
	options := append([]AttachOption(nil), q.options...)
	q.mu.Unlock()

	for _, item := range items {
		if err := item.cookie.Attach(w, options...); err != nil {
			return err
		}
	}

	q.mu.Lock()
	q.items = make(map[Scope]queuedCookie)
	q.mu.Unlock()
	return nil
}

// ensure 延迟初始化内部 map，支持零值 Queue 可用。
func (q *Queue) ensure() {
	if q.items == nil {
		q.items = make(map[Scope]queuedCookie)
	}
}

// orderedLocked 在已持锁条件下按入队顺序返回队列快照。
//
// 设计思路：map 用于去重，order 用于保持最终写出顺序可预测。
func (q *Queue) orderedLocked() []queuedCookie {
	items := make([]queuedCookie, 0, len(q.items))
	for _, item := range q.items {
		items = append(items, item)
	}
	for i := 1; i < len(items); i++ {
		item := items[i]
		j := i - 1
		for ; j >= 0 && items[j].order > item.order; j-- {
			items[j+1] = items[j]
		}
		items[j+1] = item
	}
	return items
}

// scopeFor 根据 cookie 的 name/path/domain 生成队列去重键。
func scopeFor(c Cookie) Scope {
	path := c.Path
	if path == "" {
		path = DefaultPath
	}
	return Scope{Name: c.Name, Path: path, Domain: c.Domain}
}

// scopeKey 根据查询参数生成队列查找键。
func scopeKey(name string, scope ...Scope) Scope {
	key := Scope{Name: name, Path: DefaultPath}
	if len(scope) > 0 {
		key.Path = scope[0].Path
		key.Domain = scope[0].Domain
	}
	return key
}
