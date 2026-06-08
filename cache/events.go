package cache

import (
	"context"
	"sync"
)

const (
	// EventCacheRetrieving 在读取缓存前派发。
	EventCacheRetrieving = "cache.retrieving"
	// EventCacheHit 在缓存命中后派发。
	EventCacheHit = "cache.hit"
	// EventCacheMissed 在缓存未命中后派发。
	EventCacheMissed = "cache.missed"
	// EventCacheWriting 在写入缓存前派发。
	EventCacheWriting = "cache.writing"
	// EventCacheWritten 在写入缓存成功后派发。
	EventCacheWritten = "cache.written"
	// EventCacheWriteFailed 在写入缓存失败后派发。
	EventCacheWriteFailed = "cache.write_failed"
	// EventCacheForgetting 在删除缓存前派发。
	EventCacheForgetting = "cache.forgetting"
	// EventCacheForgotten 在删除缓存成功后派发。
	EventCacheForgotten = "cache.forgotten"
	// EventCacheForgetFailed 在删除缓存失败后派发。
	EventCacheForgetFailed = "cache.forget_failed"
	// EventCacheFlushing 在清空缓存前派发。
	EventCacheFlushing = "cache.flushing"
	// EventCacheFlushed 在清空缓存成功后派发。
	EventCacheFlushed = "cache.flushed"
	// EventCacheFlushFailed 在清空缓存失败后派发。
	EventCacheFlushFailed = "cache.flush_failed"
	// EventCacheLocksFlushed 在清理锁成功后派发。
	EventCacheLocksFlushed = "cache.locks_flushed"
	// EventCacheLockFlushFailed 在清理锁失败后派发。
	EventCacheLockFlushFailed = "cache.lock_flush_failed"
	// EventCacheFailedOver 在 failover store 切换到后备 store 时派发。
	EventCacheFailedOver = "cache.failed_over"
)

// CacheEvent 是 prismgo/cache 派发到事件总线的统一事件负载。
//
// Event 保存事件名；Store / Key / Keys / Tags 描述发生操作的缓存范围；
// Error、From 和 To 用于失败与 failover 场景。
type CacheEvent struct {
	Event string
	Store string
	Key   string
	Keys  []string
	Tags  []string
	Error error
	From  string
	To    string
}

// Name 实现 event.Event。
func (e CacheEvent) Name() string {
	return e.Event
}

// EventSink 接收 cache 事件。foundation 会把它桥接到 prismgo/event，避免 cache
// 直接依赖 event 包而与 queue 的缓存能力形成 import cycle。
type EventSink func(context.Context, CacheEvent)

var (
	eventSinkMu sync.RWMutex
	eventSink   EventSink
)

// UseEventSink 设置 cache 事件接收器；传 nil 可关闭事件转发。
func UseEventSink(sink EventSink) {
	eventSinkMu.Lock()
	eventSink = sink
	eventSinkMu.Unlock()
}

func (r *Repository) dispatch(ctx context.Context, name string, payload CacheEvent) {
	if r == nil || !r.events {
		return
	}
	payload.Event = name
	if payload.Store == "" {
		payload.Store = r.name
	}
	dispatchCacheEvent(ctx, name, payload)
}

func dispatchCacheEvent(ctx context.Context, name string, payload CacheEvent) {
	payload.Event = name
	eventSinkMu.RLock()
	sink := eventSink
	eventSinkMu.RUnlock()
	if sink != nil {
		sink(ctx, payload)
	}
}
