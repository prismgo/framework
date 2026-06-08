package cache

import (
	"context"
	"encoding/json"
	"time"

	cachecontract "github.com/prismgo/framework/contracts/cache"
	"github.com/prismgo/framework/routine"
)

// FlexibleWindow 描述热点缓存的 fresh/stale 两段窗口。
type FlexibleWindow = cachecontract.FlexibleWindow

// flexibleEntry 是 Flexible 写入缓存的包裹结构。
//
// WrittenAt 用于判断当前缓存处于 fresh、stale 还是过期重算阶段。
type flexibleEntry struct {
	Value     json.RawMessage `json:"value"`
	WrittenAt int64           `json:"written_at"`
}

// flexibleTyped 实现 fresh/stale 两段式热点缓存读取。
func flexibleTyped[T any](ctx context.Context, repo *Repository, key string, window FlexibleWindow, loader Loader[T]) (T, error) {
	if window.Fresh < 0 {
		window.Fresh = 0
	}
	if window.Stale <= 0 {
		window.Stale = window.Fresh
	}
	if window.Stale < window.Fresh {
		window.Stale = window.Fresh
	}

	now := time.Now()
	data, err := repo.getTyped(ctx, key)
	if err == nil {
		var entry flexibleEntry
		if decErr := repo.decode(data, &entry); decErr == nil {
			var value T
			if decErr := repo.decode(entry.Value, &value); decErr != nil {
				var zero T
				return zero, decErr
			}
			age := now.Sub(time.Unix(0, entry.WrittenAt))
			if age < window.Fresh {
				return value, nil
			}
			if age < window.Stale {
				scheduleRefreshTyped(ctx, repo, key, window, loader)
				return value, nil
			}
		}
	} else if !isMiss(err) {
		var zero T
		return zero, err
	}

	return refreshFlexibleTyped(ctx, repo, key, window, loader)
}

// scheduleRefreshTyped 安排 stale 期缓存的后台刷新任务。
//
// HTTP 请求中会把刷新任务加入 deferred 队列；非 HTTP 场景退化为 goroutine。
func scheduleRefreshTyped[T any](ctx context.Context, repo *Repository, key string, window FlexibleWindow, loader Loader[T]) {
	if !repo.startRefresh(key) {
		return
	}
	task := func() {
		defer repo.finishRefresh(key)
		timeout := repo.manager.refreshTimeout
		runCtx := context.Background()
		if timeout > 0 {
			var cancel context.CancelFunc
			runCtx, cancel = context.WithTimeout(runCtx, timeout)
			defer cancel()
		}
		_, _ = refreshFlexibleTyped(runCtx, repo, key, window, loader)
	}
	if q := deferredFromContext(ctx); q != nil {
		q.Push(task)
		return
	}
	routine.Task(context.Background(), func(context.Context) error {
		task()
		return nil
	}).
		Component("cache").
		Name("flexible.refresh").
		Fields(map[string]any{"key": key}).
		Go()
}

// refreshFlexibleTyped 同步执行 loader，并把带写入时间的 flexibleEntry 写回缓存。
func refreshFlexibleTyped[T any](ctx context.Context, repo *Repository, key string, window FlexibleWindow, loader Loader[T]) (T, error) {
	value, err := loader(ctx)
	if err != nil {
		var zero T
		return zero, err
	}
	payload, err := repo.encode(value)
	if err != nil {
		var zero T
		return zero, err
	}
	entry := flexibleEntry{
		Value:     payload,
		WrittenAt: time.Now().UnixNano(),
	}
	data, err := repo.encode(entry)
	if err != nil {
		var zero T
		return zero, err
	}
	if err := repo.putEncoded(ctx, key, data, window.Stale); err != nil {
		var zero T
		return zero, err
	}
	return value, nil
}
