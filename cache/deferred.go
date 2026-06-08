package cache

import (
	"context"
	"github.com/prismgo/framework/routine"
	"sync"
)

// deferredContextKey 是挂载请求后置任务队列的 context key。
type deferredContextKey struct{}

// deferredQueue 保存请求结束后需要异步执行的任务。
type deferredQueue struct {
	mu    sync.Mutex
	tasks []func()
}

// Push 把一个任务加入队列；nil 任务会被忽略。
func (q *deferredQueue) Push(task func()) {
	if task == nil {
		return
	}
	q.mu.Lock()
	q.tasks = append(q.tasks, task)
	q.mu.Unlock()
}

// Run 取出当前队列中的所有任务，并分别放入 goroutine 执行。
func (q *deferredQueue) Run() {
	q.mu.Lock()
	tasks := append([]func(){}, q.tasks...)
	q.tasks = nil
	q.mu.Unlock()

	for _, task := range tasks {
		routine.Task(context.Background(), func(context.Context) error {
			task()
			return nil
		}).
			Component("cache").
			Name("deferred.run").
			Go()
	}
}

// deferredFromContext 从 context 中读取请求后置任务队列。
func deferredFromContext(ctx context.Context) *deferredQueue {
	if ctx == nil {
		return nil
	}
	q, _ := ctx.Value(deferredContextKey{}).(*deferredQueue)
	return q
}

// WithDeferred 给任意 context 挂载一个后置任务队列。
//
// 返回的 run 函数需要由调用方在合适时机执行，通常用于非 HTTP 测试或 CLI 场景。
func WithDeferred(ctx context.Context) (context.Context, func()) {
	q := &deferredQueue{}
	return context.WithValue(ctx, deferredContextKey{}, q), q.Run
}
