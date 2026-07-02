package container

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"

	containercontract "github.com/prismgo/framework/contracts/container"
)

// ErrNoCurrentContainer 表示包级 container 入口在没有当前 Application Container 时被调用。
var ErrNoCurrentContainer = errors.New("no current application container")

// currentProvider 保存 foundation 注入的当前容器获取方法。
//
// foundation.Application 是“当前容器是谁”的唯一权威来源。container 包不反向依赖
// foundation，而是通过回调间接获取当前 Application 的容器。
var currentProvider struct {
	mu       sync.RWMutex
	provider func() *Container
}

// SetContainerProvider 注入当前 Application 容器的获取方法。
//
// foundation.NewApplication 创建当前 Application 时注册 provider；Application.CloseContext
// 完成后传入 nil 清空，避免包级 facade 继续解析已经关闭的容器。
func SetProvider(provider func() *Container) {
	currentProvider.mu.Lock()
	defer currentProvider.mu.Unlock()
	currentProvider.provider = provider
}

func current() *Container {
	currentProvider.mu.RLock()
	provider := currentProvider.provider
	currentProvider.mu.RUnlock()
	if provider != nil {
		return provider()
	}
	return nil
}

// WithCloser 配置共享服务的 typed 关闭函数。
//
// 用途：
// Singleton 和 Instance 会把实例交给容器托管；Application 关闭时容器会按注册反序调用
// closer。Bind 返回的是瞬时对象，容器不会保存，也不会关闭。
//
// 示例：
//
//	app.Container().Singleton("cache.manager", newCacheManager,
//		container.WithCloser(func(m *cache.Manager) error {
//			return m.Close()
//		}),
//	)
func WithCloser[T any](closer func(T) error) containercontract.BindingOption {
	return func(binding *containercontract.Binding) {
		if binding == nil || closer == nil {
			return
		}
		binding.Closer = func(_ context.Context, value any) error {
			typed, ok := value.(T)
			if !ok || isNilValue(typed) {
				return nil
			}
			return closer(typed)
		}
	}
}

// WithContextCloser 配置需要关闭 context 的 typed 关闭函数。
//
// 用途：
// 文件、网络连接池、队列消费者等释放操作可能需要遵守 Application.CloseContext 的超时或取消信号。
// 这类服务应使用 WithContextCloser，而不是在 closer 内创建新的后台 context。
func WithContextCloser[T any](closer func(context.Context, T) error) containercontract.BindingOption {
	return func(binding *containercontract.Binding) {
		if binding == nil || closer == nil {
			return
		}
		binding.Closer = func(ctx context.Context, value any) error {
			typed, ok := value.(T)
			if !ok || isNilValue(typed) {
				return nil
			}
			return closer(ctx, typed)
		}
	}
}

// WithCloseGroup 配置共享服务的关闭分组。
//
// 默认分组是 CloseGroupNormal。日志、异常处理器和错误上报 client 应使用
// CloseGroupReporting，让普通资源关闭失败时仍有上报能力可用。
func WithCloseGroup(group CloseGroup) containercontract.BindingOption {
	return func(binding *containercontract.Binding) {
		if binding != nil {
			binding.CloseGroup = group
		}
	}
}

// Make 从当前 Application 容器解析服务。
//
// 包级 facade 快捷入口没有 Application 参数时使用该函数。没有当前容器时返回
// ErrNoCurrentContainer，调用方可以用 errors.Is 判断装配缺失。
func Make[T any](key string) (T, error) {
	c := current()
	if c == nil {
		var zero T
		return zero, newNoCurrentContainerError(key)
	}
	raw, err := c.Make(key)
	if err != nil {
		var zero T
		return zero, err
	}
	typed, ok := raw.(T)
	if !ok {
		var zero T
		return zero, fmt.Errorf("container %q resolved %T, want %T", key, raw, zero)
	}
	return typed, nil
}

// Value 从当前 Application 容器读取已存在的共享实例。
//
// Value 不创建服务；未设置当前容器、服务未解析或类型不匹配时返回 T 的零值。
func Value[T any](key string) T {
	c := current()
	if c == nil {
		var zero T
		return zero
	}
	raw, ok := c.value(key)
	if !ok {
		var zero T
		return zero
	}
	typed, ok := raw.(T)
	if !ok || isNilValue(typed) {
		var zero T
		return zero
	}
	return typed
}

// Close 按注册反序关闭当前 Application 容器里仍 registered 的实例。
func Close(ctx context.Context) error {
	c := current()
	if c == nil {
		return ErrNoCurrentContainer
	}
	return c.Close(ctx)
}

// List 返回当前 Application 容器中所有已知条目的元信息。
//
// 没有当前 Application 容器时返回 nil。
func List() []EntryInfo {
	c := current()
	if c != nil {
		return c.List()
	}
	return nil
}

func isNilValue[T any](value T) bool {
	v := reflect.ValueOf(value)
	if !v.IsValid() {
		return true
	}
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}

func newNoCurrentContainerError(key string) error {
	if key == "" {
		return ErrNoCurrentContainer
	}
	return fmt.Errorf("container %q: %w", key, ErrNoCurrentContainer)
}
