package cache

import "context"

// Loader 是 Remember / Flexible 使用的延迟加载函数。
type Loader[T any] func(context.Context) (T, error)

// Fallback 表示 Get 未命中时的默认值策略。
//
// 它可以保存一个立即返回的值，也可以保存一个在未命中时才执行的闭包。
type Fallback[T any] struct {
	value  T
	loader Loader[T]
	lazy   bool
}

// Value 创建一个立即返回的默认值。
func Value[T any](value T) Fallback[T] {
	return Fallback[T]{value: value}
}

// Lazy 创建一个延迟执行的默认值加载函数。
func Lazy[T any](loader Loader[T]) Fallback[T] {
	return Fallback[T]{loader: loader, lazy: true}
}

// resolveDefault 解析泛型 Get 的默认值参数。
func resolveDefault[T any](ctx context.Context, fallback []Fallback[T]) (T, error) {
	if len(fallback) == 0 {
		var zero T
		return zero, ErrCacheMiss
	}
	def := fallback[0]
	if def.lazy && def.loader != nil {
		return def.loader(ctx)
	}
	return def.value, nil
}

// resolveAnyDefault 解析 Repository.Get 的非泛型默认值参数。
//
// 为了兼容 Laravel 风格，它允许调用方传入普通值、无参闭包或带 context 的闭包。
func resolveAnyDefault(ctx context.Context, fallback []any) (any, error) {
	if len(fallback) == 0 {
		return nil, ErrCacheMiss
	}
	def := fallback[0]
	switch fn := def.(type) {
	case func() any:
		return fn(), nil
	case func() (any, error):
		return fn()
	case func(context.Context) any:
		return fn(ctx), nil
	case func(context.Context) (any, error):
		return fn(ctx)
	default:
		return def, nil
	}
}
