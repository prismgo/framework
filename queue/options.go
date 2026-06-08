package queue

import (
	"time"

	cachecontract "github.com/prismgo/framework/contracts/cache"
)

// With appends one or more options to the current option chain.
func (option DispatchOption) With(options ...DispatchOption) DispatchOption {
	return chainDispatchOptions(append([]DispatchOption{option}, options...)...)
}

// OnConnection adds a connection override to the current option chain.
func (option DispatchOption) OnConnection(name string) DispatchOption {
	return option.With(OnConnection(name))
}

// OnQueue adds a queue-name override to the current option chain.
func (option DispatchOption) OnQueue(name string) DispatchOption {
	return option.With(OnQueue(name))
}

// Delay adds a delay override to the current option chain.
func (option DispatchOption) Delay(d time.Duration) DispatchOption {
	return option.With(Delay(d))
}

// DelaySeconds adds a second-based delay override to the current option chain.
func (option DispatchOption) DelaySeconds(seconds int) DispatchOption {
	return option.With(DelaySeconds(seconds))
}

// Tries adds a max-attempts override to the current option chain.
func (option DispatchOption) Tries(n int) DispatchOption {
	return option.With(Tries(n))
}

// MaxExceptions adds a max-exceptions override to the current option chain.
func (option DispatchOption) MaxExceptions(n int) DispatchOption {
	return option.With(MaxExceptions(n))
}

// Timeout adds an execution timeout override to the current option chain.
func (option DispatchOption) Timeout(d time.Duration) DispatchOption {
	return option.With(Timeout(d))
}

// FailOnTimeout enables fail-on-timeout for the current option chain.
func (option DispatchOption) FailOnTimeout() DispatchOption {
	return option.With(FailOnTimeout())
}

// Encrypt enables payload encryption for the current option chain.
func (option DispatchOption) Encrypt() DispatchOption {
	return option.With(Encrypt())
}

// Tags 为当前投递选项链追加 Horizon 可展示的安全标签。
//
// 参数说明：values 是调用方显式提供的标签列表；空白标签会被忽略，重复标签会被去重。
// 设计思路：tags 必须在入队前确定并随 envelope 流转，Horizon 不会反序列化 payload
// 或反射业务模型来推导标签。
func (option DispatchOption) Tags(values ...string) DispatchOption {
	return option.With(Tags(values...))
}

// Silenced 将当前投递标记为 Horizon 默认列表静默。
//
// 逻辑说明：该标记只影响 Horizon 默认展示过滤，不影响 processed/failed 等聚合计数。
func (option DispatchOption) Silenced() DispatchOption {
	return option.With(Silenced())
}

// Backoff adds retry backoff values to the current option chain.
func (option DispatchOption) Backoff(values ...time.Duration) DispatchOption {
	return option.With(Backoff(values...))
}

// RetryUntil adds a retry deadline to the current option chain.
func (option DispatchOption) RetryUntil(t time.Time) DispatchOption {
	return option.With(RetryUntil(t))
}

// Unique adds a unique-job key and TTL to the current option chain.
func (option DispatchOption) Unique(key string, ttl time.Duration) DispatchOption {
	return option.With(Unique(key, ttl))
}

// UniqueVia adds a unique-job cache store to the current option chain.
func (option DispatchOption) UniqueVia(store cachecontract.Repository) DispatchOption {
	return option.With(UniqueVia(store))
}

// UniqueUntilProcessing releases the unique lock before processing starts.
func (option DispatchOption) UniqueUntilProcessing() DispatchOption {
	return option.With(UniqueUntilProcessing())
}

// Debounce adds a debounce key and window to the current option chain.
func (option DispatchOption) Debounce(key string, ttl time.Duration) DispatchOption {
	return option.With(Debounce(key, ttl))
}

// DebounceVia adds a debounce cache store to the current option chain.
func (option DispatchOption) DebounceVia(store cachecontract.Repository) DispatchOption {
	return option.With(DebounceVia(store))
}

func chainDispatchOptions(options ...DispatchOption) DispatchOption {
	return func(o *DispatchOptions) {
		for _, option := range options {
			if option != nil {
				option(o)
			}
		}
	}
}
