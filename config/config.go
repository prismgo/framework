package config

import (
	"strings"
	"sync"

	"github.com/spf13/cast"
)

// Config 封装运行时配置访问能力，适合显式依赖注入与 facade 共用。
// 每个 Config 实例持有独立配置仓库，读取与重载都在实例边界内完成。
type Config struct {
	mu    sync.RWMutex
	store map[string]any
}

// New 创建一个空的配置访问器实例。
func New() *Config {
	return &Config{store: make(map[string]any)}
}

const maxCloneDepth = 128

// Clone 复制当前配置实例，返回持有独立仓库副本的新实例。
// 嵌套 map/slice 被深度复制（最大深度 maxCloneDepth 层）；超过深度上限时退化为浅拷贝，
// 但生产配置嵌套深度通常远低于此阈值。
func (c *Config) Clone() *Config {
	if c == nil {
		return New()
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return &Config{store: cloneStore(c.store, maxCloneDepth)}
}

// Empty 返回当前配置实例是否尚未持有任何配置项。
func (c *Config) Empty() bool {
	if c == nil {
		return true
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.store) == 0
}

// Reload 重新从默认 .env 文件加载当前配置实例。
func (c *Config) Reload() error {
	if c == nil {
		return nil
	}
	fresh, err := NewFromDefaultFile()
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.store = fresh.store
	c.mu.Unlock()
	return nil
}

// ReloadFromFile 从指定 .env 文件重载当前配置实例。
func (c *Config) ReloadFromFile(path string) error {
	if c == nil {
		return nil
	}
	fresh, err := NewFromFile(path)
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.store = fresh.store
	c.mu.Unlock()
	return nil
}

// Get 读取字符串配置，缺失时回退到默认值。
func (c *Config) Get(path string, defaultValue ...any) string {
	return cast.ToString(c.valueOrDefault(path, defaultValue...))
}

// GetString 读取字符串配置。
func (c *Config) GetString(path string, defaultValue ...any) string {
	return cast.ToString(c.valueOrDefault(path, defaultValue...))
}

// GetInt 读取整型配置。
func (c *Config) GetInt(path string, defaultValue ...any) int {
	return cast.ToInt(c.valueOrDefault(path, defaultValue...))
}

// GetFloat64 读取 float64 配置。
func (c *Config) GetFloat64(path string, defaultValue ...any) float64 {
	return cast.ToFloat64(c.valueOrDefault(path, defaultValue...))
}

// GetInt64 读取 int64 配置。
func (c *Config) GetInt64(path string, defaultValue ...any) int64 {
	return cast.ToInt64(c.valueOrDefault(path, defaultValue...))
}

// GetUint 读取 uint 配置。
func (c *Config) GetUint(path string, defaultValue ...any) uint {
	return cast.ToUint(c.valueOrDefault(path, defaultValue...))
}

// GetBool 读取布尔配置。
func (c *Config) GetBool(path string, defaultValue ...any) bool {
	return cast.ToBool(c.valueOrDefault(path, defaultValue...))
}

// GetStringMapString 读取 map[string]string 配置。
func (c *Config) GetStringMapString(path string) map[string]string {
	value, ok := c.lookup(path)
	if !ok {
		return map[string]string{}
	}
	return cloneStringMap(cast.ToStringMapString(value))
}

// GetStringMap 读取 map[string]any 配置。
func (c *Config) GetStringMap(path string) map[string]any {
	value, ok := c.lookup(path)
	if !ok {
		return map[string]any{}
	}
	return cloneAnyMap(cast.ToStringMap(value))
}

// valueOrDefault 读取配置项，缺失时回退为调用方提供的默认值。
// 当配置项存在但值为 nil 时，仍返回默认值而非 nil。
func (c *Config) valueOrDefault(path string, defaultValue ...any) any {
	value, ok := c.lookup(path)
	if ok && value != nil {
		return value
	}
	if len(defaultValue) > 0 {
		return defaultValue[0]
	}
	return nil
}

// lookup 按 Laravel 风格点路径从当前配置实例中查找配置值。
// 读锁在整个遍历过程中保持，确保并发安全。
func (c *Config) lookup(path string) (any, bool) {
	if c == nil {
		return nil, false
	}

	path = strings.TrimSpace(path)
	if path == "" {
		return nil, false
	}

	c.mu.RLock()
	defer c.mu.RUnlock()
	if len(c.store) == 0 {
		return nil, false
	}

	current := any(c.store)
	for _, segment := range strings.Split(path, ".") {
		if strings.TrimSpace(segment) == "" {
			return nil, false
		}

		node, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}

		next, exists := node[segment]
		if !exists {
			return nil, false
		}
		current = next
	}
	return current, true
}

func cloneAnyMap(input map[string]any) map[string]any {
	return cloneStore(input, maxCloneDepth)
}

func cloneStringMap(input map[string]string) map[string]string {
	if len(input) == 0 {
		return make(map[string]string)
	}
	result := make(map[string]string, len(input))
	for key, value := range input {
		result[key] = value
	}
	return result
}

// cloneStore 递归复制 map。depth 参数防止过深递归导致栈溢出。
func cloneStore(input map[string]any, depth int) map[string]any {
	if depth <= 0 {
		// 达到最大深度时返回浅拷贝，避免栈溢出。
		result := make(map[string]any, len(input))
		for k, v := range input {
			result[k] = v
		}
		return result
	}
	if len(input) == 0 {
		return make(map[string]any)
	}
	result := make(map[string]any, len(input))
	for key, value := range input {
		result[key] = cloneValue(value, depth-1)
	}
	return result
}

// cloneValue 递归复制值。depth 用于限制 map/slice 的嵌套深度。
func cloneValue(value any, depth int) any {
	if depth <= 0 {
		return value
	}
	switch typed := value.(type) {
	case map[string]any:
		return cloneStore(typed, depth-1)
	case []any:
		result := make([]any, len(typed))
		for i, item := range typed {
			result[i] = cloneValue(item, depth-1)
		}
		return result
	default:
		return typed
	}
}
