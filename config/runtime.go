package config

import (
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"sync"

	"github.com/spf13/cast"
	"github.com/spf13/viper"

	"github.com/prismgo/framework/support"
)

const defaultEnvFile = ".env"

// Func 定义单个配置文件的加载函数，返回类似 Laravel config 文件的层级结构。
type Func func() map[string]any

var (
	// registryMu 保护配置注册表，避免并发注册时发生竞态。
	registryMu sync.RWMutex
	// registry 保存所有通过 Add 注册的配置文件加载函数。
	registry = make(map[string]Func)

	// initMu 确保配置文件加载过程串行执行，避免多个构建过程共享环境读取上下文。
	initMu sync.Mutex

	// envLoaderMu 保护当前构建阶段使用的环境读取器。
	envLoaderMu sync.RWMutex
	// currentEnvLoader 仅在构建 Config 期间临时生效，供 Env 在配置注册函数中读取值。
	currentEnvLoader *viper.Viper
)

// Add 注册一个配置命名空间，通常由 config 目录下的配置文件在 init 中调用。
func Add(name string, configFn Func) {
	registryMu.Lock()
	defer registryMu.Unlock()
	registry[name] = configFn
}

// NewFromDefaultFile 从项目根目录 .env 构建一个新的 Config 实例。
func NewFromDefaultFile() (*Config, error) {
	return NewFromFile(support.BasePath(defaultEnvFile))
}

// NewFromFile 从指定 .env 文件安全构建一个新的 Config 实例。
func NewFromFile(path string) (*Config, error) {
	initMu.Lock()
	defer initMu.Unlock()

	v := newViper()
	if err := readEnvFile(v, path); err != nil {
		return nil, err
	}

	return &Config{store: loadRuntimeConfig(v)}, nil
}

// Env 读取单个环境变量，并在缺失时返回给定默认值。
func Env(envName string, defaultValue ...any) any {
	fallback := firstDefault(defaultValue...)
	if strings.TrimSpace(envName) == "" {
		return fallback
	}
	if value, ok := readEnvValue(activeEnvLoader(), envName, fallback); ok {
		return value
	}
	return fallback
}

// loadRuntimeConfig 根据已注册的配置文件构建运行时配置仓库。
func loadRuntimeConfig(v *viper.Viper) map[string]any {
	loaders := snapshotRegistry()
	store := make(map[string]any, len(loaders))

	setCurrentEnvLoader(v)
	defer clearCurrentEnvLoader()

	for name, loader := range loaders {
		store[name] = normalizeValue(loader())
	}
	return store
}

// snapshotRegistry 复制当前注册表，避免加载过程中受到后续注册影响。
func snapshotRegistry() map[string]Func {
	registryMu.RLock()
	defer registryMu.RUnlock()

	loaders := make(map[string]Func, len(registry))
	for name, loader := range registry {
		loaders[name] = loader
	}
	return loaders
}

// setCurrentEnvLoader 暂存正在构建仓库时使用的环境读取器。
func setCurrentEnvLoader(v *viper.Viper) {
	envLoaderMu.Lock()
	defer envLoaderMu.Unlock()
	currentEnvLoader = v
}

// clearCurrentEnvLoader 在配置仓库构建结束后清空临时读取器。
func clearCurrentEnvLoader() {
	envLoaderMu.Lock()
	defer envLoaderMu.Unlock()
	currentEnvLoader = nil
}

// activeEnvLoader 返回当前用于 Env 读取的 viper 实例。
func activeEnvLoader() *viper.Viper {
	envLoaderMu.RLock()
	defer envLoaderMu.RUnlock()
	return currentEnvLoader
}

// readEnvValue 从 viper 或系统环境中读取值，并尽量按默认值类型做转换。
func readEnvValue(v *viper.Viper, envName string, defaultValue any) (any, bool) {
	if v != nil && isProvided(v, envName) {
		value := v.Get(envName)
		if !isBlankValue(value) {
			return castEnvValue(value, defaultValue), true
		}
	}

	value, ok := os.LookupEnv(envName)
	if !ok || strings.TrimSpace(value) == "" {
		return nil, false
	}
	return castEnvValue(value, defaultValue), true
}

// castEnvValue 按默认值类型把环境变量转换为更稳定的 Go 类型。
func castEnvValue(value any, defaultValue any) any {
	switch defaultValue.(type) {
	case bool:
		return cast.ToBool(value)
	case int:
		return cast.ToInt(value)
	case int64:
		return cast.ToInt64(value)
	case uint:
		return cast.ToUint(value)
	case float64:
		return cast.ToFloat64(value)
	case string:
		return cast.ToString(value)
	default:
		if defaultValue != nil {
			return castByKind(value, reflect.TypeOf(defaultValue).Kind())
		}
		return value
	}
}

// castByKind 为常见标量类型提供统一转换。
func castByKind(value any, kind reflect.Kind) any {
	switch kind {
	case reflect.Bool:
		return cast.ToBool(value)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return cast.ToInt64(value)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return cast.ToUint64(value)
	case reflect.Float32, reflect.Float64:
		return cast.ToFloat64(value)
	case reflect.String:
		return cast.ToString(value)
	default:
		return value
	}
}

// normalizeValue 把嵌套 map 统一整理为 map[string]any，便于 Config 按点路径读取。
func normalizeValue(value any) any {
	if value == nil {
		return nil
	}

	rv := reflect.ValueOf(value)
	switch rv.Kind() {
	case reflect.Map:
		if rv.Type().Key().Kind() != reflect.String {
			return value
		}
		result := make(map[string]any, rv.Len())
		for _, key := range rv.MapKeys() {
			result[key.String()] = normalizeValue(rv.MapIndex(key).Interface())
		}
		return result
	case reflect.Slice, reflect.Array:
		result := make([]any, rv.Len())
		for i := 0; i < rv.Len(); i++ {
			result[i] = normalizeValue(rv.Index(i).Interface())
		}
		return result
	default:
		return value
	}
}

// newViper 构建用于读取 .env 文件的 viper 实例。
func newViper() *viper.Viper {
	v := viper.New()
	v.SetConfigType("env")
	v.AutomaticEnv()
	return v
}

// readEnvFile 读取 .env 文件，文件不存在时允许继续使用默认值和系统环境变量。
func readEnvFile(v *viper.Viper, path string) error {
	v.SetConfigFile(path)
	if err := v.ReadInConfig(); err != nil {
		var notFound viper.ConfigFileNotFoundError
		if errors.As(err, &notFound) {
			return nil
		}
		var pathErr *os.PathError
		if errors.As(err, &pathErr) && os.IsNotExist(pathErr) {
			return nil
		}
		return fmt.Errorf("read config file %s: %w", path, err)
	}
	return nil
}

// isProvided 判断配置项是否由 .env 文件或系统环境变量显式提供。
func isProvided(v *viper.Viper, key string) bool {
	if _, ok := os.LookupEnv(key); ok {
		return true
	}
	return v.InConfig(key)
}

// isBlankValue 判断配置值是否为空白字符串或 nil。
func isBlankValue(value any) bool {
	if value == nil {
		return true
	}

	switch value.(type) {
	case bool,
		int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64, uintptr,
		float32, float64:
		return false
	}
	if str, ok := value.(string); ok {
		return strings.TrimSpace(str) == ""
	}
	return support.Empty(value)
}

// firstDefault 安全读取可选默认值的第一个元素。
func firstDefault(values ...any) any {
	if len(values) == 0 {
		return nil
	}
	return values[0]
}
