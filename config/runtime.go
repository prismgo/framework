package config

import (
	"errors"
	"fmt"
	"log"
	"os"
	"reflect"
	"strings"
	"sync"

	"github.com/prismgo/framework/internal/runtimex"

	"github.com/spf13/cast"
	"github.com/spf13/viper"

	"github.com/prismgo/framework/support"
)

const defaultEnvFile = ".env"

// maxNormalizeDepth 防止 normalizeValue 递归过深导致栈溢出。
const maxNormalizeDepth = 128

// Func 定义单个配置文件的加载函数，返回类似 Laravel config 文件的层级结构。
type Func func() map[string]any

var (
	// registryMu 保护配置注册表，避免并发注册时发生竞态。
	registryMu sync.RWMutex
	// registry 保存所有通过 Add 注册的配置文件加载函数。
	registry = make(map[string]Func)

	// initMu 确保配置文件加载过程串行执行，避免多个构建过程共享环境读取上下文。
	initMu sync.Mutex

	// envSnapshots 按 goroutine ID 存储当前构造中的环境快照，使并发 NewFromFile 调用互不干扰。
	envSnapshots sync.Map
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
// 先读取 .env 文件并在 initMu 保护下快照注册表，然后释放锁再执行加载函数，
// 避免加载函数中递归调用 NewFromFile 时死锁。
func NewFromFile(path string) (*Config, error) {
	v, loaders, err := prepareNewFromFile(path)
	if err != nil {
		return nil, err
	}

	store := make(map[string]any, len(loaders))
	setCurrentEnvSnapshot(v)
	defer clearCurrentEnvSnapshot()

	for name, loader := range loaders {
		store[name] = normalizeValue(loader(), maxNormalizeDepth)
	}
	return &Config{store: store}, nil
}

// prepareNewFromFile 在 initMu 保护下创建 viper 实例、读取 .env 文件并快照注册表。
// 使用 defer 确保即使在 panic 场景下 initMu 也能释放。
func prepareNewFromFile(path string) (*viper.Viper, map[string]Func, error) {
	initMu.Lock()
	defer initMu.Unlock()

	v := newViper()
	if err := readEnvFile(v, path); err != nil {
		return nil, nil, err
	}
	return v, snapshotRegistry(), nil
}

// Env 读取单个环境变量，并在缺失时返回给定默认值。
func Env(envName string, defaultValue ...any) any {
	fallback := firstDefault(defaultValue...)
	if strings.TrimSpace(envName) == "" {
		return fallback
	}
	if value, ok := readEnvValue(activeEnvSnapshot(), envName, fallback); ok {
		return value
	}
	return fallback
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

// snapshotStack 是 per-goroutine 环境快照的 LIFO 栈，支持递归 NewFromFile 场景。
// 每个 goroutine 通过 envSnapshots (sync.Map) 持有自己的栈实例。
// 添加互斥锁保护，防止 GoroutineID() 返回 0 时多个 goroutine 共享同一栈导致数据竞争。
type snapshotStack struct {
	mu    sync.Mutex
	stack []map[string]any
}

func (s *snapshotStack) Push(snap map[string]any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stack = append(s.stack, snap)
}

func (s *snapshotStack) Pop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.stack) > 0 {
		s.stack = s.stack[:len(s.stack)-1]
	}
}

func (s *snapshotStack) Peek() map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.stack) == 0 {
		return nil
	}
	return s.stack[len(s.stack)-1]
}

// getSnapshotStack 安全地从 envSnapshots 获取 snapshotStack 实例。
// 返回栈实例和是否成功获取的布尔值，避免直接类型断言导致的 panic。
func getSnapshotStack(gid uint64) (*snapshotStack, bool) {
	vRaw, ok := envSnapshots.Load(gid)
	if !ok {
		return nil, false
	}
	stk, ok := vRaw.(*snapshotStack)
	if !ok {
		log.Printf("ERROR: envSnapshots contains non-snapshotStack for gid %d", gid)
		return nil, false
	}
	return stk, true
}

// setCurrentEnvSnapshot 从 viper 实例创建环境键值对快照并推入当前 goroutine 的栈。
// 快照是一次性构建的不可变 map，消除共享 *viper.Viper 带来的并发安全问题。
// 键统一转换为小写以匹配 viper 的大小写不敏感行为。
// 每个 goroutine 持有自己的快照，并发 NewFromFile 调用互不干扰。
func setCurrentEnvSnapshot(v *viper.Viper) {
	keys := v.AllKeys()
	snap := make(map[string]any, len(keys))
	for _, key := range keys {
		snap[strings.ToLower(key)] = v.Get(key)
	}
	gid := runtimex.GoroutineID()
	vRaw, _ := envSnapshots.LoadOrStore(gid, &snapshotStack{})
	if stk, ok := vRaw.(*snapshotStack); ok {
		stk.Push(snap)
	} else {
		log.Printf("ERROR: envSnapshots contains non-snapshotStack for gid %d", gid)
	}
}

// clearCurrentEnvSnapshot 弹出当前 goroutine 的快照栈顶，栈空时删除 sync.Map 条目。
func clearCurrentEnvSnapshot() {
	gid := runtimex.GoroutineID()
	stk, ok := getSnapshotStack(gid)
	if !ok {
		return
	}
	stk.Pop()
	if stk.Peek() == nil {
		envSnapshots.Delete(gid)
	}
}

// activeEnvSnapshot 返回当前 goroutine 快照栈顶的环境键值对快照。
func activeEnvSnapshot() map[string]any {
	gid := runtimex.GoroutineID()
	stk, ok := getSnapshotStack(gid)
	if !ok {
		return nil
	}
	return stk.Peek()
}

// readEnvValue 从环境快照或系统环境中读取值，并尽量按默认值类型做转换。
// envName 在快照查找时统一转为小写，与 viper 的大小写不敏感语义保持一致。
func readEnvValue(snap map[string]any, envName string, defaultValue any) (any, bool) {
	if snap != nil {
		if value, ok := snap[strings.ToLower(envName)]; ok {
			if !isBlankValue(value) {
				return castEnvValue(value, defaultValue), true
			}
		}
	}

	value, ok := os.LookupEnv(envName)
	if !ok || strings.TrimSpace(value) == "" {
		return nil, false
	}
	return castEnvValue(value, defaultValue), true
}

// castEnvValue 按默认值类型把环境变量转换为更稳定的 Go 类型。
// 已覆盖所有内置标量类型的窄变体，确保返回类型与 defaultValue 一致。
func castEnvValue(value any, defaultValue any) any {
	switch defaultValue.(type) {
	case bool:
		return cast.ToBool(value)
	case int:
		return cast.ToInt(value)
	case int8:
		return int8(cast.ToInt(value))
	case int16:
		return int16(cast.ToInt(value))
	case int32:
		return int32(cast.ToInt(value))
	case int64:
		return cast.ToInt64(value)
	case uint:
		return cast.ToUint(value)
	case uint8:
		return uint8(cast.ToUint(value))
	case uint16:
		return uint16(cast.ToUint(value))
	case uint32:
		return uint32(cast.ToUint(value))
	case uint64:
		return cast.ToUint64(value)
	case float32:
		return float32(cast.ToFloat64(value))
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
// depth 参数用于限制递归深度。
func normalizeValue(value any, depth int) any {
	if value == nil || depth <= 0 {
		return value
	}

	rv := reflect.ValueOf(value)
	switch rv.Kind() {
	case reflect.Map:
		if rv.Type().Key().Kind() != reflect.String {
			return value
		}
		result := make(map[string]any, rv.Len())
		for _, key := range rv.MapKeys() {
			result[key.String()] = normalizeValue(rv.MapIndex(key).Interface(), depth-1)
		}
		return result
	case reflect.Slice, reflect.Array:
		result := make([]any, rv.Len())
		for i := 0; i < rv.Len(); i++ {
			result[i] = normalizeValue(rv.Index(i).Interface(), depth-1)
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

// readEnvFile 读取 .env 文件，文件不存在时记录 warning 并允许继续使用默认值和系统环境变量。
func readEnvFile(v *viper.Viper, path string) error {
	v.SetConfigFile(path)
	if err := v.ReadInConfig(); err != nil {
		// .env 文件不存在的两种报告方式（ConfigFileNotFoundError / os.PathError）统一处理。
		var notFound viper.ConfigFileNotFoundError
		var pathErr *os.PathError
		if errors.As(err, &notFound) || (errors.As(err, &pathErr) && os.IsNotExist(pathErr)) {
			log.Printf("WARNING: .env file not found at %s, using system environment only", path)
			return nil
		}
		return fmt.Errorf("read config file %s: %w", path, err)
	}
	return nil
}

// isBlankValue 判断配置值是否为空白字符串或 nil。
// 使用 reflect.Kind 而非类型 switch，以正确匹配命名类型（如 type Port int）。
func isBlankValue(value any) bool {
	if value == nil {
		return true
	}

	rv := reflect.ValueOf(value)
	switch rv.Kind() {
	case reflect.Bool,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
		reflect.Float32, reflect.Float64:
		return false
	case reflect.String:
		return strings.TrimSpace(rv.String()) == ""
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
