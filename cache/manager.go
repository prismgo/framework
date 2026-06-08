package cache

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	redisstore "github.com/eko/gocache/store/redis/v4"
	configpkg "github.com/prismgo/framework/config"
	cachecontract "github.com/prismgo/framework/contracts/cache"
	encodingcontract "github.com/prismgo/framework/contracts/encoding"
	encodingpkg "github.com/prismgo/framework/encoding"
	redisfacade "github.com/prismgo/framework/redis"
	"github.com/redis/go-redis/v9"
)

// Manager 管理全部缓存 store 的生命周期与懒加载实例。
//
// Manager 本身不直接读写缓存，而是按 store 名称返回 Repository；
// Repository 再负责具体的读写、锁和 flexible 逻辑。
type Manager struct {
	// mu 保护 repositories 懒加载缓存。
	mu             sync.Mutex
	defaultName    string
	prefix         string
	codec          encodingcontract.Codec
	specs          map[string]StoreConfig
	repositories   map[string]*Repository
	lockPrefix     string
	lockRetrySleep time.Duration
	refreshTimeout time.Duration
}

// NewManager 根据配置创建缓存 Manager。
//
// 它只校验配置与保存 store 定义，不会立即连接 Redis；具体 store 会在首次 Store 调用时构建。
func NewManager(cfg Config) (*Manager, error) {
	def := strings.TrimSpace(cfg.Default)
	if def == "" {
		def = "memory"
	}
	if len(cfg.Stores) == 0 {
		cfg.Stores = map[string]StoreConfig{
			"memory": {Driver: "memory"},
		}
	}
	if _, ok := cfg.Stores[def]; !ok {
		return nil, fmt.Errorf("%w: %s", ErrStoreNotFound, def)
	}
	codec, err := encodingpkg.ResolveWithDefault(encodingpkg.NameMsgpack, cfg.Encoding)
	if err != nil {
		return nil, fmt.Errorf("cache.encoding: %w", err)
	}

	retrySleep := cfg.Lock.RetrySleep
	if retrySleep <= 0 {
		retrySleep = 50 * time.Millisecond
	}
	refreshTimeout := cfg.Flexible.RefreshTimeout
	if refreshTimeout <= 0 {
		refreshTimeout = 30 * time.Second
	}
	lockPrefix := strings.Trim(strings.TrimSpace(cfg.Lock.Prefix), ":")
	if lockPrefix == "" {
		lockPrefix = "locks"
	}

	specs := make(map[string]StoreConfig, len(cfg.Stores))
	for name, spec := range cfg.Stores {
		specs[strings.TrimSpace(name)] = cloneStoreConfig(spec)
	}

	return &Manager{
		defaultName:    def,
		prefix:         strings.Trim(strings.TrimSpace(cfg.Prefix), ":"),
		codec:          codec,
		specs:          specs,
		repositories:   make(map[string]*Repository),
		lockPrefix:     lockPrefix,
		lockRetrySleep: retrySleep,
		refreshTimeout: refreshTimeout,
	}, nil
}

// DefaultName 返回默认 store 名称。
func (m *Manager) DefaultName() string {
	return m.defaultName
}

// Default 返回默认 store 的 Repository 契约。
func (m *Manager) Default() cachecontract.Repository {
	return m.defaultRepository()
}

// Store 返回指定名称的 Repository 契约。
//
// Repository 会懒加载并缓存；未知 store 返回携带错误的 Repository，
// 让后续 Put/Get 调用能以普通 error 形式暴露配置问题。
func (m *Manager) Store(name string) cachecontract.Repository {
	return m.storeRepository(name)
}

// defaultRepository 返回默认 store 的具体 Repository，供实现包内部强类型 facade 复用。
func (m *Manager) defaultRepository() *Repository {
	return m.storeRepository(m.defaultName)
}

// storeRepository 返回指定名称的具体 Repository。
//
// 设计原因：公共 Manager.Store 对外只承诺 contracts/cache.Repository；实现包内部仍需要
// 具体 Repository 执行 Payload Encoding 解码、批量原始 bytes 读取等强类型 facade 能力。
func (m *Manager) storeRepository(name string) *Repository {
	name = strings.TrimSpace(name)
	if name == "" {
		name = m.defaultName
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if repo, ok := m.repositories[name]; ok {
		return repo
	}
	spec, ok := m.specs[name]
	if !ok {
		return newErrorRepository(name, fmt.Errorf("%w: %s", ErrStoreNotFound, name), m)
	}

	repo, err := m.buildRepositoryLocked(name, spec)
	if err != nil {
		return newErrorRepository(name, err, m)
	}
	m.repositories[name] = repo
	return repo
}

// Close 释放所有已构建 store 持有的外部资源。
//
// memory store 会停止清理 goroutine，redis/custom store 会关闭底层资源。
func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var firstErr error
	for _, repo := range m.repositories {
		if err := repo.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	m.repositories = make(map[string]*Repository)
	return firstErr
}

// buildRepositoryLocked 在持有 Manager 锁时按 driver 构造 Repository。
func (m *Manager) buildRepositoryLocked(name string, spec StoreConfig) (*Repository, error) {
	driver := normalizeDriverName(spec.Driver)
	if driver == "" {
		driver = "memory"
	}
	prefix := joinPrefix(m.prefix, spec.Prefix)
	lockPrefix := joinPrefix(prefix, m.lockPrefix)

	switch driver {
	case "memory", "go-cache", "go_cache":
		store := newMemoryStore(spec.DefaultTTL, spec.CleanupInterval)
		return newRepository(name, prefix, store, store, store, store, nil, store, store, store, store, lockPrefix, m, storeEvents(spec)), nil
	case "redis":
		client, err := resolveRedisClient(spec.Redis)
		if err != nil {
			return nil, err
		}
		base := redisstore.NewRedis(client)
		store := &redisStore{RedisStore: base, client: client}
		return newRepository(name, prefix, store, store, store, store, store, store, store, store, store, lockPrefix, m, storeEvents(spec)), nil
	case "file", "filesystem":
		store := newFileStore(spec.File, spec.DefaultTTL, prefix, lockPrefix)
		return newRepository(name, prefix, store, store, store, store, store, nil, store, store, nil, lockPrefix, m, storeEvents(spec)), nil
	case "failover":
		store := newFailoverStore(m, name, spec.Stores)
		return newRepository(name, "", store, store, store, store, store, store, store, store, nil, "", m, storeEvents(spec)), nil
	default:
		factory, ok := lookupStoreFactory(driver)
		if !ok {
			return nil, fmt.Errorf("cache: unknown driver %q for store %q", driver, name)
		}
		return m.buildCustomRepositoryLocked(name, driver, spec, prefix, lockPrefix, factory)
	}
}

// buildCustomRepositoryLocked 调用用户通过 Extend 注册的 driver 工厂。
func (m *Manager) buildCustomRepositoryLocked(name, driver string, spec StoreConfig, prefix, lockPrefix string, factory StoreFactory) (*Repository, error) {
	storeDriver, err := factory(StoreFactoryContext{
		Name:         name,
		Driver:       driver,
		Config:       cloneStoreConfig(spec),
		GlobalPrefix: m.prefix,
		StorePrefix:  trimPrefix(spec.Prefix),
		Prefix:       prefix,
		LockPrefix:   lockPrefix,
	})
	if err != nil {
		return nil, err
	}
	storeDriver, err = storeDriver.normalize()
	if err != nil {
		return nil, fmt.Errorf("cache: build custom driver %q for store %q: %w", driver, name, err)
	}
	return newRepository(
		name,
		prefix,
		storeDriver.Store,
		storeDriver.Touch,
		storeDriver.Atomic,
		storeDriver.Bulk,
		storeDriver.Flush,
		storeDriver.Tags,
		storeDriver.Lock,
		storeDriver.LockFlush,
		storeDriver.Close,
		lockPrefix,
		m,
		storeEvents(spec),
	), nil
}

func resolveRedisClient(cfg RedisConfig) (*redis.Client, error) {
	connection := strings.TrimSpace(cfg.Connection)
	client, err := redisfacade.Client(connection)
	if err == nil {
		typed, ok := client.(*redis.Client)
		if !ok {
			return nil, fmt.Errorf("cache: redis connection %q is %T, want *redis.Client", connection, client)
		}
		return typed, nil
	}
	return nil, err
}

// joinPrefix 把多个 key 前缀片段拼成冒号分隔形式。
func joinPrefix(parts ...string) string {
	clean := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.Trim(strings.TrimSpace(part), ":")
		if part != "" {
			clean = append(clean, part)
		}
	}
	return strings.Join(clean, ":")
}

// NewManagerFromConfig 根据当前 config facade 创建缓存 Manager。
//
// 需求背景：provider 的 lazy factory 需要复用同一套配置解析和 manager 构造逻辑，
// 但不再暴露旧的 Application 装配命名，避免调用方误以为这是启动注册入口。
func NewManagerFromConfig(...any) (func() error, *Manager, error) {
	cfg, err := buildConfig()
	if err != nil {
		return nil, nil, err
	}
	m, err := NewManager(cfg)
	if err != nil {
		return nil, nil, err
	}
	return m.Close, m, nil
}

// buildConfig 从当前 Application config facade 严格构造缓存配置。
//
// 需求背景：cache provider 的 lazy factory 必须把 config.Resolve() 错误暴露给
// cache.Resolve()，避免在完整应用装配路径中静默回退到进程级默认配置。
func buildConfig() (Config, error) {
	repo := configpkg.Resolve()
	if repo == nil {
		return Config{}, fmt.Errorf("cache: config facade not initialized")
	}
	rawStores := repo.GetStringMap("cache.stores")
	if len(rawStores) == 0 {
		return Config{}, fmt.Errorf("cache.stores is empty")
	}

	stores := make(map[string]StoreConfig, len(rawStores))
	for name, raw := range rawStores {
		spec, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		stores[name] = StoreConfig{
			Driver:          castString(spec["driver"]),
			Prefix:          castString(spec["prefix"]),
			DefaultTTL:      secondsDuration(spec["default_ttl"]),
			CleanupInterval: secondsDuration(spec["cleanup_interval"]),
			Redis: RedisConfig{
				Connection: castString(spec["connection"]),
			},
			File: FileConfig{
				Path:     castString(spec["path"]),
				LockPath: castString(spec["lock_path"]),
			},
			Stores:  castStringSlice(spec["stores"]),
			Events:  castBoolPtr(spec["events"]),
			Options: cloneAnyMap(spec),
		}
	}

	cacheCodec, err := encodingpkg.ResolveWithDefault(repo.GetString("encoding.default", encodingpkg.NameMsgpack), repo.GetString("cache.encoding", ""))
	if err != nil {
		return Config{}, fmt.Errorf("cache.encoding: %w", err)
	}

	return Config{
		Default:  repo.GetString("cache.default", "memory"),
		Encoding: cacheCodec.Name(),
		Prefix:   repo.GetString("cache.prefix", "prismgo_cache"),
		Stores:   stores,
		Lock: LockConfig{
			Prefix:     repo.GetString("cache.lock.prefix", "locks"),
			RetrySleep: time.Duration(repo.GetInt("cache.lock.retry_sleep_ms", 50)) * time.Millisecond,
		},
		Flexible: FlexibleConfig{
			RefreshTimeout: time.Duration(repo.GetInt("cache.flexible.refresh_timeout", 30)) * time.Second,
		},
	}, nil
}

func secondsDuration(v any) time.Duration {
	n := castInt(v)
	if n <= 0 {
		return 0
	}
	return time.Duration(n) * time.Second
}

func castInt(v any) int {
	switch value := v.(type) {
	case int:
		return value
	case int64:
		return int(value)
	case float64:
		return int(value)
	case string:
		return parseInt(value, 0)
	default:
		return parseInt(castString(value), 0)
	}
}

func castString(v any) string {
	if v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

func cloneStoreConfig(spec StoreConfig) StoreConfig {
	spec.Options = cloneAnyMap(spec.Options)
	if spec.Events != nil {
		events := *spec.Events
		spec.Events = &events
	}
	spec.Stores = append([]string(nil), spec.Stores...)
	return spec
}

func cloneAnyMap(input map[string]any) map[string]any {
	if len(input) == 0 {
		return nil
	}
	out := make(map[string]any, len(input))
	for key, value := range input {
		out[key] = value
	}
	return out
}

func trimPrefix(prefix string) string {
	return strings.Trim(strings.TrimSpace(prefix), ":")
}

func parseInt(value string, fallback int) int {
	value = strings.TrimSpace(value)
	if value == "" {
		return fallback
	}
	n, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return n
}

func storeEvents(spec StoreConfig) bool {
	if spec.Events == nil {
		return true
	}
	return *spec.Events
}

func castBoolPtr(v any) *bool {
	if v == nil {
		return nil
	}
	var out bool
	switch value := v.(type) {
	case bool:
		out = value
	case string:
		switch strings.ToLower(strings.TrimSpace(value)) {
		case "1", "true", "yes", "on":
			out = true
		default:
			out = false
		}
	default:
		return nil
	}
	return &out
}

func castStringSlice(v any) []string {
	switch value := v.(type) {
	case []string:
		return append([]string(nil), value...)
	case []any:
		out := make([]string, 0, len(value))
		for _, item := range value {
			if s := strings.TrimSpace(castString(item)); s != "" {
				out = append(out, s)
			}
		}
		return out
	case string:
		parts := strings.Split(value, ",")
		out := make([]string, 0, len(parts))
		for _, part := range parts {
			if s := strings.TrimSpace(part); s != "" {
				out = append(out, s)
			}
		}
		return out
	default:
		return nil
	}
}
