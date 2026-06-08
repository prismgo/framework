package redis

import (
	"context"
	"crypto/tls"
	"fmt"
	"strings"
	"sync"
	"time"

	rediscontract "github.com/prismgo/framework/contracts/redis"
	goredis "github.com/redis/go-redis/v9"
)

var _ rediscontract.Factory = (*Manager)(nil)

// Manager 是 PrismGo Redis 能力的默认工厂实现，按名称懒加载并缓存 Redis 连接。
//
// 需求背景：
// cache、queue、session、horizon 都需要 Redis 连接，但如果每个包都自行 new client，
// 会导致配置重复、连接生命周期分散、Application 关闭时无法统一释放资源。
//
// 设计思路：
// Manager 只负责“根据 database.redis 的连接名称创建 go-redis client 并复用”，不重新
// 抽象 Redis 命令全集。调用方通过 Connection.Client 使用 go-redis 原生能力，PrismGo
// 管理的 client 会通过 hook 自动派发事件。
//
// 使用示例：
//
//	manager, err := redis.NewManager(redis.Config{Connections: conns})
//	if err != nil {
//	    return err
//	}
//	conn, err := manager.Connection("cache")
//	if err != nil {
//	    return err
//	}
//	return conn.Client().Set(ctx, "key", "value", 0).Err()
type Manager struct {
	mu          sync.RWMutex
	cfg         Config
	connections map[string]*NamedConnection
	events      bool
}

// NewManager 使用显式配置创建 Redis Manager。
//
// 参数说明：
// cfg 来自 database.redis 或测试手工构造；cfg.Client 只接受 go/go-redis/redis 这类
// 当前实现支持的客户端标识；cfg.Connections 保存 default、cache 等命名连接。
//
// 逻辑说明：
// 该函数只校验和补齐配置，不会立即连接 Redis。真正的 client 会在 Connection 首次解析
// 某个连接名时创建，保证 ServiceProvider 注册阶段不会产生网络副作用。
func NewManager(cfg Config) (*Manager, error) {
	if err := validateClient(cfg.Client); err != nil {
		return nil, err
	}
	if cfg.DefaultName == "" {
		cfg.DefaultName = DefaultConnectionName
	}
	if len(cfg.Connections) == 0 {
		cfg.Connections = defaultConnections()
	}
	return &Manager{
		cfg:         cfg,
		connections: make(map[string]*NamedConnection),
		events:      true,
	}, nil
}

// NewManagerFromConfig 从当前 Application config facade 创建 Redis Manager。
//
// 需求背景：
// RedisServiceProvider 需要在容器中注册一个懒加载 factory，而不是要求业务代码手动传入
// Config。该函数把当前应用配置读取逻辑集中在 Redis 包内部。
//
// 使用示例：
//
//	manager, err := redis.NewManagerFromConfig()
//	if err != nil {
//	    return err
//	}
func NewManagerFromConfig() (*Manager, error) {
	cfg, err := ConfigFromFacadeStrict()
	if err != nil {
		return nil, err
	}
	return NewManager(cfg)
}

// Connection 返回指定名称的 Redis 连接；未传名称时使用默认连接。
//
// 参数说明：
// name 是 database.redis 下的连接名称，例如 default 或 cache；未传、空字符串或空白字符串
// 都会回退到 Config.DefaultName。
//
// 逻辑说明：
// 同名连接第一次调用时创建 go-redis client 并缓存，后续调用直接返回缓存对象。缺失连接
// 会返回明确错误，避免 cache/queue/session 等调用方静默连到错误 Redis。
func (m *Manager) Connection(name ...string) (rediscontract.Connection, error) {
	connectionName := m.cfg.DefaultName
	if len(name) > 0 && strings.TrimSpace(name[0]) != "" {
		connectionName = strings.TrimSpace(name[0])
	}
	return m.resolve(connectionName)
}

// DefaultConnection 返回默认 Redis 连接。
//
// 设计思路：
// 该方法与 Connection() 行为一致，但给 provider 注册 redis.connection、测试断言和业务
// 代码一个更显式的入口，避免依赖可变参数的空值语义。
func (m *Manager) DefaultConnection() (rediscontract.Connection, error) {
	return m.Connection(m.cfg.DefaultName)
}

// Connections 返回已解析连接的只读快照。
//
// 逻辑说明：
// 返回值只包含已经通过 Connection/DefaultConnection 创建过的连接，不会为了构造快照而
// 连接 Redis。返回 map 是新的容器，调用方修改 map 不会影响 Manager 内部缓存。
func (m *Manager) Connections() map[string]rediscontract.Connection {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make(map[string]rediscontract.Connection, len(m.connections))
	for name, conn := range m.connections {
		out[name] = conn
	}
	return out
}

// Purge 关闭并移除指定连接；未传名称时移除默认连接。
//
// 参数说明：
// name 是要清理的连接名；未传或空字符串表示默认连接。Purge 常用于测试隔离、配置热替换
// 或连接异常后强制重建。
//
// 逻辑说明：
// Purge 只处理已经解析过的连接；如果连接不存在则直接返回 nil。被移除的连接下一次解析时
// 会重新读取 Manager 当前配置并创建新的 go-redis client。
func (m *Manager) Purge(name ...string) error {
	connectionName := m.cfg.DefaultName
	if len(name) > 0 && strings.TrimSpace(name[0]) != "" {
		connectionName = strings.TrimSpace(name[0])
	}

	m.mu.Lock()
	conn := m.connections[connectionName]
	delete(m.connections, connectionName)
	m.mu.Unlock()
	if conn == nil || conn.client == nil {
		return nil
	}
	return conn.client.Close()
}

// EnableEvents 开启后续 Redis 命令事件派发。
//
// 逻辑说明：
// 事件覆盖通过 PrismGo 管理 client 执行的 typed commands、Do、pipeline 和 tx-pipeline。
// 开启后会同步更新已经创建的连接，后续新连接也继承 Manager 当前状态。
func (m *Manager) EnableEvents() {
	m.mu.Lock()
	m.events = true
	for _, conn := range m.connections {
		conn.setEvents(true)
	}
	m.mu.Unlock()
}

// DisableEvents 关闭后续 Command 事件派发。
//
// 需求背景：
// 高吞吐批处理或部分测试只关心 Redis 结果，不需要 command 级事件成本。关闭事件后
// go-redis 命令仍会执行并返回原始结果。
func (m *Manager) DisableEvents() {
	m.mu.Lock()
	m.events = false
	for _, conn := range m.connections {
		conn.setEvents(false)
	}
	m.mu.Unlock()
}

// Close 关闭所有已解析 Redis 连接。
//
// 参数说明：
// ctx 来自 Application.CloseContext，用于表达关闭链路是否已超时或取消；nil ctx 会被视为
// context.Background。
//
// 逻辑说明：
// Close 不会创建尚未解析的连接，只关闭缓存中已有 client。即使某个连接关闭失败，也会继续
// 尝试关闭剩余连接，并返回第一个错误，便于容器关闭流程上报根因。
func (m *Manager) Close(ctx context.Context) error {
	m.mu.Lock()
	connections := make(map[string]*NamedConnection, len(m.connections))
	for name, conn := range m.connections {
		connections[name] = conn
	}
	m.mu.Unlock()
	if ctx == nil {
		ctx = context.Background()
	}
	var firstErr error
	for name, conn := range connections {
		if ctx.Err() != nil {
			if firstErr == nil {
				firstErr = ctx.Err()
			}
			continue
		}
		if conn != nil && conn.client != nil {
			if err := conn.client.Close(); err != nil {
				if firstErr == nil {
					firstErr = err
				}
				continue
			}
			m.mu.Lock()
			if m.connections[name] == conn {
				delete(m.connections, name)
			}
			m.mu.Unlock()
		} else {
			m.mu.Lock()
			if m.connections[name] == conn {
				delete(m.connections, name)
			}
			m.mu.Unlock()
		}
	}
	return firstErr
}

func (m *Manager) resolve(name string) (*NamedConnection, error) {
	if m == nil {
		return nil, fmt.Errorf("redis manager is nil")
	}
	name = strings.TrimSpace(name)
	if name == "" {
		name = m.cfg.DefaultName
	}

	m.mu.RLock()
	conn := m.connections[name]
	m.mu.RUnlock()
	if conn != nil {
		return conn, nil
	}

	spec, ok := m.cfg.connection(name)
	if !ok {
		return nil, fmt.Errorf("redis: connection %q is not configured", name)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if conn = m.connections[name]; conn != nil {
		return conn, nil
	}
	client, err := newRedisClient(spec)
	if err != nil {
		return nil, err
	}
	conn = newConnection(name, client, m.events)
	m.connections[name] = conn
	return conn, nil
}

func newRedisClient(cfg ConnectionConfig) (goredis.UniversalClient, error) {
	options, err := redisOptionsFromConfig(cfg)
	if err != nil {
		return nil, err
	}
	return goredis.NewClient(options), nil
}

func redisOptionsFromConfig(cfg ConnectionConfig) (*goredis.Options, error) {
	options, err := baseRedisOptions(cfg)
	if err != nil {
		return nil, err
	}
	overlayExplicitConnectionFields(options, cfg)
	applyMappedRedisOptions(options, cfg.Options)
	return options, nil
}

func baseRedisOptions(cfg ConnectionConfig) (*goredis.Options, error) {
	url := strings.TrimSpace(castString(cfg.Options["url"]))
	if url == "" {
		return &goredis.Options{}, nil
	}
	options, err := goredis.ParseURL(url)
	if err != nil {
		return nil, fmt.Errorf("redis: parse connection %q url: %w", cfg.Name, err)
	}
	return options, nil
}

func overlayExplicitConnectionFields(options *goredis.Options, cfg ConnectionConfig) {
	if options == nil {
		return
	}
	scheme := strings.TrimSpace(strings.ToLower(castString(cfg.Options["scheme"])))
	if scheme == "tls" {
		options.TLSConfig = &tls.Config{}
	}
	url := strings.TrimSpace(castString(cfg.Options["url"]))
	hasExplicitAddress := strings.TrimSpace(cfg.Addr) != "" || strings.TrimSpace(cfg.Host) != "" || strings.TrimSpace(cfg.Port) != ""
	addr := strings.TrimSpace(cfg.Addr)
	if addr != "" {
		options.Addr = addr
	} else {
		host := strings.TrimSpace(cfg.Host)
		port := strings.TrimSpace(cfg.Port)
		if host != "" || port != "" {
			options.Addr = cfg.address()
		}
	}
	if options.Addr == "" {
		options.Addr = cfg.address()
	}
	if strings.TrimSpace(cfg.Username) != "" {
		options.Username = cfg.Username
	}
	if cfg.Password != "" {
		options.Password = cfg.Password
	}
	if dbValue, ok := cfg.Options["database"]; ok && strings.TrimSpace(castString(dbValue)) != "" {
		options.DB = castInt(dbValue)
	}
	if dbValue, ok := cfg.Options["db"]; ok && strings.TrimSpace(castString(dbValue)) != "" {
		options.DB = castInt(dbValue)
	}
	if url == "" || hasExplicitAddress || cfg.DB != 0 {
		options.DB = cfg.DB
	}
}

func applyMappedRedisOptions(options *goredis.Options, raw map[string]any) {
	if options == nil || len(raw) == 0 {
		return
	}
	if name := strings.TrimSpace(castString(raw["name"])); name != "" {
		options.ClientName = name
	}
	if timeout, ok := parseRedisOptionDuration(raw["timeout"]); ok {
		options.DialTimeout = timeout
	}
	if timeout, ok := parseRedisOptionDuration(raw["read_timeout"]); ok {
		options.ReadTimeout = timeout
	}
	if timeout, ok := parseRedisOptionDuration(raw["write_timeout"]); ok {
		options.WriteTimeout = timeout
	}
	if retries, ok := parseRedisOptionInt(raw["max_retries"]); ok {
		options.MaxRetries = retries
	}
}

func parseRedisOptionDuration(value any) (time.Duration, bool) {
	text := strings.TrimSpace(castString(value))
	if text == "" {
		return 0, false
	}
	duration, err := time.ParseDuration(text)
	if err == nil {
		return duration, true
	}
	seconds := castInt(value)
	if seconds == 0 && text != "0" {
		return 0, false
	}
	return time.Duration(seconds) * time.Second, true
}

func parseRedisOptionInt(value any) (int, bool) {
	text := strings.TrimSpace(castString(value))
	if text == "" {
		return 0, false
	}
	return castInt(value), true
}
