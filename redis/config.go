package redis

import (
	"fmt"
	"strings"

	configpkg "github.com/prismgo/framework/config"
)

// DefaultConnectionName 是未显式指定连接名时使用的 Redis 连接名称。
//
// 设计思路：
// Laravel 风格配置默认把 database.redis.default 作为主连接；cache、queue、session 等
// 能力如果没有声明自己的 connection，也应回退到这个稳定名称。
const DefaultConnectionName = "default"

// Config 描述 Laravel 风格 database.redis 配置。
//
// 需求背景：Redis 连接配置由 database.redis 统一保存，cache/queue/session/horizon 只保存
// connection 名称，避免各业务能力复制 host、port、database 和认证信息。
type Config struct {
	// Client 表示 Redis 客户端实现名称；v1 只支持 go/go-redis/redis 这类 Go 实现标识。
	Client string
	// Options 保存 database.redis.options 原始配置，供后续 cluster 或特殊 client 参数扩展。
	Options map[string]any
	// DefaultName 是 Connection 未传名称时使用的连接名。
	DefaultName string
	// Connections 按名称保存 default、cache 等 Redis 单机连接配置。
	Connections map[string]ConnectionConfig
}

// ConnectionConfig 描述一个单机 Redis 连接。
//
// 设计思路：v1 只实现单机连接；Options 保留原始扩展字段，避免后续接入 cluster 或特殊
// go-redis 参数时破坏配置结构。
type ConnectionConfig struct {
	// Name 是当前连接在 database.redis 下的配置键。
	Name string
	// Addr 是 host:port 形式的完整地址；设置后优先级高于 Host/Port。
	Addr string
	// Host 是 Redis 主机地址；Addr 为空时与 Port 组合使用。
	Host string
	// Port 是 Redis 端口；为空时默认使用 6379。
	Port string
	// Username 是 Redis ACL 用户名；未启用 ACL 时可以为空。
	Username string
	// Password 是 Redis 认证密码；未启用认证时可以为空。
	Password string
	// DB 是 Redis 逻辑数据库编号，对应 Laravel 配置中的 database/db。
	DB int
	// Options 保存该连接的原始扩展配置，当前版本不主动解释这些字段。
	Options map[string]any
}

// ConfigFromRepository 从配置仓库读取 Laravel 风格 database.redis 配置。
//
// 参数说明：
// repo 是 prismgo/config 的配置仓库；nil repo 会返回默认本地 Redis 配置，方便测试和
// 独立包使用。
//
// 逻辑说明：
// 新配置统一读取 database.redis。函数内部保留根 redis 命名空间兼容，是为了旧测试或旧
// 应用在迁移期不直接崩溃；项目默认配置文件已经移除 config/redis.go。
//
// 调用示例：
//
//	cfg := redis.ConfigFromRepository(configRepo)
//	manager, err := redis.NewManager(cfg)
//	if err != nil {
//	    return err
//	}
func ConfigFromRepository(repo *configpkg.Config) Config {
	if repo == nil {
		return defaultConfig()
	}
	raw := repo.GetStringMap("database.redis")
	if len(raw) == 0 {
		// 兼容仍暴露根 redis 命名空间的旧测试或旧应用；新配置统一使用 database.redis。
		raw = repo.GetStringMap("redis")
	}
	if len(raw) == 0 {
		return defaultConfig()
	}
	cfg := configFromRaw(raw)
	if len(cfg.Connections) == 0 {
		cfg.Connections = defaultConnections()
	}
	return cfg
}

// ConfigFromFacadeStrict 从当前 Application 的 config facade 读取 Redis 配置。
//
// 需求背景：
// RedisServiceProvider 注册 factory 时不应要求调用方手动传入配置仓库，因此需要一个严格
// 依赖当前 Application facade 的读取入口。config facade 未就绪时返回错误，让启动链路明确失败。
func ConfigFromFacadeStrict() (Config, error) {
	repo := configpkg.Resolve()
	if repo == nil {
		return Config{}, fmt.Errorf("redis: config facade not initialized")
	}
	return configFromRepositoryStrict(repo)
}

func configFromRepositoryStrict(repo *configpkg.Config) (Config, error) {
	if repo == nil {
		return Config{}, fmt.Errorf("redis: config repository is nil")
	}
	raw := repo.GetStringMap("database.redis")
	if len(raw) == 0 {
		return Config{}, fmt.Errorf("redis: database.redis is not configured")
	}
	cfg := configFromRaw(raw)
	if len(cfg.Connections) == 0 {
		return Config{}, fmt.Errorf("redis: database.redis has no configured connections")
	}
	if _, ok := cfg.connection(cfg.DefaultName); !ok {
		return Config{}, fmt.Errorf("redis: default connection %q is not configured", cfg.DefaultName)
	}
	return cfg, nil
}

func configFromRaw(raw map[string]any) Config {
	cfg := Config{
		Client:      strings.TrimSpace(castString(raw["client"])),
		DefaultName: strings.TrimSpace(castString(raw["connection"])),
		Options:     cloneAnyMap(asMap(raw["options"])),
		Connections: make(map[string]ConnectionConfig),
	}
	for key, value := range raw {
		name := strings.TrimSpace(key)
		if name == "" || name == "client" || name == "options" || name == "clusters" || name == "connection" {
			continue
		}
		spec, ok := value.(map[string]any)
		if !ok {
			continue
		}
		conn := connectionConfigFromMap(name, spec)
		cfg.Connections[name] = conn
	}
	if cfg.Client == "" {
		cfg.Client = "go"
	}
	if cfg.DefaultName == "" {
		cfg.DefaultName = DefaultConnectionName
	}
	return cfg
}

func defaultConfig() Config {
	return Config{
		Client:      "go",
		DefaultName: DefaultConnectionName,
		Connections: defaultConnections(),
	}
}

func defaultConnections() map[string]ConnectionConfig {
	return map[string]ConnectionConfig{
		"default": {Name: "default", Host: "127.0.0.1", Port: "6379", DB: 0},
		"cache":   {Name: "cache", Host: "127.0.0.1", Port: "6379", DB: 1},
	}
}

func connectionConfigFromMap(name string, spec map[string]any) ConnectionConfig {
	return ConnectionConfig{
		Name:     name,
		Addr:     strings.TrimSpace(castString(spec["addr"])),
		Host:     strings.TrimSpace(castString(spec["host"])),
		Port:     strings.TrimSpace(castString(spec["port"])),
		Username: strings.TrimSpace(castString(spec["username"])),
		Password: castString(spec["password"]),
		DB:       castInt(firstPresent(spec, "database", "db")),
		Options:  cloneAnyMap(spec),
	}
}

func (c ConnectionConfig) address() string {
	addr := strings.TrimSpace(c.Addr)
	if addr != "" {
		return addr
	}
	host := strings.TrimSpace(c.Host)
	if host == "" {
		host = "127.0.0.1"
	}
	port := strings.TrimSpace(c.Port)
	if port == "" {
		port = "6379"
	}
	return host + ":" + port
}

func (c Config) connection(name string) (ConnectionConfig, bool) {
	name = strings.TrimSpace(name)
	if name == "" {
		name = c.DefaultName
	}
	spec, ok := c.Connections[name]
	if ok && strings.TrimSpace(spec.Name) == "" {
		spec.Name = name
	}
	return spec, ok
}

func validateClient(client string) error {
	client = strings.TrimSpace(strings.ToLower(client))
	if client == "" || client == "go" || client == "go-redis" || client == "redis" {
		return nil
	}
	return fmt.Errorf("redis: unsupported client %q", client)
}

func asMap(v any) map[string]any {
	if m, ok := v.(map[string]any); ok {
		return m
	}
	return nil
}

func firstPresent(m map[string]any, keys ...string) any {
	for _, key := range keys {
		if value, ok := m[key]; ok {
			return value
		}
	}
	return nil
}

func castString(v any) string {
	if v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprint(v)
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
		var out int
		if _, err := fmt.Sscanf(value, "%d", &out); err == nil {
			return out
		}
	}
	return 0
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
