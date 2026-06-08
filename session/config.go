package session

import (
	"fmt"
	"strings"
	"time"

	configpkg "github.com/prismgo/framework/config"
	encodingpkg "github.com/prismgo/framework/encoding"
	encryptionpkg "github.com/prismgo/framework/encryption"
)

const (
	// DefaultDriver keeps the project-requested default even though Laravel 13
	// currently documents database as its application default.
	DefaultDriver          = "file"
	DefaultLifetimeMinutes = 120
	DefaultCookieName      = "prismgo_session"
	DefaultCookiePath      = "/"
	DefaultSameSite        = "lax"
	DefaultFilesPath       = "storage/framework/sessions"
	DefaultLockSeconds     = 10
	DefaultLockWaitSeconds = 10
	DefaultRedisConnection = "default"
	DefaultRedisPrefix     = "prismgo_session"
)

// Config describes the session manager, cookie, persistence, and lock settings.
type Config struct {
	// Driver names the persistence backend, for example file.
	Driver string
	// Lifetime is the server-side validity window for a session.
	Lifetime time.Duration
	// ExpireOnClose controls whether the browser receives a persistent cookie.
	ExpireOnClose bool
	// Encrypt controls encryption of server-side payloads, not the ID cookie.
	Encrypt bool
	// Encoding 是 session payload 的 Payload Encoding 名称。
	//
	// 需求背景：issue 01 只要求建立配置和严格校验基线，session 的实际 payload 读写切换由后续
	// issue 完成。空值表示继承 encoding.default，严格装配路径会归一为 msgpack 或 json。
	Encoding string
	// Encryptor encrypts file payload bytes when Encrypt is true.
	Encryptor Encryptor
	// Cookie describes the session ID cookie sent to the client.
	Cookie CookieConfig
	// Files is the root directory used by the file driver.
	Files string
	// Redis 描述 Redis session driver 使用的连接名和 key 命名空间。
	Redis RedisConfig
	// Lock controls per-session ID exclusive access.
	Lock LockConfig
}

// RedisConfig 描述 Redis-backed session 的连接和 key 前缀配置。
//
// 需求背景：Redis session 必须独立于 cache 配置，避免缓存连接或缓存前缀变化影响登录态、
// flash 数据和其他服务端 session payload。
// 设计思路：ConfigFromRepository 只保存 session.connection 选择的 Redis 连接名，具体连接
// 由 prismgo/redis 统一解析。
type RedisConfig struct {
	// Connection 选择 database.redis 中的连接名称。
	Connection string
	// Prefix 隔离 Redis session payload key 和 lock key，不能复用 cache prefix。
	Prefix string
}

// CookieConfig describes the session ID cookie written to the client.
type CookieConfig struct {
	// Name is the cookie key that carries only the opaque session ID.
	Name string
	// Path and Domain define the browser scope for the session cookie.
	Path   string
	Domain string
	// Secure and HTTPOnly map directly to Set-Cookie attributes.
	Secure   bool
	HTTPOnly bool
	// SameSite stores the config string before it is mapped to net/http.
	SameSite string
}

// ConfigFromRepository 从 prismgo/config 仓库读取 SESSION_* 对应的 session 配置。
//
// 需求背景：应用层通过 config/session.go 注册 Laravel 风格 SESSION_* 环境变量，session 包只依赖
// 通用 config 仓库，不反向依赖业务 configs 包，避免 prismgo 模块与业务模块耦合。
// 参数 repo 是已加载的配置仓库；传入 nil 或缺失字段时按 DefaultConfig 补齐。
func ConfigFromRepository(repo *configpkg.Config) Config {
	def := DefaultConfig()
	connection := strings.TrimSpace(repoString(repo, "session.connection", def.Redis.Connection))
	cfg := Config{
		Driver:        strings.TrimSpace(repoString(repo, "session.driver", def.Driver)),
		Lifetime:      minutesDuration(repoInt(repo, "session.lifetime", DefaultLifetimeMinutes)),
		ExpireOnClose: repoBool(repo, "session.expire_on_close", def.ExpireOnClose),
		Encrypt:       repoBool(repo, "session.encrypt", def.Encrypt),
		Encoding:      strings.TrimSpace(repoString(repo, "session.encoding", def.Encoding)),
		Cookie: CookieConfig{
			Name:     strings.TrimSpace(repoString(repo, "session.cookie", def.Cookie.Name)),
			Path:     strings.TrimSpace(repoString(repo, "session.path", def.Cookie.Path)),
			Domain:   strings.TrimSpace(repoString(repo, "session.domain", def.Cookie.Domain)),
			Secure:   repoBool(repo, "session.secure", def.Cookie.Secure),
			HTTPOnly: repoBool(repo, "session.http_only", def.Cookie.HTTPOnly),
			SameSite: strings.ToLower(strings.TrimSpace(repoString(repo, "session.same_site", def.Cookie.SameSite))),
		},
		Files: strings.TrimSpace(repoString(repo, "session.files", def.Files)),
		Redis: RedisConfig{
			Connection: connection,
			Prefix:     strings.TrimSpace(repoString(repo, "session.prefix", def.Redis.Prefix)),
		},
		Lock: LockConfig{
			TTL:  secondsDuration(repoInt(repo, "session.lock_seconds", DefaultLockSeconds)),
			Wait: secondsDuration(repoInt(repo, "session.lock_wait", DefaultLockWaitSeconds)),
		},
	}
	return normalizeConfig(cfg)
}

// ConfigFromFacade 从全局 config facade 构造 session 配置。
func ConfigFromFacade() Config {
	return ConfigFromRepository(configpkg.Resolve())
}

// ConfigFromFacadeStrict 从当前 Application config facade 构造 session 配置。
//
// 需求背景：provider lazy factory 属于严格装配路径，必须把 config.Resolve()
// 的错误返回给 session.Resolve()，不能像 Default() 便捷入口那样回退到进程级配置。
func ConfigFromFacadeStrict() (Config, error) {
	repo := configpkg.Resolve()
	if repo == nil {
		return Config{}, fmt.Errorf("session: config facade not initialized")
	}
	cfg := ConfigFromRepository(repo)
	// 严格装配路径在 manager 创建前先校验 Payload Encoding。
	//
	// 设计原因：ConfigFromFacadeStrict 被 provider lazy factory 调用，非法 SESSION_ENCODING
	// 必须通过 Resolve() 返回给启动路径；不能像便捷 fallback API 一样隐式使用默认配置。
	codec, err := encodingpkg.ResolveWithDefault(repo.GetString("encoding.default", encodingpkg.NameMsgpack), cfg.Encoding)
	if err != nil {
		return Config{}, err
	}
	cfg.Encoding = codec.Name()
	if cfg.Encrypt && cfg.Encryptor == nil {
		encryptor, err := resolveEncryptionForConfig()
		if err != nil {
			return Config{}, err
		}
		cfg.Encryptor = encryptor
	}
	return cfg, nil
}

// resolveEncryptionForConfig 通过 encryption facade 解析默认 byte 加密契约。
//
// 设计原因：ConfigFromFacadeStrict 是 provider lazy factory 的装配入口，必须把
// facade.Resolve 的 panic 转回 error，让 session.Resolve() 能暴露明确配置错误。
func resolveEncryptionForConfig() (encryptor Encryptor, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			if recoveredErr, ok := recovered.(error); ok {
				err = recoveredErr
				return
			}
			err = fmt.Errorf("session: resolve encryption service: %v", recovered)
		}
	}()
	return encryptionpkg.Resolve(), nil
}

func repoString(repo *configpkg.Config, path string, fallback string) string {
	if repo == nil {
		return fallback
	}
	return repo.GetString(path, fallback)
}

func repoInt(repo *configpkg.Config, path string, fallback int) int {
	if repo == nil {
		return fallback
	}
	return repo.GetInt(path, fallback)
}

func repoBool(repo *configpkg.Config, path string, fallback bool) bool {
	if repo == nil {
		return fallback
	}
	return repo.GetBool(path, fallback)
}

func minutesDuration(minutes int) time.Duration {
	if minutes <= 0 {
		minutes = DefaultLifetimeMinutes
	}
	return time.Duration(minutes) * time.Minute
}

func secondsDuration(seconds int) time.Duration {
	if seconds <= 0 {
		seconds = DefaultLockSeconds
	}
	return time.Duration(seconds) * time.Second
}

// LockConfig describes per-session lock timing.
type LockConfig struct {
	// TTL is the maximum time a lock may be held.
	TTL time.Duration
	// Wait is the maximum time a request waits to acquire a lock.
	Wait time.Duration
}

// DefaultConfig returns the Laravel-compatible defaults selected for this project.
func DefaultConfig() Config {
	return Config{
		Driver:        DefaultDriver,
		Lifetime:      time.Duration(DefaultLifetimeMinutes) * time.Minute,
		ExpireOnClose: false,
		Encrypt:       false,
		Cookie: CookieConfig{
			Name:     DefaultCookieName,
			Path:     DefaultCookiePath,
			Domain:   "",
			Secure:   false,
			HTTPOnly: true,
			SameSite: DefaultSameSite,
		},
		Files: DefaultFilesPath,
		Redis: RedisConfig{
			Connection: DefaultRedisConnection,
			Prefix:     DefaultRedisPrefix,
		},
		Lock: LockConfig{
			TTL:  time.Duration(DefaultLockSeconds) * time.Second,
			Wait: time.Duration(DefaultLockWaitSeconds) * time.Second,
		},
	}
}
