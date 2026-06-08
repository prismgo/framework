package session

import (
	"context"
	"errors"
	"net/http"
	"time"

	cookiepkg "github.com/prismgo/framework/cookie"
	encodingpkg "github.com/prismgo/framework/encoding"
)

// Manager 负责 session 的启动、恢复、保存、锁保护和 session ID cookie 输出。
//
// 需求背景：业务 handler 只应该面对 Store 的 Laravel 风格 API，不应该直接处理 driver、过期判断或 cookie 细节。
// 设计思路：Manager 串联 request cookie、Driver payload、Store 生命周期和 response cookie，保持 session 包职责集中。
type Manager struct {
	cfg    Config
	driver Driver
	clock  func() time.Time
}

// NewManager 使用显式 driver 创建 session manager。
//
// 参数 cfg 是 session/cookie/锁配置，空值会被 normalizeConfig 补齐；driver 是实际持久化实现。
// driver 为空时按 cfg.Driver 解析，默认落到 file driver，使应用不显式配置也能跨请求持久化。
func NewManager(cfg Config, driver Driver) (*Manager, error) {
	cfg = normalizeConfig(cfg)
	// NewManager 是 session 能力的统一构造入口，因此也承担显式 Config 的严格校验。
	//
	// 需求背景：即使调用方绕过 config facade 直接传入 Config，非法 Encoding 也不能静默回退。
	// 当前 issue 只校验并归一配置，实际 session payload 读写切换由后续 issue 接入。
	codec, err := encodingpkg.ResolveWithDefault(encodingpkg.NameMsgpack, cfg.Encoding)
	if err != nil {
		return nil, err
	}
	cfg.Encoding = codec.Name()
	if driver == nil {
		resolved, err := ResolveDriver(cfg.Driver, cfg)
		if err != nil {
			return nil, err
		}
		driver = resolved
	}
	return &Manager{cfg: cfg, driver: driver, clock: time.Now}, nil
}

// Config 返回已归一化后的 manager 配置。
func (m *Manager) Config() Config { return m.cfg }

// Start 从请求 cookie 恢复 session；缺失、非法、过期或损坏时创建新 session。
//
// 参数 ctx 传给 driver 读写；r 提供客户端 session ID cookie；w 保存到 Store 中，用于 Save 时写出新 cookie。
// 设计原因：可恢复错误不向调用方泄露 payload 内容，统一降级为新 session，符合敏感错误处理要求。
func (m *Manager) Start(ctx context.Context, r *http.Request, w http.ResponseWriter) (*Store, error) {
	if m == nil || m.driver == nil {
		return nil, ErrInvalidConfig
	}
	id := requestSessionID(r, m.cfg.Cookie.Name)
	if id == "" {
		return newStore(m, Payload{}, w), nil
	}
	lock, err := m.acquireLock(ctx, id)
	if err != nil {
		return nil, err
	}
	var payload Payload
	payload, err = m.driver.Read(ctx, id)
	if err != nil {
		if recoverableReadError(err) {
			_ = m.driver.Destroy(ctx, id)
			_ = releaseLock(ctx, lock)
			return newStore(m, Payload{}, w), nil
		}
		_ = releaseLock(ctx, lock)
		return nil, err
	}
	if payload.ID != id || m.expired(payload) {
		_ = m.driver.Destroy(ctx, id)
		_ = releaseLock(ctx, lock)
		return newStore(m, Payload{}, w), nil
	}
	store := newStore(m, payload, w)
	store.attachRequestLock(id, lock)
	return store, nil
}

// Save 持久化 Store，并在响应对象存在时写出 session ID cookie。
//
// 保存顺序先推进 flash 生命周期，再在同 session ID 锁保护下写 driver，最后写出 cookie，避免并发请求互相覆盖状态。
func (m *Manager) Save(ctx context.Context, store *Store) error {
	if m == nil || m.driver == nil || store == nil {
		return ErrInvalidConfig
	}
	expiresAt := m.expiresAt()
	save := func() error {
		store.advanceFlash()
		store.payload.ExpiresAt = expiresAt
		return m.driver.Write(ctx, store.id, store.payload, expiresAt)
	}
	var err error
	if store.ownsRequestLock(store.id) {
		err = save()
		releaseErr := store.releaseRequestLock(ctx)
		if err == nil {
			err = releaseErr
		}
	} else {
		err = m.withLock(ctx, store.id, save)
	}
	if err != nil {
		return err
	}
	if store.response != nil {
		http.SetCookie(store.response, m.sessionCookie(store.id, expiresAt))
	}
	store.dirty = false
	return nil
}

func (m *Manager) acquireLock(ctx context.Context, id string) (Lock, error) {
	locker, ok := m.driver.(Locker)
	if !ok {
		return nil, nil
	}
	return locker.Lock(ctx, id, m.cfg.Lock.TTL, m.cfg.Lock.Wait)
}

func (m *Manager) withLock(ctx context.Context, id string, fn func() error) error {
	lock, err := m.acquireLock(ctx, id)
	if err != nil {
		return err
	}
	defer releaseLock(ctx, lock)
	return fn()
}

func releaseLock(ctx context.Context, lock Lock) error {
	if lock == nil {
		return nil
	}
	return lock.Release(ctx)
}

// expired 根据显式过期时间和 LastActivity 判断服务端 session 是否失效。
func (m *Manager) expired(payload Payload) bool {
	now := m.now()
	if payload.ExpiresAt != nil && !payload.ExpiresAt.After(now) {
		return true
	}
	return m.cfg.Lifetime > 0 && payload.LastActivity.Add(m.cfg.Lifetime).Before(now)
}

// expiresAt 计算本次保存后的服务端过期时间。
func (m *Manager) expiresAt() *time.Time {
	if m.cfg.Lifetime <= 0 {
		return nil
	}
	expires := m.now().Add(m.cfg.Lifetime)
	return &expires
}

// sessionCookie 构造只包含 session ID 的客户端 cookie。
//
// 需求背景：session payload 必须只保存在服务端，客户端 cookie 只承担不透明 ID 传递职责。
func (m *Manager) sessionCookie(id string, expiresAt *time.Time) *http.Cookie {
	options := []cookiepkg.Option{
		cookiepkg.Path(m.cfg.Cookie.Path),
		cookiepkg.Domain(m.cfg.Cookie.Domain),
		cookiepkg.Secure(m.cfg.Cookie.Secure),
		cookiepkg.HTTPOnly(m.cfg.Cookie.HTTPOnly),
		cookiepkg.SameSite(cookiepkg.SameSiteMode(m.cfg.Cookie.SameSite)),
	}
	if !m.cfg.ExpireOnClose && expiresAt != nil {
		options = append(options, cookiepkg.ExpiresAt(*expiresAt), cookiepkg.MaxAge(int(expiresAt.Sub(m.now()).Seconds())))
	}
	c := cookiepkg.New(m.cfg.Cookie.Name, id, 0, options...)
	httpCookie, err := c.ToHTTP()
	if err != nil {
		return &http.Cookie{Name: m.cfg.Cookie.Name, Value: id, Path: m.cfg.Cookie.Path, HttpOnly: true}
	}
	return httpCookie
}

// now 返回当前时间；测试可以替换 clock 以稳定验证生命周期。
func (m *Manager) now() time.Time {
	if m.clock != nil {
		return m.clock()
	}
	return time.Now()
}

// requestSessionID 从请求中读取并校验 session ID cookie。
func requestSessionID(r *http.Request, name string) string {
	if r == nil || name == "" {
		return ""
	}
	c, err := r.Cookie(name)
	if err != nil || !validSessionID(c.Value) {
		return ""
	}
	return c.Value
}

// recoverableReadError 判断读取失败是否可以安全恢复为新 session。
//
// 这些错误都可能来自缺失、过期、损坏或解密失败的服务端记录，不应把底层内容暴露给业务层。
func recoverableReadError(err error) bool {
	return errors.Is(err, ErrSessionNotFound) ||
		errors.Is(err, ErrSessionExpired) ||
		errors.Is(err, ErrPayloadMalformed) ||
		errors.Is(err, ErrPayloadDeserialize) ||
		errors.Is(err, ErrDecryptionFailed) ||
		errors.Is(err, ErrInvalidSessionID)
}

// normalizeConfig 补齐 Manager 运行所需的默认配置。
func normalizeConfig(cfg Config) Config {
	def := DefaultConfig()
	if cfg.Driver == "" {
		cfg.Driver = def.Driver
	}
	if cfg.Lifetime <= 0 {
		cfg.Lifetime = def.Lifetime
	}
	if cfg.Cookie.Name == "" {
		cfg.Cookie.Name = def.Cookie.Name
	}
	if cfg.Cookie.Path == "" {
		cfg.Cookie.Path = def.Cookie.Path
	}
	if cfg.Cookie.SameSite == "" {
		cfg.Cookie.SameSite = def.Cookie.SameSite
	}
	if cfg.Files == "" {
		cfg.Files = def.Files
	}
	if cfg.Redis.Connection == "" {
		cfg.Redis.Connection = def.Redis.Connection
	}
	cfg.Redis.Prefix = normalizeRedisPrefix(cfg.Redis.Prefix)
	if cfg.Lock.TTL <= 0 {
		cfg.Lock.TTL = def.Lock.TTL
	}
	if cfg.Lock.Wait <= 0 {
		cfg.Lock.Wait = def.Lock.Wait
	}
	return cfg
}
