package cookie

import (
	"context"
	"net/http"
	"time"
)

const (
	// DefaultPath 是 Laravel 风格 cookie 的默认路径，表示当前站点全部路径可见。
	DefaultPath = "/"
	// ForeverMinutes 表示长期 cookie 的兼容时长，按 Laravel 常用语义固定为五年分钟数。
	ForeverMinutes = 2628000
	// SameSiteDefault 保持 Go 标准库默认 SameSite 行为，不额外写出 SameSite 属性。
	SameSiteDefault = SameSiteMode("")
	SameSiteLax     = SameSiteMode("lax")
	SameSiteStrict  = SameSiteMode("strict")
	SameSiteNone    = SameSiteMode("none")
	// SameSiteDisabled 保留给调用方表达显式关闭 SameSite 的意图，当前转换时等价于默认模式。
	SameSiteDisabled = SameSiteMode("disabled")
)

// SameSiteMode 表示 cookie SameSite 策略。
//
// 用途：在 prismgo/cookie 内部屏蔽 net/http 的枚举细节，让业务代码使用稳定的包级常量。
// 设计思路：仅保存可序列化字符串，最终写响应前再转换为 http.SameSite，便于后续扩展配置解析。
type SameSiteMode string

// Cookie 表示尚未写入响应的 HTTP cookie 值对象。
//
// 用途：承载创建、排队、替换、过期和删除 cookie 所需的全部浏览器属性。
// 使用方式：调用 New/Make/Forever 创建，再通过 Attach 或 Queue.Flush 写入 http.ResponseWriter。
// 设计原因：值对象不直接依赖具体响应，便于在 middleware 结束前统一排队和去重。
// 需求背景：Phase 4 要求提供 Laravel 风格 cookie 创建、读取、排队、过期和删除能力。
type Cookie struct {
	// Name 是 cookie 名称，必须符合 HTTP token 规则。
	Name string
	// Value 是写入浏览器的 cookie 值；启用安全扩展时会先加密/签名再输出。
	Value string
	// Raw 控制是否让 net/http 按原始值写出，默认 false。
	Raw bool
	// Minutes 表示相对过期分钟数；大于 0 时会生成 Expires 和 Max-Age。
	Minutes int
	// Path 是浏览器路径作用域，默认使用 /。
	Path string
	// Domain 是浏览器域名作用域，空值表示当前 host。
	Domain string
	// Secure 控制是否仅允许 HTTPS 传输。
	Secure bool
	// HTTPOnly 控制是否禁止客户端脚本读取，默认 true。
	HTTPOnly bool
	// SameSite 保存 SameSite 策略，写响应时转换为 Go 标准库枚举。
	SameSite SameSiteMode
	// Expires 允许调用方显式指定浏览器过期时间。
	Expires *time.Time
	// MaxAge 允许调用方显式指定 Max-Age（单位：秒）；负数用于删除 cookie。
	MaxAge int
}

// Scope 标识浏览器匹配 cookie 时使用的 name/path/domain 范围。
//
// 用途：Queue 按 Scope 去重，确保同名同作用域 cookie 后入替换先入。
// 设计原因：浏览器删除 cookie 必须使用与创建时一致的 path/domain，因此队列不能只按 name 去重。
type Scope struct {
	// Name 是 cookie 名称。
	Name string
	// Path 是 cookie 路径作用域。
	Path string
	// Domain 是 cookie 域名作用域。
	Domain string
}

// Option 表示 Cookie 构造阶段的属性修改函数。
//
// 参数说明：入参是待创建的 Cookie 指针，Option 可以调整 path/domain/secure/httpOnly/sameSite 等属性。
type Option func(*Cookie)

// New 创建 cookie 值对象但不写入响应。
//
// 参数 name 是 cookie 名称，value 是原始值，minutes 是相对过期分钟数，options 用于覆盖默认属性。
// 设计思路：默认 Path=/ 且 HTTPOnly=true，贴近服务端状态 cookie 的安全默认值。
func New(name string, value string, minutes int, options ...Option) Cookie {
	c := Cookie{
		Name:     name,
		Value:    value,
		Minutes:  minutes,
		Path:     DefaultPath,
		HTTPOnly: true,
	}
	for _, option := range options {
		if option != nil {
			option(&c)
		}
	}
	return c
}

// Make 是 New 的 Laravel 风格别名。
//
// 需求背景：契约要求保留 Laravel 公共方法概念，同时使用 Go 惯用 API。
func Make(name string, value string, minutes int, options ...Option) Cookie {
	return New(name, value, minutes, options...)
}

// Forever 创建长期有效 cookie。
//
// 参数 name/value/options 与 New 一致；过期时长固定为 ForeverMinutes，便于测试和文档说明。
func Forever(name string, value string, options ...Option) Cookie {
	return New(name, value, ForeverMinutes, options...)
}

// Path 配置 cookie 的浏览器路径作用域。
func Path(path string) Option {
	return func(c *Cookie) {
		c.Path = path
	}
}

// Domain 配置 cookie 的浏览器域名作用域。
func Domain(domain string) Option {
	return func(c *Cookie) {
		c.Domain = domain
	}
}

// Secure 配置 Set-Cookie 的 Secure 属性。
func Secure(enabled bool) Option {
	return func(c *Cookie) {
		c.Secure = enabled
	}
}

// HTTPOnly 配置 Set-Cookie 的 HttpOnly 属性。
func HTTPOnly(enabled bool) Option {
	return func(c *Cookie) {
		c.HTTPOnly = enabled
	}
}

// SameSite 配置 Set-Cookie 的 SameSite 属性。
func SameSite(mode SameSiteMode) Option {
	return func(c *Cookie) {
		c.SameSite = mode
	}
}

// Raw 配置 net/http 写出 cookie 值时是否使用原始值。
func Raw(enabled bool) Option {
	return func(c *Cookie) {
		c.Raw = enabled
	}
}

// ScopeOption 将可复用作用域应用到 cookie。
//
// 参数 scope 只使用 Path 和 Domain；Name 由创建 cookie 时的 name 参数决定，避免错误复用名称。
func ScopeOption(scope Scope) Option {
	return func(c *Cookie) {
		if scope.Path != "" {
			c.Path = scope.Path
		}
		if scope.Domain != "" {
			c.Domain = scope.Domain
		}
	}
}

// ExpiresAt 设置显式过期时间。
func ExpiresAt(expires time.Time) Option {
	return func(c *Cookie) {
		c.Expires = &expires
	}
}

// MaxAge 设置显式 Max-Age 秒数。
func MaxAge(seconds int) Option {
	return func(c *Cookie) {
		c.MaxAge = seconds
	}
}

// ToHTTP 将包内 Cookie 转换为 net/http.Cookie。
//
// 错误处理：名称非法时返回 ErrInvalidCookieName，不写出无效 Set-Cookie。
func (c Cookie) ToHTTP() (*http.Cookie, error) {
	return c.toHTTPAt(time.Now())
}

func (c Cookie) toHTTPAt(now time.Time) (*http.Cookie, error) {
	if !validCookieName(c.Name) {
		return nil, ErrInvalidCookieName
	}
	out := &http.Cookie{
		Name:     c.Name,
		Value:    c.Value,
		Path:     c.Path,
		Domain:   c.Domain,
		Secure:   c.Secure,
		HttpOnly: c.HTTPOnly,
		SameSite: c.SameSite.ToHTTPSameSite(),
		MaxAge:   c.MaxAge,
	}
	if c.Expires != nil {
		out.Expires = *c.Expires
	} else if c.Minutes > 0 {
		if now.IsZero() {
			now = time.Now()
		}
		out.Expires = now.Add(time.Duration(c.Minutes) * time.Minute)
		out.MaxAge = int((time.Duration(c.Minutes) * time.Minute).Seconds())
	}
	return out, nil
}

// Attach 将当前 cookie 写入响应。
//
// 参数 w 是目标响应写入器；options 可提供 context、签名器、加密器和确定性时钟。
// 设计思路：写出前统一调用 secureOutgoing，保证加密/签名扩展点与 RequestCookie 的读取顺序匹配。
func (c Cookie) Attach(w http.ResponseWriter, options ...AttachOption) error {
	cfg := attachConfig{}
	for _, option := range options {
		if option != nil {
			option(&cfg)
		}
	}
	ctx := cfg.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	secured, err := secureOutgoing(ctx, c, cfg.signer, cfg.encryptor)
	if err != nil {
		return err
	}
	httpCookie, err := secured.toHTTPAt(cfg.now)
	if err != nil {
		return err
	}
	http.SetCookie(w, httpCookie)
	return nil
}

// Attach 将指定 cookie 值对象写入响应，是方法形式的包级便捷入口。
func Attach(w http.ResponseWriter, c Cookie, options ...AttachOption) error {
	return c.Attach(w, options...)
}

// Expire 创建浏览器可删除的过期 cookie。
//
// 参数 name 是待删除 cookie 名称，options 必须传入与原 cookie 一致的 Path/Domain 才能覆盖浏览器记录。
func Expire(name string, options ...Option) Cookie {
	expired := time.Unix(1, 0).UTC()
	opts := append([]Option{
		ExpiresAt(expired),
		MaxAge(-1),
	}, options...)
	return New(name, "", -1, opts...)
}

// Forget 是 Expire 的 Laravel 风格别名。
func Forget(name string, options ...Option) Cookie {
	return Expire(name, options...)
}

// attachConfig 保存写响应阶段的可选依赖。
//
// 设计原因：签名、加密和时钟都属于横切能力，不应该进入 Cookie 值对象本身。
type attachConfig struct {
	ctx       context.Context
	signer    Signer
	encryptor Encryptor
	now       time.Time
}

// AttachOption 配置 cookie 写响应阶段行为。
type AttachOption func(*attachConfig)

// WithContext 配置签名/加密扩展点使用的 context。
func WithContext(ctx context.Context) AttachOption {
	return func(cfg *attachConfig) {
		cfg.ctx = ctx
	}
}

// WithSigner 配置写出 cookie 时使用的签名器。
func WithSigner(signer Signer) AttachOption {
	return func(cfg *attachConfig) {
		cfg.signer = signer
	}
}

// WithEncryptor 配置写出 cookie 时使用的加密器。
func WithEncryptor(encryptor Encryptor) AttachOption {
	return func(cfg *attachConfig) {
		cfg.encryptor = encryptor
	}
}

// WithNow 配置相对过期计算使用的确定性当前时间。
//
// 需求背景：签名、固定过期时间生成、批处理补偿或可重复响应构造需要在生产路径中复用同一
// 业务时间点。该选项只影响本次 Attach，不修改进程级时钟。
func WithNow(now time.Time) AttachOption {
	return func(cfg *attachConfig) {
		cfg.now = now
	}
}

// ToHTTPSameSite 将包级 SameSite 值映射到 Go 标准库枚举。
func (mode SameSiteMode) ToHTTPSameSite() http.SameSite {
	switch mode {
	case SameSiteLax:
		return http.SameSiteLaxMode
	case SameSiteStrict:
		return http.SameSiteStrictMode
	case SameSiteNone:
		return http.SameSiteNoneMode
	default:
		return http.SameSiteDefaultMode
	}
}

// validCookieName 校验 cookie 名称是否符合 HTTP token 规则。
//
// 设计原因：Go 标准库会忽略或清理部分无效值；这里提前返回明确错误，避免调用方误以为写出成功。
func validCookieName(name string) bool {
	if name == "" {
		return false
	}
	for i := 0; i < len(name); i++ {
		if !isCookieTokenByte(name[i]) {
			return false
		}
	}
	return true
}

// isCookieTokenByte 判断单字节是否允许出现在 cookie 名称中。
func isCookieTokenByte(b byte) bool {
	if b >= 'a' && b <= 'z' {
		return true
	}
	if b >= 'A' && b <= 'Z' {
		return true
	}
	if b >= '0' && b <= '9' {
		return true
	}
	switch b {
	case '!', '#', '$', '%', '&', '\'', '*', '+', '-', '.', '^', '_', '`', '|', '~':
		return true
	default:
		return false
	}
}
