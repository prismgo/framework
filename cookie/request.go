package cookie

import (
	"context"
	"errors"
	"net/http"
)

type requestConfig struct {
	ctx       context.Context
	signer    Signer
	encryptor Encryptor
}

// RequestOption 配置请求 cookie 读取阶段行为。
type RequestOption func(*requestConfig)

// RequestWithContext 配置签名/解密扩展点使用的 context。
func RequestWithContext(ctx context.Context) RequestOption {
	return func(cfg *requestConfig) {
		cfg.ctx = ctx
	}
}

// RequestWithSigner 配置请求 cookie 的签名验证器。
func RequestWithSigner(signer Signer) RequestOption {
	return func(cfg *requestConfig) {
		cfg.signer = signer
	}
}

// RequestWithEncryptor 配置请求 cookie 的解密器。
func RequestWithEncryptor(encryptor Encryptor) RequestOption {
	return func(cfg *requestConfig) {
		cfg.encryptor = encryptor
	}
}

// RequestCookie 从请求中读取指定 cookie 值。
//
// 参数 r 是当前 HTTP 请求，name 是 cookie 名称，options 可传入签名器、解密器和 context。
// 设计思路：读取顺序与 Attach 写出相反，先验签再解密；错误统一脱敏，避免泄露客户端原始值。
func RequestCookie(r *http.Request, name string, options ...RequestOption) (string, error) {
	if r == nil || !validCookieName(name) {
		return "", ErrCookieNotFound
	}
	cfg := requestConfig{}
	for _, option := range options {
		if option != nil {
			option(&cfg)
		}
	}
	ctx := cfg.ctx
	if ctx == nil {
		ctx = r.Context()
	}
	c, err := r.Cookie(name)
	if err != nil {
		if errors.Is(err, http.ErrNoCookie) {
			return "", ErrCookieNotFound
		}
		return "", err
	}
	return secureIncoming(ctx, name, c.Value, cfg.signer, cfg.encryptor)
}
