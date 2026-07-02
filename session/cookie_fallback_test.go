package session

import (
	"net/http"
	"testing"
	"time"
)

// TestSessionCookieFallbackPreservesSecurityAttributes 验证当 ToHTTP() 失败时，
// fallback cookie 仍然保留所有安全属性。
func TestSessionCookieFallbackPreservesSecurityAttributes(t *testing.T) {
	// 创建一个直接构造的 manager，绕过 normalizeConfig
	// 使用空 name 触发 ToHTTP() 失败
	manager := &Manager{
		cfg: Config{
			Cookie: CookieConfig{
				Name:     "", // 空 name 会导致 ToHTTP() 失败
				Path:     "/app",
				Domain:   "example.com",
				Secure:   true,
				HTTPOnly: true,
				SameSite: "strict",
			},
			Lifetime:      time.Hour,
			ExpireOnClose: false,
		},
		clock: time.Now,
	}

	expiresAt := time.Now().Add(time.Hour)
	cookie := manager.sessionCookie("test-id-123", &expiresAt)

	// 验证 fallback cookie 保留了所有安全属性
	if cookie.Name != "" {
		t.Errorf("cookie.Name = %q, want empty", cookie.Name)
	}
	if cookie.Value != "test-id-123" {
		t.Errorf("cookie.Value = %q, want %q", cookie.Value, "test-id-123")
	}
	if cookie.Path != "/app" {
		t.Errorf("cookie.Path = %q, want %q", cookie.Path, "/app")
	}
	if cookie.Domain != "example.com" {
		t.Errorf("cookie.Domain = %q, want %q", cookie.Domain, "example.com")
	}
	if !cookie.Secure {
		t.Errorf("cookie.Secure = false, want true")
	}
	if !cookie.HttpOnly {
		t.Errorf("cookie.HttpOnly = false, want true")
	}
	if cookie.SameSite != http.SameSiteStrictMode {
		t.Errorf("cookie.SameSite = %v, want %v", cookie.SameSite, http.SameSiteStrictMode)
	}
	if cookie.Expires.IsZero() {
		t.Errorf("cookie.Expires is zero, want non-zero")
	}
	if cookie.MaxAge <= 0 {
		t.Errorf("cookie.MaxAge = %d, want positive", cookie.MaxAge)
	}
}

// TestSessionCookieNormalPathPreservesSecurityAttributes 验证正常路径下 cookie 保留所有安全属性。
func TestSessionCookieNormalPathPreservesSecurityAttributes(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Cookie.Name = "test_session"
	cfg.Cookie.Path = "/app"
	cfg.Cookie.Domain = "example.com"
	cfg.Cookie.Secure = true
	cfg.Cookie.HTTPOnly = true
	cfg.Cookie.SameSite = "strict"
	cfg.Lifetime = 2 * time.Hour

	manager, err := NewManager(cfg, nil)
	if err != nil {
		t.Fatalf("NewManager error = %v", err)
	}

	expiresAt := time.Now().Add(2 * time.Hour)
	cookie := manager.sessionCookie("test-id-123", &expiresAt)

	// 验证所有安全属性都被正确设置
	if cookie.Name != cfg.Cookie.Name {
		t.Errorf("cookie.Name = %q, want %q", cookie.Name, cfg.Cookie.Name)
	}
	if cookie.Value != "test-id-123" {
		t.Errorf("cookie.Value = %q, want %q", cookie.Value, "test-id-123")
	}
	if cookie.Path != cfg.Cookie.Path {
		t.Errorf("cookie.Path = %q, want %q", cookie.Path, cfg.Cookie.Path)
	}
	if cookie.Domain != cfg.Cookie.Domain {
		t.Errorf("cookie.Domain = %q, want %q", cookie.Domain, cfg.Cookie.Domain)
	}
	if !cookie.Secure {
		t.Errorf("cookie.Secure = false, want true")
	}
	if !cookie.HttpOnly {
		t.Errorf("cookie.HttpOnly = false, want true")
	}
	if cookie.SameSite != http.SameSiteStrictMode {
		t.Errorf("cookie.SameSite = %v, want %v", cookie.SameSite, http.SameSiteStrictMode)
	}
	if cookie.Expires.IsZero() {
		t.Errorf("cookie.Expires is zero, want non-zero")
	}
}
