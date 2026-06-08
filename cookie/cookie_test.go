package cookie

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type testContextKey string

// testResponse 构造 cookie 包测试使用的响应记录器。
//
// 参数 t 用于标记测试辅助函数。返回值用于断言 Attach、Queue Flush 等行为写出的
// Set-Cookie 头。
func testResponse(t *testing.T) *httptest.ResponseRecorder {
	t.Helper()
	return httptest.NewRecorder()
}

// testRequest 构造带 cookie 的 HTTP 请求。
//
// 参数 t 用于标记测试辅助函数；cookies 表示要写入请求头的客户端 cookie，供后续
// RequestCookie 和安全校验测试复用。
func testRequest(t *testing.T, cookies ...*http.Cookie) *http.Request {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "http://example.test/", nil)
	for _, c := range cookies {
		req.AddCookie(c)
	}
	return req
}

// setCookieHeaders 返回响应中的所有 Set-Cookie 头。
//
// 参数 t 用于标记测试辅助函数；rr 是 httptest 响应记录器。集中读取逻辑可以让后续测试
// 只关注 cookie 内容和属性断言。
func setCookieHeaders(t *testing.T, rr *httptest.ResponseRecorder) []string {
	t.Helper()
	return rr.Result().Header.Values("Set-Cookie")
}

func TestCookieMakeMapsAttributesAndSameSite(t *testing.T) {
	now := time.Date(2026, 5, 6, 12, 0, 0, 0, time.UTC)
	c := Make("notice", "ok", 5,
		Path("/admin"),
		Domain("example.test"),
		Secure(true),
		HTTPOnly(false),
		SameSite(SameSiteStrict),
	)

	httpCookie, err := c.toHTTPAt(now)
	if err != nil {
		t.Fatalf("ToHTTP returned error: %v", err)
	}
	if httpCookie.Name != "notice" || httpCookie.Value != "ok" {
		t.Fatalf("unexpected cookie pair: %#v", httpCookie)
	}
	if httpCookie.Path != "/admin" || httpCookie.Domain != "example.test" {
		t.Fatalf("unexpected scope: %#v", httpCookie)
	}
	if !httpCookie.Secure || httpCookie.HttpOnly {
		t.Fatalf("unexpected security attributes: %#v", httpCookie)
	}
	if httpCookie.SameSite != http.SameSiteStrictMode {
		t.Fatalf("unexpected SameSite: %v", httpCookie.SameSite)
	}
	if httpCookie.MaxAge != 300 || !httpCookie.Expires.Equal(now.Add(5*time.Minute)) {
		t.Fatalf("unexpected expiration: max-age=%d expires=%s", httpCookie.MaxAge, httpCookie.Expires)
	}
}

func TestCookieAttachWritesSetCookie(t *testing.T) {
	rr := testResponse(t)
	c := New("theme", "dark", 10, SameSite(SameSiteLax))

	if err := c.Attach(rr, WithNow(time.Date(2026, 5, 6, 12, 0, 0, 0, time.UTC))); err != nil {
		t.Fatalf("Attach returned error: %v", err)
	}

	headers := setCookieHeaders(t, rr)
	if len(headers) != 1 {
		t.Fatalf("expected one Set-Cookie header, got %d", len(headers))
	}
	header := headers[0]
	for _, want := range []string{"theme=dark", "Path=/", "Max-Age=600", "HttpOnly", "SameSite=Lax"} {
		if !strings.Contains(header, want) {
			t.Fatalf("Set-Cookie %q missing %q", header, want)
		}
	}
}

func TestCookieExpireAndForgetUseRemovalSemantics(t *testing.T) {
	for _, c := range []Cookie{
		Expire("legacy", Path("/admin"), Domain("example.test")),
		Forget("legacy", Path("/admin"), Domain("example.test")),
	} {
		httpCookie, err := c.ToHTTP()
		if err != nil {
			t.Fatalf("ToHTTP returned error: %v", err)
		}
		if httpCookie.Value != "" || httpCookie.MaxAge != -1 {
			t.Fatalf("unexpected expired cookie: %#v", httpCookie)
		}
		if httpCookie.Path != "/admin" || httpCookie.Domain != "example.test" {
			t.Fatalf("expire must preserve removal scope: %#v", httpCookie)
		}
	}
}

func TestCookieRejectsInvalidName(t *testing.T) {
	_, err := New("bad name", "x", 1).ToHTTP()
	if !errors.Is(err, ErrInvalidCookieName) {
		t.Fatalf("expected ErrInvalidCookieName, got %v", err)
	}
}

func TestCookieOptionsCoverRawScopeAndSameSiteModes(t *testing.T) {
	c := New("scoped", "ok", 0,
		Raw(true),
		ScopeOption(Scope{Path: "/panel", Domain: "example.test"}),
		SameSite(SameSiteNone),
	)
	if !c.Raw || c.Path != "/panel" || c.Domain != "example.test" {
		t.Fatalf("options were not applied: %#v", c)
	}
	if c.SameSite.ToHTTPSameSite() != http.SameSiteNoneMode {
		t.Fatalf("unexpected SameSite none mapping")
	}
	if SameSiteDefault.ToHTTPSameSite() != http.SameSiteDefaultMode {
		t.Fatalf("unexpected SameSite default mapping")
	}
}

func TestPackageAttachAndContextOption(t *testing.T) {
	rr := testResponse(t)
	ctx := context.WithValue(context.Background(), testContextKey("trace"), "ok")
	signer := contextCheckingSigner{t: t, want: ctx}

	if err := Attach(rr, New("signed", "v", 0), WithContext(ctx), WithSigner(signer)); err != nil {
		t.Fatalf("Attach returned error: %v", err)
	}
	headers := setCookieHeaders(t, rr)
	if len(headers) != 1 || !strings.Contains(headers[0], "signed=ctx:v") {
		t.Fatalf("unexpected signed cookie header: %#v", headers)
	}
}

func TestCookieAttachSecurityFailuresAreSensitive(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts []AttachOption
		want error
	}{
		{name: "encrypt", opts: []AttachOption{WithEncryptor(failingEncryptor{})}, want: ErrCookieEncryption},
		{name: "sign", opts: []AttachOption{WithSigner(failingSigner{})}, want: ErrCookieSignature},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := New("secret", "raw-client-secret", 0).Attach(testResponse(t), tc.opts...)
			if !errors.Is(err, tc.want) {
				t.Fatalf("expected %v, got %v", tc.want, err)
			}
			if strings.Contains(err.Error(), "raw-client-secret") {
				t.Fatalf("error leaked sensitive value: %v", err)
			}
		})
	}
}

func TestCookieNameValidationBranches(t *testing.T) {
	valid := []string{"azAZ09", "!#$%&'*+-.^_`|~"}
	for _, name := range valid {
		if !validCookieName(name) {
			t.Fatalf("expected %q to be valid", name)
		}
	}
	for _, name := range []string{"", "bad/name", "bad;name"} {
		if validCookieName(name) {
			t.Fatalf("expected %q to be invalid", name)
		}
	}
}

type contextCheckingSigner struct {
	t    *testing.T
	want context.Context
}

func (s contextCheckingSigner) Sign(ctx context.Context, _ string, value string) (string, error) {
	s.t.Helper()
	if ctx != s.want {
		s.t.Fatalf("unexpected context")
	}
	return "ctx:" + value, nil
}

func (s contextCheckingSigner) Unsign(_ context.Context, _ string, value string) (string, error) {
	return strings.TrimPrefix(value, "ctx:"), nil
}

type failingSigner struct{}

func (failingSigner) Sign(context.Context, string, string) (string, error) {
	return "", errors.New("raw-client-secret")
}

func (failingSigner) Unsign(context.Context, string, string) (string, error) {
	return "", errors.New("raw-client-secret")
}

type failingEncryptor struct{}

func (failingEncryptor) EncryptString(context.Context, string) (string, error) {
	return "", errors.New("raw-client-secret")
}

func (failingEncryptor) DecryptString(context.Context, string) (string, error) {
	return "", errors.New("raw-client-secret")
}
