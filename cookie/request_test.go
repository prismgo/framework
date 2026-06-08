package cookie

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
)

type testSigner struct {
	fail bool
}

func (s testSigner) Sign(_ context.Context, _ string, value string) (string, error) {
	return "signed:" + value, nil
}

func (s testSigner) Unsign(_ context.Context, _ string, value string) (string, error) {
	if s.fail || !strings.HasPrefix(value, "signed:") {
		return "", errors.New("raw-client-secret")
	}
	return strings.TrimPrefix(value, "signed:"), nil
}

type testEncryptor struct {
	fail bool
}

func (e testEncryptor) EncryptString(_ context.Context, value string) (string, error) {
	return "enc:" + value, nil
}

func (e testEncryptor) DecryptString(_ context.Context, value string) (string, error) {
	if e.fail || !strings.HasPrefix(value, "enc:") {
		return "", errors.New("raw-client-secret")
	}
	return strings.TrimPrefix(value, "enc:"), nil
}

func TestRequestCookieReadsPlainValue(t *testing.T) {
	req := testRequest(t, &http.Cookie{Name: "notice", Value: "ok"})

	value, err := RequestCookie(req, "notice")
	if err != nil {
		t.Fatalf("RequestCookie returned error: %v", err)
	}
	if value != "ok" {
		t.Fatalf("unexpected value: %q", value)
	}
}

func TestRequestCookieSecurityHooksAndSensitiveErrors(t *testing.T) {
	rr := testResponse(t)
	c := New("secret", "value", 5)
	if err := c.Attach(rr, WithSigner(testSigner{}), WithEncryptor(testEncryptor{})); err != nil {
		t.Fatalf("Attach returned error: %v", err)
	}
	cookies := rr.Result().Cookies()
	if len(cookies) != 1 {
		t.Fatalf("expected one cookie, got %d", len(cookies))
	}
	req := testRequest(t, cookies[0])

	value, err := RequestCookie(req, "secret", RequestWithSigner(testSigner{}), RequestWithEncryptor(testEncryptor{}))
	if err != nil {
		t.Fatalf("RequestCookie returned error: %v", err)
	}
	if value != "value" {
		t.Fatalf("unexpected decoded value: %q", value)
	}

	_, err = RequestCookie(req, "secret", RequestWithSigner(testSigner{fail: true}))
	if !errors.Is(err, ErrCookieSignature) {
		t.Fatalf("expected ErrCookieSignature, got %v", err)
	}
	if strings.Contains(err.Error(), cookies[0].Value) || strings.Contains(err.Error(), "raw-client-secret") {
		t.Fatalf("error leaked sensitive value: %v", err)
	}
}

func TestRequestCookieMissing(t *testing.T) {
	_, err := RequestCookie(testRequest(t), "missing")
	if !errors.Is(err, ErrCookieNotFound) {
		t.Fatalf("expected ErrCookieNotFound, got %v", err)
	}
}

func TestRequestCookieInvalidRequestAndDecryptFailure(t *testing.T) {
	if _, err := RequestCookie(nil, "notice"); !errors.Is(err, ErrCookieNotFound) {
		t.Fatalf("expected not found for nil request, got %v", err)
	}
	if _, err := RequestCookie(testRequest(t), "bad name"); !errors.Is(err, ErrCookieNotFound) {
		t.Fatalf("expected not found for invalid name, got %v", err)
	}

	req := testRequest(t, &http.Cookie{Name: "secret", Value: "signed:enc:value"})
	ctx := context.WithValue(context.Background(), testContextKey("request"), "ok")
	_, err := RequestCookie(req, "secret",
		RequestWithContext(ctx),
		RequestWithSigner(testSigner{}),
		RequestWithEncryptor(testEncryptor{fail: true}),
	)
	if !errors.Is(err, ErrCookieDecryption) {
		t.Fatalf("expected ErrCookieDecryption, got %v", err)
	}
	if strings.Contains(err.Error(), "signed:enc:value") || strings.Contains(err.Error(), "raw-client-secret") {
		t.Fatalf("error leaked sensitive value: %v", err)
	}
}
