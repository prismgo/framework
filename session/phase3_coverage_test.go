package session

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	configpkg "github.com/prismgo/framework/config"
	containercontract "github.com/prismgo/framework/contracts/container"
)

type failingEncryptor struct{}

func (failingEncryptor) Encrypt(context.Context, []byte) ([]byte, error) {
	return nil, errors.New("secret payload should not leak")
}

func (failingEncryptor) Decrypt(context.Context, []byte) ([]byte, error) {
	return nil, errors.New("secret payload should not leak")
}

func TestDriverRegistryAndResolution(t *testing.T) {
	name := "memory_phase3"
	first := newMemoryDriver()
	second := newMemoryDriver()
	Extend(name, func(Config) (Driver, error) { return first, nil })
	Extend(name, func(Config) (Driver, error) { return second, nil })
	resolved, err := ResolveDriver(name, DefaultConfig())
	if err != nil || resolved != second {
		t.Fatalf("ResolveDriver = %v, %v", resolved, err)
	}
	if _, err := ResolveDriver("missing_phase3", DefaultConfig()); !errors.Is(err, ErrDriverNotFound) {
		t.Fatalf("missing driver error = %v", err)
	}
	Extend("", func(Config) (Driver, error) { return first, nil })
	Extend("ignored_nil_phase3", nil)
	if _, err := ResolveDriver("ignored_nil_phase3", DefaultConfig()); !errors.Is(err, ErrDriverNotFound) {
		t.Fatalf("nil factory should be ignored, got %v", err)
	}
	nilName := "nil_phase3"
	Extend(nilName, func(Config) (Driver, error) { return nil, nil })
	if _, err := ResolveDriver(nilName, DefaultConfig()); !errors.Is(err, ErrDriverNotFound) {
		t.Fatalf("nil resolved driver error = %v", err)
	}
	if fileDriver, err := ResolveDriver(DefaultDriver, DefaultConfig()); err != nil || fileDriver == nil {
		t.Fatalf("builtin file driver = %v, %v", fileDriver, err)
	}
}

func TestEncryptionAndSensitiveErrors(t *testing.T) {
	plain := []byte("payload")
	encrypted, err := encryptPayload(context.Background(), nil, plain)
	if err != nil || string(encrypted) != "payload" {
		t.Fatalf("encrypt nop = %q, %v", encrypted, err)
	}
	encrypted[0] = 'P'
	if string(plain) != "payload" {
		t.Fatalf("encrypt returned shared slice")
	}
	decrypted, err := decryptPayload(context.Background(), nil, []byte("cipher"))
	if err != nil || string(decrypted) != "cipher" {
		t.Fatalf("decrypt nop = %q, %v", decrypted, err)
	}
	if _, err := encryptPayload(context.Background(), failingEncryptor{}, plain); !errors.Is(err, ErrEncryptionFailed) {
		t.Fatalf("encrypt failure = %v", err)
	}
	if _, err := decryptPayload(context.Background(), failingEncryptor{}, plain); !errors.Is(err, ErrDecryptionFailed) {
		t.Fatalf("decrypt failure = %v", err)
	}
	err = safeError("read", ErrPayloadMalformed)
	if err == nil || err.Error() != "session: read failed" || !errors.Is(err, ErrPayloadMalformed) {
		t.Fatalf("safeError = %v", err)
	}
	if got := (SensitiveError{}).Error(); got != "session: sensitive operation failed" {
		t.Fatalf("empty sensitive error = %q", got)
	}
	if safeError("noop", nil) != nil {
		t.Fatalf("safeError nil should return nil")
	}
}

func TestManagerBranchesAndLocking(t *testing.T) {
	manager, driver := newTestManager(t)
	if manager, err := NewManager(testConfig(t), nil); err != nil || manager == nil {
		t.Fatalf("NewManager nil driver default = %v, %v", manager, err)
	}
	if manager.Config().Cookie.Name == "" {
		t.Fatalf("Config not normalized")
	}
	payload := Payload{
		ID:           newSessionID(),
		Values:       map[string]any{"a": "b"},
		CreatedAt:    testNow().Add(-2 * time.Hour),
		LastActivity: testNow().Add(-2 * time.Hour),
	}
	driver.payloads[payload.ID] = payload
	store, err := manager.Start(context.Background(), testRequest(t, &http.Cookie{
		Name:  manager.cfg.Cookie.Name,
		Value: payload.ID,
	}), nil)
	if err != nil {
		t.Fatalf("Start expired error = %v", err)
	}
	if store.ID() == payload.ID {
		t.Fatalf("expired session was restored")
	}

	manager.cfg.Cookie.SameSite = "strict"
	if manager.sessionCookie(store.ID(), manager.expiresAt()).SameSite != http.SameSiteStrictMode {
		t.Fatalf("strict SameSite not mapped")
	}
	manager.cfg.Cookie.SameSite = "none"
	if manager.sessionCookie(store.ID(), manager.expiresAt()).SameSite != http.SameSiteNoneMode {
		t.Fatalf("none SameSite not mapped")
	}
	manager.cfg.Cookie.SameSite = "unknown"
	if manager.sessionCookie(store.ID(), manager.expiresAt()).SameSite != http.SameSiteDefaultMode {
		t.Fatalf("default SameSite not mapped")
	}
}

func TestFacadeFallbackAndMissingContextBranches(t *testing.T) {
	manager, _ := NewManager(testConfig(t), newMemoryDriver())
	bindSessionManagerForTest(t, manager)
	if got := Get(nil, "missing", "fallback"); got != "fallback" {
		t.Fatalf("missing context Get = %v", got)
	}
	if got := Pull(nil, "missing", "fallback"); got != "fallback" {
		t.Fatalf("missing context Pull = %v", got)
	}
	for name, err := range map[string]error{
		"Put":        Put(nil, "a", "b"),
		"Forget":     Forget(nil, "a"),
		"Flush":      Flush(nil),
		"Flash":      Flash(nil, "a", "b"),
		"Now":        Now(nil, "a", "b"),
		"Reflash":    Reflash(nil),
		"Keep":       Keep(nil, "a"),
		"Regenerate": Regenerate(nil),
		"Invalidate": Invalidate(nil),
		"Save":       Save(nil),
	} {
		if !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("%s missing context error = %v", name, err)
		}
	}
	store, err := Start(context.Background(), testRequest(t), nil)
	if err != nil {
		t.Fatalf("Start error = %v", err)
	}
	store.Put("key", "value")
	if err := store.Save(context.Background()); err != nil {
		t.Fatalf("Save error = %v", err)
	}
	restored, err := manager.Start(context.Background(), testRequest(t, &http.Cookie{
		Name:  manager.cfg.Cookie.Name,
		Value: store.ID(),
	}), nil)
	if err != nil || restored.Get("key") != "value" {
		t.Fatalf("restore = %v, %v", restored.Get("key"), err)
	}
	if err := manager.driver.Destroy(context.Background(), store.ID()); err != nil {
		t.Fatalf("default destroy error = %v", err)
	}
	if err := manager.driver.GC(context.Background(), testNow()); err != nil {
		t.Fatalf("default gc error = %v", err)
	}
	if _, err := manager.driver.Read(context.Background(), store.ID()); !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("volatile read after destroy = %v", err)
	}
}

func TestApplicationRegistrationAndVolatileDriverBranches(t *testing.T) {
	registry := useSessionTestContainer(t)
	if err := registry.Singleton(serviceKey, func(containercontract.Resolver) (any, error) {
		return NewManager(testConfig(t), nil)
	}); err != nil {
		t.Fatalf("register session factory: %v", err)
	}
	if resolved := Resolve(); resolved == nil {
		t.Fatal("Resolve registered factory returned nil")
	}
	registry.Forget(serviceKey)
	bindSessionConfigInRegistry(t, registry, configpkg.New())
	if err := (ServiceProvider{}).Register(providerTestApp{registry: registry}); err != nil {
		t.Fatalf("register service provider: %v", err)
	}
	if resolved := Resolve(); resolved == nil {
		t.Fatal("Resolve application factory returned nil")
	}
	if manager, err := NewManagerFromConfig(); err != nil || manager == nil {
		t.Fatalf("NewManagerFromConfig = %v, %v", manager, err)
	}

	driver := newMemoryDriver()
	id := newSessionID()
	payload := Payload{ID: id, Values: map[string]any{"a": "b"}, OldFlash: []string{"old"}, NewFlash: []string{"new"}}
	if err := driver.Write(context.Background(), id, payload, nil); err != nil {
		t.Fatalf("volatile Write error = %v", err)
	}
	restored, err := driver.Read(context.Background(), id)
	if err != nil || restored.Values["a"] != "b" {
		t.Fatalf("volatile Read = %#v, %v", restored, err)
	}
	if err := driver.Destroy(context.Background(), id); err != nil {
		t.Fatalf("volatile Destroy error = %v", err)
	}
	if _, err := driver.Read(context.Background(), id); !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("volatile missing error = %v", err)
	}
	if err := driver.GC(context.Background(), testNow()); err != nil {
		t.Fatalf("volatile GC error = %v", err)
	}
}

func TestNumericAndResponseWriterBranches(t *testing.T) {
	for _, value := range []any{int8(1), int16(1), int32(1), uint(1), uint8(1), uint16(1), uint32(1), uint64(1), float64(1)} {
		if got, err := numericValue(value); err != nil || got != 1 {
			t.Fatalf("numericValue(%T) = %d, %v", value, got, err)
		}
	}
	if _, err := numericValue(float64(1.5)); !errors.Is(err, ErrInvalidValueType) {
		t.Fatalf("float numeric error = %v", err)
	}
	if _, err := unsignedToInt64(uint64(^uint64(0))); !errors.Is(err, ErrInvalidValueType) {
		t.Fatalf("overflow error = %v", err)
	}

	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	buffered := newBufferedResponseWriter(ctx.Writer)
	if _, err := buffered.Write([]byte("ok")); err != nil {
		t.Fatalf("buffered write error = %v", err)
	}
	buffered.FlushBuffered()
	if rec.Code != http.StatusOK || rec.Body.String() != "ok" {
		t.Fatalf("buffered response = %d %q", rec.Code, rec.Body.String())
	}
}

type countingLockDriver struct {
	*memoryDriver
	locked int32
}

func (d *countingLockDriver) Lock(context.Context, string, time.Duration, time.Duration) (Lock, error) {
	atomic.AddInt32(&d.locked, 1)
	return countingLock{}, nil
}

type countingLock struct{}

func (countingLock) Release(context.Context) error { return nil }

func TestManagerLockAndErrorBranches(t *testing.T) {
	driver := &countingLockDriver{memoryDriver: newMemoryDriver()}
	manager, err := NewManager(testConfig(t), driver)
	if err != nil {
		t.Fatalf("NewManager error = %v", err)
	}
	store := newStore(manager, Payload{}, nil)
	store.Put("a", "b")
	if err := store.Save(context.Background()); err != nil {
		t.Fatalf("locked Save error = %v", err)
	}
	if atomic.LoadInt32(&driver.locked) != 1 {
		t.Fatalf("lock count = %d", driver.locked)
	}
	if err := manager.Save(context.Background(), nil); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("Save nil error = %v", err)
	}
	if err := (*Store)(nil).Save(context.Background()); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("nil store Save error = %v", err)
	}
	if _, err := (*Manager)(nil).Start(context.Background(), nil, nil); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("nil manager Start error = %v", err)
	}
	if got := requestSessionID(nil, "session"); got != "" {
		t.Fatalf("nil request id = %q", got)
	}
	if cloneBytes(nil) != nil {
		t.Fatalf("cloneBytes nil should stay nil")
	}
}
