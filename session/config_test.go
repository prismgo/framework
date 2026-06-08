package session

import (
	"testing"
	"time"

	configpkg "github.com/prismgo/framework/config"
)

func TestConfigFromRepositoryUsesSessionDefaults(t *testing.T) {
	cfg := ConfigFromRepository(configpkg.New())

	if cfg.Driver != DefaultDriver || cfg.Files != DefaultFilesPath {
		t.Fatalf("defaults driver=%q files=%q", cfg.Driver, cfg.Files)
	}
	if cfg.Lifetime != time.Duration(DefaultLifetimeMinutes)*time.Minute {
		t.Fatalf("lifetime = %v", cfg.Lifetime)
	}
	if cfg.Cookie.Name != DefaultCookieName || cfg.Cookie.Path != DefaultCookiePath {
		t.Fatalf("cookie defaults = %#v", cfg.Cookie)
	}
	if !cfg.Cookie.HTTPOnly || cfg.Cookie.SameSite != DefaultSameSite {
		t.Fatalf("cookie security defaults = %#v", cfg.Cookie)
	}
	if cfg.Lock.TTL != time.Duration(DefaultLockSeconds)*time.Second ||
		cfg.Lock.Wait != time.Duration(DefaultLockWaitSeconds)*time.Second {
		t.Fatalf("lock defaults = %#v", cfg.Lock)
	}
}

func TestConfigFallbackBranches(t *testing.T) {
	cfg := ConfigFromRepository(nil)
	if cfg.Driver != DefaultDriver || cfg.Cookie.Name != DefaultCookieName {
		t.Fatalf("nil repository config = %#v", cfg)
	}
	if minutesDuration(0) != time.Duration(DefaultLifetimeMinutes)*time.Minute {
		t.Fatalf("minutesDuration did not use fallback")
	}
	if secondsDuration(0) != time.Duration(DefaultLockSeconds)*time.Second {
		t.Fatalf("secondsDuration did not use fallback")
	}

	raw := Config{Cookie: CookieConfig{HTTPOnly: false}, Lifetime: -time.Minute}
	normalized := normalizeConfig(raw)
	if normalized.Driver != DefaultDriver || normalized.Lifetime != time.Duration(DefaultLifetimeMinutes)*time.Minute {
		t.Fatalf("normalized base config = %#v", normalized)
	}
	if normalized.Cookie.Name != DefaultCookieName || normalized.Files != DefaultFilesPath {
		t.Fatalf("normalized paths = %#v", normalized)
	}
}

func TestConfigFromRepositoryReadsSessionNamespace(t *testing.T) {
	configpkg.Add("session", func() map[string]any {
		return map[string]any{
			"driver":          configpkg.Env("SESSION_DRIVER", "file"),
			"lifetime":        configpkg.Env("SESSION_LIFETIME", 120),
			"expire_on_close": configpkg.Env("SESSION_EXPIRE_ON_CLOSE", false),
			"encrypt":         configpkg.Env("SESSION_ENCRYPT", false),
			"cookie":          configpkg.Env("SESSION_COOKIE", "prismgo_session"),
			"path":            configpkg.Env("SESSION_PATH", "/"),
			"domain":          configpkg.Env("SESSION_DOMAIN", ""),
			"secure":          configpkg.Env("SESSION_SECURE_COOKIE", false),
			"http_only":       configpkg.Env("SESSION_HTTP_ONLY", true),
			"same_site":       configpkg.Env("SESSION_SAME_SITE", "lax"),
			"files":           configpkg.Env("SESSION_FILES", "storage/framework/sessions"),
			"lock_seconds":    configpkg.Env("SESSION_LOCK_SECONDS", 10),
			"lock_wait":       configpkg.Env("SESSION_LOCK_WAIT_SECONDS", 10),
		}
	})
	t.Setenv("SESSION_LIFETIME", "45")
	t.Setenv("SESSION_EXPIRE_ON_CLOSE", "true")
	t.Setenv("SESSION_ENCRYPT", "true")
	t.Setenv("SESSION_COOKIE", "custom_session")
	t.Setenv("SESSION_DOMAIN", "example.test")
	t.Setenv("SESSION_SECURE_COOKIE", "true")
	t.Setenv("SESSION_SAME_SITE", "strict")
	t.Setenv("SESSION_FILES", t.TempDir())
	t.Setenv("SESSION_LOCK_SECONDS", "2")
	t.Setenv("SESSION_LOCK_WAIT_SECONDS", "3")

	repo, err := configpkg.NewFromFile(t.TempDir() + "/missing.env")
	if err != nil {
		t.Fatalf("NewFromFile error = %v", err)
	}
	cfg := ConfigFromRepository(repo)

	if cfg.Lifetime != 45*time.Minute || !cfg.ExpireOnClose || !cfg.Encrypt {
		t.Fatalf("session flags = %#v", cfg)
	}
	if cfg.Cookie.Name != "custom_session" || cfg.Cookie.Domain != "example.test" || !cfg.Cookie.Secure {
		t.Fatalf("cookie config = %#v", cfg.Cookie)
	}
	if cfg.Cookie.SameSite != "strict" || cfg.Lock.TTL != 2*time.Second || cfg.Lock.Wait != 3*time.Second {
		t.Fatalf("sameSite/lock config = %#v %#v", cfg.Cookie, cfg.Lock)
	}
}

func TestNewManagerDefaultsToFileDriver(t *testing.T) {
	cfg := testConfig(t)
	manager, err := NewManager(cfg, nil)
	if err != nil {
		t.Fatalf("NewManager default file error = %v", err)
	}
	if _, ok := manager.driver.(*FileDriver); !ok {
		t.Fatalf("driver = %T, want *FileDriver", manager.driver)
	}
}
