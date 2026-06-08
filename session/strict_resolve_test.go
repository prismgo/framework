package session

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	configpkg "github.com/prismgo/framework/config"
	"github.com/prismgo/framework/container"
	contractsencryption "github.com/prismgo/framework/contracts/encryption"
)

func TestResolveReturnsDriverErrorForStrictPath(t *testing.T) {
	registry := useSessionTestContainer(t)
	registerStrictSessionConfig(t, registry, "strict-missing-driver")
	if err := (ServiceProvider{}).Register(providerTestApp{registry: registry}); err != nil {
		t.Fatalf("register service provider: %v", err)
	}

	_, err := registry.Make(serviceKey)
	if !errors.Is(err, ErrDriverNotFound) {
		t.Fatalf("container Make error = %v, want ErrDriverNotFound", err)
	}
}

func TestResolvePanicsWhenStrictResolveFails(t *testing.T) {
	registry := useSessionTestContainer(t)
	registerStrictSessionConfig(t, registry, "strict-missing-driver")
	if err := (ServiceProvider{}).Register(providerTestApp{registry: registry}); err != nil {
		t.Fatalf("register service provider: %v", err)
	}

	defer func() {
		if recovered := recover(); recovered == nil {
			t.Fatal("Resolve with invalid strict config did not panic")
		}
	}()

	_ = Resolve()
}

func TestConfigFromFacadeStrictResolvesEncryptionContractWhenEnabled(t *testing.T) {
	registry := useSessionTestContainer(t)
	registerStrictEncryptedSessionConfig(t, registry)
	encryptor := strictTestEncrypter{}
	if err := registry.Instance("encryption.default", contractsencryption.Encrypter(encryptor)); err != nil {
		t.Fatalf("bind encryption service: %v", err)
	}

	cfg, err := ConfigFromFacadeStrict()
	if err != nil {
		t.Fatalf("ConfigFromFacadeStrict error = %v", err)
	}
	if cfg.Encryptor == nil {
		t.Fatal("ConfigFromFacadeStrict did not resolve encryption.default for encrypted sessions")
	}
	encrypted, err := cfg.Encryptor.Encrypt(context.Background(), []byte("payload"))
	if err != nil {
		t.Fatalf("resolved encryptor Encrypt error = %v", err)
	}
	if string(encrypted) != "strict:payload" {
		t.Fatalf("resolved encryptor output = %q", encrypted)
	}
}

func TestConfigFromFacadeStrictFailsWhenEncryptedSessionHasNoEncryptionService(t *testing.T) {
	registry := useSessionTestContainer(t)
	registerStrictEncryptedSessionConfig(t, registry)

	cfg, err := ConfigFromFacadeStrict()
	if err == nil {
		t.Fatalf("ConfigFromFacadeStrict = %#v, want encryption service error", cfg)
	}
}

func registerStrictSessionConfig(t *testing.T, registry *container.Container, driver string) {
	t.Helper()
	configpkg.Add("session", func() map[string]any {
		return map[string]any{
			"driver":     driver,
			"connection": "default",
			"prefix":     "strict_test",
			"lifetime":   120,
		}
	})
	configpkg.Add("redis", func() map[string]any {
		return map[string]any{
			"default": map[string]any{"host": "127.0.0.1", "port": "6379", "database": 0},
		}
	})
	cfg := configpkg.New()
	if err := cfg.ReloadFromFile(filepath.Join(t.TempDir(), ".env")); err != nil {
		t.Fatalf("reload config: %v", err)
	}
	bindSessionConfigInRegistry(t, registry, cfg)
}

func registerStrictEncryptedSessionConfig(t *testing.T, registry *container.Container) {
	t.Helper()
	configpkg.Add("session", func() map[string]any {
		return map[string]any{
			"driver":     "file",
			"encrypt":    true,
			"connection": "default",
			"prefix":     "strict_test",
			"lifetime":   120,
		}
	})
	cfg := configpkg.New()
	if err := cfg.ReloadFromFile(filepath.Join(t.TempDir(), ".env")); err != nil {
		t.Fatalf("reload config: %v", err)
	}
	bindSessionConfigInRegistry(t, registry, cfg)
}

type strictTestEncrypter struct{}

func (strictTestEncrypter) Encrypt(_ context.Context, plaintext []byte) ([]byte, error) {
	return append([]byte("strict:"), plaintext...), nil
}

func (strictTestEncrypter) Decrypt(_ context.Context, ciphertext []byte) ([]byte, error) {
	return append([]byte(nil), ciphertext...), nil
}
