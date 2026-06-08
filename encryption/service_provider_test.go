package encryption

import (
	"errors"
	"testing"

	"github.com/prismgo/framework/container"
	containercontract "github.com/prismgo/framework/contracts/container"
	contractsencryption "github.com/prismgo/framework/contracts/encryption"
)

// TestServiceProviderRegistersLazyFactory 验证 provider 只注册 lazy singleton，不在 Register 阶段读取 APP_KEY。
func TestServiceProviderRegistersLazyFactory(t *testing.T) {
	registry := container.NewContainer()
	if err := (ServiceProvider{}).Register(providerTestApp{registry: registry}); err != nil {
		t.Fatalf("Register failed: %v", err)
	}
	if !registry.Bound(serviceKey) {
		t.Fatal("provider Register should bind encryption singleton factory")
	}
	if registry.Resolved(serviceKey) {
		t.Fatal("provider Register should not resolve encryption singleton")
	}
}

// TestServiceProviderPreservesExplicitBinding 验证业务或测试显式注入的 encrypter 不会被默认 provider 覆盖。
func TestServiceProviderPreservesExplicitBinding(t *testing.T) {
	registry := container.NewContainer()
	explicit := &recordingEncrypter{}
	if err := registry.Instance(serviceKey, contractsencryption.Encrypter(explicit)); err != nil {
		t.Fatalf("seed explicit encrypter: %v", err)
	}
	container.SetProvider(func() *container.Container { return registry })
	t.Cleanup(func() { container.SetProvider(nil) })

	if err := (ServiceProvider{}).Register(providerTestApp{registry: registry}); err != nil {
		t.Fatalf("Register failed: %v", err)
	}
	if got := Resolve(); got != explicit {
		t.Fatal("service provider should preserve explicit encryption binding")
	}
}

// TestServiceProviderInvalidAppKeyFailsOnlyOnResolve 验证无效 APP_KEY 不阻塞 Register，只在首次解析时报错。
func TestServiceProviderInvalidAppKeyFailsOnlyOnResolve(t *testing.T) {
	registry := container.NewContainer()
	container.SetProvider(func() *container.Container { return registry })
	t.Cleanup(func() { container.SetProvider(nil) })
	if err := registry.Instance("config.default", newAppConfigFromEnv(t, map[string]string{
		"APP_KEY":    "",
		"APP_CIPHER": CipherAES256GCM,
	})); err != nil {
		t.Fatalf("bind config: %v", err)
	}

	if err := (ServiceProvider{}).Register(providerTestApp{registry: registry}); err != nil {
		t.Fatalf("Register failed: %v", err)
	}
	_, err := container.Make[contractsencryption.Encrypter](serviceKey)
	if !errors.Is(err, ErrInvalidKey) {
		t.Fatalf("resolve error = %v, want ErrInvalidKey", err)
	}
}

// TestServiceProviderFactoryUsesDefaultConfigFacade 验证默认 singleton factory 首次解析时调用 NewFromConfig(nil)。
func TestServiceProviderFactoryUsesDefaultConfigFacade(t *testing.T) {
	registry := container.NewContainer()
	container.SetProvider(func() *container.Container { return registry })
	t.Cleanup(func() { container.SetProvider(nil) })
	if err := registry.Instance("config.default", newAppConfigFromEnv(t, map[string]string{
		"APP_KEY":    testKey(4),
		"APP_CIPHER": CipherAES256GCM,
	})); err != nil {
		t.Fatalf("bind config: %v", err)
	}
	if err := (ServiceProvider{}).Register(providerTestApp{registry: registry}); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	got, err := container.Make[contractsencryption.Encrypter](serviceKey)
	if err != nil {
		t.Fatalf("resolve encrypter: %v", err)
	}
	token, err := got.Encrypt(t.Context(), []byte("from provider"))
	if err != nil {
		t.Fatalf("encrypt through provider encrypter: %v", err)
	}
	plain, err := got.Decrypt(t.Context(), token)
	if err != nil {
		t.Fatalf("decrypt through provider encrypter: %v", err)
	}
	if string(plain) != "from provider" {
		t.Fatalf("plain = %q", plain)
	}
}

type providerTestApp struct {
	registry containercontract.Container
}

func (a providerTestApp) Container() containercontract.Container { return a.registry }
