package foundation

import (
	"errors"
	"sync"
	"testing"

	containercontract "github.com/prismgo/framework/contracts/container"
	providercontract "github.com/prismgo/framework/contracts/provider"
)

type deferredTestResource struct {
	value string
}

// deferredTestProvider 模拟只在 strict facade Resolve 时加载的 provider。
//
// 设计说明：测试通过计数器观察 Register/Boot 的执行次数，避免断言
// foundation 内部 map 结构，从外部行为验证 deferred provider 生命周期。
type deferredTestProvider struct {
	name          string
	keys          []string
	registerErr   error
	bootErr       error
	registerCount int
	bootCount     int
	mu            sync.Mutex
}

func (p *deferredTestProvider) Name() string { return p.name }

func (p *deferredTestProvider) Provides() []string { return p.keys }

func (p *deferredTestProvider) Register(app providercontract.Application) error {
	p.mu.Lock()
	p.registerCount++
	p.mu.Unlock()
	if p.registerErr != nil {
		return p.registerErr
	}
	for _, key := range p.keys {
		key := key
		if err := app.Container().Singleton(key, func(containercontract.Resolver) (any, error) {
			return &deferredTestResource{value: p.name}, nil
		}); err != nil {
			return err
		}
	}
	return nil
}

func (p *deferredTestProvider) Boot(providercontract.Application) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.bootCount++
	return p.bootErr
}

func (p *deferredTestProvider) counts() (int, int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.registerCount, p.bootCount
}

func TestDeferredProviderLoadsOnFirstStrictResolveAfterBoot(t *testing.T) {
	app := NewApplication()
	t.Cleanup(func() { _ = app.Close() })

	provider := &deferredTestProvider{name: "deferred.after.boot", keys: []string{"deferred.after.boot"}}
	if err := app.RegisterProvider(provider); err != nil {
		t.Fatalf("register deferred provider: %v", err)
	}
	if err := app.Boot(); err != nil {
		t.Fatalf("boot app: %v", err)
	}
	if registerCount, bootCount := provider.counts(); registerCount != 0 || bootCount != 0 {
		t.Fatalf("deferred provider should not run during boot, register=%d boot=%d", registerCount, bootCount)
	}

	raw, err := app.Make("deferred.after.boot")
	if err != nil {
		t.Fatalf("resolve deferred service: %v", err)
	}
	got, ok := raw.(*deferredTestResource)
	if !ok {
		t.Fatalf("resolved resource type = %T, want *deferredTestResource", raw)
	}
	if got == nil || got.value != "deferred.after.boot" {
		t.Fatalf("resolved resource = %#v", got)
	}
	if registerCount, bootCount := provider.counts(); registerCount != 1 || bootCount != 1 {
		t.Fatalf("deferred provider counts after resolve = register:%d boot:%d, want 1/1", registerCount, bootCount)
	}
}

func TestDeferredProviderLoadedBeforeBootBootsInRepositoryOrder(t *testing.T) {
	app := NewApplication()
	t.Cleanup(func() { _ = app.Close() })

	provider := &deferredTestProvider{name: "deferred.before.boot", keys: []string{"deferred.before.boot"}}
	if err := app.RegisterProvider(provider); err != nil {
		t.Fatalf("register deferred provider: %v", err)
	}
	if _, err := app.Make("deferred.before.boot"); err != nil {
		t.Fatalf("resolve deferred service before boot: %v", err)
	}
	if registerCount, bootCount := provider.counts(); registerCount != 1 || bootCount != 0 {
		t.Fatalf("before boot resolve counts = register:%d boot:%d, want 1/0", registerCount, bootCount)
	}

	if err := app.Boot(); err != nil {
		t.Fatalf("boot app: %v", err)
	}
	if registerCount, bootCount := provider.counts(); registerCount != 1 || bootCount != 1 {
		t.Fatalf("after boot counts = register:%d boot:%d, want 1/1", registerCount, bootCount)
	}
}

func TestDeferredProviderConcurrentResolveRegistersOnce(t *testing.T) {
	app := NewApplication()
	t.Cleanup(func() { _ = app.Close() })

	provider := &deferredTestProvider{name: "deferred.concurrent", keys: []string{"deferred.concurrent"}}
	if err := app.RegisterProvider(provider); err != nil {
		t.Fatalf("register deferred provider: %v", err)
	}

	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := app.Make("deferred.concurrent")
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("resolve deferred service: %v", err)
		}
	}
	if registerCount, bootCount := provider.counts(); registerCount != 1 || bootCount != 0 {
		t.Fatalf("concurrent resolve counts = register:%d boot:%d, want 1/0", registerCount, bootCount)
	}
}

func TestDeferredProviderProvidesValidation(t *testing.T) {
	app := NewApplication()
	t.Cleanup(func() { _ = app.Close() })

	if err := app.RegisterProvider(&deferredTestProvider{name: "empty.provides"}); err == nil {
		t.Fatal("expected empty Provides error")
	}
	if err := app.RegisterProvider(&deferredTestProvider{name: "blank.provides", keys: []string{"  "}}); err == nil {
		t.Fatal("expected blank service key error")
	}

	first := &deferredTestProvider{name: "first.provider", keys: []string{"duplicate.service"}}
	second := &deferredTestProvider{name: "second.provider", keys: []string{"duplicate.service"}}
	if err := app.RegisterProvider(first); err != nil {
		t.Fatalf("register first deferred provider: %v", err)
	}
	if err := app.RegisterProvider(second); err == nil {
		t.Fatal("expected duplicate deferred service key error")
	}
}

func TestDeferredProviderOneKeyLoadsAllProvidedKeys(t *testing.T) {
	app := NewApplication()
	t.Cleanup(func() { _ = app.Close() })

	provider := &deferredTestProvider{name: "deferred.multi", keys: []string{"deferred.multi.a", "deferred.multi.b"}}
	if err := app.RegisterProvider(provider); err != nil {
		t.Fatalf("register deferred provider: %v", err)
	}
	if _, err := app.Make("deferred.multi.b"); err != nil {
		t.Fatalf("resolve second deferred key: %v", err)
	}
	if _, err := app.Make("deferred.multi.a"); err != nil {
		t.Fatalf("resolve first deferred key after provider load: %v", err)
	}
	if registerCount, _ := provider.counts(); registerCount != 1 {
		t.Fatalf("multi-key provider register count = %d, want 1", registerCount)
	}
}

func TestDeferredProviderRegisterFailureCanRetry(t *testing.T) {
	app := NewApplication()
	t.Cleanup(func() { _ = app.Close() })

	registerErr := errors.New("register failed")
	provider := &deferredTestProvider{name: "deferred.register.retry", keys: []string{"deferred.register.retry"}, registerErr: registerErr}
	if err := app.RegisterProvider(provider); err != nil {
		t.Fatalf("register deferred provider: %v", err)
	}
	if _, err := app.Make("deferred.register.retry"); !errors.Is(err, registerErr) {
		t.Fatalf("first resolve error = %v, want %v", err, registerErr)
	}

	provider.registerErr = nil
	if _, err := app.Make("deferred.register.retry"); err != nil {
		t.Fatalf("retry resolve deferred service: %v", err)
	}
	if registerCount, bootCount := provider.counts(); registerCount != 2 || bootCount != 0 {
		t.Fatalf("retry counts = register:%d boot:%d, want 2/0", registerCount, bootCount)
	}
}

func TestDeferredProviderBootFailureAfterBootCanRetry(t *testing.T) {
	app := NewApplication()
	t.Cleanup(func() { _ = app.Close() })

	bootErr := errors.New("boot failed")
	provider := &deferredTestProvider{name: "deferred.boot.retry", keys: []string{"deferred.boot.retry"}, bootErr: bootErr}
	if err := app.RegisterProvider(provider); err != nil {
		t.Fatalf("register deferred provider: %v", err)
	}
	if err := app.Boot(); err != nil {
		t.Fatalf("boot app: %v", err)
	}
	if _, err := app.Make("deferred.boot.retry"); !errors.Is(err, bootErr) {
		t.Fatalf("first resolve error = %v, want %v", err, bootErr)
	}

	provider.bootErr = nil
	if _, err := app.Make("deferred.boot.retry"); err != nil {
		t.Fatalf("retry resolve deferred service: %v", err)
	}
	if registerCount, bootCount := provider.counts(); registerCount != 1 || bootCount != 2 {
		t.Fatalf("retry counts = register:%d boot:%d, want 1/2", registerCount, bootCount)
	}
}
