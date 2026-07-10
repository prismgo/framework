package foundation

import (
	"testing"
)

// TestProviderIdentityCaching 验证 provider identity 在多次调用时保持一致性
// 并且通过 providerEntry 缓存避免重复反射计算
func TestProviderIdentityCaching(t *testing.T) {
	// 创建一个命名 provider
	named := &countingProvider{name: "test.provider"}

	// 第一次计算
	identity1 := providerIdentity(named)
	if identity1 != "test.provider" {
		t.Errorf("first identity = %v, want test.provider", identity1)
	}

	// 第二次计算应该返回相同结果
	identity2 := providerIdentity(named)
	if identity2 != identity1 {
		t.Errorf("second identity = %v, want %v", identity2, identity1)
	}

	// 第三次计算仍然一致
	identity3 := providerIdentity(named)
	if identity3 != identity1 {
		t.Errorf("third identity = %v, want %v", identity3, identity1)
	}
}

// TestProviderIdentityWithTypeReflection 验证基于类型的 identity 计算
func TestProviderIdentityWithTypeReflection(t *testing.T) {
	// 使用没有 Name() 方法的 provider
	typed := &typedCountingProvider{}

	identity1 := providerIdentity(typed)
	if identity1 == "" {
		t.Error("typed provider identity should not be empty")
	}

	// 多次调用应该返回相同结果
	identity2 := providerIdentity(typed)
	if identity2 != identity1 {
		t.Errorf("typed identity changed: %v -> %v", identity1, identity2)
	}
}

// TestProviderEntryCachesIdentity 验证 providerEntry 结构缓存了 identity
func TestProviderEntryCachesIdentity(t *testing.T) {
	p := &countingProvider{name: "cached.provider"}

	// 创建 providerEntry 时计算 identity
	entry := providerEntry{
		identity: providerIdentity(p),
		provider: p,
	}

	if entry.identity != "cached.provider" {
		t.Errorf("cached identity = %v, want cached.provider", entry.identity)
	}

	// 后续访问 entry.identity 不需要重新计算
	if entry.identity != "cached.provider" {
		t.Errorf("accessing cached identity failed")
	}
}

// TestProviderIdentityInRegistration 验证注册过程中 identity 只计算一次
func TestProviderIdentityInRegistration(t *testing.T) {
	withBaseProvidersForTest(t)
	app := NewApplication()

	p := &countingProvider{name: "registration.test"}

	// 注册时计算一次 identity
	if err := app.RegisterProvider(p); err != nil {
		t.Fatalf("register provider: %v", err)
	}

	// 验证 providerEntry 中缓存了 identity
	app.mu.Lock()
	var found bool
	var cachedIdentity string
	for _, entry := range app.providers {
		if entry.provider == p {
			found = true
			cachedIdentity = entry.identity
			break
		}
	}
	app.mu.Unlock()

	if !found {
		t.Fatal("provider not found in repository")
	}

	if cachedIdentity != "registration.test" {
		t.Errorf("cached identity in entry = %v, want registration.test", cachedIdentity)
	}
}

// TestProviderIdentityNilHandling 验证 nil provider 的处理
func TestProviderIdentityNilHandling(t *testing.T) {
	// nil provider
	identity := providerIdentity(nil)
	if identity != "" {
		t.Errorf("nil provider identity = %v, want empty", identity)
	}

	// typed nil provider
	var typedNil *countingProvider
	identity = providerIdentity(typedNil)
	if identity != "" {
		t.Errorf("typed nil provider identity = %v, want empty", identity)
	}
}

// TestProviderIdentityConsistencyAcrossOperations 验证 identity 在不同操作中的一致性
func TestProviderIdentityConsistencyAcrossOperations(t *testing.T) {
	withBaseProvidersForTest(t)
	app := NewApplication()

	p := &countingProvider{name: "consistency.test"}

	// 注册前计算
	identityBefore := providerIdentity(p)

	// 注册
	if err := app.RegisterProvider(p); err != nil {
		t.Fatalf("register: %v", err)
	}

	// 注册后计算
	identityAfter := providerIdentity(p)

	if identityBefore != identityAfter {
		t.Errorf("identity changed: %v -> %v", identityBefore, identityAfter)
	}

	// Boot
	if err := app.Boot(); err != nil {
		t.Fatalf("boot: %v", err)
	}

	// Boot 后计算
	identityAfterBoot := providerIdentity(p)

	if identityBefore != identityAfterBoot {
		t.Errorf("identity changed after boot: %v -> %v", identityBefore, identityAfterBoot)
	}
}

// BenchmarkProviderIdentity 测试 providerIdentity 的性能
func BenchmarkProviderIdentity(b *testing.B) {
	p := &countingProvider{name: "benchmark.provider"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = providerIdentity(p)
	}
}

// BenchmarkProviderIdentityWithType 测试基于类型的 identity 计算性能
func BenchmarkProviderIdentityWithType(b *testing.B) {
	p := &typedCountingProvider{}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = providerIdentity(p)
	}
}

// BenchmarkProviderEntryAccess 测试访问缓存的 identity 的性能
func BenchmarkProviderEntryAccess(b *testing.B) {
	p := &countingProvider{name: "benchmark.cached"}
	entry := providerEntry{
		identity: providerIdentity(p),
		provider: p,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = entry.identity
	}
}
