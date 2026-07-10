package foundation

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	configpkg "github.com/prismgo/framework/config"
	providerpkg "github.com/prismgo/framework/contracts/provider"
	"github.com/prismgo/framework/event"
	"github.com/prismgo/framework/logger"
	"github.com/prismgo/framework/translation"
)

// providerEntry 是 provider repository 的只读快照项。
//
// 设计思路：Boot 过程中不长时间持有 Application 锁；先复制 identity 与 provider，
// 再在锁外执行 Register/Boot，避免 provider 内部调用其它 Application 方法时自锁。
type providerEntry struct {
	identity string
	provider providerpkg.ServiceProvider
}

// namedProvider 是 provider 可选实现的稳定 identity 扩展点。
//
// 需求背景：多数 provider 可以用完整 Go 类型路径去重；少数适配器或多实例 provider
// 需要显式声明同一个逻辑身份时，通过 Name() 返回非空字符串即可。
type namedProvider = providerpkg.NamedProvider

// applicationBaseProviders 返回 Application 构造阶段需要立即 register 的基础 provider。
//
// 设计思路：base providers 是 foundation 内部层，不通过业务 WithProviders 暴露。
// issue 01 的测试会替换该函数来注入 fake base provider；真实 event/config/logger
// provider 在后续切片接入。
var applicationBaseProviders = func() []providerpkg.ServiceProvider {
	return []providerpkg.ServiceProvider{
		event.ServiceProvider{},
		configpkg.ServiceProvider{},
		logger.ServiceProvider{},
		translation.ServiceProvider{},
	}
}

// RegisterProvider 追加一个服务提供者。
//
// 需求背景：Laravel 允许应用 boot 后追加 provider 并立即补 boot。该方法返回 error
// 用于暴露追加 provider 的 register/boot 失败；旧调用方仍可忽略返回值以保持声明式用法。
func (a *Application) RegisterProvider(provider providerpkg.ServiceProvider) error {
	if a == nil || isNilProvider(provider) {
		return nil
	}
	identity := providerIdentity(provider)
	if identity == "" {
		return nil
	}

	a.mu.Lock()
	a.initProviderRepositoryLocked()
	if a.booting {
		a.mu.Unlock()
		return fmt.Errorf("register provider %s: application boot is in progress", identity)
	}
	if a.closing {
		a.mu.Unlock()
		return fmt.Errorf("register provider %s: application is closing", identity)
	}
	if existing, ok := a.providerByIdentityLocked(identity); ok {
		provider = existing.provider
	} else {
		if err := a.registerDeferredProviderLocked(provider, identity); err != nil {
			a.mu.Unlock()
			return err
		}
		a.providers = append(a.providers, providerEntry{identity: identity, provider: provider})
		a.providerIDs[identity] = struct{}{}
	}
	booted := a.booted
	registered := a.registeredProviders[identity]
	providerBooted := a.bootedProviders[identity]
	a.mu.Unlock()

	if !booted {
		return nil
	}
	if !registered {
		if err := a.registerProviderPhase(provider, identity); err != nil {
			return err
		}
	}
	if providerBooted {
		return nil
	}
	return a.bootProviderPhase(provider, identity)
}

// Boot 执行全部 provider 生命周期。
//
// 生命周期顺序与 Laravel provider repository 对齐：先完成所有 eager provider 的
// Register，再派发 AppBooting，然后按 repository 顺序执行已注册 provider 的 Boot，
// 最后标记应用已 boot 并派发 AppBooted。
func (a *Application) Boot() error {
	if a == nil {
		return fmt.Errorf("application is not initialized")
	}

	a.mu.Lock()
	if a.booted {
		a.mu.Unlock()
		return nil
	}
	if a.booting {
		a.mu.Unlock()
		return fmt.Errorf("application boot is in progress")
	}
	a.booting = true
	a.mu.Unlock()
	defer func() {
		a.mu.Lock()
		a.booting = false
		a.mu.Unlock()
	}()

	start := time.Now()
	a.mu.Lock()
	a.startedAt = start
	a.mu.Unlock()

	for _, entry := range a.providerSnapshot() {
		if a.deferredProviderPending(entry.identity) {
			continue
		}
		if err := a.registerProviderPhase(entry.provider, entry.identity); err != nil {
			return err
		}
	}

	a.dispatchLifecycleEvent(event.AppBooting{})

	for _, entry := range a.providerSnapshot() {
		if a.deferredProviderPending(entry.identity) {
			continue
		}
		if err := a.bootProviderPhase(entry.provider, entry.identity); err != nil {
			return err
		}
	}
	a.mu.Lock()
	a.booted = true
	a.mu.Unlock()
	a.dispatchLifecycleEvent(event.AppBooted{Duration: time.Since(start)})
	return nil
}

func (a *Application) initProviderRepository() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.initProviderRepositoryLocked()
}

// initProviderRepositoryLocked 初始化 provider repository 的幂等状态表。
//
// 调用约束：调用方必须已经持有 a.mu。该函数允许测试或旧构造路径多次调用，
// 因此只补齐 nil map，不重置已经记录的 provider 阶段状态。
func (a *Application) initProviderRepositoryLocked() {
	if a.providerIDs == nil {
		a.providerIDs = make(map[string]struct{})
	}
	if a.registeredProviders == nil {
		a.registeredProviders = make(map[string]bool)
	}
	if a.bootedProviders == nil {
		a.bootedProviders = make(map[string]bool)
	}
	if a.deferredServices == nil {
		a.deferredServices = make(map[string]string)
	}
	if a.deferredKeys == nil {
		a.deferredKeys = make(map[string][]string)
	}
}

// registerBaseProviders 在 Application 构造期注册基础 provider。
//
// 设计背景：event/config/logger 需要尽早提供 lazy factory，但它们的 Boot 仍属于
// Application.Boot 生命周期。这里只执行 Register 阶段，并把完成状态记入同一套 repository。
func (a *Application) registerBaseProviders(providers ...providerpkg.ServiceProvider) {
	for _, provider := range providers {
		if isNilProvider(provider) {
			continue
		}
		identity := providerIdentity(provider)
		if identity == "" {
			continue
		}

		a.mu.Lock()
		a.initProviderRepositoryLocked()
		if _, ok := a.providerIDs[identity]; ok {
			a.mu.Unlock()
			continue
		}
		a.providers = append(a.providers, providerEntry{identity: identity, provider: provider})
		a.providerIDs[identity] = struct{}{}
		a.mu.Unlock()

		// NewApplication 没有错误返回值；base provider register 失败时保留未完成状态，
		// 后续 Boot 会按同一阶段重试并把错误返回给调用方。
		_ = a.registerProviderPhase(provider, identity)
	}
}

// providerSnapshot 返回当前 provider repository 的稳定快照。
//
// 逻辑说明：Boot 会先跑一轮 Register，再重新取快照执行 Boot；这样 Register 阶段
// 已经存在于 repository 的 provider 都会按顺序参与后续 Boot，同时拒绝 boot 过程中的重入追加。
func (a *Application) providerSnapshot() []providerEntry {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.initProviderRepositoryLocked()

	out := make([]providerEntry, 0, len(a.providers))
	for _, entry := range a.providers {
		if entry.identity == "" {
			continue
		}
		out = append(out, entry)
	}
	return out
}

// providerByIdentityLocked 按 identity 查找已进入 repository 的 provider。
//
// 调用约束：调用方必须持有 a.mu；重复 provider 复用首次进入 repository 的实例，
// 避免后续重复注册者改变启动顺序或替换已记录的生命周期状态。
func (a *Application) providerByIdentityLocked(identity string) (providerEntry, bool) {
	for _, entry := range a.providers {
		if entry.identity == identity {
			return entry, true
		}
	}
	return providerEntry{}, false
}

// registerProviderPhase 执行单个 provider 的 Register 阶段。
//
// 失败语义：只派发 ProviderRegistering，不标记 registered，也不派发 ProviderRegistered；
// 下次 Boot 或 boot 后 RegisterProvider 重试时会再次执行该阶段。
// registerDeferredProviderLocked 记录 deferred provider 提供的 container service key。
//
// 调用约束：调用方必须持有 a.mu。这里只登记映射，不执行 provider.Register，
// 让真实注册延迟到对应 container service 首次解析时发生。
func (a *Application) registerDeferredProviderLocked(provider providerpkg.ServiceProvider, identity string) error {
	keys, deferred, err := deferredProviderKeys(provider)
	if err != nil || !deferred {
		return err
	}
	a.initProviderRepositoryLocked()
	for _, key := range keys {
		if existing := a.deferredServices[key]; existing != "" && existing != identity {
			return fmt.Errorf("deferred service %q already provided by %s", key, existing)
		}
	}
	a.deferredKeys[identity] = keys
	for _, key := range keys {
		a.deferredServices[key] = identity
	}
	return nil
}

// deferredProviderKeys 提取并校验 DeferrableProvider 声明的 service key。
func deferredProviderKeys(provider providerpkg.ServiceProvider) ([]string, bool, error) {
	deferred, ok := provider.(providerpkg.DeferrableProvider)
	if !ok {
		return nil, false, nil
	}
	rawKeys := deferred.Provides()
	if len(rawKeys) == 0 {
		return nil, true, fmt.Errorf("deferred provider must provide at least one service key")
	}
	seen := make(map[string]struct{}, len(rawKeys))
	keys := make([]string, 0, len(rawKeys))
	for _, raw := range rawKeys {
		key := strings.TrimSpace(raw)
		if key == "" {
			return nil, true, fmt.Errorf("deferred provider service key is empty")
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		keys = append(keys, key)
	}
	return keys, true, nil
}

// deferredProviderPending 判断 provider 是否仍处于 deferred 且未 Register 的状态。
func (a *Application) deferredProviderPending(identity string) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.deferredKeys[identity]) == 0 {
		return false
	}
	return !a.registeredProviders[identity]
}

// loadDeferredProviderForService 在容器缺失 binding 时按 service key 加载 provider。
//
// 并发安全性：多个 goroutine 同时解析同一个 deferred service 时，registerProviderPhase
// 和 bootProviderPhase 内部的 phaseMu 锁保证 provider 只注册和启动一次。
// 即使多个 goroutine 同时进入此函数，phase 检查会确保幂等性。
func (a *Application) loadDeferredProviderForService(key string) error {
	key = strings.TrimSpace(key)
	if a == nil || key == "" {
		return nil
	}

	a.mu.Lock()
	a.initProviderRepositoryLocked()
	identity := a.deferredServices[key]
	if identity == "" {
		a.mu.Unlock()
		return nil
	}
	if a.booting {
		a.mu.Unlock()
		return fmt.Errorf("load deferred service %q: application boot is in progress", key)
	}
	if a.closing {
		a.mu.Unlock()
		return fmt.Errorf("load deferred service %q: application is closing", key)
	}
	entry, ok := a.providerByIdentityLocked(identity)
	booted := a.booted
	a.mu.Unlock()
	if !ok {
		return fmt.Errorf("load deferred service %q: provider %s is not registered", key, identity)
	}

	if err := a.registerProviderPhase(entry.provider, identity); err != nil {
		return err
	}
	if booted {
		if err := a.bootProviderPhase(entry.provider, identity); err != nil {
			return err
		}
	}

	a.mu.Lock()
	if !booted || a.bootedProviders[identity] {
		a.removeDeferredMappingsLocked(identity)
	}
	a.mu.Unlock()
	return nil
}

// removeDeferredMappingsLocked 移除一个 provider 声明的全部 deferred service key。
func (a *Application) removeDeferredMappingsLocked(identity string) {
	for _, key := range a.deferredKeys[identity] {
		if a.deferredServices[key] == identity {
			delete(a.deferredServices, key)
		}
	}
	delete(a.deferredKeys, identity)
}

func (a *Application) registerProviderPhase(provider providerpkg.ServiceProvider, identity string) error {
	a.phaseMu.Lock()
	defer a.phaseMu.Unlock()

	a.mu.Lock()
	a.initProviderRepositoryLocked()
	if a.registeredProviders[identity] {
		a.mu.Unlock()
		return nil
	}
	a.mu.Unlock()

	a.dispatchLifecycleEvent(event.ProviderRegistering{Provider: identity})
	if err := provider.Register(a); err != nil {
		return fmt.Errorf("provider %s register: %w", identity, err)
	}

	a.mu.Lock()
	a.registeredProviders[identity] = true
	a.mu.Unlock()
	a.dispatchLifecycleEvent(event.ProviderRegistered{Provider: identity})
	return nil
}

// bootProviderPhase 执行单个 provider 的 Boot 阶段。
//
// 失败语义与 Register 一致：只派发 ProviderBooting，不标记 booted，也不派发
// ProviderBooted，保证失败后可以按原阶段重试。
func (a *Application) bootProviderPhase(provider providerpkg.ServiceProvider, identity string) error {
	a.phaseMu.Lock()
	defer a.phaseMu.Unlock()

	a.mu.Lock()
	a.initProviderRepositoryLocked()
	if a.bootedProviders[identity] {
		a.mu.Unlock()
		return nil
	}
	a.mu.Unlock()

	a.dispatchLifecycleEvent(event.ProviderBooting{Provider: identity})
	if err := provider.Boot(a); err != nil {
		return fmt.Errorf("provider %s boot: %w", identity, err)
	}

	a.mu.Lock()
	a.bootedProviders[identity] = true
	a.mu.Unlock()
	a.dispatchLifecycleEvent(event.ProviderBooted{Provider: identity})
	return nil
}

// dispatchLifecycleEvent 以 best-effort 方式派发启动生命周期事件。
//
// 设计思路：event provider 自身也是 provider，因此 dispatcher 可能在早期尚不可用。
// 每次派发前都从当前 Application registry 解析，解析失败只跳过事件，不影响 Boot 结果。
//
// 有意设计：解析失败时静默跳过，不记录错误也不返回错误。原因：
// 1. 这是启动早期的正常状态，不是配置错误或异常情况
// 2. 事件派发是辅助功能，不应影响核心启动流程
// 3. 避免在 logger/exception handler 尚未就绪时尝试记录错误
func (a *Application) dispatchLifecycleEvent(ev event.Event) {
	bus, err := resolveEventDispatcher(a.container)
	if err != nil || bus == nil {
		return
	}
	bus.Dispatch(a.Context(), ev)
}

// providerIdentity 计算 provider 的生命周期身份。
//
// 规则说明：Name() 非空时优先使用该稳定 key；否则退回去指针后的完整 Go 类型路径，
// 与 Laravel 按 provider class 去重的语义对齐，同时避免把实例地址纳入 identity。
func providerIdentity(provider providerpkg.ServiceProvider) string {
	if isNilProvider(provider) {
		return ""
	}
	if named, ok := provider.(namedProvider); ok {
		if name := strings.TrimSpace(named.Name()); name != "" {
			return name
		}
	}
	t := reflect.TypeOf(provider)
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	return t.String()
}

// providerTypeName 保留旧测试和诊断路径使用的 provider 名称入口。
func providerTypeName(provider providerpkg.ServiceProvider) string {
	if isNilProvider(provider) {
		return "<nil>"
	}
	return providerIdentity(provider)
}

// isNilProvider 识别 interface 中包裹的 nil 指针、nil map 等值。
//
// 需求背景：ServiceProvider 现在是强类型接口，但调用方仍可能传入 typed nil provider。
// 统一入口可以避免 nil provider 进入 repository 并影响后续生命周期状态。
func isNilProvider(provider providerpkg.ServiceProvider) bool {
	if provider == nil {
		return true
	}
	value := reflect.ValueOf(provider)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

// defaultBaseProviders 返回基础 provider 清单副本。
//
// 设计思路：测试会临时替换 applicationBaseProviders；这里复制切片，避免调用方
// 追加或重排返回值时污染后续 Application 构造。
func defaultBaseProviders() []providerpkg.ServiceProvider {
	baseProviders := applicationBaseProviders()
	if len(baseProviders) == 0 {
		return nil
	}
	return append([]providerpkg.ServiceProvider(nil), baseProviders...)
}
