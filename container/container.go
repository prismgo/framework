package container

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	containercontract "github.com/prismgo/framework/contracts/container"
)

// ErrFactoryNotRegistered 表示服务没有注册可用于解析的工厂。
var ErrFactoryNotRegistered = errors.New("container factory is not registered")

// ErrFactoryReturnedNil 表示服务工厂成功返回但没有提供有效资源。
var ErrFactoryReturnedNil = errors.New("container factory returned nil")

// EntryInfo 描述一个由容器托管的服务条目。
//
// List 返回 EntryInfo 用于诊断容器状态、测试关闭行为，或在应用终止阶段记录尚未释放的资源。
// Registered 表示容器当前持有实例；Closable 表示该实例绑定了 closer；CloseGroup 表示
// Application.CloseContext 会在哪个阶段释放它。
type EntryInfo struct {
	Key        string
	Type       string
	Registered bool
	Closable   bool
	CloseGroup CloseGroup
}

// CloseGroup 描述 container resource 在应用关闭链路中的释放阶段。
type CloseGroup = containercontract.CloseGroup

const (
	// CloseGroupNormal 是默认分组，在关闭期错误上报前释放。
	CloseGroupNormal = containercontract.CloseGroupNormal
	// CloseGroupReporting 用于 logger、exception handler 及 reporter 依赖资源，在关闭期错误上报后释放。
	CloseGroupReporting = containercontract.CloseGroupReporting
)

func normalizedContractCloseGroup(group containercontract.CloseGroup) CloseGroup {
	if group == "" {
		return CloseGroupNormal
	}
	return CloseGroup(group)
}

type entry struct {
	key        string
	typeName   string
	registered bool
	value      any
	closer     func(context.Context, any) error
	group      CloseGroup
	// version 标记当前 value 的代次，防止 Close 期间旧 value 成功关闭后误清新注册的 value。
	version   uint64
	resolved  bool
	lazyMu    sync.Mutex
	factory   func() (any, error)
	singleton bool
	once      sync.Once
	initErr   error
	// initDone 记录 lazy factory 是否已完成尝试，用于保证 singleton 失败后可重试、成功后只初始化一次。
	initDone bool
}

var _ containercontract.Container = (*Container)(nil)

// closeItem 是 Close 执行 closer 前捕获的资源快照。
//
// 设计原因：Close 不能持有 Container 锁执行用户传入的 closer，否则 closer 内再次访问 container
// 可能死锁；同时关闭成功后必须确认 Container 中仍是同一个 value，再清空注册状态。
type closeItem struct {
	key     string
	value   any
	closer  func(context.Context, any) error
	version uint64
}

// Container 是 PrismGo 的具体服务容器实现。
//
// 它同时实现 contracts/container 的 Resolver、Binder 和 Container 契约。Provider 通常通过
// foundation.Application.Container 获取该实例并注册服务；cache.Get、queue.Dispatch 这类包级
// facade helper 会通过当前 Container 解析底层服务。
type Container struct {
	mu                   sync.RWMutex
	entries              map[string]*entry
	order                []string
	aliases              map[string]string
	missingFactoryLoader func(string) error
}

// NewContainer 创建一个空的服务容器。
//
// 新容器不会自动绑定到当前 Application。foundation.NewApplication 通过
// SetContainerProvider 回调将容器挂入 facade 解析链路；测试如需包级 Resolve
// 行为应显式创建 Application 或通过 SetContainerProvider 装配测试容器。
func NewContainer() *Container {
	return &Container{entries: make(map[string]*entry), aliases: make(map[string]string)}
}

// Forget 清空指定服务的已解析实例和工厂。
//
// 它不会调用 closer；如果 key 当前持有外部资源，调用方必须先自行关闭或使用 CloseGroup/Close。
//
// 示例：
//
//	c.Forget("cache.manager")
//	_ = c.Singleton("cache.manager", newTestCache)
func (r *Container) Forget(key string) {
	if r == nil {
		return
	}
	key = r.canonical(key)
	if key == "" {
		return
	}
	r.mu.Lock()
	ent := r.ensureEntryLocked(key)
	ent.registered = false
	ent.value = nil
	ent.factory = nil
	ent.resolved = false
	ent.once = sync.Once{}
	ent.initErr = nil
	ent.initDone = false
	ent.version++
	r.mu.Unlock()
}

// Close 按注册反序关闭当前注册中心里仍 registered 的当前值。
//
// 关闭语义：
//   - context 在开始前已取消时，不改变 Container 状态；
//   - closer 成功或没有 closer 的值会被清空；
//   - closer 返回错误的值会保留 registered 状态；
//   - context 在关闭途中取消时，尚未执行的值会保留 registered 状态。
//
// 设计原因：关闭失败后的资源必须仍可通过 List 观察，并允许调用方再次 Close 推进 remaining resources。
func (r *Container) Close(ctx context.Context) error {
	return r.close(ctx, nil)
}

// CloseGroup 按注册反序关闭当前注册中心里指定分组的 registered value。
//
// 失败保留和重试语义与 Close 一致；不同分组互不影响。
func (r *Container) CloseGroup(ctx context.Context, group CloseGroup) error {
	return r.close(ctx, func(ent *entry) bool {
		return normalizedCloseGroup(ent.group) == normalizedCloseGroup(group)
	})
}

// Bind 注册瞬时服务。
//
// 每次 Make 都会执行 factory，容器不会保存返回实例，也不会在 Close 时释放返回实例。适合轻量、
// 无共享状态的对象。需要应用级共享资源时使用 Singleton 或 Instance。
//
// 示例：
//
//	_ = c.Bind("uuid.generator", func(containercontract.Resolver) (any, error) {
//		return uuid.NewString, nil
//	})
func (r *Container) Bind(key string, factory containercontract.Factory, options ...containercontract.BindingOption) error {
	return r.bind(key, factory, false, options...)
}

// Singleton 注册共享服务。
//
// factory 会在首次 Make 成功时执行一次，之后复用同一个实例。适合 manager、dispatcher、
// logger、连接池等应用级服务。factory 返回错误时不会缓存失败结果，调用方可以修正依赖后重试。
//
// 示例：
//
//	_ = c.Singleton("event.dispatcher", func(containercontract.Resolver) (any, error) {
//		return event.NewDispatcher(), nil
//	})
func (r *Container) Singleton(key string, factory containercontract.Factory, options ...containercontract.BindingOption) error {
	return r.bind(key, factory, true, options...)
}

// Instance 注册已构造的共享实例。
//
// Instance 立即把服务标记为 Resolved。它适合测试替换、启动阶段已有对象注入。
// 传入 nil 会保留条目但标记为未注册；清空服务通常应使用 Forget。
//
// 示例：
//
//	_ = c.Instance("config.repository", configRepo)
func (r *Container) Instance(key string, value any, options ...containercontract.BindingOption) error {
	if r == nil {
		return ErrNoCurrentContainer
	}
	key = r.canonical(key)
	if key == "" {
		return fmt.Errorf("container key is empty")
	}
	binding := bindingOptions(options...)
	r.mu.Lock()
	ent := r.ensureEntryLocked(key)
	ent.typeName = typeOf(value)
	ent.closer = binding.Closer
	ent.group = normalizedContractCloseGroup(binding.CloseGroup)
	ent.registered = value != nil
	ent.value = value
	ent.singleton = true
	ent.factory = nil
	ent.resolved = value != nil
	ent.once = sync.Once{}
	ent.initErr = nil
	ent.initDone = false
	ent.version++
	r.mu.Unlock()
	return nil
}

// Factory 返回延迟解析闭包。
//
// 返回的闭包内部调用 Make，因此会保留原绑定的生命周期语义：Bind 每次创建新值，Singleton
// 复用共享实例。Factory 会先尝试 deferred loader；服务仍不可解析时返回 ErrFactoryNotRegistered。
//
// 示例：
//
//	makeQueue, err := c.Factory("queue.manager")
//	if err == nil {
//		raw, err := makeQueue()
//		_, _ = raw, err
//	}
func (r *Container) Factory(key string) (func() (any, error), error) {
	if r == nil {
		return nil, ErrNoCurrentContainer
	}
	key = r.canonical(key)
	if key == "" {
		return nil, fmt.Errorf("container key is empty")
	}
	if err := r.loadMissingFactory(key); err != nil {
		return nil, err
	}
	if !r.Has(key) {
		return nil, fmt.Errorf("container %q: %w", key, ErrFactoryNotRegistered)
	}
	return func() (any, error) { return r.Make(key) }, nil
}

// Alias 为服务 key 注册别名。
//
// alias 会在 Make、Has、Bound、Resolved、Value 等入口解析到原始 key。Alias 不复制绑定，也不
// 改变关闭顺序；关闭仍按原始 key 的注册顺序执行。
//
// 示例：
//
//	_ = c.Alias("cache.manager", "cache")
//	_, _ = c.Make("cache")
func (r *Container) Alias(key, alias string) error {
	if r == nil {
		return ErrNoCurrentContainer
	}
	key = r.canonical(key)
	if key == "" || alias == "" {
		return fmt.Errorf("container alias requires non-empty key and alias")
	}
	r.mu.Lock()
	if r.aliases == nil {
		r.aliases = make(map[string]string)
	}
	r.aliases[alias] = key
	r.mu.Unlock()
	return nil
}

// Bound 判断 key 是否已经在当前容器中绑定。
//
// 语义对齐 Laravel Container::bound：只检查当前绑定表和 alias，不触发 deferred provider 加载。
// 如果需要“首次访问时加载 provider 后是否可解析”的判断，请使用 Has。
func (r *Container) Bound(key string) bool {
	if r == nil || key == "" {
		return false
	}
	r.mu.RLock()
	_, hasAlias := r.aliases[key]
	canonical := r.canonicalLocked(key)
	ent := r.entries[canonical]
	bound := hasAlias || (ent != nil && (ent.registered || ent.factory != nil))
	r.mu.RUnlock()
	return bound
}

// Has 判断服务是否可解析。
//
// 与 Bound 不同，Has 会在当前绑定表找不到服务时调用 missing factory loader。foundation 用它
// 支持 deferred provider：首次访问 cache.manager 等 key 时才注册对应 provider。
func (r *Container) Has(key string) bool {
	if r == nil {
		return false
	}
	key = r.canonical(key)
	if key == "" {
		return false
	}
	r.mu.RLock()
	ent := r.entries[key]
	ok := ent != nil && (ent.registered || ent.factory != nil)
	r.mu.RUnlock()
	if ok {
		return true
	}
	if err := r.loadMissingFactory(key); err != nil {
		return false
	}
	r.mu.RLock()
	ent = r.entries[key]
	ok = ent != nil && (ent.registered || ent.factory != nil)
	r.mu.RUnlock()
	return ok
}

// Resolved 判断服务是否已经产出过实例。
//
// Instance 注册后立即视为 resolved；Singleton 首次 Make 成功后视为 resolved；Bind 每次 Make
// 成功后也会标记为 resolved。Resolved 也支持 alias。
func (r *Container) Resolved(key string) bool {
	if r == nil || key == "" {
		return false
	}
	key = r.canonical(key)
	r.mu.RLock()
	ent := r.entries[key]
	resolved := ent != nil && ent.resolved
	r.mu.RUnlock()
	return resolved
}

// Make 按 key 解析服务。
//
// Make 是容器的核心解析入口。它会先返回已注册实例，再尝试 deferred loader，最后执行绑定的
// factory。调用方需要自行做类型断言；新代码通常应使用泛型 helper Make[T]。
//
// 示例：
//
//	raw, err := c.Make("cache.manager")
//	if err != nil {
//		return err
//	}
//	manager := raw.(*cache.Manager)
func (r *Container) Make(key string) (any, error) {
	if r == nil {
		return nil, ErrNoCurrentContainer
	}
	key = r.canonical(key)
	if key == "" {
		return nil, fmt.Errorf("container key is empty")
	}
	if raw, ok := r.get(key); ok {
		return raw, nil
	}
	if err := r.loadMissingFactory(key); err != nil {
		return nil, err
	}
	factory, singleton, version, err := r.factorySnapshot(key)
	if err != nil {
		return nil, err
	}
	if !singleton {
		value, err := factory()
		if err != nil {
			return nil, err
		}
		r.markResolvedIfCurrent(key, version)
		return value, nil
	}
	return r.makeSingleton(key)
}

// Call 用显式参数和容器解析出的剩余参数调用函数。
//
// 显式 args 按参数位置优先使用。未提供的参数会使用参数类型字符串作为服务 key 调用 Make。
// 这是一个有意保持简单的调用 helper，不实现 Laravel 完整 method injection、contextual binding
// 或标签解析。
//
// 示例：
//
//	_, err := c.Call(func(dispatcher event.Dispatcher) error {
//		return dispatcher.Dispatch(ctx, evt)
//	})
func (r *Container) Call(callback any, args ...any) ([]any, error) {
	value := reflect.ValueOf(callback)
	if !value.IsValid() || value.Kind() != reflect.Func {
		return nil, fmt.Errorf("container call target must be a function")
	}
	fnType := value.Type()
	in := make([]reflect.Value, 0, fnType.NumIn())
	for i := 0; i < fnType.NumIn(); i++ {
		if i < len(args) {
			arg := reflect.ValueOf(args[i])
			if !arg.IsValid() {
				in = append(in, reflect.Zero(fnType.In(i)))
				continue
			}
			if !arg.Type().AssignableTo(fnType.In(i)) {
				if arg.Type().ConvertibleTo(fnType.In(i)) {
					arg = arg.Convert(fnType.In(i))
				} else {
					return nil, fmt.Errorf("container call argument %d has type %s, want %s", i, arg.Type(), fnType.In(i))
				}
			}
			in = append(in, arg)
			continue
		}
		resolved, err := r.Make(fnType.In(i).String())
		if err != nil {
			return nil, fmt.Errorf("container call argument %d: %w", i, err)
		}
		arg := reflect.ValueOf(resolved)
		if !arg.IsValid() || !arg.Type().AssignableTo(fnType.In(i)) {
			return nil, fmt.Errorf("container call argument %d resolved type mismatch", i)
		}
		in = append(in, arg)
	}
	values := value.Call(in)
	out := make([]any, 0, len(values))
	for _, item := range values {
		out = append(out, item.Interface())
	}
	return out, nil
}

func (r *Container) bind(key string, factory containercontract.Factory, singleton bool, options ...containercontract.BindingOption) error {
	if r == nil {
		return ErrNoCurrentContainer
	}
	key = r.canonical(key)
	if key == "" {
		return fmt.Errorf("container key is empty")
	}
	if factory == nil {
		return fmt.Errorf("container %q: factory is nil", key)
	}
	binding := bindingOptions(options...)
	r.mu.Lock()
	ent := r.ensureEntryLocked(key)
	ent.closer = binding.Closer
	ent.group = normalizedContractCloseGroup(binding.CloseGroup)
	ent.registered = false
	ent.value = nil
	ent.singleton = singleton
	ent.resolved = false
	ent.factory = func() (any, error) { return factory(r) }
	ent.once = sync.Once{}
	ent.initErr = nil
	ent.initDone = false
	ent.version++
	r.mu.Unlock()
	return nil
}

func (r *Container) makeSingleton(key string) (any, error) {
	ent := r.lazyEntry(key)
	if ent == nil {
		return nil, fmt.Errorf("container %q: %w", key, ErrFactoryNotRegistered)
	}

	retry := false
	value, err := func() (any, error) {
		ent.lazyMu.Lock()
		defer ent.lazyMu.Unlock()

		if raw, ok := r.get(key); ok {
			return raw, nil
		}
		factory, singleton, version, err := r.factorySnapshot(key)
		if err != nil {
			return nil, err
		}
		if !singleton {
			value, err := factory()
			if err != nil {
				return nil, err
			}
			r.markResolvedIfCurrent(key, version)
			return value, nil
		}
		value, err := factory()
		if err != nil {
			r.recordSingletonAttempt(key, version, err)
			return nil, err
		}
		if value == nil {
			err := fmt.Errorf("container %q: %w", key, ErrFactoryReturnedNil)
			r.recordSingletonAttempt(key, version, err)
			return nil, err
		}
		if !r.setSingletonIfCurrent(key, version, value) {
			retry = true
			return nil, nil
		}
		return value, nil
	}()
	if retry {
		return r.Make(key)
	}
	return value, err
}

func (r *Container) factorySnapshot(key string) (func() (any, error), bool, uint64, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	ent := r.entries[key]
	if ent == nil || ent.factory == nil {
		return nil, false, 0, fmt.Errorf("container %q: %w", key, ErrFactoryNotRegistered)
	}
	return ent.factory, ent.singleton, ent.version, nil
}

// recordSingletonAttempt 只记录当前绑定代次的初始化诊断，不把失败变成 sticky 状态。
//
// 需求背景：Singleton factory 失败后必须允许下一次 Make 重试；这里保存 initErr/initDone
// 仅服务 Snapshot/Restore 和诊断，不参与阻止后续初始化。
func (r *Container) recordSingletonAttempt(key string, version uint64, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	ent := r.entries[key]
	if ent == nil || ent.version != version {
		return
	}
	ent.initErr = err
	ent.initDone = true
}

func (r *Container) setSingletonIfCurrent(key string, version uint64, value any) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	ent := r.entries[key]
	if ent == nil || ent.version != version || ent.factory == nil || !ent.singleton {
		return false
	}
	ent.typeName = typeOf(value)
	ent.registered = true
	ent.value = value
	ent.resolved = true
	ent.initErr = nil
	ent.initDone = true
	ent.version++
	return true
}

func bindingOptions(options ...containercontract.BindingOption) containercontract.Binding {
	binding := containercontract.Binding{CloseGroup: containercontract.CloseGroupNormal}
	for _, option := range options {
		if option != nil {
			option(&binding)
		}
	}
	return binding
}

func (r *Container) close(ctx context.Context, include func(*entry) bool) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	r.mu.RLock()
	order := append([]string(nil), r.order...)
	r.mu.RUnlock()

	var errs []error
	for i := len(order) - 1; i >= 0; i-- {
		if err := ctx.Err(); err != nil {
			errs = append(errs, err)
			break
		}
		item, ok, err := r.closeItem(ctx, order[i], include)
		if err != nil {
			errs = append(errs, err)
			break
		}
		if !ok {
			continue
		}
		if item.closer != nil {
			if err := item.closer(ctx, item.value); err != nil {
				errs = append(errs, err)
				continue
			}
		}
		r.clearClosed(item)
	}
	return errors.Join(errs...)
}

// closeItem 等待指定 key 已开始的 lazy singleton 初始化稳定后，再读取当前关闭快照。
//
// 参数说明：ctx 控制等待 lazy 初始化的取消；key 是关闭顺序快照中的服务名；include 用于
// CloseGroup 过滤关闭分组。返回 false 表示该服务不存在、分组不匹配或当前没有 registered value。
func (r *Container) closeItem(ctx context.Context, key string, include func(*entry) bool) (closeItem, bool, error) {
	ent, wait := r.closeEntry(key, include)
	if ent == nil {
		return closeItem{}, false, nil
	}
	if wait {
		unlock, err := lockLazyForClose(ctx, ent)
		if err != nil {
			return closeItem{}, false, err
		}
		defer unlock()
	}
	item, ok := r.closeItemSnapshot(key, include)
	return item, ok, nil
}

// closeEntry 只负责在容器锁内找到关闭候选 entry，并执行 CloseGroup 的分组过滤。
//
// 设计原因：CloseGroup 不能等待非目标分组的 lazy factory，否则目标分组关闭会被无关服务拖住。
// 返回的 wait 表示该 entry 是 lazy singleton，需要在读取关闭快照前等待同 key 初始化锁空闲。
func (r *Container) closeEntry(key string, include func(*entry) bool) (*entry, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	ent := r.entries[key]
	if ent == nil {
		return nil, false
	}
	if include != nil && !include(ent) {
		return nil, false
	}
	return ent, ent.singleton && ent.factory != nil
}

// lockLazyForClose 等待同一个 entry 的 lazy factory 离开临界区，并在成功后保持锁到快照完成。
//
// 需求背景：singleton factory 在锁外执行用户代码后才注册 value；Close 如果提前读取快照，
// 会看不到即将注册的资源。这里用 TryLock 轮询而不是阻塞 Lock，确保 ctx 取消能及时返回，
// 并保持关闭失败可重试语义。
func lockLazyForClose(ctx context.Context, ent *entry) (func(), error) {
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()

	for {
		if ent.lazyMu.TryLock() {
			return ent.lazyMu.Unlock, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
		}
	}
}

// closeItemSnapshot 只负责在锁内读取指定 key 的当前关闭快照。
//
// 返回 false 表示该服务不存在、分组不匹配或当前没有 registered value，Close 应跳过它。
func (r *Container) closeItemSnapshot(key string, include func(*entry) bool) (closeItem, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	ent := r.entries[key]
	if ent == nil || !ent.registered {
		return closeItem{}, false
	}
	if include != nil && !include(ent) {
		return closeItem{}, false
	}
	return closeItem{
		key:     key,
		value:   ent.value,
		closer:  ent.closer,
		version: ent.version,
	}, true
}

// clearClosed 在 closer 成功后清空仍匹配原快照的 value。
//
// 如果关闭过程中其他 goroutine 重新注册了同一个 key，version 会变化，此时不能清掉新 value。
func (r *Container) clearClosed(item closeItem) {
	r.mu.Lock()
	defer r.mu.Unlock()

	ent := r.entries[item.key]
	if ent == nil || !ent.registered || ent.version != item.version {
		return
	}
	ent.registered = false
	ent.value = nil
	ent.resolved = false
	ent.version++
}

// List 返回当前容器中所有已知服务的元信息。
//
// List 不触发 deferred loader，也不执行 factory。返回顺序是服务首次出现的顺序，方便测试关闭
// 反序和诊断 Application 生命周期问题。
func (r *Container) List() []EntryInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()

	out := make([]EntryInfo, 0, len(r.order))
	for _, key := range r.order {
		ent := r.entries[key]
		if ent == nil {
			continue
		}
		out = append(out, EntryInfo{
			Key:        ent.key,
			Type:       ent.typeName,
			Registered: ent.registered,
			Closable:   ent.closer != nil,
			CloseGroup: normalizedCloseGroup(ent.group),
		})
	}
	return out
}

// SetMissingFactoryLoader 安装缺失 factory 时的按需加载回调。
//
// 需求背景：deferred provider 需要在 strict Resolve 发现 factory 缺失时，按 service key
// 加载对应 provider。container 包只保存回调，不反向依赖 foundation。
//
// loader 应只注册与 key 对应的服务绑定，不应直接创建重资源。返回错误时，Has 会把服务视为
// 不可解析，Make/Factory 会把错误返回给调用方。
func (r *Container) SetMissingFactoryLoader(loader func(string) error) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.missingFactoryLoader = loader
}

// loadMissingFactory 调用 container 上安装的缺失 factory 加载器。
func (r *Container) loadMissingFactory(key string) error {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	loader := r.missingFactoryLoader
	r.mu.RUnlock()
	if loader == nil {
		return nil
	}
	return loader(key)
}

func (r *Container) register(key, typeName string, closer func(context.Context, any) error, group CloseGroup) {
	r.mu.Lock()
	defer r.mu.Unlock()

	ent := r.ensureEntryLocked(key)
	ent.typeName = typeName
	ent.closer = closer
	ent.group = normalizedCloseGroup(group)
}

func (r *Container) set(key string, value any) {
	r.mu.Lock()
	defer r.mu.Unlock()

	ent := r.ensureEntryLocked(key)
	ent.typeName = typeOf(value)
	ent.registered = true
	ent.value = value
	ent.resolved = true
	ent.version++
}

func (r *Container) setFactory(key string, factory func() (any, error)) {
	r.mu.Lock()
	defer r.mu.Unlock()

	ent := r.ensureEntryLocked(key)
	ent.registered = false
	ent.value = nil
	ent.resolved = false
	ent.version++
	ent.factory = factory
	ent.singleton = true
	ent.once = sync.Once{}
	ent.initErr = nil
	ent.initDone = false
}

func (r *Container) clear(key string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	ent := r.ensureEntryLocked(key)
	ent.registered = false
	ent.value = nil
	ent.resolved = false
	ent.version++
	ent.once = sync.Once{}
	ent.initErr = nil
	ent.initDone = false
}

func (r *Container) get(key string) (any, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	ent := r.entries[key]
	if ent == nil || !ent.registered {
		return nil, false
	}
	return ent.value, true
}

func (r *Container) value(key string) (any, bool) {
	key = r.canonical(key)
	if key == "" {
		return nil, false
	}
	return r.get(key)
}

func (r *Container) lazyEntry(key string) *entry {
	r.mu.RLock()
	ent := r.entries[key]
	r.mu.RUnlock()
	return ent
}

// markResolvedIfCurrent 只在绑定代次未变化时标记 resolved。
//
// 需求背景：Bind 的瞬时 factory 在锁外执行，执行期间同一个 key 可能被 Singleton、Instance、
// Forget 或 Restore 重绑。version 参数表示 factory 开始时看到的绑定代次；如果提交时
// version 已变化，说明当前 key 已经属于另一份绑定，旧 factory 不能污染新绑定的 Resolved 状态。
func (r *Container) markResolvedIfCurrent(key string, version uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	ent := r.entries[key]
	if ent == nil || ent.version != version {
		return
	}
	ent.resolved = true
}

// ensureEntryLocked 统一维护注册顺序，确保关闭时可以按反序释放资源。调用方必须持有 r.mu。
func (r *Container) ensureEntryLocked(key string) *entry {
	if r.entries == nil {
		r.entries = make(map[string]*entry)
	}
	ent := r.entries[key]
	if ent == nil {
		ent = &entry{key: key}
		r.entries[key] = ent
		r.order = append(r.order, key)
	}
	return ent
}

// canonical 在读取时解析 alias 链，绑定仍保存在原始服务 key 下。
func (r *Container) canonical(key string) string {
	if r == nil || key == "" {
		return key
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.canonicalLocked(key)
}

func (r *Container) canonicalLocked(key string) string {
	seen := map[string]struct{}{}
	for {
		next := r.aliases[key]
		if next == "" {
			return key
		}
		if _, ok := seen[next]; ok {
			return key
		}
		seen[key] = struct{}{}
		key = next
	}
}

func typeOf(value any) string {
	if value == nil {
		return "<nil>"
	}
	return reflect.TypeOf(value).String()
}

func normalizedCloseGroup(group CloseGroup) CloseGroup {
	if group == "" {
		return CloseGroupNormal
	}
	return group
}
