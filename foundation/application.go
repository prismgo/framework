package foundation

import (
	"context"
	"sync"
	"time"

	"github.com/prismgo/framework/container"
	containercontract "github.com/prismgo/framework/contracts/container"
	eventcontract "github.com/prismgo/framework/contracts/event"
	pathutil "github.com/prismgo/framework/internal/path"
	"github.com/prismgo/framework/kernel"
)

// 注册全局 App 实例
var App *Application

// Application 是启动流程和 provider 生命周期的统一入口。
//
// 通用资源实例不再保存在 Application 内部；事件、配置、日志、缓存、文件系统和
// 数据库连接统一由各自 package facade 挂到全局注册中心。
type Application struct {
	ctx      context.Context
	cancel   context.CancelCauseFunc
	basePath string

	// providers 保存 base/default/application providers 的统一 repository 顺序。
	// 使用 providerEntry 缓存 identity，避免每次访问都反射计算。
	providers []providerEntry
	// providerIDs 以 provider identity 去重，避免同一 provider 重复进入生命周期。
	providerIDs map[string]struct{}
	// registeredProviders 记录 Register 阶段是否完成，用于 Boot 失败后的幂等重试。
	registeredProviders map[string]bool
	// bootedProviders 记录 Boot 阶段是否完成，用于 provider boot 失败后的幂等重试。
	bootedProviders map[string]bool
	// deferredServices 保存 container service key 到 deferred provider identity 的映射。
	deferredServices map[string]string
	// deferredKeys 保存 deferred provider identity 声明的全部 service key，用于加载成功后成组移除。
	deferredKeys map[string][]string
	cleanups     []func(*Application) error
	runtime      *runtimeRegistries
	container    *container.Container

	mu      sync.Mutex
	phaseMu sync.Mutex
	booted  bool
	booting bool
	// closing 表示首次关闭流程已经启动，后续 Close 只重试 container remaining resources。
	closing bool
	// closeActive 表示当前正在执行一次 CloseContext 关闭尝试；重入调用不会再次执行用户关闭钩子。
	closeActive bool
	// closeOwner 标记当前关闭尝试所属 goroutine，用于允许生命周期回调内重入 CloseContext。
	closeOwner uint64
	// closeDone 在一次关闭尝试完成时关闭，外部并发 CloseContext 调用通过它等待稳定结果。
	closeDone chan struct{}
	// terminated 表示 container remaining resources 已全部关闭，并且 AppTerminated 已完成派发。
	terminated bool
	// closeErr 保存首次 provider terminate 和 cleanup 阶段的错误，供 container 重试成功后写入 AppTerminated 事件。
	closeErr error
	// closeResult 保存最近一次 CloseContext 尝试的完整返回值，供等待中的外部关闭调用复用。
	closeResult error
	// closeBus 保存首次关闭时解析到的事件总线，避免重试时重新解析已经处于关闭中的资源。
	closeBus eventcontract.Dispatcher
	// signalsRegistered 标记是否已注册信号监听，避免重复注册
	signalsRegistered bool
	// startedAt 记录应用启动时间，用于计算生命周期总时长
	startedAt time.Time
	// closeStartedAt 记录关闭流程开始时间，用于计算 CloseDuration
	closeStartedAt time.Time
}

// NewApplication 创建应用实例，并注册默认通用资源工厂。
func NewApplication(basePath ...string) *Application {
	ctx, cancel := context.WithCancelCause(context.Background())

	app := &Application{
		ctx:       ctx,
		cancel:    cancel,
		runtime:   newRuntimeRegistries(),
		container: container.NewContainer(),
	}
	App = app

	app.SetBasePath(pathutil.Base(basePath...))
	app.initProviderRepository()
	container.SetProvider(func() *container.Container {
		return app.container
	})
	app.container.SetMissingFactoryLoader(app.loadDeferredProviderForService)
	_ = app.container.Instance(kernel.StartingRegistrarKey, kernel.StartingRegistrar(app.registerConsoleStarting))
	app.registerBaseProviders(defaultBaseProviders()...)
	return app
}

// Container 返回当前 Application 拥有的服务容器。
//
// 用途：测试和底层装配代码可以查看或显式传入容器；外部不能替换该容器，资源生命周期仍由
// Application.CloseContext 统一关闭。
func (a *Application) Container() containercontract.Container {
	if a == nil {
		return nil
	}
	return a.container
}

// SetBasePath 设置应用根目录，并把 Laravel 风格的 path.* 路径绑定写入容器。
func (a *Application) SetBasePath(basePath string) *Application {
	if a == nil {
		return nil
	}
	a.basePath = pathutil.Clean(basePath)
	c := a.container
	if c != nil {
		// SetBasePath 在 NewApplication 构造阶段调用，此时容器刚创建，Instance 绑定
		// 不可能失败（仅写入内存 map）。忽略返回值避免无意义的错误处理。
		_ = c.Instance("path.base", a.BasePath())
		_ = c.Instance("path.app", a.AppPath())
		_ = c.Instance("path.config", a.ConfigPath())
		_ = c.Instance("path.database", a.DatabasePath())
		_ = c.Instance("path.public", a.PublicPath())
		_ = c.Instance("path.resources", a.ResourcePath())
		_ = c.Instance("path.storage", a.StoragePath())
		_ = c.Instance("path.lang", a.LangPath())
	}
	return a
}

// BasePath 返回相对应用根目录的路径。
func (a *Application) BasePath(path ...string) string {
	return pathutil.Join(a.basePath, path...)
}

// AppPath 返回相对 app 目录的路径。
func (a *Application) AppPath(path ...string) string {
	return pathutil.Join(a.basePath, append([]string{"app"}, path...)...)
}

// ConfigPath 返回相对 config 目录的路径。
func (a *Application) ConfigPath(path ...string) string {
	return pathutil.Join(a.basePath, append([]string{"config"}, path...)...)
}

// DatabasePath 返回相对 database 目录的路径。
func (a *Application) DatabasePath(path ...string) string {
	return pathutil.Join(a.basePath, append([]string{"database"}, path...)...)
}

// PublicPath 返回相对 public 目录的路径。
func (a *Application) PublicPath(path ...string) string {
	return pathutil.Join(a.basePath, append([]string{"public"}, path...)...)
}

// ResourcePath 返回相对 resources 目录的路径。
func (a *Application) ResourcePath(path ...string) string {
	return pathutil.Join(a.basePath, append([]string{"resources"}, path...)...)
}

// StoragePath 返回相对 storage 目录的路径。
func (a *Application) StoragePath(path ...string) string {
	return pathutil.Join(a.basePath, append([]string{"storage"}, path...)...)
}

// LangPath 返回相对 lang 目录的路径。
func (a *Application) LangPath(path ...string) string {
	return pathutil.Join(a.basePath, append([]string{"lang"}, path...)...)
}
