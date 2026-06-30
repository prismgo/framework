package foundation

import (
	"context"
	"io"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/prismgo/framework/console"
	containercontract "github.com/prismgo/framework/contracts/container"
	prismhttp "github.com/prismgo/framework/http"
	"github.com/prismgo/framework/kernel"
	"github.com/prismgo/framework/route"
	"github.com/prismgo/framework/timer"
)

// runtimeRegistries 保存当前 Application 声明的运行时注册信息。
//
// 需求背景：早期 HTTP route、console command、schedule、migration path 等声明直接写入
// 进程级全局注册表，多次 Configure().Create() 只能依赖 reset 清理旧状态。现在主启动路径需要让
// Application 持有自己的声明快照，再由 kernel/http 在运行时读取当前 Application。
//
// 设计思路：该结构只保存声明函数和路径，不创建 Kernel 或业务依赖实例；HTTP Server 只由当前
// Application runtime 生成，不再经过 http 包级注册表。
type runtimeRegistries struct {
	app            *Application
	commands       []console.CommandFactory
	starting       []kernel.StartingCallback
	preMiddlewares []func(*gin.Engine)
	middlewares    []func(*gin.Engine)
	routes         []func(*Application, *gin.Engine) error
	schedules      []func(*timer.Schedule)
	migrationPaths []string
	seedPaths      []string
}

func (r *runtimeRegistries) Container() containercontract.Container {
	if r == nil || r.app == nil {
		return nil
	}
	return r.app.container
}

func newRuntimeRegistries() *runtimeRegistries {
	return &runtimeRegistries{}
}

// NewHTTPServer 使用当前 runtime registry 创建 HTTP Server。
func (r *runtimeRegistries) NewHTTPServer(ctx context.Context, port string) (*http.Server, error) {
	serverConfig := prismhttp.CurrentServerConfig()
	serverConfig.Port = port
	return prismhttp.NewApplicationServer(
		port,
		r.HTTPServerConfigurator(),
		prismhttp.WithBaseContext(ctx),
		prismhttp.WithServerConfig(serverConfig),
	)
}

// LoadHTTPRoutes 构建一次临时 HTTP Server 以加载当前 runtime registry 的 route facade。
func (r *runtimeRegistries) LoadHTTPRoutes() error {
	mode := gin.Mode()
	writer := gin.DefaultWriter
	gin.SetMode(gin.TestMode)
	gin.DefaultWriter = io.Discard
	defer func() {
		gin.DefaultWriter = writer
		gin.SetMode(mode)
	}()

	_, err := prismhttp.NewApplicationServer("0", r.HTTPServerConfigurator())
	return err
}

// configureRuntimeRegistries 把 Builder 阶段收集到的声明写入 Application 自身。
//
// 参数说明：
// app 是本次创建的 Application；后续 HTTP route registrar 需要使用它装配业务依赖。
// routing 是 bootstrap 声明的路由、命令、调度、迁移和 seed 路径集合。
// middleware 是 bootstrap 声明的 HTTP 中间件集合。
func configureRuntimeRegistries(app *Application, commands []console.CommandFactory, routing Routing, middleware Middleware) {
	if app == nil || app.runtime == nil {
		return
	}
	app.runtime.app = app
	app.runtime.commands = append([]console.CommandFactory(nil), commands...)
	app.runtime.commands = append(app.runtime.commands, routing.commands...)
	app.runtime.preMiddlewares = append([]func(*gin.Engine){}, middleware.preRegistrars...)
	app.runtime.middlewares = append([]func(*gin.Engine){}, middleware.registrars...)
	app.runtime.routes = append([]func(*Application, *gin.Engine) error{}, routing.routes...)
	app.runtime.schedules = append([]func(*timer.Schedule){}, routing.schedules...)
	app.runtime.migrationPaths = append([]string(nil), routing.migrationPaths...)
	app.runtime.seedPaths = append([]string(nil), routing.seedPaths...)
}

// CommandFactories 返回当前 Application 声明的 console command 工厂快照。
//
// 设计思路：返回副本，避免 kernel 装配命令时意外修改 Application 内部注册表。
func (r *runtimeRegistries) CommandFactories() []console.CommandFactory {
	if r == nil {
		return nil
	}
	return append([]console.CommandFactory(nil), r.commands...)
}

// StartingCallbacks 返回当前 Application 声明的 Console starting callbacks 快照。
func (r *runtimeRegistries) StartingCallbacks() []kernel.StartingCallback {
	if r == nil {
		return nil
	}
	return append([]kernel.StartingCallback(nil), r.starting...)
}

// RegisterStarting 注册 Console 启动回调。
//
// 并发约束：当前实现未加锁，依赖调用方（如 registerConsoleStarting）持有 Application.mu。
// 若未来新增其他调用路径，需在此处加锁或明确文档说明并发要求。
func (r *runtimeRegistries) RegisterStarting(callbacks ...kernel.StartingCallback) {
	if r == nil {
		return
	}
	r.starting = append(r.starting, callbacks...)
}

// ScheduleRegistrars 返回当前 Application 声明的定时任务注册器快照。
func (r *runtimeRegistries) ScheduleRegistrars() []func(*timer.Schedule) {
	if r == nil {
		return nil
	}
	return append([]func(*timer.Schedule){}, r.schedules...)
}

// MigrationPaths 返回当前 Application 声明的 migration 扫描路径快照。
func (r *runtimeRegistries) MigrationPaths() []string {
	if r == nil {
		return nil
	}
	return append([]string(nil), r.migrationPaths...)
}

// SeedPaths 返回当前 Application 声明的 seeder 扫描路径快照。
func (r *runtimeRegistries) SeedPaths() []string {
	if r == nil {
		return nil
	}
	return append([]string(nil), r.seedPaths...)
}

// HTTPServerConfigurator 把 Application 声明的 HTTP 集合适配为 prismhttp Server 装配闭包。
func (r *runtimeRegistries) HTTPServerConfigurator() prismhttp.ApplicationServerConfigurator {
	routes := r.HTTPRoutes()
	if routes == nil {
		return nil
	}
	preMiddlewares := r.HTTPPreMiddlewares()
	middlewares := r.HTTPMiddlewares()
	return func(engine *gin.Engine, useInternalMiddlewares func(*gin.Engine)) error {
		if preMiddlewares != nil {
			preMiddlewares(engine)
		}
		if useInternalMiddlewares != nil {
			useInternalMiddlewares(engine)
		}
		if middlewares != nil {
			middlewares(engine)
		}
		return routes(engine)
	}
}

// HTTPPreMiddlewares 把 Application 声明的前置中间件集合适配为 Server 构建函数。
func (r *runtimeRegistries) HTTPPreMiddlewares() func(*gin.Engine) {
	if r == nil || len(r.preMiddlewares) == 0 {
		return nil
	}
	registrars := append([]func(*gin.Engine){}, r.preMiddlewares...)
	return func(engine *gin.Engine) {
		for _, registrar := range registrars {
			if registrar != nil {
				registrar(engine)
			}
		}
	}
}

// HTTPMiddlewares 把 Application 声明的中间件集合适配为 Server 构建函数。
func (r *runtimeRegistries) HTTPMiddlewares() func(*gin.Engine) {
	if r == nil || len(r.middlewares) == 0 {
		return nil
	}
	registrars := append([]func(*gin.Engine){}, r.middlewares...)
	return func(engine *gin.Engine) {
		for _, registrar := range registrars {
			if registrar != nil {
				registrar(engine)
			}
		}
	}
}

// HTTPRoutes 把 Application 声明的路由集合适配为 Server 构建函数。
//
// 设计思路：业务侧通过 route facade 声明 Laravel 风格路由；facade 通过容器解析
// 当前 Application 的 Router，无需 foundation 显式管理 Router 生命周期。
func (r *runtimeRegistries) HTTPRoutes() func(*gin.Engine) error {
	if r == nil || len(r.routes) == 0 {
		return nil
	}
	registrars := append([]func(*Application, *gin.Engine) error{}, r.routes...)
	return func(engine *gin.Engine) error {
		for _, registrar := range registrars {
			if registrar == nil {
				continue
			}
			if err := registrar(r.app, engine); err != nil {
				return err
			}
		}
		return route.Mount(engine)
	}
}
