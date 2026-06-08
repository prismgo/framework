package foundation

import (
	"fmt"

	"github.com/gin-gonic/gin"

	"github.com/prismgo/framework/config"
	"github.com/prismgo/framework/console"
	"github.com/prismgo/framework/container"
	contractprovider "github.com/prismgo/framework/contracts/provider"
	pathutil "github.com/prismgo/framework/internal/path"
	providerpkg "github.com/prismgo/framework/provider"
	"github.com/prismgo/framework/timer"

	goexception "github.com/prismgo/framework/exception"
)

// Builder 提供 Laravel 风格的应用配置入口。
type Builder struct {
	basePath string
	// providers 保存项目级 Application Provider，框架 default providers 会在 Create 中自动前置。
	providers  []contractprovider.ServiceProvider
	commands   []console.CommandFactory
	routing    Routing
	middleware Middleware
	exceptions Exceptions
}

// Configure 创建应用构建器。
func Configure(basePath ...string) *Builder {
	return &Builder{basePath: pathutil.Base(basePath...)}
}

// WithProviders 声明项目级 Application Provider。
//
// 设计说明：调用方只传业务 provider；框架 default provider 清单由 Create 自动加载，
// 避免业务 bootstrap 手动混入 cache/database/schema 等框架包。
func (b *Builder) WithProviders(providers ...contractprovider.ServiceProvider) *Builder {
	b.providers = append(b.providers, providers...)
	return b
}

// WithCommands 声明当前应用的 console command factories。
//
// 设计说明：该入口保留给框架模块、测试和特殊装配场景；标准业务 bootstrap
// 优先通过 WithRouting(... Commands ...) 声明命令，对齐 Laravel 13 skeleton。
func (b *Builder) WithCommands(factories ...console.CommandFactory) *Builder {
	b.commands = append(b.commands, factories...)
	return b
}

// WithRouting 声明 HTTP 路由、Console 命令、定时任务和路径注册配置。
func (b *Builder) WithRouting(configure func(*Routing)) *Builder {
	if configure != nil {
		configure(&b.routing)
	}
	return b
}

// WithMiddleware 声明 HTTP middleware 注册配置。
func (b *Builder) WithMiddleware(configure func(*Middleware)) *Builder {
	if configure != nil {
		configure(&b.middleware)
	}
	return b
}

// WithExceptions 声明应用级异常处理配置。
func (b *Builder) WithExceptions(configure func(*Exceptions)) *Builder {
	if configure != nil {
		configure(&b.exceptions)
	}
	return b
}

// Create 构建并装配 Application。
//
// 需求背景：该方法对应 Laravel configured providers bootstrap。它先放入框架
// default providers，再放入业务 providers，保证业务 provider 可以覆盖或使用框架能力。
//
// v4 异常处理重构：统一使用 prismgo/exception.Handler，同时服务 HTTP 和非 HTTP 上下文。
func (b *Builder) Create() *Application {
	app := NewApplication(b.basePath)

	configureRuntimeRegistries(app, b.commands, b.routing, b.middleware)

	b.registerProvider(app)
	b.registerExceptionHandler(app)

	return app
}

func (b *Builder) registerProvider(app *Application) {
	// Create 保持 Laravel 风格的无错误返回签名；provider 声明错误属于启动装配失败，
	// 必须立即 panic，避免返回一个半装配的 Application 继续运行。
	for _, provider := range providerpkg.DefaultProviders() {
		if err := app.RegisterProvider(provider); err != nil {
			panic(fmt.Errorf("register provider %s: %w", providerIdentity(provider), err))
		}
	}
	for _, provider := range b.providers {
		if err := app.RegisterProvider(provider); err != nil {
			panic(fmt.Errorf("register provider %s: %w", providerIdentity(provider), err))
		}
	}
}

func (b *Builder) registerExceptionHandler(app *Application) {
	// 构建并注册统一异常处理器，对齐 Laravel ExceptionHandler 装配流程。
	//
	// 装配顺序：
	//   1. 框架默认 Option（Recovery/Logging/ClientErrorLogging/PanicStack）
	//   2. 业务侧 WithExceptions Option（Context/Render/Report/DontReport/Level）
	//   3. 业务侧 HandlerFactory（struct embedding 替换/包裹）
	//   4. 注册到 facade registry，供 HTTP（RenderError）和非 HTTP（exception.Report）统一使用
	handler := goexception.BuildAndRegister(
		append(
			[]goexception.Option{
				goexception.WithRecovery(true),
				goexception.WithLogging(true),
				goexception.WithClientErrorLogging(true),
				goexception.WithPanicStack(true),
				goexception.WithDebugResolver(func() bool { return config.GetBool("app.debug", false) }),
			},
			b.exceptions.options...,
		),
		b.exceptions.handlerFactory,
	)
	if app != nil && handler != nil {
		_ = app.Instance("exception.handler", handler, container.WithCloseGroup(container.CloseGroupReporting))
	}
}

// Routing 描述 HTTP 路由、Console 命令、定时任务和路径配置。
type Routing struct {
	commands       []console.CommandFactory
	routes         []func(*Application, *gin.Engine) error
	schedules      []func(*timer.Schedule)
	migrationPaths []string
	seedPaths      []string
}

// Commands 注册业务侧 Console command factories。
func (r *Routing) Commands(factories ...console.CommandFactory) {
	r.commands = append(r.commands, factories...)
}

// Routes 注册 HTTP 路由挂载回调。
func (r *Routing) Routes(registrars ...func(*Application, *gin.Engine) error) {
	r.routes = append(r.routes, registrars...)
}

// Schedules 注册定时任务回调。
func (r *Routing) Schedules(registrars ...func(*timer.Schedule)) {
	r.schedules = append(r.schedules, registrars...)
}

// MigrationPaths 注册 migration 扫描路径。
func (r *Routing) MigrationPaths(paths ...string) {
	r.migrationPaths = append(r.migrationPaths, paths...)
}

// SeedPaths 注册 seeder 扫描路径。
func (r *Routing) SeedPaths(paths ...string) {
	r.seedPaths = append(r.seedPaths, paths...)
}

// Middleware 描述 HTTP middleware 配置。
type Middleware struct {
	preRegistrars []func(*gin.Engine)
	registrars    []func(*gin.Engine)
}

// Prepend 前置 HTTP middleware 注册器，使其早于 prismgo 内置中间件执行。
func (m *Middleware) Prepend(registrars ...func(*gin.Engine)) {
	m.preRegistrars = append(m.preRegistrars, registrars...)
}

// Use 追加 HTTP middleware 注册器。
func (m *Middleware) Use(registrars ...func(*gin.Engine)) {
	m.registrars = append(m.registrars, registrars...)
}

// Apply 把已声明的 middleware 注册到 Gin engine。
func (m *Middleware) Apply(engine *gin.Engine) {
	for _, registrar := range m.registrars {
		if registrar != nil {
			registrar(engine)
		}
	}
}

// Exceptions 保存应用级异常处理选项。
//
// 所有 Option 和 factory
// 最终在 Create() 中注入到同一个 Handler 实例，HTTP 和非 HTTP 共享配置。
type Exceptions struct {
	// options 保存 Render/Report/DontReport/Level/Context 等细粒度扩展 Option。
	options []goexception.Option
	// handlerFactory 保存自定义 Handler 工厂（struct embedding 替换或包裹）。
	handlerFactory func(*goexception.Handler) *goexception.Handler
}

// Context 把请求上下文字段附加到异常日志，对齐 Laravel Handler::context()。
func (e *Exceptions) Context(extract goexception.ContextExtractor) {
	e.options = append(e.options, goexception.WithContext(extract))
}

// Render 注册自定义公开错误渲染器。
//
// 参数可传 prismgo/exception.Renderer（返回 Problem 的 JSON 渲染器），
// 也可传 prismgo/exception.ResponseRenderer（完整响应控制的渲染器）。
func (e *Exceptions) Render(renderer any) {
	switch r := renderer.(type) {
	case nil:
		return
	case goexception.Renderer:
		e.options = append(e.options, goexception.WithRenderer(r))
	case func(*gin.Context, error) (goexception.Problem, bool):
		e.options = append(e.options, goexception.WithRenderer(r))
	case goexception.ResponseRenderer:
		e.options = append(e.options, goexception.WithResponseRenderer(r))
	case func(*gin.Context, error) bool:
		e.options = append(e.options, goexception.WithResponseRenderer(r))
	}
}

// RenderResponse 注册一个自行写出完整响应的渲染器。
func (e *Exceptions) RenderResponse(renderer goexception.ResponseRenderer) {
	e.options = append(e.options, goexception.WithResponseRenderer(renderer))
}

// Report 注册自定义异常上报器，对齐 Laravel Handler::reportable()。
func (e *Exceptions) Report(reporter goexception.Reporter) {
	e.options = append(e.options, goexception.WithReporter(reporter))
}

// DontReport 跳过匹配错误的异常上报，对齐 Laravel Handler::$dontReport。
func (e *Exceptions) DontReport(predicate goexception.Predicate) {
	e.options = append(e.options, goexception.WithDontReport(predicate))
}

// Level 自定义异常日志级别，对齐 Laravel Handler::level()。
func (e *Exceptions) Level(resolve goexception.LevelResolver) {
	e.options = append(e.options, goexception.WithLevel(resolve))
}

// Handler 替换或包裹完整异常处理器（struct embedding 替换路径）。
//
// factory 在 Create() 中执行，可读取已装配 Option 的默认 Handler 并返回包裹后的 Handler。
// 返回 nil 时回退到默认 Handler。
func (e *Exceptions) Handler(factory func(*goexception.Handler) *goexception.Handler) {
	if factory != nil {
		e.handlerFactory = factory
	}
}

// Use 直接指定完整异常处理器实例（忽略默认 Option 装配）。
//
// 适用场景：项目已有稳定的自定义 Handler，不需要默认 Option。
func (e *Exceptions) Use(handler *goexception.Handler) {
	if handler != nil {
		e.handlerFactory = func(_ *goexception.Handler) *goexception.Handler {
			return handler
		}
	}
}
