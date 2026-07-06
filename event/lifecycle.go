package event

import "time"

// 内置生命周期事件名统一使用 <domain>.<stage> 格式。
//
// 设计说明：
// 1. app.* 描述应用整体生命周期，供启动探针、退出清理观察和运行期监控订阅；
// 2. app.provider.* 描述 ServiceProvider 装配阶段，供调试启动顺序与定位 provider 问题；
// 3. server.* 描述 HTTP 服务监听与优雅关闭阶段；
// 4. request.* 描述单个 HTTP 请求进入、完成和失败阶段。
//
// 事件 payload 只包含基础类型或可安全序列化的快照，不携带 gin.Context、http.Server、
// provider 实例、数据库连接等运行时对象，避免通用事件层反向耦合具体基础设施。
const (
	EventAppBooting     = "app.booting"
	EventAppBooted      = "app.booted"
	EventAppTerminating = "app.terminating"
	EventAppTerminated  = "app.terminated"

	EventProviderRegistering = "app.provider.registering"
	EventProviderRegistered  = "app.provider.registered"
	EventProviderBooting     = "app.provider.booting"
	EventProviderBooted      = "app.provider.booted"

	EventServerStarting = "server.starting"
	EventServerStarted  = "server.started"
	EventServerStopping = "server.stopping"
	EventServerStopped  = "server.stopped"

	EventRequestReceived = "request.received"
	EventRequestHandled  = "request.handled"
	EventRequestFailed   = "request.failed"
	EventRequestFinished = "request.finished"

	EventConsoleApplicationStarting = "console.application.starting"
	EventCommandStarting            = "console.command.starting"
	EventCommandFinished            = "console.command.finished"
)

// AppBooting 表示应用即将开始执行 provider 注册与启动流程。
type AppBooting struct {
	Args []string
}

// Name 实现 Event 接口。
func (AppBooting) Name() string { return EventAppBooting }

// AppBooted 表示应用已经完成 provider 注册与启动流程。
type AppBooted struct {
	Duration time.Duration
}

// Name 实现 Event 接口。
func (AppBooted) Name() string { return EventAppBooted }

// AppTerminating 表示应用已经收到关闭意图，即将执行 cleanup 与 facade 资源释放。
type AppTerminating struct {
	Reason string
}

// Name 实现 Event 接口。
func (AppTerminating) Name() string { return EventAppTerminating }

// AppTerminated 表示应用已经执行完 cleanup 与 facade 资源释放。
//
// Duration 表示应用从启动到终止的总生命周期时长。
// CloseDuration 表示关闭流程（从 Close 调用到所有 cleanup 完成）的耗时。
type AppTerminated struct {
	Duration      time.Duration
	CloseDuration time.Duration
	Error         string
}

// Name 实现 Event 接口。
func (AppTerminated) Name() string { return EventAppTerminated }

// ProviderRegistering 表示某个 ServiceProvider 即将执行 Register 阶段。
type ProviderRegistering struct {
	Provider string
}

// Name 实现 Event 接口。
func (ProviderRegistering) Name() string { return EventProviderRegistering }

// ProviderRegistered 表示某个 ServiceProvider 已完成 Register 阶段。
type ProviderRegistered struct {
	Provider string
}

// Name 实现 Event 接口。
func (ProviderRegistered) Name() string { return EventProviderRegistered }

// ProviderBooting 表示某个 ServiceProvider 即将执行 Boot 阶段。
type ProviderBooting struct {
	Provider string
}

// Name 实现 Event 接口。
func (ProviderBooting) Name() string { return EventProviderBooting }

// ProviderBooted 表示某个 ServiceProvider 已完成 Boot 阶段。
type ProviderBooted struct {
	Provider string
}

// Name 实现 Event 接口。
func (ProviderBooted) Name() string { return EventProviderBooted }

// ServerStarting 表示 HTTP 服务即将开始监听端口。
type ServerStarting struct {
	Addr string
	PID  int
}

// Name 实现 Event 接口。
func (ServerStarting) Name() string { return EventServerStarting }

// ServerStarted 表示 HTTP 服务已经开始监听，可以对外提供服务。
type ServerStarted struct {
	Addr string
	PID  int
}

// Name 实现 Event 接口。
func (ServerStarted) Name() string { return EventServerStarted }

// ServerStopping 表示 HTTP 服务开始优雅关闭。
type ServerStopping struct {
	Addr   string
	Reason string
}

// Name 实现 Event 接口。
func (ServerStopping) Name() string { return EventServerStopping }

// ServerStopped 表示 HTTP 服务已经结束监听并完成关闭流程。
type ServerStopped struct {
	Addr     string
	Duration time.Duration
	Error    string
}

// Name 实现 Event 接口。
func (ServerStopped) Name() string { return EventServerStopped }

// RequestReceived 表示 HTTP 请求进入路由层。
//
// 字段刻意保持基础类型，避免把 gin.Context 这类传输层对象泄露到 prismgo/event。
type RequestReceived struct {
	Method     string
	Path       string
	ClientIP   string
	RequestID  string
	ReceivedAt time.Time
}

// Name 实现 Event 接口。
func (RequestReceived) Name() string { return EventRequestReceived }

// RequestHandled 表示 HTTP 请求已经正常处理完成。
type RequestHandled struct {
	Method    string
	Path      string
	RequestID string
	Status    int
	Duration  time.Duration
}

// Name 实现 Event 接口。
func (RequestHandled) Name() string { return EventRequestHandled }

// RequestFailed 表示 HTTP 请求以 5xx 或 panic 结束。
type RequestFailed struct {
	Method    string
	Path      string
	RequestID string
	Status    int
	Duration  time.Duration
	Error     string
	Stack     string
}

// Name 实现 Event 接口。
func (RequestFailed) Name() string { return EventRequestFailed }

// RequestFinished 表示 HTTP 请求已经完成收尾。
//
// 设计说明：该事件无论请求成功、5xx 失败还是 panic 都会派发，并且总是在
// RequestHandled 或 RequestFailed 之后派发，适合统一统计请求耗时、状态码和收尾指标。
type RequestFinished struct {
	Method    string
	Path      string
	RequestID string
	Status    int
	Duration  time.Duration
	Error     string
}

// Name 实现 Event 接口。
func (RequestFinished) Name() string { return EventRequestFinished }

// ConsoleApplicationStarting 表示 Console Kernel 首次进入 Artisan starting 阶段。
//
// 需求背景：Laravel Artisan 会在应用 boot 完成后、starting callbacks 和延迟命令解析前提供观测点。
// Prismgo 每个 Kernel 实例只派发一次该事件，用于记录当前 Kernel 的命令注册生命周期。
type ConsoleApplicationStarting struct {
	KernelName string
}

// Name 实现 Event 接口。
func (ConsoleApplicationStarting) Name() string { return EventConsoleApplicationStarting }

// CommandStarting 表示具体 console command 即将执行。
//
// 设计说明：Input 保留原始命令名、argument 和显式 option 快照，不做默认脱敏或截断。监听器如果转发到日志、
// 指标或外部监控，需要自行承担敏感参数过滤责任。
type CommandStarting struct {
	Command string
	Input   []string
}

// Name 实现 Event 接口。
func (CommandStarting) Name() string { return EventCommandStarting }

// CommandFinished 表示具体 console command 已结束。
//
// Error 仅保存错误摘要，命令成功或失败仍由原始 error 返回值决定；console 事件只提供观测能力。
type CommandFinished struct {
	Command   string
	Succeeded bool
	Error     string
	Duration  time.Duration
}

// Name 实现 Event 接口。
func (CommandFinished) Name() string { return EventCommandFinished }
