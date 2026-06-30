package exception

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	"github.com/prismgo/framework/internal/stackx"
	"github.com/prismgo/framework/logger"
)

// DefaultDontReport 过滤 context 取消和超时错误，避免将进程生命周期信号当作异常上报。
//
// New() 自动将 DefaultDontReport 加入 Handler.DontReport 链；
// 业务侧可通过 WithDontReport 追加额外的过滤断言。
// 如需移除默认行为，可在构造后清空 Handler.DontReport 或使用自定义初始化。
var DefaultDontReport = Predicate(func(err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	return false
})

// exceptionReportedKey 在 gin.Context 中标记异常已被上报，防止同一请求重复记录日志。
// 设计背景：middleware 中 deferred recover 和 c.Next() 之后的错误检查可能
// 都触发上报，通过 context key 标记确保每个请求只上报一次。
const exceptionReportedKey = "_exception_reported"

// Handler 是框架统一异常处理器，对齐 Laravel ExceptionHandler。
//
// 需求背景：
//
//	Prismgo 原有两套独立的异常处理体系（httpkit.ExceptionHandler 面向 HTTP，
//	debug.ExceptionHandler 面向非 HTTP），dontReport/level/reporter 配置不共享，
//	CLI/Queue/Horizon 侧的上报能力远弱于 HTTP 侧。
//
// 设计思路：
//  1. 单一 Handler 同时服务 HTTP（Render+Report）和非 HTTP（仅 Report）
//  2. dontReport / level / reporter 在两种上下文中完全共享
//  3. 通过 Option 函数式配置，避免构造函数参数膨胀
//  4. 公开字段允许业务侧通过 struct embedding 覆盖特定行为
//  5. reportHTTP 提取 HTTP 字段后委托 Report 写入日志，减少重复代码
type Handler struct {
	// DontReport 过滤不上报的异常类型，对齐 Laravel Handler::$dontReport。
	DontReport []Predicate
	// LevelResolver 按异常类型和 HTTP 状态码映射日志级别，对齐 Laravel Handler::level()。
	LevelResolver LevelResolver
	// Reporters 在异常日志写入后执行，对齐 Laravel Handler::reportable()。
	Reporters []Reporter

	// RecoverPanics 控制是否在 HTTP 中间件中恢复 panic。
	RecoverPanics bool
	// LogErrors 控制是否写入内置异常日志。
	LogErrors bool
	// LogClientErrors 控制是否记录 4xx 客户端错误。
	LogClientErrors bool
	// PanicStack 控制异常日志是否附带完整调用栈。
	// HTTP 路径中由 middleware 采集堆栈并通过 fields["stack"] 传入；
	// 非 HTTP 路径中由 Report 在 LevelError 时自动采集。
	// 堆栈信息会被截断至 4KB 左右（详见 internal/stackx）。
	PanicStack bool
	// DebugResolver 读取应用调试开关；默认读取 app.debug，对齐 Laravel APP_DEBUG 语义。
	DebugResolver func() bool
	// ContextExtractor 从请求上下文提取身份字段，附加到异常日志。
	ContextExtractor ContextExtractor
	// Renderers 自定义错误渲染器链，对齐 Laravel Handler::renderable()。
	Renderers []rendererEntry
}

// rendererEntry 保存一对可选的渲染器。
type rendererEntry struct {
	problem  Renderer
	response ResponseRenderer
}

// New 创建统一异常处理器。
func New(opts ...Option) *Handler {
	h := &Handler{
		RecoverPanics:   true,
		LogErrors:       true,
		LogClientErrors: true,
		PanicStack:      true,
		DebugResolver:   DefaultDebugResolver,
		DontReport:      []Predicate{DefaultDontReport},
	}
	for _, opt := range opts {
		if opt != nil {
			opt(h)
		}
	}
	return h
}

// DefaultDebugResolver 是未经过 foundation 装配时的安全兜底。
// 标准应用启动会由 foundation 注入读取 app.debug 的 resolver。
func DefaultDebugResolver() bool {
	return false
}

// ApplyOptions 批量应用 Option，用于 foundation 装配流程中合并多来源配置。
func (h *Handler) ApplyOptions(opts ...Option) {
	if h == nil {
		return
	}
	for _, opt := range opts {
		if opt != nil {
			opt(h)
		}
	}
}

// Report 上报异常。HTTP 和非 HTTP 上下文均可使用。
//
// 设计思路：
//
//	reportHTTP 提取 HTTP 请求字段后调用 Report，非 HTTP 调用方直接调用 Report。
//	两者共享同一套日志写入、级别判断、PanicStack 和 reporter 逻辑。
//
// 参数说明：
//   - ctx：在 HTTP 上下文中为 *gin.Context，非 HTTP 为 context.Context
//   - err：要上报的原始异常。若为 BizError，会提取 Code/Type/Context/Fields 元数据但不替换为 Cause
//   - fields：调用方自定义日志字段。HTTP 路径包含 status/method/path/stack/message 等
func (h *Handler) Report(ctx context.Context, err error, fields map[string]any) {
	if h == nil || err == nil {
		return
	}
	if fields == nil {
		fields = map[string]any{}
	}

	// 提取 HTTP 状态码用于 ShouldReport 和日志级别判断。
	// 非 HTTP 路径中调用方通常在 fields 中设置 status=500，若未设置则默认 500。
	status, _ := fields["status"].(int)
	if status == 0 {
		status = 500
	}

	// ShouldReport 统一检查 LogErrors / LogClientErrors / DontReport，
	// 确保 HTTP（reportHTTP→ShouldReport）和非 HTTP（Report→ShouldReport）走同一道门。
	if !h.ShouldReport(err, status) {
		return
	}

	addErrorMetadata(fields, err)

	// 当 PanicStack 启用且 fields 中尚无调用栈时自动采集（仅 5xx 状态码）
	if h.PanicStack {
		if _, hasStack := fields["stack"]; !hasStack {
			if status >= http.StatusInternalServerError {
				fields["stack"] = string(stackx.Capture())
			}
		}
	}
	fields = scrubLogFields(fields)

	// 使用已提取的 status 进行日志级别判断
	level := h.Level(err, status)
	message, _ := fields["message"].(string)
	if message == "" {
		message = err.Error()
	}

	// 按配置级别写入日志
	entry := logger.WithContext(ctx).WithFields(fields).WithError(err)
	switch level {
	case LevelDebug:
		entry.Debug(message)
	case LevelInfo:
		entry.Info(message)
	case LevelWarn:
		entry.Warn(message)
	default:
		entry.Error(message)
	}

	// 执行自定义 reporter
	for _, reporter := range h.Reporters {
		reporter(ctx, err, fields)
	}
}

// Render 将异常渲染为安全的 HTTP 响应，返回最终 HTTP 状态码。
func (h *Handler) Render(c *gin.Context, err error) int {
	if h == nil {
		h = New()
	}
	return h.renderResponse(c, err)
}

// ShouldReport 判断是否应上报该异常。
func (h *Handler) ShouldReport(err error, status int) bool {
	if h == nil || !h.LogErrors {
		return false
	}
	if status < http.StatusInternalServerError && !h.LogClientErrors {
		return false
	}
	for _, predicate := range h.DontReport {
		if predicate != nil && predicate(err) {
			return false
		}
	}
	return true
}

// Level 返回异常日志级别。
func (h *Handler) Level(err error, status int) Level {
	if h != nil && h.LevelResolver != nil {
		if level := h.LevelResolver(err, status); level != "" {
			return level
		}
	}
	if status >= http.StatusInternalServerError {
		return LevelError
	}
	return LevelWarn
}

// renderResponse 按优先级尝试渲染器链，最终回退到框架默认 Problem JSON。
func (h *Handler) renderResponse(c *gin.Context, err error) int {
	if c.Writer.Written() {
		c.Abort()
		return c.Writer.Status()
	}
	for _, renderer := range h.Renderers {
		if renderer.response != nil && renderer.response(c, err) {
			return finishRenderedResponse(c, err)
		}
		if renderer.problem == nil {
			continue
		}
		if problem, ok := renderer.problem(c, err); ok {
			if problem.RequestID == "" {
				problem.RequestID = requestIDFromContext(c)
			}
			c.AbortWithStatusJSON(problem.Status, problem)
			return problem.Status
		}
	}
	problem := problemForError(err, requestIDFromContext(c))
	if h.Debug() {
		problem = problem.WithDebug(err)
	}
	c.AbortWithStatusJSON(problem.Status, problem)
	return problem.Status
}

// Debug 返回当前应用是否允许向 HTTP 客户端展示调试细节。
func (h *Handler) Debug() bool {
	if h == nil || h.DebugResolver == nil {
		return false
	}
	return h.DebugResolver()
}

// finishRenderedResponse 在自定义 ResponseRenderer 写入响应后完成 abort。
func finishRenderedResponse(c *gin.Context, err error) int {
	if c.Writer.Written() {
		c.Abort()
		return c.Writer.Status()
	}
	status := c.Writer.Status()
	if status < http.StatusBadRequest {
		status = problemForError(err, requestIDFromContext(c)).Status
	}
	c.AbortWithStatus(status)
	return status
}

type businessCodeProvider interface {
	BusinessCode() int
}

type errorTypeProvider interface {
	ErrorType() string
}

type errorContextProvider interface {
	ErrorContext() map[string]any
}

func addErrorMetadata(fields map[string]any, err error) {
	var codeProvider businessCodeProvider
	if errors.As(err, &codeProvider) && codeProvider != nil {
		fields["error_code"] = codeProvider.BusinessCode()
	}
	var typeProvider errorTypeProvider
	if errors.As(err, &typeProvider) && typeProvider != nil {
		if kind := typeProvider.ErrorType(); kind != "" {
			fields["error_type"] = kind
		}
	}
	var contextProvider errorContextProvider
	if errors.As(err, &contextProvider) && contextProvider != nil {
		if context := contextProvider.ErrorContext(); len(context) > 0 {
			fields["error_context"] = context
		}
	}
	if fieldErrors := publicFieldErrors(err); len(fieldErrors) > 0 {
		fields["field_errors"] = fieldErrors
	}
}

func scrubLogFields(fields map[string]any) map[string]any {
	if len(fields) == 0 {
		return fields
	}
	out := make(map[string]any, len(fields))
	for key, value := range fields {
		out[key] = scrubLogValue(key, value)
	}
	return out
}

func scrubLogValue(key string, value any) any {
	if isSensitiveLogKey(key) {
		return "[redacted]"
	}
	switch typed := value.(type) {
	case map[string]any:
		return scrubLogFields(typed)
	case map[string]string:
		out := make(map[string]string, len(typed))
		for k, v := range typed {
			if isSensitiveLogKey(k) {
				out[k] = "[redacted]"
				continue
			}
			out[k] = v
		}
		return out
	case []any:
		out := make([]any, len(typed))
		for i, item := range typed {
			out[i] = scrubLogValue("", item)
		}
		return out
	default:
		return value
	}
}

func isSensitiveLogKey(key string) bool {
	normalized := strings.ToLower(strings.TrimSpace(key))
	normalized = strings.NewReplacer("-", "_", ".", "_").Replace(normalized)
	// service_key 是框架容器绑定标识，不包含凭据；保留它能让 facade fallback 报告定位到具体服务。
	if normalized == "service_key" {
		return false
	}
	for _, token := range []string{"password", "secret", "token", "key", "authorization", "cookie", "payload", "raw_body", "body_base64"} {
		if normalized == token || strings.Contains(normalized, "_"+token) || strings.Contains(normalized, token+"_") {
			return true
		}
	}
	return false
}

// -------- Option 函数 --------

// WithDontReport 跳过匹配错误的日志和上报，对齐 Laravel Handler::$dontReport。
func WithDontReport(p Predicate) Option {
	return func(h *Handler) {
		if p != nil {
			h.DontReport = append(h.DontReport, p)
		}
	}
}

// WithLevel 自定义异常日志级别映射，对齐 Laravel Handler::level()。
func WithLevel(resolve LevelResolver) Option {
	return func(h *Handler) {
		h.LevelResolver = resolve
	}
}

// WithReporter 注册自定义异常上报器，对齐 Laravel Handler::reportable()。
func WithReporter(reporter Reporter) Option {
	return func(h *Handler) {
		if reporter != nil {
			h.Reporters = append(h.Reporters, reporter)
		}
	}
}

// WithRecovery 控制是否恢复 panic（仅 HTTP 中间件生效）。
func WithRecovery(enabled bool) Option {
	return func(h *Handler) {
		h.RecoverPanics = enabled
	}
}

// WithLogging 控制是否写入内置异常日志。
func WithLogging(enabled bool) Option {
	return func(h *Handler) {
		h.LogErrors = enabled
	}
}

// WithClientErrorLogging 控制是否记录 4xx 客户端错误。
func WithClientErrorLogging(enabled bool) Option {
	return func(h *Handler) {
		h.LogClientErrors = enabled
	}
}

// WithPanicStack 控制异常日志是否附带完整调用栈。
func WithPanicStack(enabled bool) Option {
	return func(h *Handler) {
		h.PanicStack = enabled
	}
}

// WithDebug 固定设置 HTTP 调试响应开关，主要用于测试或极简应用装配。
func WithDebug(enabled bool) Option {
	return func(h *Handler) {
		h.DebugResolver = func() bool { return enabled }
	}
}

// WithDebugResolver 设置调试开关读取函数；生产默认由 app.debug 驱动。
func WithDebugResolver(resolve func() bool) Option {
	return func(h *Handler) {
		if resolve != nil {
			h.DebugResolver = resolve
		}
	}
}

// WithContext 为异常日志附加请求身份字段，对齐 Laravel Handler::context()。
func WithContext(extract ContextExtractor) Option {
	return func(h *Handler) {
		h.ContextExtractor = extract
	}
}

// WithRenderer 注册自定义 ProblemResponse 渲染器，对齐 Laravel Handler::renderable()（JSON 路径）。
func WithRenderer(renderer Renderer) Option {
	return func(h *Handler) {
		if renderer != nil {
			h.Renderers = append(h.Renderers, rendererEntry{problem: renderer})
		}
	}
}

// WithResponseRenderer 注册自定义完整响应渲染器，对齐 Laravel Handler::renderable()（自定义响应路径）。
func WithResponseRenderer(renderer ResponseRenderer) Option {
	return func(h *Handler) {
		if renderer != nil {
			h.Renderers = append(h.Renderers, rendererEntry{response: renderer})
		}
	}
}
