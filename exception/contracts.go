// Package exception 提供 Prismgo 统一异常处理器实现，对齐 Laravel 13 App\Exceptions\Handler。
//
// 需求背景：
//
//	Prismgo 原本存在两套独立的异常处理体系：
//	  - httpkit.ExceptionHandler：面向 HTTP，负责 panic 恢复、JSON 渲染、日志上报、自定义 renderer/reporter
//	  - debug.ExceptionHandler / foundation.DefaultExceptionHandler：面向非 HTTP（CLI/Queue/Horizon），仅负责 Report
//	两套体系的 dontReport / level / reporter 配置完全不共享，导致 CLI/Queue/Horizon 侧的上报能力远弱于 HTTP 侧。
//
// 设计思路：
//  1. 单一 Handler 同时服务 HTTP（Render+Report）和非 HTTP（仅 Report）
//  2. dontReport / level / reporter 在两种上下文中完全共享
//  3. 通过 Option 函数式配置，避免构造函数参数膨胀
//  4. 公开字段允许业务侧通过 struct embedding 覆盖特定行为
//  5. 对齐 Laravel 13 的 Handler 设计哲学：异常 = 数据，Handler = 策略
//
// 使用方式：
//
//	// 轻量定制（路径 1）：通过 foundation.Exceptions 注册回调
//	foundation.Configure().
//	    WithExceptions(func(e *foundation.Exceptions) {
//	        e.Context(extractor)
//	        e.DontReport(predicate)
//	        e.Level(resolver)
//	    })
//
//	// 完全替换（路径 2）：struct embedding 覆盖 Handler 方法
//	type AppHandler struct { *exception.Handler }
//	func (h *AppHandler) Render(c *gin.Context, err error) int { ... }
package exception

import (
	"github.com/gin-gonic/gin"
)

// Predicate 用于 DontReport 风格的异常过滤，对齐 Laravel Handler::$dontReport。
//
// 当 Predicate 返回 true 时，匹配的异常将被跳过日志写入和上报器调用。
// 多个 Predicate 按注册顺序求值，任一命中即跳过。
//
// 使用示例：
//
//	exception.WithDontReport(func(err error) bool {
//	    return errkit.IsBizError(err, errkit.CodeNotFound)
//	})
type Predicate func(error) bool

// Level 表示异常上报日志级别，对齐 Laravel 的日志级别体系。
type Level string

const (
	LevelDebug Level = "debug" // 调试级别，用于可忽略的异常
	LevelInfo  Level = "info"  // 信息级别，用于值得关注但不影响功能的事件
	LevelWarn  Level = "warn"  // 警告级别，默认用于 4xx 客户端错误
	LevelError Level = "error" // 错误级别，默认用于 5xx 服务端错误
)

// LevelResolver 根据错误和 HTTP 状态码选择日志级别，对齐 Laravel Handler::level()。
//
// err 为当前原始异常；若调用方传入 BizError wrapper，不会自动替换为其底层 cause。
// status 为 HTTP 响应状态码（非 HTTP 上下文中为 0）。
// 返回空字符串时回退到默认规则（5xx→error，其余→warn）。
//
// 使用示例：
//
//	exception.WithLevel(func(err error, status int) exception.Level {
//	    if errors.Is(err, ErrDegraded) {
//	        return exception.LevelInfo
//	    }
//	    return exception.LevelWarn
//	})
type LevelResolver func(err error, status int) Level

// Reporter 在异常日志写入后执行，对齐 Laravel Handler::reportable()。
//
// ctx 在 HTTP 上下文中为 *gin.Context，在非 HTTP 上下文中为 context.Context。
// err 为当前原始异常；若调用方传入 BizError wrapper，不会自动替换为其底层 cause。
// fields 为已填充的日志字段（包含 status/method/path/client_ip 等 HTTP 字段或调用方自定义字段）。
//
// 使用示例：
//
//	exception.WithReporter(func(ctx any, err error, fields map[string]any) {
//	    sentry.CaptureException(err)
//	})
type Reporter func(ctx any, err error, fields map[string]any)

// ContextExtractor 从请求上下文提取身份字段，对齐 Laravel Handler::context()。
//
// 提取的字段仅附加到异常日志，不会被渲染到客户端错误响应中。
// 典型用途：从 gin.Context 中提取 tenant_id、user_id、role 等业务身份信息。
//
// 使用示例：
//
//	exception.WithContext(func(c *gin.Context) map[string]any {
//	    return map[string]any{"tenant_id": c.GetString("tenant_id")}
//	})
type ContextExtractor func(c *gin.Context) map[string]any

// Renderer 为特定错误构建 Problem，对齐 Laravel Handler::renderable()（JSON 路径）。
//
// 返回值：(Problem, true) 表示已处理，框架将以此响应渲染 JSON。
// 返回值：(Problem{}, false) 表示未处理，继续尝试下一个渲染器或回退默认。
//
// 使用示例：
//
//	exception.WithRenderer(func(c *gin.Context, err error) (exception.Problem, bool) {
//	    if errors.Is(err, ErrPaymentFailed) {
//	        return exception.Problem{...}, true
//	    }
//	    return exception.Problem{}, false
//	})
type Renderer func(c *gin.Context, err error) (Problem, bool)

// ResponseRenderer 为特定错误写出完整自定义响应，对齐 Laravel Handler::renderable()（自定义响应路径）。
//
// 返回 true 表示已处理该错误，可使用 c.HTML、c.Data、c.JSON 或 c.Redirect 等任意 Gin 响应方法。
// 返回 false 表示未处理，继续尝试下一个渲染器或回退默认 JSON。
//
// 使用示例：
//
//	exception.WithResponseRenderer(func(c *gin.Context, err error) bool {
//	    if errors.Is(err, ErrMaintenance) {
//	        c.HTML(503, "maintenance.html", nil)
//	        return true
//	    }
//	    return false
//	})
type ResponseRenderer func(c *gin.Context, err error) bool

// Option 配置 Handler，对齐 Laravel Handler 构造函数的参数化风格。
//
// 所有 With* 函数返回 Option，通过 New(opts...) 或 ApplyOptions(opts...) 应用。
// Option 函数式配置避免了构造函数参数膨胀，且易于扩展新配置项。
type Option func(*Handler)
