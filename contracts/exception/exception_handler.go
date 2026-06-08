// Package exception 定义 Prismgo 统一异常处理的公共 contract，对齐 Laravel Illuminate\Contracts\Debug\ExceptionHandler。
//
// 需求背景：
// foundation、horizon、queue、CLI 命令等包需要表达"上报异常"和"渲染异常"的能力，
// 但不应依赖 httpkit 的 *gin.Context 异常处理器或 horizon 的具体实现。
//
// 设计思路：
// 本包只保留异常处理的最小 interface（Report + Render）。具体日志写入、外部服务上报、
// HTTP 渲染仍在 exception 实现包中。
//
// 与旧 debug.ExceptionHandler 的区别：
//   - 旧接口仅 Report(ctx, err)，Render 被放在了 httpkit 包中
//   - 新接口 Report + Render 统一，HTTP/CLI/Queue 共用同一个 contract
//   - Report 增加 fields 参数，支持调用方传入自定义日志字段
package exception

import (
	"context"

	"github.com/gin-gonic/gin"
)

// ExceptionHandler 是框架统一异常处理器 contract，对齐 Laravel Contracts\Debug\ExceptionHandler。
//
// 使用方式：
//   - 实现包通过 facade registry key "exception.handler" 解析
//   - exception.Report(ctx, err, nil) 提供便捷的包级上报入口
//   - HTTP 中间件通过 Render 渲染安全错误响应
type ExceptionHandler interface {
	// Report 上报异常。HTTP 和非 HTTP 上下文均可使用。
	//
	// ctx 在 HTTP 上下文中为 *gin.Context（实现了 context.Context），
	// 在非 HTTP 上下文中为标准 context.Context。
	// fields 为调用方自定义的日志字段，HTTP 路径由 middleware 预填充请求信息，
	// 非 HTTP 路径由调用方提供（可传 nil）。
	Report(ctx context.Context, err error, fields map[string]any)

	// Render 将异常渲染为安全的 HTTP 响应，返回最终 HTTP 状态码。
	Render(c *gin.Context, err error) int
}
