package responsekit

import (
	"bytes"
	"net/http"

	"github.com/gin-gonic/gin"
)

// DeferredResponseCommitter 统一管理 Gin 响应的延迟提交。
//
// 用途：让 session 和 cookie 中间件共享同一套缓冲、hook 执行、panic 恢复和最终提交逻辑。
// 设计思路：先把 c.Writer 替换成缓冲写入器，业务 handler 只看到延迟响应；中间件收尾阶段先执行 hook，
// 再按当前错误状态决定是否把缓存响应真正写回底层连接。
type DeferredResponseCommitter struct {
	context  *gin.Context
	original gin.ResponseWriter
	writer   *DeferredResponseWriter
}

// NewDeferredResponseCommitter 为当前 Gin 上下文安装延迟响应写入器。
//
// 参数 c 是当前请求上下文；返回值会保留原始 writer，便于在提交完成或 panic 时恢复。
func NewDeferredResponseCommitter(c *gin.Context) *DeferredResponseCommitter {
	if c == nil {
		return &DeferredResponseCommitter{}
	}
	original := c.Writer
	writer := NewDeferredResponseWriter(original)
	c.Writer = writer
	return &DeferredResponseCommitter{context: c, original: original, writer: writer}
}

// Restore 恢复 Gin 原始 writer。
//
// 复杂逻辑说明：panic 恢复路径和 hook 失败路径都需要把 c.Writer 还原给外层中间件，避免后续错误渲染仍写到缓冲 writer 上。
func (d *DeferredResponseCommitter) Restore() {
	if d == nil || d.context == nil {
		return
	}
	d.context.Writer = d.original
}

// Commit 先执行业务 hook，再按当前错误状态决定是否提交缓冲响应。
//
// 参数 hook 在提交前执行，通常用于 flush cookie 队列或保存 session；hook 会拿到当前缓冲 writer。
// 如果 hook 返回错误，Commit 会恢复原始 writer 并直接把错误交给调用方，不提交半完成响应。
func (d *DeferredResponseCommitter) Commit(hook func(http.ResponseWriter) error) error {
	if d == nil || d.context == nil || d.writer == nil {
		if hook == nil {
			return nil
		}
		return hook(nil)
	}
	if hook != nil {
		if err := hook(d.writer); err != nil {
			d.Restore()
			return err
		}
	}
	d.Restore()
	if len(d.context.Errors) > 0 && !d.writer.Written() {
		return nil
	}
	d.writer.FlushBuffered()
	return nil
}

// DeferredResponseWriter 延迟提交 Gin 响应头和响应体。
//
// 需求背景：cookie 队列和 session 保存都要在 handler 结束后才能确认所有待写 Set-Cookie；如果业务
// 响应头已经提前写出，后续 cookie 将失效。
// 设计思路：中间件执行期间先缓存状态码和响应体，直到提交阶段再一次性写回底层 ResponseWriter。
type DeferredResponseWriter struct {
	gin.ResponseWriter
	code    int
	size    int
	written bool
	body    bytes.Buffer
}

// NewDeferredResponseWriter 创建延迟提交的 Gin 响应写入器。
func NewDeferredResponseWriter(target gin.ResponseWriter) *DeferredResponseWriter {
	return &DeferredResponseWriter{ResponseWriter: target, size: -1}
}

// WriteHeader 记录状态码但不立即提交到底层连接。
func (w *DeferredResponseWriter) WriteHeader(statusCode int) {
	if statusCode <= 0 || w.written {
		return
	}
	w.code = statusCode
}

// WriteHeaderNow 标记响应已被业务代码写入，但仍延迟真实提交。
func (w *DeferredResponseWriter) WriteHeaderNow() {
	if w.written {
		return
	}
	if w.code == 0 {
		w.code = http.StatusOK
	}
	w.written = true
	if w.size < 0 {
		w.size = 0
	}
}

// Write 缓存响应体并维护 Gin 期望的写入状态。
func (w *DeferredResponseWriter) Write(data []byte) (int, error) {
	w.WriteHeaderNow()
	n, err := w.body.Write(data)
	w.size += n
	return n, err
}

// WriteString 兼容 Gin 的字符串响应写入路径。
func (w *DeferredResponseWriter) WriteString(data string) (int, error) {
	return w.Write([]byte(data))
}

// Status 返回业务代码设置的状态码。
func (w *DeferredResponseWriter) Status() int {
	if w.code != 0 {
		return w.code
	}
	return w.ResponseWriter.Status()
}

// Size 返回当前缓存的响应体大小。
func (w *DeferredResponseWriter) Size() int {
	return w.size
}

// Written 返回业务代码是否已经尝试写响应。
func (w *DeferredResponseWriter) Written() bool {
	return w.written
}

// Flush 兼容 http.Flusher 语义，但只标记响应已写入。
//
// 约束说明：该实现不适合 SSE 或长连接流式响应；这类路由不应挂载 session 或 cookie 延迟提交中间件。
func (w *DeferredResponseWriter) Flush() {
	w.WriteHeaderNow()
}

// FlushBuffered 把缓存的状态码和响应体提交到底层 ResponseWriter。
func (w *DeferredResponseWriter) FlushBuffered() {
	if w.code != 0 {
		w.ResponseWriter.WriteHeader(w.code)
	}
	if w.body.Len() > 0 {
		_, _ = w.ResponseWriter.Write(w.body.Bytes())
	}
}
