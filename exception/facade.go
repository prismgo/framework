package exception

import (
	"context"

	"github.com/gin-gonic/gin"
	"github.com/prismgo/framework/facade"
)

const serviceKey = "exception.handler"

// Resolve 从当前 Application 容器解析异常处理器。
func Resolve() *Handler {
	return facade.Resolve[*Handler](serviceKey)
}

// Report 通过当前 Handler 上报异常。
func Report(ctx context.Context, err error, fields map[string]any) {
	if err == nil {
		return
	}
	h := Resolve()
	if h != nil {
		h.Report(ctx, err, fields)
	}
}

// Render 通过当前 Handler 将异常渲染为安全的 HTTP 响应。
func Render(c *gin.Context, err error) int {
	if c == nil {
		return 500
	}
	h := Resolve()
	if h == nil {
		return 500
	}
	return h.Render(c, err)
}

// BuildAndRegister 构建 Handler 并返回实例。
// bootstrap 负责调用 container.Use 将其注入容器。
func BuildAndRegister(opts []Option, factory func(*Handler) *Handler) *Handler {
	h := New()
	for _, opt := range opts {
		if opt != nil {
			opt(h)
		}
	}
	if factory != nil {
		if wrapped := factory(h); wrapped != nil {
			h = wrapped
		}
	}
	return h
}
