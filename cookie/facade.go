package cookie

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/prismgo/framework/facade"
)

const serviceKey = "cookie.queue"

// Resolve 从当前 Application 容器解析 cookie 队列。
func Resolve() *Queue {
	return facade.Resolve[*Queue](serviceKey)
}

// QueueCookie 将 cookie 值对象加入进程级默认队列。
func QueueCookie(c Cookie) {
	Resolve().Queue(c)
}

// QueueCookieFrom 将 cookie 值对象加入当前 Gin 请求的 Request Cookie Queue。
func QueueCookieFrom(c *gin.Context, value Cookie) error {
	queue, err := requestQueue(c)
	if err != nil {
		return err
	}
	queue.Queue(value)
	return nil
}

// QueueMake 创建 cookie 并加入进程级默认队列。
func QueueMake(name string, value string, minutes int, options ...Option) Cookie {
	return Resolve().Make(name, value, minutes, options...)
}

// QueueMakeFrom 创建 cookie 并加入当前 Gin 请求的 Request Cookie Queue。
func QueueMakeFrom(c *gin.Context, name string, value string, minutes int, options ...Option) (Cookie, error) {
	queue, err := requestQueue(c)
	if err != nil {
		return Cookie{}, err
	}
	return queue.Make(name, value, minutes, options...), nil
}

// QueueForever 创建长期 cookie 并加入进程级默认队列。
func QueueForever(name string, value string, options ...Option) Cookie {
	return Resolve().Forever(name, value, options...)
}

// QueueForeverFrom 创建长期 cookie 并加入当前 Gin 请求的 Request Cookie Queue。
func QueueForeverFrom(c *gin.Context, name string, value string, options ...Option) (Cookie, error) {
	queue, err := requestQueue(c)
	if err != nil {
		return Cookie{}, err
	}
	return queue.Forever(name, value, options...), nil
}

// QueueExpire 创建过期 cookie 并加入进程级默认队列。
func QueueExpire(name string, options ...Option) Cookie {
	return Resolve().Expire(name, options...)
}

// QueueExpireFrom 创建过期 cookie 并加入当前 Gin 请求的 Request Cookie Queue。
func QueueExpireFrom(c *gin.Context, name string, options ...Option) (Cookie, error) {
	queue, err := requestQueue(c)
	if err != nil {
		return Cookie{}, err
	}
	return queue.Expire(name, options...), nil
}

// QueueForget 是 QueueExpire 的 Laravel 语义别名，作用于进程级默认队列。
func QueueForget(name string, options ...Option) Cookie {
	return Resolve().Forget(name, options...)
}

// QueueForgetFrom 是 QueueExpireFrom 的 Laravel 语义别名。
func QueueForgetFrom(c *gin.Context, name string, options ...Option) (Cookie, error) {
	queue, err := requestQueue(c)
	if err != nil {
		return Cookie{}, err
	}
	return queue.Forget(name, options...), nil
}

// Queued 返回进程级默认队列中指定 name/scope 的 cookie。
func Queued(name string, scope ...Scope) (Cookie, bool) {
	return Resolve().Queued(name, scope...)
}

// QueuedFrom 返回当前 Gin 请求的 Request Cookie Queue 中指定 name/scope 的 cookie。
func QueuedFrom(c *gin.Context, name string, scope ...Scope) (Cookie, bool, error) {
	queue, err := requestQueue(c)
	if err != nil {
		return Cookie{}, false, err
	}
	queued, ok := queue.Queued(name, scope...)
	return queued, ok, nil
}

// HasQueued 判断进程级默认队列中是否存在指定 name/scope 的 cookie。
func HasQueued(name string, scope ...Scope) bool {
	return Resolve().HasQueued(name, scope...)
}

// HasQueuedFrom 判断当前 Gin 请求的 Request Cookie Queue 中是否存在指定 name/scope 的 cookie。
func HasQueuedFrom(c *gin.Context, name string, scope ...Scope) (bool, error) {
	queue, err := requestQueue(c)
	if err != nil {
		return false, err
	}
	return queue.HasQueued(name, scope...), nil
}

// Unqueue 从进程级默认队列移除指定 name/scope 的 cookie。
func Unqueue(name string, scope ...Scope) {
	Resolve().Unqueue(name, scope...)
}

// UnqueueFrom 从当前 Gin 请求的 Request Cookie Queue 移除指定 name/scope 的 cookie。
func UnqueueFrom(c *gin.Context, name string, scope ...Scope) error {
	queue, err := requestQueue(c)
	if err != nil {
		return err
	}
	queue.Unqueue(name, scope...)
	return nil
}

// Flush 将进程级默认队列中的 cookie 写入响应并清空队列。
func Flush(w http.ResponseWriter) error {
	return Resolve().Flush(w)
}

// QueueFrom 从 Gin 上下文读取当前请求级 cookie 队列。
func QueueFrom(c *gin.Context) (*Queue, bool) {
	if c == nil {
		return nil, false
	}
	if queue, ok := c.Get(QueueKey); ok {
		if typed, ok := queue.(*Queue); ok {
			return typed, true
		}
	}
	return nil, false
}
