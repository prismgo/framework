package route

import (
	"fmt"
	"net/http"
	"reflect"
	"regexp"
	"strings"
	"sync"

	"github.com/gin-gonic/gin"
)

// Route 是单条路由的链式配置对象。
type Route struct {
	router *Router
	entry  *routeEntry
}

// Name 设置当前路由名称后缀。分组命名前缀会在注册时自动保留。
func (r *Route) Name(name string) *Route {
	if r == nil || r.entry == nil {
		return r
	}
	r.router.mu.Lock()
	defer r.router.mu.Unlock()
	r.router.updateRouteName(r.entry, name)
	return r
}

// Where 设置参数正则约束。
func (r *Route) Where(param, expr string) *Route {
	if r == nil || r.entry == nil {
		return r
	}
	param = strings.TrimSpace(param)
	expr = strings.TrimSpace(expr)
	if param == "" || expr == "" {
		return r
	}

	// 预编译验证正则表达式
	fullExpr := expr
	if !strings.HasPrefix(fullExpr, "^") {
		fullExpr = "^" + fullExpr
	}
	if !strings.HasSuffix(fullExpr, "$") {
		fullExpr += "$"
	}
	compiled, err := regexp.Compile(fullExpr)
	if err != nil {
		panic(fmt.Sprintf("route: invalid constraint for %q: %v", param, err))
	}

	r.router.mu.Lock()
	defer r.router.mu.Unlock()
	if r.entry.where == nil {
		r.entry.where = map[string]string{}
	}
	r.entry.where[param] = expr

	// 复用已编译的正则表达式
	if r.entry.compiledConstraints == nil {
		r.entry.compiledConstraints = map[string]*regexp.Regexp{}
	}
	r.entry.compiledConstraints[param] = compiled

	// 约束变更影响路径编译（末尾通配符），需要重新编译
	r.entry.compiledPaths = compilePaths(r.entry.uri, r.entry.where)

	return r
}

// WhereNumber 要求参数是数字。
func (r *Route) WhereNumber(param string) *Route {
	return r.Where(param, `^\d+$`)
}

// WhereAlpha 要求参数仅包含字母。
func (r *Route) WhereAlpha(param string) *Route {
	return r.Where(param, `^[A-Za-z]+$`)
}

// WhereAlphaNumeric 要求参数仅包含字母或数字。
func (r *Route) WhereAlphaNumeric(param string) *Route {
	return r.Where(param, `^[A-Za-z0-9]+$`)
}

// WhereUuid 要求参数符合 UUID 格式。
func (r *Route) WhereUuid(param string) *Route {
	return r.Where(param, `^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$`)
}

// WhereUlid 要求参数符合 ULID 格式。
func (r *Route) WhereUlid(param string) *Route {
	return r.Where(param, `^[0-9A-HJKMNP-TV-Z]{26}$`)
}

// WhereIn 要求参数落在给定值集合中。
//
// 设计说明：空字符串不会参与约束构造；如果过滤后没有有效值，则视为不追加约束。
// 这样可以避免生成只匹配空串的无意义正则，同时保持空切片与“全空值切片”语义一致。
func (r *Route) WhereIn(param string, values []string) *Route {
	if len(values) == 0 {
		return r
	}
	escaped := make([]string, 0, len(values))
	for _, value := range values {
		if value != "" {
			escaped = append(escaped, regexp.QuoteMeta(value))
		}
	}
	if len(escaped) == 0 {
		return r
	}
	return r.Where(param, `^(?:`+strings.Join(escaped, "|")+`)$`)
}

// Missing 设置模型绑定失败时的兜底处理。
func (r *Route) Missing(handler gin.HandlerFunc) *Route {
	if r == nil || r.entry == nil {
		return r
	}
	r.router.mu.Lock()
	defer r.router.mu.Unlock()
	r.entry.missing = handler
	return r
}

// Middleware 向当前路由追加中间件。
func (r *Route) Middleware(handlers ...gin.HandlerFunc) *Route {
	if r == nil || r.entry == nil {
		return r
	}
	r.router.mu.Lock()
	defer r.router.mu.Unlock()
	r.entry.middleware = append(r.entry.middleware, handlers...)
	for _, handler := range handlers {
		r.entry.middlewareIDs = append(r.entry.middlewareIDs, middlewareID(handler))
	}
	return r
}

// WithoutMiddleware 从当前路由移除指定名称或函数名的中间件。
func (r *Route) WithoutMiddleware(names ...string) *Route {
	if r == nil || r.entry == nil {
		return r
	}
	without := stringSet(names...)
	r.router.mu.Lock()
	defer r.router.mu.Unlock()
	handlers := make([]gin.HandlerFunc, 0, len(r.entry.middleware))
	ids := make([]string, 0, len(r.entry.middlewareIDs))
	for i, handler := range r.entry.middleware {
		id := ""
		if i < len(r.entry.middlewareIDs) {
			id = r.entry.middlewareIDs[i]
		}
		if _, skip := without[id]; skip {
			continue
		}
		if _, skip := without[functionName(handler)]; skip {
			continue
		}
		handlers = append(handlers, handler)
		ids = append(ids, id)
	}
	r.entry.middleware = handlers
	r.entry.middlewareIDs = ids
	return r
}

// ScopeBindings 保留 Laravel 链式语义；当前绑定器由显式 Bind/Model 决定作用域。
func (r *Route) ScopeBindings() *Route { return r }

// WithoutScopedBindings 保留 Laravel 链式语义；当前绑定器由显式 Bind/Model 决定作用域。
func (r *Route) WithoutScopedBindings() *Route { return r }

// Registrar 保存一次链式分组声明中的附加属性。
type Registrar struct {
	router *Router
	attrs  groupAttributes
}

// Prefix 追加路径前缀。
func (r *Registrar) Prefix(prefix string) *Registrar {
	next := r.clone()
	next.attrs.prefix = joinPaths(next.attrs.prefix, prefix)
	return next
}

// Name 追加命名前缀。
func (r *Registrar) Name(name string) *Registrar {
	next := r.clone()
	next.attrs.name += strings.TrimSpace(name)
	return next
}

// Domain 设置域名约束。
func (r *Registrar) Domain(domain string) *Registrar {
	next := r.clone()
	next.attrs.domain = strings.TrimSpace(domain)
	return next
}

// Middleware 追加分组中间件。
func (r *Registrar) Middleware(handlers ...gin.HandlerFunc) *Registrar {
	next := r.clone()
	next.attrs.middleware = append(next.attrs.middleware, handlers...)
	for _, handler := range handlers {
		next.attrs.middlewareIDs = append(next.attrs.middlewareIDs, middlewareID(handler))
	}
	return next
}

// WithoutMiddleware 从当前分组排除指定名称或函数名的中间件。
func (r *Registrar) WithoutMiddleware(names ...string) *Registrar {
	next := r.clone()
	next.attrs.withoutMiddleware = mergeStringSet(next.attrs.withoutMiddleware, stringSet(names...))
	return next
}

// Controller 设置控制器对象，供 Action 方法按方法名解析 handler。
func (r *Registrar) Controller(controller any) *Registrar {
	next := r.clone()
	next.attrs.controller = controller
	return next
}

// Where 设置分组参数约束。
func (r *Registrar) Where(param, expr string) *Registrar {
	next := r.clone()
	if next.attrs.where == nil {
		next.attrs.where = map[string]string{}
	}
	next.attrs.where[strings.TrimSpace(param)] = strings.TrimSpace(expr)
	return next
}

// ScopeBindings 保留 Laravel 链式语义；当前绑定器由显式 Bind/Model 决定作用域。
func (r *Registrar) ScopeBindings() *Registrar { return r.clone() }

// WithoutScopedBindings 保留 Laravel 链式语义；当前绑定器由显式 Bind/Model 决定作用域。
func (r *Registrar) WithoutScopedBindings() *Registrar { return r.clone() }

// Group 执行分组闭包，闭包内使用 facade 注册的路由会继承当前属性。
func (r *Registrar) Group(fn func()) {
	if fn == nil {
		return
	}
	// Group 的闭包仍保持 Laravel 风格用法：闭包内可以继续通过 Router 或 facade 声明路由。
	// 当前 Registrar 的属性会被压入显式 scope registry，而不是写入 Router 自身状态。
	done := routeScopes.push(r.router, r.attrs)
	defer done()
	fn()
}

func (r *Registrar) Get(uri string, handlers ...gin.HandlerFunc) *Route {
	return r.add([]string{http.MethodGet}, uri, handlers...)
}

func (r *Registrar) Post(uri string, handlers ...gin.HandlerFunc) *Route {
	return r.add([]string{http.MethodPost}, uri, handlers...)
}

func (r *Registrar) Put(uri string, handlers ...gin.HandlerFunc) *Route {
	return r.add([]string{http.MethodPut}, uri, handlers...)
}

func (r *Registrar) Patch(uri string, handlers ...gin.HandlerFunc) *Route {
	return r.add([]string{http.MethodPatch}, uri, handlers...)
}

func (r *Registrar) Delete(uri string, handlers ...gin.HandlerFunc) *Route {
	return r.add([]string{http.MethodDelete}, uri, handlers...)
}

func (r *Registrar) Options(uri string, handlers ...gin.HandlerFunc) *Route {
	return r.add([]string{http.MethodOptions}, uri, handlers...)
}

func (r *Registrar) Match(methods []string, uri string, handlers ...gin.HandlerFunc) *Route {
	return r.add(methods, uri, handlers...)
}

func (r *Registrar) Any(uri string, handlers ...gin.HandlerFunc) *Route {
	return r.add([]string{http.MethodGet, http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete, http.MethodOptions, http.MethodHead}, uri, handlers...)
}

func (r *Registrar) Redirect(uri, destination string, status ...int) *Route {
	code := http.StatusFound
	if len(status) > 0 && status[0] > 0 {
		code = status[0]
	}
	return r.Get(uri, func(c *gin.Context) { c.Redirect(code, destination) })
}

func (r *Registrar) PermanentRedirect(uri, destination string) *Route {
	return r.Redirect(uri, destination, http.StatusMovedPermanently)
}

// Action 从 Controller 中按方法名注册 handler。
func (r *Registrar) Action(method, uri, action string, middleware ...gin.HandlerFunc) *Route {
	// Action 需要在解析 Controller 方法前先合并外层 scope，否则嵌套 Controller 分组会失效。
	attrs := mergeGroupAttributes(activeGroupAttributes(r.router), r.attrs)
	handler := resolveControllerAction(attrs.controller, action)
	handlers := append(middleware, handler)
	return r.router.addWithAttributes(attrs, []string{method}, uri, handlers...)
}

func (r *Registrar) add(methods []string, uri string, handlers ...gin.HandlerFunc) *Route {
	// Registrar 自己保存链式声明属性；注册时再读取当前活动分组，形成本条路由的最终 scope。
	attrs := mergeGroupAttributes(activeGroupAttributes(r.router), r.attrs)
	return r.router.addWithAttributes(attrs, methods, uri, handlers...)
}

func (r *Registrar) clone() *Registrar {
	next := &Registrar{router: r.router, attrs: r.attrs}
	next.attrs.middleware = append([]gin.HandlerFunc(nil), r.attrs.middleware...)
	next.attrs.middlewareIDs = append([]string(nil), r.attrs.middlewareIDs...)
	next.attrs.withoutMiddleware = mergeStringSet(r.attrs.withoutMiddleware)
	next.attrs.where = mergeStringMap(r.attrs.where)
	return next
}

func resolveControllerAction(controller any, action string) gin.HandlerFunc {
	if controller == nil {
		panic("route: controller is not configured")
	}
	method := reflectValue(controller).MethodByName(action)
	if !method.IsValid() {
		panic("route: controller action " + action + " not found")
	}
	if !isRouteActionSignature(method.Type()) {
		panic("route: controller action " + action + " must have signature func(*gin.Context), got " + method.Type().String())
	}
	return method.Interface().(func(*gin.Context))
}

// isRouteActionSignature 校验 Action 允许的控制器方法签名。
//
// 需求背景：Action 仍然保持启动期 fail-fast 的 panic 语义，但 panic 信息需要由 route 包
// 自己给出清晰契约，不能把 Go 原始类型断言错误直接暴露给调用方。
func isRouteActionSignature(methodType reflect.Type) bool {
	contextType := reflect.TypeOf((*gin.Context)(nil))
	return methodType != nil && methodType.Kind() == reflect.Func && methodType.NumIn() == 1 && methodType.In(0) == contextType && methodType.NumOut() == 0
}

func reflectValue(value any) reflect.Value {
	v := reflect.ValueOf(value)
	if v.Kind() == reflect.Pointer && v.IsNil() {
		panic("route: controller is nil")
	}
	return v
}

type routeScopeRegistry struct {
	mu     sync.Mutex
	stacks map[*Router][]groupAttributes
}

var routeScopes = &routeScopeRegistry{stacks: map[*Router][]groupAttributes{}}

// push 把当前 Group 的属性压入“按 Router 隔离”的声明期 scope。
//
// 参数说明：
// - router：正在注册路由的 Router 实例，用作隔离键，避免不同 Router 并发构建时互相污染。
// - attrs：当前 Registrar 链式声明得到的分组属性，压栈前会复制切片和 map。
//
// 返回值说明：
// 返回的函数负责弹出本次压入的 scope，调用方必须 defer 执行，保证 panic 或提前返回时
// 不会把分组状态泄漏给后续声明。
//
// 设计思路：
// 这里仍然使用“栈”表达嵌套 group，但栈不再属于 Router 的长期状态，也不再按 goroutine
// ID 归属。Router 只作为隔离 key，真正的路由定义仍由 Router.mu 保护写入。
func (s *routeScopeRegistry) push(router *Router, attrs groupAttributes) func() {
	if router == nil {
		return func() {}
	}
	s.mu.Lock()
	s.stacks[router] = append(s.stacks[router], cloneGroupAttributes(attrs))
	s.mu.Unlock()
	return func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		stack := s.stacks[router]
		if len(stack) == 0 {
			return
		}
		stack = stack[:len(stack)-1]
		if len(stack) == 0 {
			delete(s.stacks, router)
			return
		}
		s.stacks[router] = stack
	}
}

// activeGroupAttributes 返回指定 Router 当前活动的嵌套 group 合并结果。
//
// 逻辑说明：
// 读取 registry 时先复制当前 stack，再在锁外执行合并，避免把路径拼接、map 复制等工作
// 放在临界区里。调用方拿到的是一次性快照，后续 routeEntry 会保存最终属性，不依赖
// registry 的生命周期。
func activeGroupAttributes(router *Router) groupAttributes {
	if router == nil {
		return groupAttributes{}
	}
	routeScopes.mu.Lock()
	stack := append([]groupAttributes(nil), routeScopes.stacks[router]...)
	routeScopes.mu.Unlock()
	return mergeGroupAttributes(stack...)
}

// cloneGroupAttributes 深拷贝 groupAttributes 中可变的切片和 map。
//
// 需求背景：
// Registrar 的链式方法会不断 clone 自身属性。如果 scope registry 直接保存原始 attrs，
// 后续链式调用或测试并发声明时可能复用底层切片，导致已入栈的 scope 被意外修改。
func cloneGroupAttributes(attrs groupAttributes) groupAttributes {
	attrs.middleware = append([]gin.HandlerFunc(nil), attrs.middleware...)
	attrs.middlewareIDs = append([]string(nil), attrs.middlewareIDs...)
	attrs.withoutMiddleware = mergeStringSet(attrs.withoutMiddleware)
	attrs.where = mergeStringMap(attrs.where)
	return attrs
}
