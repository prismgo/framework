package route

import (
	"fmt"
	"net/http"
	"net/url"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"sync"

	"github.com/gin-gonic/gin"
)

// HandlerFunc 是 route 包对业务 HTTP 处理函数的统一别名。
//
// 设计说明：底层仍然使用 Gin，业务层无需改写现有 handler 签名；route 包只负责
// Laravel 风格的声明、元数据收集、约束检查与最终挂载。
type HandlerFunc = gin.HandlerFunc

// Binder 把路由参数解析为业务对象并写入 gin.Context。
//
// value 是原始路径参数值；返回对象会以参数名写入 Context，后续 handler 可通过
// c.Get(param) 读取。返回错误时会触发路由 Missing handler 或 404。
type Binder func(c *gin.Context, value string) (any, error)

// RouteInfo 是 route:list、命名路由和调试场景使用的只读路由快照。
type RouteInfo struct {
	Methods    []string `json:"methods"`
	URI        string   `json:"uri"`
	GinPath    string   `json:"gin_path"`
	Name       string   `json:"name,omitempty"`
	Domain     string   `json:"domain,omitempty"`
	Handler    string   `json:"handler,omitempty"`
	Middleware []string `json:"middleware,omitempty"`
	SourcePath string   `json:"source_path,omitempty"`
}

// routeEntry 保存单条路由在挂载前的稳定快照。
//
// 设计说明：
// - namePrefix 保存分组阶段已经合并好的命名前缀。
// - nameSuffix 保存当前路由自己声明的名称后缀。
// - name 始终保存最终对外可见的完整路由名，供 List 和 URL 使用。
//
// 这样做的原因是 Route.Name 支持重复调用覆盖后缀。如果不把前缀和后缀拆开保存，
// 二次命名时只能拿到上一次的完整名字，无法在保留分组前缀的同时安全替换路由后缀。
type routeEntry struct {
	methods       []string
	uri           string
	name          string
	namePrefix    string
	nameSuffix    string
	domain        string
	action        HandlerFunc
	middleware    []HandlerFunc
	middlewareIDs []string
	where         map[string]string
	missing       HandlerFunc
	controller    any
	handlerName   string
	sourcePath    string
}

type groupAttributes struct {
	prefix            string
	name              string
	domain            string
	middleware        []HandlerFunc
	middlewareIDs     []string
	withoutMiddleware map[string]struct{}
	where             map[string]string
	controller        any
}

// Router 保存当前进程内声明的全部 Laravel 风格路由。
//
// Router 本身不监听端口，也不创建 Gin engine；它只维护路由定义，并在 HTTP server
// 构建阶段一次性 Mount 到外部传入的 engine。
type Router struct {
	mu       sync.RWMutex
	routes   []*routeEntry
	names    map[string]*routeEntry
	binders  map[string]Binder
	patterns map[string]string
}

// New 创建一个空路由器。
func New() *Router {
	return &Router{
		names:    make(map[string]*routeEntry),
		binders:  make(map[string]Binder),
		patterns: make(map[string]string),
	}
}

// Reset 清空路由、命名索引、绑定器和全局参数约束。
func (r *Router) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.routes = nil
	r.names = make(map[string]*routeEntry)
	r.binders = make(map[string]Binder)
	r.patterns = make(map[string]string)
}

// Clone 复制当前 Router 中已经声明完成的路由、命名索引、参数绑定和全局约束。
//
// 需求背景：Application 构建 HTTP server 时需要以 provider 已声明路由为基线，再运行本次
// server 构建专属的 route registrar。复制 Router 可以避免重复构建 server 时把 registrar
// 声明反复写回 Application 持有的基线 Router。
//
// 设计思路：Router 只复制稳定的路由定义，不复制声明期 group scope；scope 只在 Group 闭包
// 执行期间有效，闭包退出后不属于 Router 的持久运行状态。
func (r *Router) Clone() *Router {
	if r == nil {
		return New()
	}
	r.mu.RLock()
	defer r.mu.RUnlock()

	cloned := New()
	cloned.binders = cloneBinders(r.binders)
	cloned.patterns = mergeStringMap(nil, r.patterns)
	cloned.routes = make([]*routeEntry, 0, len(r.routes))
	cloned.ensureRegistries()
	for _, entry := range r.routes {
		copyEntry := entry.clone()
		cloned.routes = append(cloned.routes, copyEntry)
		cloned.indexName(copyEntry)
	}
	return cloned
}

// ensureRegistries 保证 Router 的声明期索引可写。
//
// 需求背景：Clone 会保留 mergeStringMap(nil, nil) 返回 nil 的语义，避免改变 route where
// 等内部 map 形态；但 Router 自身的 names/binders/patterns 是生命周期不变量，Clone 后仍必须
// 像 New() 创建的 Router 一样支持继续注册 Bind、Pattern 和 Add。
func (r *Router) ensureRegistries() {
	if r.names == nil {
		r.names = make(map[string]*routeEntry)
	}
	if r.binders == nil {
		r.binders = make(map[string]Binder)
	}
	if r.patterns == nil {
		r.patterns = make(map[string]string)
	}
}

// Bind 注册显式参数绑定器。
func (r *Router) Bind(param string, binder Binder) {
	r.mu.Lock()
	defer r.mu.Unlock()
	param = strings.TrimSpace(param)
	if param == "" || binder == nil {
		return
	}
	r.binders[param] = binder
}

// Model 是 Bind 的语义化别名，用于表达 Laravel 隐式模型绑定意图。
func (r *Router) Model(param string, binder Binder) {
	r.Bind(param, binder)
}

// Pattern 注册全局参数约束。局部 Where 会覆盖同名全局约束。
func (r *Router) Pattern(param, expr string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	param = strings.TrimSpace(param)
	expr = strings.TrimSpace(expr)
	if param == "" || expr == "" {
		return
	}
	r.patterns[param] = expr
}

// Add 注册一条路由定义。
func (r *Router) Add(methods []string, uri string, handlers ...HandlerFunc) *Route {
	return r.addWithAttributes(activeGroupAttributes(r), methods, uri, handlers...)
}

// addWithAttributes 是 Router 写入路由表的唯一底层入口。
//
// 参数说明：
// - attrs：调用方已经显式解析出的分组属性，包含 Prefix、Name、Middleware、Where、Controller 等。
// - methods：需要注册的 HTTP method 列表，内部会统一规范化并去重。
// - uri：当前路由自己的 URI，会与 attrs.prefix 合并。
// - handlers：最后一个 handler 是业务 action，前面的 handler 会作为当前路由局部中间件。
//
// 设计说明：
// Router 只负责保存最终路由定义、命名索引、参数绑定器和全局约束，不再持有“当前分组”
// 这类调用期状态。这样可以避免通过 runtime stack 反查 goroutine ID，也让 Registrar
// 在并发声明和嵌套声明时明确传入本次注册需要的 scope。
func (r *Router) addWithAttributes(attrs groupAttributes, methods []string, uri string, handlers ...HandlerFunc) *Route {
	if len(handlers) == 0 {
		panic("route: handler is required")
	}
	action := handlers[len(handlers)-1]
	localMiddleware := handlers[:len(handlers)-1]

	r.mu.Lock()
	defer r.mu.Unlock()

	middleware, middlewareIDs := mergeMiddleware(attrs, localMiddleware)
	entry := &routeEntry{
		methods:       normalizeMethods(methods),
		uri:           joinPaths(attrs.prefix, uri),
		namePrefix:    attrs.name,
		name:          attrs.name,
		domain:        attrs.domain,
		action:        action,
		middleware:    middleware,
		middlewareIDs: middlewareIDs,
		where:         mergeStringMap(r.patterns, attrs.where),
		controller:    attrs.controller,
		handlerName:   functionName(action),
		sourcePath:    functionFile(action),
	}
	r.routes = append(r.routes, entry)
	route := &Route{router: r, entry: entry}
	r.indexName(entry)
	return route
}

// Get 注册 GET 路由。
func (r *Router) Get(uri string, handlers ...HandlerFunc) *Route {
	return r.Add([]string{http.MethodGet}, uri, handlers...)
}

// Post 注册 POST 路由。
func (r *Router) Post(uri string, handlers ...HandlerFunc) *Route {
	return r.Add([]string{http.MethodPost}, uri, handlers...)
}

// Put 注册 PUT 路由。
func (r *Router) Put(uri string, handlers ...HandlerFunc) *Route {
	return r.Add([]string{http.MethodPut}, uri, handlers...)
}

// Patch 注册 PATCH 路由。
func (r *Router) Patch(uri string, handlers ...HandlerFunc) *Route {
	return r.Add([]string{http.MethodPatch}, uri, handlers...)
}

// Delete 注册 DELETE 路由。
func (r *Router) Delete(uri string, handlers ...HandlerFunc) *Route {
	return r.Add([]string{http.MethodDelete}, uri, handlers...)
}

// Options 注册 OPTIONS 路由。
func (r *Router) Options(uri string, handlers ...HandlerFunc) *Route {
	return r.Add([]string{http.MethodOptions}, uri, handlers...)
}

// Match 为指定 HTTP method 集合注册同一个处理函数。
func (r *Router) Match(methods []string, uri string, handlers ...HandlerFunc) *Route {
	return r.Add(methods, uri, handlers...)
}

// Any 为常见 HTTP method 注册同一个处理函数。
func (r *Router) Any(uri string, handlers ...HandlerFunc) *Route {
	return r.Add([]string{
		http.MethodGet, http.MethodPost, http.MethodPut, http.MethodPatch,
		http.MethodDelete, http.MethodOptions, http.MethodHead,
	}, uri, handlers...)
}

// Redirect 注册重定向路由。
func (r *Router) Redirect(uri, destination string, status ...int) *Route {
	code := http.StatusFound
	if len(status) > 0 && status[0] > 0 {
		code = status[0]
	}
	return r.Get(uri, func(c *gin.Context) {
		c.Redirect(code, destination)
	})
}

// PermanentRedirect 注册 301 永久重定向路由。
func (r *Router) PermanentRedirect(uri, destination string) *Route {
	return r.Redirect(uri, destination, http.StatusMovedPermanently)
}

// Static 注册静态文件目录。
func (r *Router) Static(uri, root string) *Route {
	return r.Get(joinPaths(uri, "{filepath}"), func(c *gin.Context) {
		c.FileFromFS(strings.TrimPrefix(c.Param("filepath"), "/"), http.Dir(root))
	}).Where("filepath", ".*")
}

// Fallback 注册 Gin NoRoute 处理函数。
func (r *Router) Fallback(handler HandlerFunc) *Route {
	return r.Add([]string{"FALLBACK"}, "/{fallback}", handler).Where("fallback", ".*")
}

// Prefix 创建带路径前缀的路由声明器。
func (r *Router) Prefix(prefix string) *Registrar {
	return (&Registrar{router: r}).Prefix(prefix)
}

// Name 创建带命名前缀的路由声明器。
func (r *Router) Name(name string) *Registrar {
	return (&Registrar{router: r}).Name(name)
}

// Domain 创建带域名约束的路由声明器。
func (r *Router) Domain(domain string) *Registrar {
	return (&Registrar{router: r}).Domain(domain)
}

// Middleware 创建带中间件的路由声明器。
func (r *Router) Middleware(handlers ...HandlerFunc) *Registrar {
	return (&Registrar{router: r}).Middleware(handlers...)
}

// Controller 创建带控制器对象的路由声明器。
func (r *Router) Controller(controller any) *Registrar {
	return (&Registrar{router: r}).Controller(controller)
}

// Group 在当前路由器上执行一个分组闭包。
func (r *Router) Group(fn func()) {
	(&Registrar{router: r}).Group(fn)
}

// List 返回当前全部路由快照。
func (r *Router) List() []RouteInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]RouteInfo, 0, len(r.routes))
	for _, entry := range r.routes {
		for _, path := range compilePaths(entry.uri, entry.where) {
			out = append(out, entry.info(path))
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].URI == out[j].URI {
			return strings.Join(out[i].Methods, ",") < strings.Join(out[j].Methods, ",")
		}
		return out[i].URI < out[j].URI
	})
	return out
}

// URL 根据命名路由和参数生成路径。
func (r *Router) URL(name string, params map[string]any) (string, error) {
	r.mu.RLock()
	entry := r.names[name]
	r.mu.RUnlock()
	if entry == nil {
		return "", fmt.Errorf("route: named route %q not found", name)
	}
	path, err := fillURI(entry.uri, params)
	if err != nil {
		return "", err
	}
	return path, nil
}

// Mount 把当前路由表挂载到 Gin engine。
func (r *Router) Mount(engine *gin.Engine) error {
	if engine == nil {
		return fmt.Errorf("route: gin engine is nil")
	}

	r.mu.RLock()
	routes := make([]*routeEntry, 0, len(r.routes))
	for _, entry := range r.routes {
		routes = append(routes, entry.clone())
	}
	binders := make(map[string]Binder, len(r.binders))
	for key, binder := range r.binders {
		binders[key] = binder
	}
	r.mu.RUnlock()

	for _, entry := range routes {
		paths := compilePaths(entry.uri, entry.where)
		if len(entry.methods) == 1 && entry.methods[0] == "FALLBACK" {
			engine.NoRoute(entry.chain(binders)...)
			continue
		}
		for _, method := range entry.methods {
			for _, path := range paths {
				engine.Handle(method, path, entry.chain(binders)...)
			}
		}
	}
	return nil
}

// mergeGroupAttributes 按 Laravel 路由分组语义合并多层分组属性。
//
// 合并规则：
// - Prefix 使用路径拼接，避免调用方重复处理斜杠。
// - Name 直接追加，用于形成 api.admin.users.show 这类命名前缀。
// - Domain 和 Controller 使用内层覆盖外层，符合“更靠近路由的声明优先”的直觉。
// - Middleware、WithoutMiddleware 和 Where 会累加，路由注册时再统一过滤和应用。
//
// 需求背景：
// issue 06 要求移除 Router 对 goroutine ID 的依赖，因此分组属性必须成为 Registrar
// 调用链上的显式数据。本函数把“如何合并 scope”的复杂度集中在一个小入口，避免
// Router、Registrar 和测试代码各自重复拼接规则。
func mergeGroupAttributes(attrsList ...groupAttributes) groupAttributes {
	var merged groupAttributes
	for _, attrs := range attrsList {
		merged.prefix = joinPaths(merged.prefix, attrs.prefix)
		merged.name += attrs.name
		if attrs.domain != "" {
			merged.domain = attrs.domain
		}
		merged.middleware = append(merged.middleware, attrs.middleware...)
		merged.middlewareIDs = append(merged.middlewareIDs, attrs.middlewareIDs...)
		merged.withoutMiddleware = mergeStringSet(merged.withoutMiddleware, attrs.withoutMiddleware)
		merged.where = mergeStringMap(merged.where, attrs.where)
		if attrs.controller != nil {
			merged.controller = attrs.controller
		}
	}
	return merged
}

// updateRouteName 统一维护路由名称与命名索引。
//
// 逻辑说明：
// Route.Name 的公开语义是“覆盖当前路由名称后缀”，而不是继续在完整名字后面拼接。
// 这里先删除旧索引，再基于分组前缀和新后缀重建完整名字，避免 URL 仍然命中旧名称。
func (r *Router) updateRouteName(entry *routeEntry, suffix string) {
	if entry == nil {
		return
	}
	if entry.name != "" {
		delete(r.names, entry.name)
	}
	entry.nameSuffix = strings.TrimSpace(suffix)
	entry.name = entry.namePrefix + entry.nameSuffix
	r.indexName(entry)
}

func (r *Router) indexName(entry *routeEntry) {
	if entry == nil || entry.name == "" {
		return
	}
	r.names[entry.name] = entry
}

func (e *routeEntry) chain(binders map[string]Binder) []gin.HandlerFunc {
	handlers := make([]gin.HandlerFunc, 0, len(e.middleware)+4)
	handlers = append(handlers, e.domainMiddleware())
	handlers = append(handlers, e.constraintMiddleware())
	handlers = append(handlers, e.bindingMiddleware(binders))
	handlers = append(handlers, e.middleware...)
	handlers = append(handlers, e.currentRouteMiddleware())
	handlers = append(handlers, e.action)
	return handlers
}

func (e *routeEntry) domainMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		if e.domain == "" || hostMatches(e.domain, c.Request.Host) {
			c.Next()
			return
		}
		c.AbortWithStatus(http.StatusNotFound)
	}
}

func (e *routeEntry) constraintMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		for param, expr := range e.where {
			value := c.Param(param)
			if value == "" {
				continue
			}
			if !matchesConstraint(expr, value) {
				c.AbortWithStatus(http.StatusNotFound)
				return
			}
		}
		c.Next()
	}
}

func (e *routeEntry) bindingMiddleware(binders map[string]Binder) gin.HandlerFunc {
	return func(c *gin.Context) {
		for _, param := range c.Params {
			binder := binders[param.Key]
			if binder == nil {
				continue
			}
			value, err := binder(c, param.Value)
			if err != nil {
				if e.missing != nil {
					e.missing(c)
					c.Abort()
					return
				}
				c.AbortWithStatus(http.StatusNotFound)
				return
			}
			c.Set(param.Key, value)
		}
		c.Next()
	}
}

func (e *routeEntry) currentRouteMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Set("route.current", e.info(c.FullPath()))
		c.Next()
	}
}

func (e *routeEntry) info(path string) RouteInfo {
	return RouteInfo{
		Methods:    append([]string(nil), e.methods...),
		URI:        e.uri,
		GinPath:    path,
		Name:       e.name,
		Domain:     e.domain,
		Handler:    e.handlerName,
		Middleware: append([]string(nil), e.middlewareIDs...),
		SourcePath: e.sourcePath,
	}
}

func (e *routeEntry) clone() *routeEntry {
	if e == nil {
		return nil
	}
	return &routeEntry{
		methods:       append([]string(nil), e.methods...),
		uri:           e.uri,
		name:          e.name,
		namePrefix:    e.namePrefix,
		nameSuffix:    e.nameSuffix,
		domain:        e.domain,
		action:        e.action,
		middleware:    append([]HandlerFunc(nil), e.middleware...),
		middlewareIDs: append([]string(nil), e.middlewareIDs...),
		where:         mergeStringMap(e.where),
		missing:       e.missing,
		controller:    e.controller,
		handlerName:   e.handlerName,
		sourcePath:    e.sourcePath,
	}
}

func normalizeMethods(methods []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(methods))
	for _, method := range methods {
		method = strings.ToUpper(strings.TrimSpace(method))
		if method == "" {
			continue
		}
		if _, ok := seen[method]; ok {
			continue
		}
		seen[method] = struct{}{}
		out = append(out, method)
	}
	return out
}

func joinPaths(base, path string) string {
	base = strings.TrimSpace(base)
	path = strings.TrimSpace(path)
	if base == "" {
		base = "/"
	}
	if path == "" {
		path = "/"
	}
	joined := "/" + strings.Trim(strings.TrimRight(base, "/")+"/"+strings.TrimLeft(path, "/"), "/")
	if joined == "/" {
		return joined
	}
	return strings.TrimRight(joined, "/")
}

func mergeStringMap(maps ...map[string]string) map[string]string {
	out := map[string]string{}
	for _, item := range maps {
		for key, value := range item {
			if strings.TrimSpace(key) != "" && strings.TrimSpace(value) != "" {
				out[key] = value
			}
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func cloneBinders(input map[string]Binder) map[string]Binder {
	if len(input) == 0 {
		return nil
	}
	out := make(map[string]Binder, len(input))
	for key, value := range input {
		out[key] = value
	}
	return out
}

func functionName(handler HandlerFunc) string {
	if handler == nil {
		return ""
	}
	value := reflect.ValueOf(handler)
	if value.Kind() != reflect.Func {
		return ""
	}
	fn := runtime.FuncForPC(value.Pointer())
	if fn == nil {
		return ""
	}
	return fn.Name()
}

func functionFile(handler HandlerFunc) string {
	if handler == nil {
		return ""
	}
	value := reflect.ValueOf(handler)
	if value.Kind() != reflect.Func {
		return ""
	}
	fn := runtime.FuncForPC(value.Pointer())
	if fn == nil {
		return ""
	}
	file, _ := fn.FileLine(value.Pointer())
	return file
}

var namedMiddleware sync.Map

// NamedMiddleware 给 Gin 中间件附加 Laravel 风格的字符串名称。
//
// WithoutMiddleware 会优先按这里声明的名称排除中间件；未声明名称时回退到函数名。
func NamedMiddleware(name string, handler HandlerFunc) HandlerFunc {
	if handler == nil {
		return nil
	}
	if name = strings.TrimSpace(name); name != "" {
		namedMiddleware.Store(reflect.ValueOf(handler).Pointer(), name)
	}
	return handler
}

func middlewareID(handler HandlerFunc) string {
	if handler == nil {
		return ""
	}
	ptr := reflect.ValueOf(handler).Pointer()
	if name, ok := namedMiddleware.Load(ptr); ok {
		return name.(string)
	}
	return functionName(handler)
}

func mergeMiddleware(attrs groupAttributes, local []HandlerFunc) ([]HandlerFunc, []string) {
	handlers := append([]HandlerFunc{}, attrs.middleware...)
	ids := append([]string{}, attrs.middlewareIDs...)
	for _, handler := range local {
		handlers = append(handlers, handler)
		ids = append(ids, middlewareID(handler))
	}
	if len(attrs.withoutMiddleware) == 0 {
		return handlers, ids
	}
	filteredHandlers := make([]HandlerFunc, 0, len(handlers))
	filteredIDs := make([]string, 0, len(ids))
	for i, handler := range handlers {
		id := ""
		if i < len(ids) {
			id = ids[i]
		}
		if _, skip := attrs.withoutMiddleware[id]; skip {
			continue
		}
		if _, skip := attrs.withoutMiddleware[functionName(handler)]; skip {
			continue
		}
		filteredHandlers = append(filteredHandlers, handler)
		filteredIDs = append(filteredIDs, id)
	}
	return filteredHandlers, filteredIDs
}

func mergeStringSet(sets ...map[string]struct{}) map[string]struct{} {
	out := map[string]struct{}{}
	for _, set := range sets {
		for value := range set {
			if strings.TrimSpace(value) != "" {
				out[value] = struct{}{}
			}
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func fillURI(uri string, params map[string]any) (string, error) {
	path := uri
	for key, value := range params {
		replacements := []string{"{" + key + "}", "{" + key + "?}", ":" + key, "*" + key}
		escaped := url.PathEscape(fmt.Sprint(value))
		for _, pattern := range replacements {
			path = strings.ReplaceAll(path, pattern, escaped)
		}
	}
	if strings.Contains(path, "{") || strings.Contains(path, ":") || strings.Contains(path, "*") {
		return "", fmt.Errorf("route: missing parameters for %q", uri)
	}
	return path, nil
}

func hostMatches(pattern, host string) bool {
	host = strings.Split(strings.TrimSpace(host), ":")[0]
	pattern = strings.TrimSpace(pattern)
	if pattern == "" || pattern == host {
		return true
	}
	if !strings.Contains(pattern, "{") {
		return false
	}
	patternParts := strings.Split(pattern, ".")
	hostParts := strings.Split(host, ".")
	if len(patternParts) != len(hostParts) {
		return false
	}
	for i := range patternParts {
		if strings.HasPrefix(patternParts[i], "{") && strings.HasSuffix(patternParts[i], "}") {
			continue
		}
		if patternParts[i] != hostParts[i] {
			return false
		}
	}
	return true
}
