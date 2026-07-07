package route

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prismgo/framework/cache"
	"github.com/prismgo/framework/config"
	"github.com/prismgo/framework/container"
	"github.com/prismgo/framework/ratelimit"
)

func useRouteMemoryCache(t *testing.T) {
	t.Helper()
	manager, err := cache.NewManager(cache.Config{
		Default: "memory",
		Stores:  map[string]cache.StoreConfig{"memory": {Driver: "memory", CleanupInterval: time.Millisecond}},
	})
	if err != nil {
		t.Fatalf("new cache manager: %v", err)
	}
	registry := container.NewContainer()
	container.SetProvider(func() *container.Container { return registry })
	if err := registry.Instance("cache.manager", manager); err != nil {
		t.Fatalf("bind cache manager: %v", err)
	}
	if err := registry.Instance("config.default", config.New()); err != nil {
		t.Fatalf("bind config: %v", err)
	}
	limiter := ratelimit.New(manager.Default())
	if err := registry.Instance("ratelimit.default", limiter); err != nil {
		t.Fatalf("bind ratelimit: %v", err)
	}
	if err := (ServiceProvider{}).Register(providerTestApp{registry: registry}); err != nil {
		t.Fatalf("register route provider: %v", err)
	}
	t.Cleanup(func() {
		_ = manager.Close()
		container.SetProvider(nil)
	})
}

func TestRouterMountsGroupsMiddlewareConstraintsAndNamedURL(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := New()
	var calls []string
	middleware := func(c *gin.Context) {
		calls = append(calls, "middleware")
		c.Next()
	}

	router.Prefix("/api").Name("api.").Middleware(middleware).Group(func() {
		router.Get("/users/{id}", func(c *gin.Context) {
			calls = append(calls, "handler")
			if c.Param("id") != "42" {
				t.Fatalf("id = %q, want 42", c.Param("id"))
			}
			current, ok := c.Get("route.current")
			if !ok || current.(RouteInfo).Name != "api.users.show" {
				t.Fatalf("missing current route info: %#v", current)
			}
			c.String(http.StatusOK, "ok")
		}).WhereNumber("id").Name("users.show")
	})

	engine := gin.New()
	if err := router.Mount(engine); err != nil {
		t.Fatalf("Mount failed: %v", err)
	}

	w := perform(engine, http.MethodGet, "/api/users/42")
	if w.Code != http.StatusOK || w.Body.String() != "ok" {
		t.Fatalf("response = %d %q", w.Code, w.Body.String())
	}
	if len(calls) != 2 || calls[0] != "middleware" || calls[1] != "handler" {
		t.Fatalf("calls = %v", calls)
	}

	w = perform(engine, http.MethodGet, "/api/users/not-number")
	if w.Code != http.StatusNotFound {
		t.Fatalf("constraint response = %d, want 404", w.Code)
	}

	got, err := router.URL("api.users.show", map[string]any{"id": 7})
	if err != nil || got != "/api/users/7" {
		t.Fatalf("URL = %q, %v", got, err)
	}
}

func TestGroupScopeAppliesToRoutesDeclaredFromChildGoroutine(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := New()
	middleware := NamedMiddleware("group-scope", func(c *gin.Context) {
		c.Header("X-Group-Scope", "yes")
		c.Next()
	})

	// 需求背景：旧实现按 goroutine ID 保存 group stack，子 goroutine 内声明的路由
	// 无法继承外层 Prefix/Name/Middleware。这里用公开 HTTP 行为验证 scope 已显式传递。
	router.Prefix("/api").Name("api.").Middleware(middleware).Group(func() {
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			router.Get("/ping", func(c *gin.Context) {
				c.String(http.StatusOK, "pong")
			}).Name("ping")
		}()
		wg.Wait()
	})

	engine := gin.New()
	if err := router.Mount(engine); err != nil {
		t.Fatalf("Mount failed: %v", err)
	}

	w := perform(engine, http.MethodGet, "/api/ping")
	if w.Code != http.StatusOK || w.Body.String() != "pong" {
		t.Fatalf("scoped route response = %d %q, want 200 pong", w.Code, w.Body.String())
	}
	if got := w.Header().Get("X-Group-Scope"); got != "yes" {
		t.Fatalf("scoped middleware header = %q, want yes", got)
	}
	if got, err := router.URL("api.ping", nil); err != nil || got != "/api/ping" {
		t.Fatalf("scoped URL = %q, %v; want /api/ping", got, err)
	}
}

func TestRouterCloneRemainsWritableForRegistriesAndRoutes(t *testing.T) {
	gin.SetMode(gin.TestMode)

	// 需求背景：空 Router 的 Clone 曾经因为 binders/patterns 被复制成 nil map，
	// 导致后续 Bind、Pattern 或 Add 路径写入 panic。这里通过公开 HTTP 行为验证 Clone 与 New 一样可写。
	cloned := New().Clone()
	cloned.Bind("id", func(_ *gin.Context, value string) (any, error) {
		return "bound:" + value, nil
	})
	cloned.Pattern("id", `[0-9]+`)
	cloned.Get("/users/{id}", func(c *gin.Context) {
		value, _ := c.Get("id")
		c.String(http.StatusOK, value.(string))
	})
	cloned.Prefix("/api").Group(func() {
		cloned.Get("/ping", func(c *gin.Context) {
			c.String(http.StatusOK, "pong")
		})
	})

	engine := gin.New()
	if err := cloned.Mount(engine); err != nil {
		t.Fatalf("Mount failed: %v", err)
	}
	if w := perform(engine, http.MethodGet, "/users/42"); w.Code != http.StatusOK || w.Body.String() != "bound:42" {
		t.Fatalf("bound response = %d %q, want 200 bound:42", w.Code, w.Body.String())
	}
	if w := perform(engine, http.MethodGet, "/users/nope"); w.Code != http.StatusNotFound {
		t.Fatalf("pattern response = %d, want 404", w.Code)
	}
	if w := perform(engine, http.MethodGet, "/api/ping"); w.Code != http.StatusOK || w.Body.String() != "pong" {
		t.Fatalf("group response = %d %q, want 200 pong", w.Code, w.Body.String())
	}
}

func TestNilRouterCloneRemainsWritable(t *testing.T) {
	gin.SetMode(gin.TestMode)

	var router *Router
	cloned := router.Clone()
	cloned.Bind("slug", func(_ *gin.Context, value string) (any, error) {
		return strings.ToUpper(value), nil
	})
	cloned.Pattern("slug", `[a-z]+`)
	cloned.Get("/posts/{slug}", func(c *gin.Context) {
		value, _ := c.Get("slug")
		c.String(http.StatusOK, value.(string))
	})

	engine := gin.New()
	if err := cloned.Mount(engine); err != nil {
		t.Fatalf("Mount failed: %v", err)
	}
	if w := perform(engine, http.MethodGet, "/posts/abc"); w.Code != http.StatusOK || w.Body.String() != "ABC" {
		t.Fatalf("nil clone response = %d %q, want 200 ABC", w.Code, w.Body.String())
	}
}

func TestNestedGroupsMergePrefixNameMiddlewareWhereAndController(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := New()
	controller := &testController{}
	outer := NamedMiddleware("outer", func(c *gin.Context) {
		c.Header("X-Outer", "yes")
		c.Next()
	})
	inner := NamedMiddleware("inner", func(c *gin.Context) {
		c.Header("X-Inner", "yes")
		c.Next()
	})

	// 设计说明：这个用例不检查内部结构，只通过 URL、响应头和 404 约束结果确认
	// Prefix、Name、Middleware、Where、Controller 的嵌套合并语义保持兼容。
	router.Prefix("/api").Name("api.").Middleware(outer).Where("slug", `[a-z]+`).Controller(controller).Group(func() {
		router.Prefix("/admin").Name("admin.").Middleware(inner).Group(func() {
			router.Prefix("").Action(http.MethodGet, "/posts/{slug}", "Show").Name("posts.show")
		})
	})

	engine := gin.New()
	if err := router.Mount(engine); err != nil {
		t.Fatalf("Mount failed: %v", err)
	}

	w := perform(engine, http.MethodGet, "/api/admin/posts/abc")
	if w.Code != http.StatusOK || w.Body.String() != "show:abc" {
		t.Fatalf("nested group response = %d %q, want 200 show:abc", w.Code, w.Body.String())
	}
	if w.Header().Get("X-Outer") != "yes" || w.Header().Get("X-Inner") != "yes" {
		t.Fatalf("nested middleware headers = outer:%q inner:%q", w.Header().Get("X-Outer"), w.Header().Get("X-Inner"))
	}
	if w := perform(engine, http.MethodGet, "/api/admin/posts/123"); w.Code != http.StatusNotFound {
		t.Fatalf("nested where response = %d, want 404", w.Code)
	}
	if got, err := router.URL("api.admin.posts.show", map[string]any{"slug": "abc"}); err != nil || got != "/api/admin/posts/abc" {
		t.Fatalf("nested URL = %q, %v; want /api/admin/posts/abc", got, err)
	}
}

func TestConcurrentRouterGroupScopesDoNotLeakBetweenInstances(t *testing.T) {
	gin.SetMode(gin.TestMode)
	const count = 24

	// 逻辑说明：并发构建多个独立 Router，每个 Router 使用不同前缀和命名前缀。
	// 如果 scope registry 没有按 Router 隔离，这里会出现路径或命名路由串扰。
	routers := make([]*Router, count)
	var wg sync.WaitGroup
	wg.Add(count)
	for i := 0; i < count; i++ {
		i := i
		go func() {
			defer wg.Done()
			router := New()
			routers[i] = router
			router.Prefix("/r" + strconv.Itoa(i)).Name("r" + strconv.Itoa(i) + ".").Group(func() {
				router.Get("/ping", func(c *gin.Context) {
					c.String(http.StatusOK, strconv.Itoa(i))
				}).Name("ping")
			})
		}()
	}
	wg.Wait()

	for i, router := range routers {
		if router == nil {
			t.Fatalf("router %d was not built", i)
		}
		engine := gin.New()
		if err := router.Mount(engine); err != nil {
			t.Fatalf("Mount router %d failed: %v", i, err)
		}
		path := "/r" + strconv.Itoa(i) + "/ping"
		w := perform(engine, http.MethodGet, path)
		if w.Code != http.StatusOK || w.Body.String() != strconv.Itoa(i) {
			t.Fatalf("router %d response = %d %q at %s", i, w.Code, w.Body.String(), path)
		}
		if w := perform(engine, http.MethodGet, "/r0/ping"); i != 0 && w.Code != http.StatusNotFound {
			t.Fatalf("router %d leaked /r0 scope: %d %q", i, w.Code, w.Body.String())
		}
		if got, err := router.URL("r"+strconv.Itoa(i)+".ping", nil); err != nil || got != path {
			t.Fatalf("router %d URL = %q, %v; want %s", i, got, err, path)
		}
	}
}

func TestRegistrarMethodsRouteMiddlewareAndFacadeHelpers(t *testing.T) {
	gin.SetMode(gin.TestMode)
	_ = setupTestContainer(t)

	router := New()
	base := NamedMiddleware("base", func(c *gin.Context) {
		c.Header("X-Base", "yes")
		c.Next()
	})
	extra := NamedMiddleware("extra", func(c *gin.Context) {
		c.Header("X-Extra", "yes")
		c.Next()
	})
	skipped := NamedMiddleware("skip-route", func(c *gin.Context) {
		c.Header("X-Skip-Route", "no")
		c.Next()
	})

	group := router.Prefix("/ops").Middleware(base).ScopeBindings().WithoutScopedBindings()
	group.Post("/post", text("post"))
	group.Put("/put", text("put"))
	group.Patch("/patch", text("patch"))
	group.Delete("/delete", text("delete"))
	group.Options("/options", text("options"))
	group.Match([]string{http.MethodGet, http.MethodHead}, "/match", text("match"))
	group.Any("/any", text("any"))
	group.Redirect("/redirect", "/target", http.StatusTemporaryRedirect)
	group.PermanentRedirect("/permanent", "/target")
	group.Get("/with-extra", text("with-extra")).Middleware(extra)
	group.Middleware(skipped).Get("/without-route", text("without-route")).WithoutMiddleware("skip-route")

	engine := gin.New()
	if err := router.Mount(engine); err != nil {
		t.Fatalf("Mount failed: %v", err)
	}

	assertBody(t, engine, http.MethodPost, "/ops/post", http.StatusOK, "post")
	assertBody(t, engine, http.MethodPut, "/ops/put", http.StatusOK, "put")
	assertBody(t, engine, http.MethodPatch, "/ops/patch", http.StatusOK, "patch")
	assertBody(t, engine, http.MethodDelete, "/ops/delete", http.StatusOK, "delete")
	assertBody(t, engine, http.MethodOptions, "/ops/options", http.StatusOK, "options")
	assertBody(t, engine, http.MethodGet, "/ops/match", http.StatusOK, "match")
	assertBody(t, engine, http.MethodGet, "/ops/any", http.StatusOK, "any")
	assertRouteMethods(t, router.List(), "/ops/any", http.MethodGet, http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete, http.MethodOptions, http.MethodHead)
	if w := perform(engine, http.MethodGet, "/ops/redirect"); w.Code != http.StatusTemporaryRedirect || w.Header().Get("Location") != "/target" {
		t.Fatalf("redirect = %d %q", w.Code, w.Header().Get("Location"))
	}
	if w := perform(engine, http.MethodGet, "/ops/permanent"); w.Code != http.StatusMovedPermanently || w.Header().Get("Location") != "/target" {
		t.Fatalf("permanent redirect = %d %q", w.Code, w.Header().Get("Location"))
	}
	if w := perform(engine, http.MethodGet, "/ops/with-extra"); w.Header().Get("X-Base") != "yes" || w.Header().Get("X-Extra") != "yes" {
		t.Fatalf("route middleware headers = base:%q extra:%q", w.Header().Get("X-Base"), w.Header().Get("X-Extra"))
	}
	if w := perform(engine, http.MethodGet, "/ops/without-route"); w.Header().Get("X-Skip-Route") != "" || w.Body.String() != "without-route" {
		t.Fatalf("without route middleware response = %d %q header=%q", w.Code, w.Body.String(), w.Header().Get("X-Skip-Route"))
	}

	c2 := container.NewContainer()
	container.SetProvider(func() *container.Container { return c2 })
	if err := (ServiceProvider{}).Register(providerTestApp{registry: c2}); err != nil {
		t.Fatalf("register route provider in c2: %v", err)
	}
	Middleware(skipped).Group(func() {
		WithoutMiddleware("skip-route").Get("/facade/without", text("facade-without"))
	})
	Group(func() {
		Get("/facade/group", text("facade-group"))
	})
	Resource("facade/widgets", &fullResourceController{}, Only("index"))
	ApiResource("facade/things", &testResourceController{}, Only("index"))
	ApiResources(map[string]ResourceController{"facade/photos": &testResourceController{}}, Only("index"))

	facadeEngine := gin.New()
	if err := Mount(facadeEngine); err != nil {
		t.Fatalf("facade Mount failed: %v", err)
	}
	assertBody(t, facadeEngine, http.MethodGet, "/facade/without", http.StatusOK, "facade-without")
	if w := perform(facadeEngine, http.MethodGet, "/facade/without"); w.Header().Get("X-Skip-Route") != "" {
		t.Fatalf("facade without middleware header=%q", w.Header().Get("X-Skip-Route"))
	}
	assertBody(t, facadeEngine, http.MethodGet, "/facade/group", http.StatusOK, "facade-group")
	assertBody(t, facadeEngine, http.MethodGet, "/facade/widgets", http.StatusOK, "full-index")
	assertBody(t, facadeEngine, http.MethodGet, "/facade/things", http.StatusOK, "index")
	assertBody(t, facadeEngine, http.MethodGet, "/facade/photos", http.StatusOK, "index")
}

func TestRouteNameOverridesRouteSuffixAndRefreshesNamedIndex(t *testing.T) {
	gin.SetMode(gin.TestMode)

	// 需求背景：Route.Name 需要支持重复调用覆盖当前路由后缀，同时保留分组前缀，
	// 并且旧名字必须从 URL 索引中移除，避免命名路由解析仍然命中历史值。
	router := New()

	var routeRef *Route
	router.Name("api.").Group(func() {
		routeRef = router.Get("/users/{id}", func(c *gin.Context) {
			c.String(http.StatusOK, c.Param("id"))
		})
	})
	if routeRef == nil {
		t.Fatal("expected route to be created")
	}

	routeRef.Name("users.show")
	routeRef.Name("users.detail")

	path, err := router.URL("api.users.detail", map[string]any{"id": 7})
	if err != nil || path != "/users/7" {
		t.Fatalf("URL(api.users.detail) = %q, %v, want /users/7", path, err)
	}
	if _, err := router.URL("api.users.show", map[string]any{"id": 7}); err == nil {
		t.Fatal("expected old named route to be removed from index")
	}

	list := router.List()
	if len(list) != 1 || list[0].Name != "api.users.detail" {
		t.Fatalf("route list = %#v, want only api.users.detail", list)
	}
}

func TestActionPanicsWithReadableMessageWhenControllerSignatureIsInvalid(t *testing.T) {
	gin.SetMode(gin.TestMode)

	// 设计说明：Action 保持启动期 fail-fast 边界不变，但错误信息必须清晰表达
	// route 包自己的签名契约，而不是把 Go 原始类型断言错误直接暴露给调用方。
	router := New()
	controller := &invalidActionController{}

	assertPanicsWith(t, "must have signature func(*gin.Context)", func() {
		router.Controller(controller).Action(http.MethodGet, "/invalid", "Show")
	})
}

func TestWherePanicsWithInvalidRegex(t *testing.T) {
	gin.SetMode(gin.TestMode)

	// 需求背景：无效正则应在 Where 设置约束时立即报错，而不是静默导致路由永远 404。
	router := New()
	route := router.Get("/test", func(c *gin.Context) {})

	assertPanicsWith(t, "invalid constraint", func() {
		route.Where("id", "[invalid")
	})
}

func TestConstraintMiddlewareUsesPrecompiledRegex(t *testing.T) {
	gin.SetMode(gin.TestMode)

	// 需求背景：约束正则应在路由注册时预编译，避免每次请求重新编译（ReDoS 风险 + 性能问题）。
	router := New()
	router.Get("/users/{id}", func(c *gin.Context) {
		c.String(http.StatusOK, "ok:"+c.Param("id"))
	}).Where("id", `^\d+$`)

	engine := gin.New()
	if err := router.Mount(engine); err != nil {
		t.Fatalf("Mount failed: %v", err)
	}

	// 有效参数应匹配
	w := perform(engine, http.MethodGet, "/users/123")
	if w.Code != http.StatusOK || w.Body.String() != "ok:123" {
		t.Fatalf("valid param response = %d %q, want 200 ok:123", w.Code, w.Body.String())
	}

	// 无效参数应 404
	w = perform(engine, http.MethodGet, "/users/abc")
	if w.Code != http.StatusNotFound {
		t.Fatalf("invalid param response = %d, want 404", w.Code)
	}
}

func TestCompilePathsOptionalParametersOnlyOmitFromTrailing(t *testing.T) {
	gin.SetMode(gin.TestMode)

	// 需求背景：多个可选参数应仅支持尾部省略，不应生成跳过中间参数的组合路径。
	// 例如 /a/{b?}/{c?} 应生成 /a、/a/:b、/a/:b/:c，而不应生成 /a/:c。
	paths := compilePaths("/a/{b?}/{c?}", nil)
	expected := map[string]bool{
		"/a":       true,
		"/a/:b":    true,
		"/a/:b/:c": true,
	}
	if len(paths) != len(expected) {
		t.Fatalf("compilePaths generated %d paths, want %d: %v", len(paths), len(expected), paths)
	}
	for _, path := range paths {
		if !expected[path] {
			t.Fatalf("unexpected path %q, expected one of %v", path, expected)
		}
	}
}

func TestRouteEntryCachesCompiledPaths(t *testing.T) {
	gin.SetMode(gin.TestMode)

	// 需求背景：compilePaths 在 List() 和 Mount() 中会被重复调用，但路由的 URI 和约束
	// 在注册后不会改变。缓存编译结果可以避免重复计算，提升性能。
	router := New()
	router.Get("/users/{id}", func(c *gin.Context) {
		c.String(http.StatusOK, c.Param("id"))
	}).WhereNumber("id")

	// 第一次调用 List() 应该触发 compilePaths 并缓存
	list1 := router.List()
	if len(list1) != 1 {
		t.Fatalf("expected 1 route, got %d", len(list1))
	}

	// 第二次调用 List() 应该使用缓存的结果
	list2 := router.List()
	if len(list2) != 1 {
		t.Fatalf("expected 1 route, got %d", len(list2))
	}

	// 验证两次返回的路径信息一致
	if list1[0].GinPath != list2[0].GinPath {
		t.Fatalf("cached paths mismatch: %q vs %q", list1[0].GinPath, list2[0].GinPath)
	}
}

func TestOptionalWildcardRedirectFallbackAndBinding(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := New()
	router.Bind("user", func(_ *gin.Context, value string) (any, error) {
		if value == "missing" {
			return nil, errMissingForTest{}
		}
		return "bound:" + value, nil
	})
	router.Get("/optional/{id?}", func(c *gin.Context) {
		c.String(http.StatusOK, c.Param("id"))
	})
	router.Get("/files/{path}", func(c *gin.Context) {
		c.String(http.StatusOK, c.Param("path"))
	}).Where("path", ".*")
	router.Get("/users/{user}", func(c *gin.Context) {
		value, ok := c.Get("user")
		if !ok {
			t.Fatalf("binding missing, params=%v", c.Params)
		}
		c.String(http.StatusOK, value.(string))
	}).Missing(func(c *gin.Context) {
		c.String(http.StatusGone, "missing")
	})
	router.Redirect("/old", "/new")
	router.Fallback(func(c *gin.Context) {
		c.String(http.StatusNotFound, "fallback")
	})

	engine := gin.New()
	if err := router.Mount(engine); err != nil {
		t.Fatalf("Mount failed: %v", err)
	}

	cases := []struct {
		path string
		code int
		body string
	}{
		{"/optional", http.StatusOK, ""},
		{"/optional/9", http.StatusOK, "9"},
		{"/files/a/b/c.txt", http.StatusOK, "/a/b/c.txt"},
		{"/users/1", http.StatusOK, "bound:1"},
		{"/users/missing", http.StatusGone, "missing"},
		{"/none", http.StatusNotFound, "fallback"},
	}
	for _, tc := range cases {
		w := perform(engine, http.MethodGet, tc.path)
		if w.Code != tc.code || w.Body.String() != tc.body {
			t.Fatalf("%s = %d %q, want %d %q", tc.path, w.Code, w.Body.String(), tc.code, tc.body)
		}
	}

	w := perform(engine, http.MethodGet, "/old")
	if w.Code != http.StatusFound || w.Header().Get("Location") != "/new" {
		t.Fatalf("redirect = %d %q", w.Code, w.Header().Get("Location"))
	}
}

func TestResourceAndList(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := New()
	controller := &testResourceController{}
	routes := router.ApiResource("photos", controller, Only("index", "show"))
	if len(routes) != 2 {
		t.Fatalf("routes len = %d, want 2", len(routes))
	}
	list := router.List()
	if len(list) != 2 {
		t.Fatalf("list len = %d, want 2", len(list))
	}
	if list[0].Name == "" || list[1].Name == "" {
		t.Fatalf("resource routes should be named: %#v", list)
	}

	engine := gin.New()
	if err := router.Mount(engine); err != nil {
		t.Fatalf("Mount failed: %v", err)
	}
	if w := perform(engine, http.MethodGet, "/photos"); w.Body.String() != "index" {
		t.Fatalf("index response = %d %q", w.Code, w.Body.String())
	}
	if w := perform(engine, http.MethodGet, "/photos/5"); w.Body.String() != "show:5" {
		t.Fatalf("show response = %d %q", w.Code, w.Body.String())
	}
	if w := perform(engine, http.MethodPost, "/photos"); w.Code != http.StatusNotFound {
		t.Fatalf("post response = %d, want 404", w.Code)
	}
}

func TestDomainActionPatternStaticAndThrottle(t *testing.T) {
	gin.SetMode(gin.TestMode)
	_ = setupTestContainer(t)
	useRouteMemoryCache(t)

	RateLimiter("once", func(c *gin.Context) []Limit {
		return []Limit{PerMinute(1).By(func(c *gin.Context) string { return "test-user" })}
	})
	controller := &testController{}
	Pattern("slug", `^[a-z]+$`)
	Domain("api.example.test").Controller(controller).Middleware(Throttle("once")).Group(func() {
		Controller(controller).Action(http.MethodGet, "/posts/{slug}", "Show")
	})

	engine := gin.New()
	if err := Mount(engine); err != nil {
		t.Fatalf("Mount failed: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/posts/abc", nil)
	req.Host = "api.example.test"
	w := httptest.NewRecorder()
	engine.ServeHTTP(w, req)
	if w.Code != http.StatusOK || w.Body.String() != "show:abc" {
		t.Fatalf("domain response = %d %q", w.Code, w.Body.String())
	}

	w = httptest.NewRecorder()
	engine.ServeHTTP(w, req)
	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("throttle response = %d, want 429", w.Code)
	}

	req = httptest.NewRequest(http.MethodGet, "/posts/abc", nil)
	req.Host = "other.example.test"
	w = httptest.NewRecorder()
	engine.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("domain miss = %d, want 404", w.Code)
	}

	_ = time.Second // keep time imported if cache implementation changes test timing.
}

func perform(engine *gin.Engine, method, path string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, nil)
	w := httptest.NewRecorder()
	engine.ServeHTTP(w, req)
	return w
}

func assertRouteMethods(t *testing.T, routes []RouteInfo, uri string, methods ...string) {
	t.Helper()
	for _, route := range routes {
		if route.URI != uri {
			continue
		}
		if fmt.Sprint(route.Methods) != fmt.Sprint(methods) {
			t.Fatalf("route %s methods = %v, want %v", uri, route.Methods, methods)
		}
		return
	}
	t.Fatalf("route %s not found in list", uri)
}

func assertPanicsWith(t *testing.T, want string, fn func()) {
	t.Helper()
	defer func() {
		recovered := recover()
		if recovered == nil {
			t.Fatalf("expected panic containing %q", want)
		}
		if !strings.Contains(fmt.Sprint(recovered), want) {
			t.Fatalf("panic = %q, want substring %q", fmt.Sprint(recovered), want)
		}
	}()
	fn()
}

type errMissingForTest struct{}

func (errMissingForTest) Error() string { return "missing" }

type testResourceController struct{}

func (c *testResourceController) Index(ctx *gin.Context) { ctx.String(http.StatusOK, "index") }
func (c *testResourceController) Store(ctx *gin.Context) { ctx.String(http.StatusOK, "store") }
func (c *testResourceController) Show(ctx *gin.Context) {
	ctx.String(http.StatusOK, "show:"+ctx.Param("photo"))
}
func (c *testResourceController) Update(ctx *gin.Context)  { ctx.String(http.StatusOK, "update") }
func (c *testResourceController) Destroy(ctx *gin.Context) { ctx.String(http.StatusOK, "destroy") }

type testController struct{}

func (c *testController) Show(ctx *gin.Context) {
	ctx.String(http.StatusOK, "show:"+ctx.Param("slug"))
}

type invalidActionController struct{}

func (c *invalidActionController) Show(ctx *gin.Context, id string) {
	ctx.String(http.StatusOK, id)
}

func TestFillURIRemovesUnfilledOptionalParameters(t *testing.T) {
	gin.SetMode(gin.TestMode)

	// 需求背景：High #2 - fillURI 可选参数未提供时应移除占位符，而非返回错误。
	router := New()
	router.Get("/users/{id?}", func(c *gin.Context) {
		c.String(http.StatusOK, "user:"+c.Param("id"))
	}).Name("users.show")

	// 不提供可选参数应生成 /users
	path, err := router.URL("users.show", nil)
	if err != nil {
		t.Fatalf("URL with nil params should not error: %v", err)
	}
	if path != "/users" {
		t.Fatalf("URL = %q, want /users", path)
	}

	// 提供可选参数应生成 /users/42
	path, err = router.URL("users.show", map[string]any{"id": 42})
	if err != nil {
		t.Fatalf("URL with id should not error: %v", err)
	}
	if path != "/users/42" {
		t.Fatalf("URL = %q, want /users/42", path)
	}
}

func TestFillURIHandlesParameterNamePrefixAmbiguity(t *testing.T) {
	gin.SetMode(gin.TestMode)

	// 需求背景：Medium #1 - 参数名前缀歧义。当参数名是另一个参数名的前缀时（如 id 和 id_extra），
	// 替换 :id 不应破坏 :id_extra。
	router := New()
	router.Get("/users/:id/posts/:id_extra", func(c *gin.Context) {
		c.String(http.StatusOK, c.Param("id")+":"+c.Param("id_extra"))
	}).Name("users.posts")

	// 只提供 id 参数，id_extra 应保持不变（但会报错因为缺少必填参数）
	_, err := router.URL("users.posts", map[string]any{"id": "123"})
	if err == nil {
		t.Fatal("URL should error when required parameter id_extra is missing")
	}

	// 提供两个参数应正确替换
	path, err := router.URL("users.posts", map[string]any{"id": "123", "id_extra": "abc"})
	if err != nil {
		t.Fatalf("URL should not error: %v", err)
	}
	if path != "/users/123/posts/abc" {
		t.Fatalf("URL = %q, want /users/123/posts/abc", path)
	}
}

func TestFillURIAllowsLiteralColonsInPath(t *testing.T) {
	gin.SetMode(gin.TestMode)

	// 需求背景：Low #5 - fillURI 冒号检查过于保守。URI 中可能包含字面冒号（如 /api/v1:batch），
	// 不应误报为缺少参数。
	router := New()
	router.Get("/api/v1:batch", func(c *gin.Context) {
		c.String(http.StatusOK, "batch")
	}).Name("api.batch")

	// 路径包含字面冒号，但所有参数已提供（此例无参数），不应报错
	path, err := router.URL("api.batch", nil)
	if err != nil {
		t.Fatalf("URL should not error for literal colon: %v", err)
	}
	if path != "/api/v1:batch" {
		t.Fatalf("URL = %q, want /api/v1:batch", path)
	}
}

func TestStaticRouteBlocksPathTraversal(t *testing.T) {
	gin.SetMode(gin.TestMode)

	// 需求背景：Medium #3 - Static 路由应显式验证路径不包含 ..，防止路径穿越攻击。
	router := New()
	router.Static("/assets", "./testdata")

	engine := gin.New()
	if err := router.Mount(engine); err != nil {
		t.Fatalf("Mount failed: %v", err)
	}

	tests := []struct {
		name string
		path string
		want int
	}{
		{
			name: "plain path traversal",
			path: "/assets/../../../etc/passwd",
			want: http.StatusNotFound,
		},
		{
			name: "URL encoded path traversal",
			path: "/assets/%2e%2e/%2e%2e/%2e%2e/etc/passwd",
			want: http.StatusNotFound,
		},
		{
			name: "mixed encoding traversal",
			path: "/assets/..%2f..%2f..%2fetc/passwd",
			want: http.StatusNotFound,
		},
		{
			name: "double encoded traversal",
			path: "/assets/%252e%252e/%252e%252e/etc/passwd",
			want: http.StatusNotFound,
		},
		{
			name: "valid filename with dots",
			path: "/assets/file..name.txt",
			want: http.StatusNotFound, // 文件不存在但不应被路径穿越检查阻止
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := perform(engine, http.MethodGet, tt.path)
			if w.Code != tt.want {
				t.Errorf("path %q: got %d, want %d", tt.path, w.Code, tt.want)
			}
		})
	}
}

func TestRouterWithoutMiddlewareMethodExists(t *testing.T) {
	gin.SetMode(gin.TestMode)

	// 需求背景：Low #6 - Router 应提供 WithoutMiddleware 方法以保持与 facade API 一致。
	router := New()

	// Router 应有 WithoutMiddleware 方法
	router.WithoutMiddleware("auth").Get("/test", func(c *gin.Context) {
		c.String(http.StatusOK, "ok")
	})

	engine := gin.New()
	if err := router.Mount(engine); err != nil {
		t.Fatalf("Mount failed: %v", err)
	}

	w := perform(engine, http.MethodGet, "/test")
	if w.Code != http.StatusOK || w.Body.String() != "ok" {
		t.Fatalf("response = %d %q, want 200 ok", w.Code, w.Body.String())
	}
}

func TestRouterPatternPanicsWithInvalidRegex(t *testing.T) {
	gin.SetMode(gin.TestMode)

	// 需求背景：Medium #3 - Router.Pattern() 应与 Route.Where() 保持一致的 fail-fast 语义，
	// 在注册无效正则时立即 panic，而非静默忽略导致运行时约束不生效。
	router := New()

	assertPanicsWith(t, "invalid pattern", func() {
		router.Pattern("id", "[invalid")
	})
}

func TestHostMatchesEdgeCases(t *testing.T) {
	gin.SetMode(gin.TestMode)

	// 需求背景：Low #7 - hostMatches 函数的多个边界条件缺少测试覆盖。
	tests := []struct {
		name    string
		pattern string
		host    string
		want    bool
	}{
		{"empty host", "api.example.com", "", false},
		{"empty pattern", "", "api.example.com", true},
		{"exact match", "api.example.com", "api.example.com", true},
		{"single wildcard", "{sub}.example.com", "api.example.com", true},
		{"multiple wildcards", "{a}.{b}.com", "x.y.com", true},
		{"segment count mismatch", "api.example.com", "example.com", false},
		{"static segment mismatch", "api.example.com", "web.example.com", false},
		{"host with port", "api.example.com", "api.example.com:8080", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := hostMatches(tt.pattern, tt.host)
			if got != tt.want {
				t.Errorf("hostMatches(%q, %q) = %v, want %v", tt.pattern, tt.host, got, tt.want)
			}
		})
	}
}

func TestURLWithNilParams(t *testing.T) {
	gin.SetMode(gin.TestMode)

	// 需求背景：Low #7 - Router.URL 方法对 params 为 nil 时的行为缺少测试覆盖。
	router := New()
	router.Get("/users", func(c *gin.Context) {
		c.String(http.StatusOK, "users")
	}).Name("users.index")

	// params 为 nil 时应正常生成路径
	path, err := router.URL("users.index", nil)
	if err != nil {
		t.Fatalf("URL with nil params should not error: %v", err)
	}
	if path != "/users" {
		t.Fatalf("URL = %q, want /users", path)
	}
}

func TestRemoveOptionalPlaceholders(t *testing.T) {
	gin.SetMode(gin.TestMode)

	// 需求背景：Medium #1 - removeOptionalPlaceholders 在处理路径中间或开头的可选参数时
	// 会产生双斜杠（如 "/posts" 变成 "//posts"）。虽然 Laravel 只支持尾部可选参数，
	// 但此函数应能正确处理各种边界情况以增强健壮性。
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"trailing optional", "/users/{id?}", "/users"},
		{"multiple trailing optional", "/users/{id?}/{name?}", "/users"},
		{"middle optional produces double slash", "/{lang?}/posts", "/posts"},
		{"leading optional produces double slash", "/{lang?}/users/{id?}", "/users"},
		{"root path", "/{lang?}", "/"},
		{"no optional", "/users/{id}", "/users/{id}"},
		{"empty path", "", ""},
		{"root only", "/", "/"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := removeOptionalPlaceholders(tt.input)
			if got != tt.want {
				t.Errorf("removeOptionalPlaceholders(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestReplaceParamPrefix(t *testing.T) {
	gin.SetMode(gin.TestMode)

	// 需求背景：Medium #4 - replaceParamPrefix 是核心逻辑函数，需要直接测试其边界条件，
	// 特别是参数名前缀歧义场景（如 :id vs :id_extra）。
	tests := []struct {
		name   string
		path   string
		prefix string
		key    string
		value  string
		want   string
	}{
		{
			name:   "exact match",
			path:   "/users/:id",
			prefix: ":",
			key:    "id",
			value:  "123",
			want:   "/users/123",
		},
		{
			name:   "prefix of longer name",
			path:   "/users/:id/posts/:id_extra",
			prefix: ":",
			key:    "id",
			value:  "123",
			want:   "/users/123/posts/:id_extra",
		},
		{
			name:   "longer name not affected",
			path:   "/users/:id/posts/:id_extra",
			prefix: ":",
			key:    "id_extra",
			value:  "456",
			want:   "/users/:id/posts/456",
		},
		{
			name:   "no match",
			path:   "/users/:name",
			prefix: ":",
			key:    "id",
			value:  "123",
			want:   "/users/:name",
		},
		{
			name:   "multiple occurrences",
			path:   "/users/:id/posts/:id",
			prefix: ":",
			key:    "id",
			value:  "123",
			want:   "/users/123/posts/123",
		},
		{
			name:   "wildcard prefix",
			path:   "/files/*path",
			prefix: "*",
			key:    "path",
			value:  "test.txt",
			want:   "/files/test.txt",
		},
		{
			name:   "prefix at end of path",
			path:   "/users/:id",
			prefix: ":",
			key:    "id",
			value:  "456",
			want:   "/users/456",
		},
		{
			name:   "prefix followed by slash",
			path:   "/users/:id/posts",
			prefix: ":",
			key:    "id",
			value:  "789",
			want:   "/users/789/posts",
		},
		{
			name:   "similar prefix with underscore",
			path:   "/api/:id_v2/users/:id",
			prefix: ":",
			key:    "id",
			value:  "100",
			want:   "/api/:id_v2/users/100",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := replaceParamPrefix(tt.path, tt.prefix, tt.key, tt.value)
			if got != tt.want {
				t.Errorf("replaceParamPrefix(%q, %q, %q, %q) = %q, want %q",
					tt.path, tt.prefix, tt.key, tt.value, got, tt.want)
			}
		})
	}
}
