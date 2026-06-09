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
