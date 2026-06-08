package route

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/prismgo/framework/container"
)

func setupTestContainer(t *testing.T) *container.Container {
	t.Helper()
	c := container.NewContainer()
	container.SetProvider(func() *container.Container { return c })
	t.Cleanup(func() { container.SetProvider(nil) })
	if err := (ServiceProvider{}).Register(providerTestApp{registry: c}); err != nil {
		t.Fatalf("register route service provider: %v", err)
	}
	return c
}

func TestResolveReturnsRouterFromContainer(t *testing.T) {
	gin.SetMode(gin.TestMode)
	_ = setupTestContainer(t)

	router := Resolve()
	if router == nil {
		t.Fatal("Resolve returned nil")
	}

	router.Get("/test", text("ok"))
	engine := gin.New()
	if err := router.Mount(engine); err != nil {
		t.Fatalf("Mount failed: %v", err)
	}
	assertBody(t, engine, http.MethodGet, "/test", http.StatusOK, "ok")
}

func TestResolveReturnsNilWhenNoContainer(t *testing.T) {
	container.SetProvider(nil)

	router := Resolve()
	if router != nil {
		t.Fatalf("Resolve returned %#v, want nil", router)
	}
}

func TestFacadeRegistersHTTPMethodsAndHelpers(t *testing.T) {
	gin.SetMode(gin.TestMode)
	_ = setupTestContainer(t)

	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "asset.txt"), []byte("asset"), 0o644); err != nil {
		t.Fatalf("write static file failed: %v", err)
	}

	Bind("bound", func(_ *gin.Context, value string) (any, error) { return "bind:" + value, nil })
	Model("model", func(_ *gin.Context, value string) (any, error) { return "model:" + value, nil })
	Pattern("alpha", `^[a-z]+$`)

	mw := func(c *gin.Context) {
		c.Header("X-Middleware", "yes")
		c.Next()
	}
	skipped := NamedMiddleware("skip-me", func(c *gin.Context) {
		c.Header("X-Skipped", "no")
		c.Next()
	})
	Middleware(mw).Group(func() {
		Get("/get/{alpha}", text("get"))
		Post("/post", text("post"))
		Put("/put", text("put"))
		Patch("/patch", text("patch"))
		Delete("/delete", text("delete"))
		Options("/options", text("options"))
		Match([]string{http.MethodHead}, "/head", text("head"))
		Any("/any", text("any"))
	})
	Middleware(skipped).WithoutMiddleware("skip-me").Get("/without", text("without")).ScopeBindings().WithoutScopedBindings()
	Get("/bind/{bound}", func(c *gin.Context) {
		value, _ := c.Get("bound")
		c.String(http.StatusOK, value.(string))
	})
	Get("/model/{model}", func(c *gin.Context) {
		value, _ := c.Get("model")
		c.String(http.StatusOK, value.(string))
	})
	Name("named.").Group(func() {
		Get("/named/{id}", text("named")).Name("show")
	})
	Prefix("/v1").Group(func() {
		Get("/ping", text("pong"))
	})
	Redirect("/temp", "/target", http.StatusTemporaryRedirect)
	PermanentRedirect("/perm", "/target")
	Static("/static", dir)
	Fallback(text("fallback"))

	engine := gin.New()
	if err := Mount(engine); err != nil {
		t.Fatalf("Mount failed: %v", err)
	}

	assertBody(t, engine, http.MethodGet, "/get/abc", http.StatusOK, "get")
	assertBody(t, engine, http.MethodPost, "/post", http.StatusOK, "post")
	assertBody(t, engine, http.MethodPut, "/put", http.StatusOK, "put")
	assertBody(t, engine, http.MethodPatch, "/patch", http.StatusOK, "patch")
	assertBody(t, engine, http.MethodDelete, "/delete", http.StatusOK, "delete")
	assertBody(t, engine, http.MethodOptions, "/options", http.StatusOK, "options")
	assertBody(t, engine, http.MethodGet, "/bind/42", http.StatusOK, "bind:42")
	assertBody(t, engine, http.MethodGet, "/model/9", http.StatusOK, "model:9")
	if w := perform(engine, http.MethodGet, "/without"); w.Header().Get("X-Skipped") != "" || w.Body.String() != "without" {
		t.Fatalf("without middleware response = %d %q header=%q", w.Code, w.Body.String(), w.Header().Get("X-Skipped"))
	}
	assertBody(t, engine, http.MethodGet, "/v1/ping", http.StatusOK, "pong")
	assertBody(t, engine, http.MethodGet, "/static/asset.txt", http.StatusOK, "asset")
	assertBody(t, engine, http.MethodGet, "/missing", http.StatusOK, "fallback")
	if w := perform(engine, http.MethodGet, "/get/123"); w.Code != http.StatusNotFound {
		t.Fatalf("global pattern response = %d, want 404", w.Code)
	}
	if got, err := URL("named.show", map[string]any{"id": 5}); err != nil || got != "/named/5" {
		t.Fatalf("URL = %q, %v", got, err)
	}
	if len(List()) == 0 {
		t.Fatal("expected facade List to return routes")
	}
	if _, err := URL("missing", nil); err == nil {
		t.Fatal("expected missing named route error")
	}
}

func TestFacadeWithContainerIsolation(t *testing.T) {
	gin.SetMode(gin.TestMode)

	c1 := container.NewContainer()
	container.SetProvider(func() *container.Container { return c1 })
	if err := (ServiceProvider{}).Register(providerTestApp{registry: c1}); err != nil {
		t.Fatalf("register route provider in c1: %v", err)
	}
	Get("/only-in-c1", text("c1"))

	c2 := container.NewContainer()
	container.SetProvider(func() *container.Container { return c2 })
	if err := (ServiceProvider{}).Register(providerTestApp{registry: c2}); err != nil {
		t.Fatalf("register route provider in c2: %v", err)
	}
	Get("/only-in-c2", text("c2"))

	router1, _ := c1.Make("route.router")
	r1 := router1.(*Router)
	if len(r1.List()) != 1 {
		t.Fatalf("container c1 should have 1 route, got %d", len(r1.List()))
	}
	hasOnlyC1 := false
	for _, info := range r1.List() {
		if info.URI == "/only-in-c1" {
			hasOnlyC1 = true
		}
		if info.URI == "/only-in-c2" {
			t.Fatal("container c1 should not contain c2 routes")
		}
	}
	if !hasOnlyC1 {
		t.Fatal("c1 should contain /only-in-c1")
	}

	container.SetProvider(func() *container.Container { return c1 })
	router1Resolved := Resolve()
	if len(router1Resolved.List()) != 1 {
		t.Fatalf("container c1 should have 1 route, got %d", len(router1Resolved.List()))
	}
	container.SetProvider(nil)
}

func TestRouterMethodsResourceOptionsAndDomainPatterns(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := New()
	router.Model("item", func(_ *gin.Context, value string) (any, error) { return value, nil })
	router.Name("root.").Get("/named", text("named")).Name("index")
	router.Middleware(func(c *gin.Context) { c.Next() }).Get("/middleware", text("mw"))
	router.Group(func() {
		router.Post("/group-post", text("group-post"))
	})
	router.Put("/items/{item}", text("put"))
	router.Patch("/items/{item}", text("patch"))
	router.Delete("/items/{item}", text("delete"))
	router.Options("/items", text("options"))
	router.Match([]string{http.MethodPost, http.MethodPost}, "/match", text("match"))
	router.Any("/any-method", text("any"))
	router.PermanentRedirect("/permanent", "/done")
	router.Get("/alpha/{v}", text("alpha")).WhereAlpha("v")
	router.Get("/alnum/{v}", text("alnum")).WhereAlphaNumeric("v")
	router.Get("/uuid/{v}", text("uuid")).WhereUuid("v")
	router.Get("/ulid/{v}", text("ulid")).WhereUlid("v")
	router.Get("/in/{v}", text("in")).WhereIn("v", []string{"a", "b"})
	router.Get("/in-empty/{v}", text("in-empty")).WhereIn("v", []string{"", ""})
	router.Get("/in-mixed/{v}", text("in-mixed")).WhereIn("v", []string{"", "a"})
	router.Get("/host", text("host"))
	router.Resource("widgets", &fullResourceController{}, Except("destroy"), Names(map[string]string{"index": "widgets"}), Parameters(map[string]string{"widgets": "widget_id"}))
	router.ApiResources(map[string]ResourceController{"things": &testResourceController{}}, Only("index"))

	engine := gin.New()
	if err := router.Mount(engine); err != nil {
		t.Fatalf("Mount failed: %v", err)
	}

	assertBody(t, engine, http.MethodPost, "/group-post", http.StatusOK, "group-post")
	assertBody(t, engine, http.MethodPut, "/items/1", http.StatusOK, "put")
	assertBody(t, engine, http.MethodPatch, "/items/1", http.StatusOK, "patch")
	assertBody(t, engine, http.MethodDelete, "/items/1", http.StatusOK, "delete")
	assertBody(t, engine, http.MethodOptions, "/items", http.StatusOK, "options")
	assertBody(t, engine, http.MethodPost, "/match", http.StatusOK, "match")
	assertBody(t, engine, http.MethodGet, "/any-method", http.StatusOK, "any")
	assertBody(t, engine, http.MethodGet, "/alpha/abc", http.StatusOK, "alpha")
	assertBody(t, engine, http.MethodGet, "/alnum/a1", http.StatusOK, "alnum")
	assertBody(t, engine, http.MethodGet, "/uuid/550e8400-e29b-41d4-a716-446655440000", http.StatusOK, "uuid")
	assertBody(t, engine, http.MethodGet, "/ulid/01ARZ3NDEKTSV4RRFFQ69G5FAV", http.StatusOK, "ulid")
	assertBody(t, engine, http.MethodGet, "/in/a", http.StatusOK, "in")
	assertBody(t, engine, http.MethodGet, "/in-empty/a", http.StatusOK, "in-empty")
	assertBody(t, engine, http.MethodGet, "/in-mixed/a", http.StatusOK, "in-mixed")
	if w := perform(engine, http.MethodGet, "/in-mixed/b"); w.Code != http.StatusNotFound {
		t.Fatalf("mixed where-in response = %d, want 404", w.Code)
	}
	assertBody(t, engine, http.MethodGet, "/widgets", http.StatusOK, "full-index")
	assertBody(t, engine, http.MethodGet, "/widgets/create", http.StatusOK, "create")
	assertBody(t, engine, http.MethodGet, "/widgets/8/edit", http.StatusOK, "edit")
	if w := perform(engine, http.MethodDelete, "/widgets/8"); w.Code != http.StatusNotFound {
		t.Fatalf("except destroy = %d, want 404", w.Code)
	}
	router.Reset()
	if len(router.List()) != 0 {
		t.Fatal("router Reset should clear routes")
	}
}

func TestDomainRouteMatchesLiteralWildcardAndPort(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := New()
	router.Domain("api.example.com").Get("/literal-host", text("literal"))
	router.Domain("{tenant}.example.com").Get("/tenant-host", text("tenant"))

	engine := gin.New()
	if err := router.Mount(engine); err != nil {
		t.Fatalf("Mount failed: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/literal-host", nil)
	req.Host = "api.example.com:8080"
	w := httptest.NewRecorder()
	engine.ServeHTTP(w, req)
	if w.Code != http.StatusOK || w.Body.String() != "literal" {
		t.Fatalf("literal host response = %d %q", w.Code, w.Body.String())
	}

	req = httptest.NewRequest(http.MethodGet, "/tenant-host", nil)
	req.Host = "acme.example.com"
	w = httptest.NewRecorder()
	engine.ServeHTTP(w, req)
	if w.Code != http.StatusOK || w.Body.String() != "tenant" {
		t.Fatalf("wildcard host response = %d %q", w.Code, w.Body.String())
	}

	req = httptest.NewRequest(http.MethodGet, "/tenant-host", nil)
	req.Host = "example.com"
	w = httptest.NewRecorder()
	engine.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("mismatched host response = %d, want 404", w.Code)
	}
}

func text(value string) gin.HandlerFunc {
	return func(c *gin.Context) { c.String(http.StatusOK, value) }
}

func assertBody(t *testing.T, engine *gin.Engine, method, path string, code int, body string) {
	t.Helper()
	req := httptest.NewRequest(method, path, nil)
	w := httptest.NewRecorder()
	engine.ServeHTTP(w, req)
	if w.Code != code || w.Body.String() != body {
		t.Fatalf("%s %s = %d %q, want %d %q", method, path, w.Code, w.Body.String(), code, body)
	}
}

type fullResourceController struct{}

func (c *fullResourceController) Index(ctx *gin.Context)   { ctx.String(http.StatusOK, "full-index") }
func (c *fullResourceController) Store(ctx *gin.Context)   { ctx.String(http.StatusOK, "store") }
func (c *fullResourceController) Show(ctx *gin.Context)    { ctx.String(http.StatusOK, "show") }
func (c *fullResourceController) Update(ctx *gin.Context)  { ctx.String(http.StatusOK, "update") }
func (c *fullResourceController) Destroy(ctx *gin.Context) { ctx.String(http.StatusOK, "destroy") }
func (c *fullResourceController) Create(ctx *gin.Context)  { ctx.String(http.StatusOK, "create") }
func (c *fullResourceController) Edit(ctx *gin.Context)    { ctx.String(http.StatusOK, "edit") }
