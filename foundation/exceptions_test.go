package foundation

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"

	prismconfig "github.com/prismgo/framework/config"
	"github.com/prismgo/framework/timer"

	goexception "github.com/prismgo/framework/exception"
)

func init() {
	prismconfig.Add("app", func() map[string]any {
		return map[string]any{
			"key":           prismconfig.Env("APP_KEY", ""),
			"cipher":        prismconfig.Env("APP_CIPHER", "AES-256-GCM"),
			"previous_keys": prismconfig.Env("APP_PREVIOUS_KEYS", ""),
			"debug":         prismconfig.Env("APP_DEBUG", false),
		}
	})
	prismconfig.Add("logging", func() map[string]any {
		return map[string]any{
			"default": "null",
			"channels": map[string]any{
				"null": map[string]any{"driver": "null"},
			},
		}
	})
}

func TestExceptionsRenderAcceptsProblemAndResponseRenderers(t *testing.T) {
	builder := Configure().WithExceptions(func(e *Exceptions) {
		e.Render(nil)
		e.Render(func(c *gin.Context, err error) (goexception.Problem, bool) {
			return goexception.Problem{}, false
		})
		e.Render(goexception.Renderer(func(c *gin.Context, err error) (goexception.Problem, bool) {
			return goexception.Problem{}, true
		}))
		e.Render(func(c *gin.Context, err error) bool {
			c.Status(418)
			return !errors.Is(err, nil)
		})
		e.Render(goexception.ResponseRenderer(func(c *gin.Context, err error) bool {
			c.Status(409)
			return true
		}))
	})

	if got := len(builder.exceptions.options); got != 4 {
		t.Fatalf("exception option count = %d, want 4", got)
	}
}

func TestExceptionsRenderPanicsOnUnsupportedType(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("Render should panic on unsupported renderer type")
		}
	}()

	e := &Exceptions{}
	e.Render("invalid renderer") // 不支持的类型，应该 panic
}

func TestBuilderFluentConfigurationBranches(t *testing.T) {
	provider := &testProvider{}
	builder := Configure().
		WithProviders(provider).
		WithRouting(func(r *Routing) {
			r.Schedules(func(_ *timer.Schedule) {})
		}).
		WithMiddleware(func(m *Middleware) {
			m.Apply(gin.New())
			m.Use(func(*gin.Engine) {})
		}).
		WithExceptions(func(e *Exceptions) {
			e.Context(func(*gin.Context) map[string]any { return map[string]any{"tenant": "t1"} })
			e.RenderResponse(func(*gin.Context, error) bool { return true })
			e.Report(func(ctx any, err error, fields map[string]any) {})
			e.DontReport(func(error) bool { return true })
			e.Level(func(err error, status int) goexception.Level { return goexception.LevelError })
			e.Handler(nil)
			e.Use(nil)
		})

	if len(builder.providers) != 1 || builder.providers[0] != provider {
		t.Fatalf("providers = %#v, want provider", builder.providers)
	}
	if len(builder.routing.schedules) != 1 {
		t.Fatalf("routing = %#v", builder.routing)
	}
	if len(builder.middleware.registrars) != 1 {
		t.Fatalf("middleware registrars = %d, want 1", len(builder.middleware.registrars))
	}
	if len(builder.exceptions.options) != 5 {
		t.Fatalf("exception options = %d, want 5", len(builder.exceptions.options))
	}
}

func TestExceptionsHandlerRegistersFactoryOnCreate(t *testing.T) {
	var gotDefault *goexception.Handler
	app := Configure().WithExceptions(func(e *Exceptions) {
		e.Handler(func(defaultHandler *goexception.Handler) *goexception.Handler {
			gotDefault = defaultHandler
			return goexception.New(goexception.WithLogging(false))
		})
	}).Create()

	if gotDefault == nil {
		t.Fatal("default handler was not passed to factory")
	}
	got, _ := app.Container().Make(ContainerKeyExceptionHandler)
	if got == nil {
		t.Fatal("handler was not registered in application container")
	}
}

func TestExceptionsUseRegistersHandlerOnCreate(t *testing.T) {
	custom := goexception.New(goexception.WithLogging(false))
	app := Configure().WithExceptions(func(e *Exceptions) {
		e.Use(custom)
	}).Create()

	got, _ := app.Container().Make(ContainerKeyExceptionHandler)
	if got != custom {
		t.Fatal("custom handler was not registered in application container")
	}
}

func TestBuilderExceptionHandlerFollowsAppDebugConfig(t *testing.T) {
	app := Configure().Create()
	useFoundationDebugConfig(t, app, true)

	raw, _ := app.Container().Make(ContainerKeyExceptionHandler)
	h, _ := raw.(*goexception.Handler)
	if h == nil {
		t.Fatal("handler was not registered in application container")
	}
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/debug", nil)

	h.Render(c, errors.New("debug config visible"))

	if !strings.Contains(w.Body.String(), "debug config visible") || !strings.Contains(w.Body.String(), `"trace"`) {
		t.Fatalf("handler should read app.debug at render time, body = %s", w.Body.String())
	}
}

func useFoundationDebugConfig(t *testing.T, app *Application, enabled bool) {
	t.Helper()

	previous := prismconfig.Clone()
	envPath := filepath.Join(t.TempDir(), ".env")
	content := "APP_DEBUG=false\n"
	if enabled {
		content = "APP_DEBUG=true\n"
	}
	if err := os.WriteFile(envPath, []byte(content), 0o600); err != nil {
		t.Fatalf("write test env: %v", err)
	}
	cfg, err := prismconfig.NewFromFile(envPath)
	if err != nil {
		t.Fatalf("load test config: %v", err)
	}
	_ = app.Container().Instance(ContainerKeyConfigDefault, cfg)
	t.Cleanup(func() {
		_ = app.Container().Instance(ContainerKeyConfigDefault, previous)
	})
}
