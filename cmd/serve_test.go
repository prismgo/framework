package cmd

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/prismgo/framework/console"
	prismhttp "github.com/prismgo/framework/http"
)

type fakeHTTPRegistrars struct {
	middlewareCalled bool
	routesCalled     bool
	routesErr        error
}

func (a *fakeHTTPRegistrars) ApplyMiddlewares(engine *gin.Engine) {
	a.middlewareCalled = engine != nil
}

func (a *fakeHTTPRegistrars) MountRoutes(engine *gin.Engine) error {
	a.routesCalled = engine != nil
	if a.routesErr != nil {
		return a.routesErr
	}
	engine.GET("/health", func(c *gin.Context) {
		c.Status(200)
	})
	return nil
}

func fakeHTTPServerFactory(app *fakeHTTPRegistrars) func(context.Context, string) (*http.Server, error) {
	return func(ctx context.Context, port string) (*http.Server, error) {
		cfg := prismhttp.CurrentServerConfig()
		cfg.Port = port
		return prismhttp.NewApplicationServer(port, func(engine *gin.Engine, useInternalMiddlewares func(*gin.Engine)) error {
			if useInternalMiddlewares != nil {
				useInternalMiddlewares(engine)
			}
			app.ApplyMiddlewares(engine)
			return app.MountRoutes(engine)
		}, prismhttp.WithBaseContext(ctx), prismhttp.WithServerConfig(cfg))
	}
}

func TestNewServeCommandUsesHTTPRegistrars(t *testing.T) {
	cmd := NewServeCommand(fakeHTTPServerFactory(&fakeHTTPRegistrars{}))
	if cmd == nil {
		t.Fatal("expected serve command")
	}
}

func TestServeCommandReadsDefaultConfig(t *testing.T) {
	if defaultHTTPPort() != "8080" {
		t.Fatalf("default port = %s, want 8080", defaultHTTPPort())
	}

	if serverShutdownTimeout() != 15*time.Second {
		t.Fatalf("shutdown timeout = %s, want 15s", serverShutdownTimeout())
	}
}

func TestServeCommandStartServerStopsWhenContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	app := &fakeHTTPRegistrars{}
	cmd := NewServeCommand(fakeHTTPServerFactory(app))

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = cmd.startServer(ctx, "0", console.NewIO(strings.NewReader(""), io.Discard, io.Discard))
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("startServer did not stop after context cancellation")
	}
	if !app.middlewareCalled || !app.routesCalled {
		t.Fatal("expected HTTP application registrars to be called")
	}
}

func TestServeCommandStartServerPropagatesRouteErrors(t *testing.T) {
	errRoutes := errors.New("routes failed")
	cmd := NewServeCommand(fakeHTTPServerFactory(&fakeHTTPRegistrars{routesErr: errRoutes}))
	err := cmd.startServer(context.Background(), "0", console.NewIO(strings.NewReader(""), io.Discard, io.Discard))
	if !errors.Is(err, errRoutes) {
		t.Fatalf("startServer error = %v, want %v", err, errRoutes)
	}
}

func TestServeCommandProcessControlWritesToCommandIO(t *testing.T) {
	cmd := NewServeCommand(fakeHTTPServerFactory(&fakeHTTPRegistrars{}))
	manager := &errorProcessManager{pid: 100}
	cmd.newProcessManager = func(string) processManager { return manager }

	stdout := &bytes.Buffer{}
	ctx := console.NewCommandContext(context.Background(), cmd, *cmd.Definition(), fakeInput{
		options: map[string]string{"port": "8051"},
		bools:   map[string]bool{"stop": true},
	}, console.NewIO(strings.NewReader(""), stdout, io.Discard), nil, nil)

	if err := cmd.Handle(ctx); err != nil {
		t.Fatalf("Handle stop returned error: %v", err)
	}
	output := stdout.String()
	if !strings.Contains(output, "graceful shutdown initiated (pid: 100)") || !strings.Contains(output, "server shutdown completed") {
		t.Fatalf("stop output should be written to command IO, got %q", output)
	}

	stdout.Reset()
	quietIO := console.NewIOWithOutputOptions(strings.NewReader(""), io.Discard, io.Discard, console.OutputOptions{Quiet: true})
	ctx = console.NewCommandContext(context.Background(), cmd, *cmd.Definition(), fakeInput{
		options: map[string]string{"port": "8051"},
		bools:   map[string]bool{"kill": true},
	}, quietIO, nil, nil)
	if err := cmd.Handle(ctx); err != nil {
		t.Fatalf("Handle quiet kill returned error: %v", err)
	}
	if stdout.String() != "" {
		t.Fatalf("quiet process control output = %q, want empty", stdout.String())
	}
}

func TestServeCommandRejectsNonNumericPort(t *testing.T) {
	cmd := NewServeCommand(fakeHTTPServerFactory(&fakeHTTPRegistrars{}))
	ctx := console.NewCommandContext(context.Background(), cmd, *cmd.Definition(), fakeInput{
		options: map[string]string{"port": "../8051"},
	}, console.NewIO(strings.NewReader(""), io.Discard, io.Discard), nil, nil)
	if err := cmd.Handle(ctx); err == nil || !strings.Contains(err.Error(), "only numeric ports") {
		t.Fatalf("expected invalid port error, got %v", err)
	}
}
