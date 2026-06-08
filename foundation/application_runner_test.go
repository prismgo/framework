package foundation

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prismgo/framework/console"
	providercontract "github.com/prismgo/framework/contracts/provider"
	"github.com/prismgo/framework/event"
	prismhttp "github.com/prismgo/framework/http"
)

// lifecycleProvider 记录 provider 生命周期调用次数。
//
// 用途：验证 Application.RunContext 是否按顺序驱动 Register 与 Boot。
type lifecycleProvider struct {
	registered int
	booted     int
}

func (p *lifecycleProvider) Register(app providercontract.Application) error {
	p.registered++
	return nil
}

func (p *lifecycleProvider) Boot(app providercontract.Application) error {
	p.booted++
	return nil
}

type bootFailingProvider struct {
	err error
}

func (p bootFailingProvider) Register(providercontract.Application) error {
	return nil
}

func (p bootFailingProvider) Boot(providercontract.Application) error {
	return p.err
}

type applicationRunnerCommand struct {
	ran *bool
}

func (c applicationRunnerCommand) Definition() *console.Definition {
	return console.MustDefinition("app:runner-output", "runner output")
}

func (c applicationRunnerCommand) Handle(console.CommandContext) error {
	if c.ran != nil {
		*c.ran = true
	}
	return nil
}

// TestApplicationRunContextBootsRunsAndCloses 验证 Application 托管完整运行生命周期。
//
// 设计说明：入口层不应重复调用 Boot、RegisterShutdownSignals 和 Close；这些通用生命周期动作
// 由 foundation.Application 统一负责。
func TestApplicationRunContextBootsRunsAndCloses(t *testing.T) {
	app := NewApplication()
	provider := &lifecycleProvider{}
	app.RegisterProvider(provider)

	cleanups := 0
	app.RegisterCleanup(func(app *Application) error {
		cleanups++
		return nil
	})

	runs := 0
	if err := app.RunContext(func(ctx context.Context) error {
		runs++
		if ctx == nil {
			t.Fatal("runner context is nil")
		}
		return nil
	}); err != nil {
		t.Fatalf("RunContext() error = %v", err)
	}

	if provider.registered != 1 || provider.booted != 1 {
		t.Fatalf("provider lifecycle = register:%d boot:%d, want 1/1", provider.registered, provider.booted)
	}
	if runs != 1 {
		t.Fatalf("runner count = %d, want 1", runs)
	}
	if cleanups != 1 {
		t.Fatalf("cleanup count = %d, want 1", cleanups)
	}
	if !errors.Is(app.Context().Err(), context.Canceled) {
		t.Fatalf("application context was not canceled")
	}
}

// TestMergeRunContextCancelsWhenApplicationStops 验证合并 context 会响应应用关闭。
//
// 设计说明：RunContext(ctx) 兼容外部运行 context，但应用自身的 Shutdown 仍必须能中断 Runner。
func TestMergeRunContextCancelsWhenApplicationStops(t *testing.T) {
	appCtx, cancelApp := context.WithCancelCause(context.Background())
	runCtx := context.Background()
	merged, release := mergeRunContext(appCtx, runCtx)
	defer release()

	shutdownReason := errors.New("test shutdown")
	cancelApp(shutdownReason)

	select {
	case <-merged.Done():
	case <-time.After(time.Second):
		t.Fatal("merged context was not canceled")
	}

	if !errors.Is(context.Cause(merged), shutdownReason) {
		t.Fatalf("merged context cause = %v, want %v", context.Cause(merged), shutdownReason)
	}
}

func TestApplicationHandleCommandUsesFullArgvEntry(t *testing.T) {
	ran := false
	app := Configure().WithCommands(func() console.Command {
		return applicationRunnerCommand{ran: &ran}
	}).Create()

	if err := app.HandleCommand(context.Background(), []string{"artisan", "app:runner-output"}); err != nil {
		t.Fatalf("HandleCommand() error = %v", err)
	}
	if !ran {
		t.Fatal("HandleCommand did not execute command from full argv input")
	}
}

func TestApplicationRunContextRejectsNilApplication(t *testing.T) {
	var app *Application
	err := app.RunContext(func(context.Context) error { return nil })
	if err == nil || !strings.Contains(err.Error(), "application is not initialized") {
		t.Fatalf("RunContext nil app error = %v", err)
	}
}

func TestApplicationRunContextRejectsNilRunner(t *testing.T) {
	app := NewApplication()
	err := app.RunContext(nil)
	if err == nil || !strings.Contains(err.Error(), "runner is not configured") {
		t.Fatalf("RunContext nil runner error = %v", err)
	}
}

func TestApplicationRunContextJoinsRunnerAndCloseErrors(t *testing.T) {
	app := NewApplication()
	runErr := errors.New("runner failed")
	closeErr := errors.New("close failed")
	cleanups := 0
	app.RegisterCleanup(func(_ *Application) error {
		cleanups++
		return closeErr
	})

	err := app.RunContext(func(context.Context) error {
		return runErr
	})
	if !errors.Is(err, runErr) {
		t.Fatalf("RunContext error = %v, want runner error", err)
	}
	if !errors.Is(err, closeErr) {
		t.Fatalf("RunContext error = %v, want close error", err)
	}
	if cleanups != 1 {
		t.Fatalf("cleanup count = %d, want 1", cleanups)
	}
}

func TestApplicationRunContextTreatsCanceledRunContextAsNormalShutdown(t *testing.T) {
	app := NewApplication()
	runCtx, cancel := context.WithCancel(context.Background())
	cancel()
	cleanups := 0
	app.RegisterCleanup(func(_ *Application) error {
		cleanups++
		return nil
	})

	err := app.RunContext(func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	}, runCtx)
	if err != nil {
		t.Fatalf("RunContext cancellation error = %v, want nil", err)
	}
	if cleanups != 1 {
		t.Fatalf("cleanup count = %d, want 1", cleanups)
	}
}

func TestApplicationRunContextKeepsContextCanceledWhenRunContextIsActive(t *testing.T) {
	app := NewApplication()

	err := app.RunContext(func(context.Context) error {
		return context.Canceled
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("RunContext error = %v, want context.Canceled", err)
	}
}

func TestApplicationRunContextJoinsBootAndCloseErrors(t *testing.T) {
	app := NewApplication()
	bootErr := errors.New("boot failed")
	closeErr := errors.New("close failed")
	app.RegisterProvider(bootFailingProvider{err: bootErr})
	cleanups := 0
	app.RegisterCleanup(func(_ *Application) error {
		cleanups++
		return closeErr
	})

	err := app.RunContext(func(context.Context) error {
		t.Fatal("runner should not execute after boot failure")
		return nil
	})
	if !errors.Is(err, bootErr) {
		t.Fatalf("RunContext error = %v, want boot error", err)
	}
	if !errors.Is(err, closeErr) {
		t.Fatalf("RunContext error = %v, want close error", err)
	}
	if cleanups != 1 {
		t.Fatalf("cleanup count = %d, want 1", cleanups)
	}
}

func TestApplicationRunContextMergesExternalCancellationCause(t *testing.T) {
	app := NewApplication()
	runCtx, cancel := context.WithCancelCause(context.Background())
	want := errors.New("external shutdown")
	causeCh := make(chan error, 1)

	done := make(chan error, 1)
	go func() {
		done <- app.RunContext(func(ctx context.Context) error {
			<-ctx.Done()
			causeCh <- context.Cause(ctx)
			return nil
		}, runCtx)
	}()

	cancel(want)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("RunContext() error = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("RunContext did not finish after external cancellation")
	}

	select {
	case got := <-causeCh:
		if !errors.Is(got, want) {
			t.Fatalf("runner context cause = %v, want %v", got, want)
		}
	case <-time.After(time.Second):
		t.Fatal("runner context cause was not recorded")
	}
}

func TestApplicationRunContextReleasesMergedContextBeforeClose(t *testing.T) {
	app := NewApplication()
	runCtx := context.Background()
	mergedCtxCh := make(chan context.Context, 1)

	err := app.RunContext(func(ctx context.Context) error {
		mergedCtxCh <- ctx
		return nil
	}, runCtx)
	if err != nil {
		t.Fatalf("RunContext() error = %v", err)
	}

	var merged context.Context
	select {
	case merged = <-mergedCtxCh:
	case <-time.After(time.Second):
		t.Fatal("runner context was not captured")
	}

	if !errors.Is(context.Cause(app.Context()), ErrApplicationShutdown) {
		t.Fatalf("application context cause = %v, want %v", context.Cause(app.Context()), ErrApplicationShutdown)
	}
	if err := merged.Err(); err != nil {
		t.Fatalf("merged context err = %v, want nil after runner returns", err)
	}
	if cause := context.Cause(merged); cause != nil {
		t.Fatalf("merged context cause = %v, want nil after runner returns", cause)
	}
}

func TestApplicationRunContextCoordinatesServerAndAppLifecycleEvents(t *testing.T) {
	withTestFacadeRegistry(t)

	app := NewApplication()
	bus := event.New()
	if err := app.Container().Instance("event.dispatcher", bus); err != nil {
		t.Fatalf("register event dispatcher: %v", err)
	}
	var (
		mu               sync.Mutex
		events           []string
		appReason        string
		serverStopReason string
	)
	bus.Listen("app.*", event.ListenerFunc(func(_ context.Context, ev event.Event) error {
		mu.Lock()
		defer mu.Unlock()
		events = append(events, ev.Name())
		if payload, ok := ev.(event.AppTerminating); ok {
			appReason = payload.Reason
		}
		return nil
	}))
	bus.Listen("server.*", event.ListenerFunc(func(_ context.Context, ev event.Event) error {
		mu.Lock()
		events = append(events, ev.Name())
		if payload, ok := ev.(event.ServerStopping); ok {
			serverStopReason = payload.Reason
		}
		mu.Unlock()
		if ev.Name() == event.EventServerStarted {
			go app.Shutdown(errors.New("runner shutdown"))
		}
		return nil
	}))

	err := app.RunContext(func(ctx context.Context) error {
		server := &http.Server{
			Addr:    "127.0.0.1:0",
			Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }),
		}

		return prismhttp.ListenAndServeGracefulContext(ctx, server, time.Second, prismhttp.WithDispatcher(bus))
	})
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		t.Fatalf("RunContext() error = %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	want := []string{
		event.EventServerStarting,
		event.EventServerStarted,
		event.EventServerStopping,
		event.EventServerStopped,
		event.EventAppTerminating,
		event.EventAppTerminated,
	}
	pos := 0
	for _, got := range events {
		if pos < len(want) && got == want[pos] {
			pos++
		}
	}
	if pos != len(want) {
		t.Fatalf("lifecycle subsequence %v not found in %v", want, events)
	}
	if appReason != "runner shutdown" {
		t.Fatalf("app terminating reason = %q, want %q", appReason, "runner shutdown")
	}
	if serverStopReason != appReason {
		t.Fatalf("server stopping reason = %q, want same as app reason %q", serverStopReason, appReason)
	}
}
