package foundation

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	configpkg "github.com/prismgo/framework/config"
	"github.com/prismgo/framework/container"
	containercontract "github.com/prismgo/framework/contracts/container"
	providercontract "github.com/prismgo/framework/contracts/provider"
	"github.com/prismgo/framework/event"
	goexception "github.com/prismgo/framework/exception"
	pathutil "github.com/prismgo/framework/internal/path"
	"github.com/prismgo/framework/logger"
)

// mockEventDispatcher 是用于测试的简单事件分发器
type mockEventDispatcher struct {
	handlers map[string]func(any)
}

func (m *mockEventDispatcher) Dispatch(ctx context.Context, ev event.Event) {
	if h, ok := m.handlers[ev.Name()]; ok {
		h(ev)
	}
}

func (m *mockEventDispatcher) Listen(eventName string, l event.Listener) {
	// 简化实现，测试中不使用
}

func (m *mockEventDispatcher) ListenFunc(eventName string, fn func(context.Context, event.Event) error) {
	// 简化实现，测试中不使用
}

func (m *mockEventDispatcher) Subscribe(s event.Subscriber) {
	// 简化实现，测试中不使用
}

func (m *mockEventDispatcher) Forget(eventName string) {
	// 简化实现，测试中不使用
}

func (m *mockEventDispatcher) Has(eventName string) bool {
	_, ok := m.handlers[eventName]
	return ok
}

func bindApplicationCloseReporterForTest(t *testing.T, app *Application) {
	t.Helper()
	if err := app.Container().Instance("exception.handler", goexception.New(goexception.WithPanicStack(false)), container.WithCloseGroup(container.CloseGroupReporting)); err != nil {
		t.Fatalf("bind exception handler: %v", err)
	}
	manager, err := logger.NewManager(logger.Config{
		Default:  "null",
		Channels: map[string]logger.ChannelOptions{"null": {Driver: "null", Level: "debug"}},
	})
	if err != nil {
		t.Fatalf("new logger manager: %v", err)
	}
	if err := app.Container().Instance("logger.manager", manager, container.WithCloseGroup(container.CloseGroupReporting)); err != nil {
		t.Fatalf("bind logger manager: %v", err)
	}
}

func TestApplicationCloseSkipsUninitializedCoreResources(t *testing.T) {
	app := NewApplication()
	if err := app.Close(); err != nil {
		t.Fatalf("Close without initialized resources failed: %v", err)
	}
}

func TestApplicationContainerHelpersForwardToOwnedContainer(t *testing.T) {
	app := NewApplication()
	t.Cleanup(func() { _ = app.Close() })

	if err := app.Singleton("foundation.helper.singleton", func(containercontract.Resolver) (any, error) {
		return &closeContextProbe{}, nil
	}); err != nil {
		t.Fatalf("Singleton failed: %v", err)
	}
	if !app.Bound("foundation.helper.singleton") {
		t.Fatal("Bound should report registered singleton")
	}
	if app.Resolved("foundation.helper.singleton") {
		t.Fatal("Resolved should be false before first Make")
	}
	raw, err := app.Make("foundation.helper.singleton")
	if err != nil {
		t.Fatalf("Make failed: %v", err)
	}
	if raw == nil {
		t.Fatal("Make returned nil service")
	}
	if !app.Resolved("foundation.helper.singleton") {
		t.Fatal("Resolved should be true after Make")
	}
	if err := app.Alias("foundation.helper.singleton", "foundation.helper.alias"); err != nil {
		t.Fatalf("Alias failed: %v", err)
	}
	aliased, err := app.Make("foundation.helper.alias")
	if err != nil {
		t.Fatalf("Make alias failed: %v", err)
	}
	if aliased != raw {
		t.Fatal("alias should resolve the same singleton instance")
	}
	makeService, err := app.Factory("foundation.helper.alias")
	if err != nil {
		t.Fatalf("Factory failed: %v", err)
	}
	fromFactory, err := makeService()
	if err != nil {
		t.Fatalf("Factory closure failed: %v", err)
	}
	if fromFactory != raw {
		t.Fatal("factory closure should preserve singleton semantics")
	}
}

func TestApplicationContainerHelpersSupportInstanceBindAndCall(t *testing.T) {
	app := NewApplication()
	t.Cleanup(func() { _ = app.Close() })

	probe := &closeContextProbe{}
	if err := app.Instance("foundation.helper.instance", probe); err != nil {
		t.Fatalf("Instance failed: %v", err)
	}
	if !app.Resolved("foundation.helper.instance") {
		t.Fatal("Instance should mark service resolved")
	}
	if err := app.Bind("*github.com/prismgo/framework/foundation.closeContextProbe", func(containercontract.Resolver) (any, error) {
		return probe, nil
	}); err != nil {
		t.Fatalf("Bind failed: %v", err)
	}
	results, err := app.Call(func(p *closeContextProbe) *closeContextProbe {
		return p
	})
	if err != nil {
		t.Fatalf("Call failed: %v", err)
	}
	if len(results) != 1 || results[0] != probe {
		t.Fatalf("Call result = %#v, want probe", results)
	}
	if !app.Has("foundation.helper.instance") {
		t.Fatal("Has should report instance as resolvable")
	}
}

func TestBuilderCreatePanicsWhenProviderRegistrationFails(t *testing.T) {
	container.SetProvider(nil)
	t.Cleanup(func() { container.SetProvider(nil) })

	var panicValue any
	func() {
		defer func() {
			panicValue = recover()
		}()

		Configure().WithProviders(&deferredTestProvider{name: "bad.deferred.provider"}).Create()
	}()

	if panicValue == nil {
		t.Fatal("Create should panic when provider registration fails")
	}
	message := fmt.Sprint(panicValue)
	if !strings.Contains(message, "bad.deferred.provider") || !strings.Contains(message, "deferred provider must provide at least one service key") {
		t.Fatalf("panic message = %q, want provider name and deferred service failure", message)
	}
}

func TestApplicationShutdownCancelsLifecycleContext(t *testing.T) {
	app := NewApplication()
	boom := errors.New("manual shutdown")

	if err := app.Context().Err(); err != nil {
		t.Fatalf("new application context err = %v, want nil", err)
	}

	app.Shutdown(boom)

	select {
	case <-app.Context().Done():
	default:
		t.Fatal("expected application context to be canceled")
	}
	if !errors.Is(context.Cause(app.Context()), boom) {
		t.Fatalf("context cause = %v, want %v", context.Cause(app.Context()), boom)
	}

	app.Shutdown(errors.New("ignored"))
	if !errors.Is(context.Cause(app.Context()), boom) {
		t.Fatalf("context cause changed to %v, want first cause %v", context.Cause(app.Context()), boom)
	}
}

func TestApplicationCloseCancelsLifecycleContext(t *testing.T) {
	app := NewApplication()

	if err := app.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	if !errors.Is(context.Cause(app.Context()), ErrApplicationShutdown) {
		t.Fatalf("context cause = %v, want %v", context.Cause(app.Context()), ErrApplicationShutdown)
	}
}

func TestApplicationCloseUsesIndependentTimeoutContext(t *testing.T) {
	app := NewApplication()
	var closeCtx context.Context
	var closeCtxErr error
	var closeCtxIsApp bool
	var hasDeadline bool
	var remaining time.Duration
	if err := app.Container().Instance("foundation.close.probe", &closeContextProbe{}, container.WithContextCloser(func(ctx context.Context, _ *closeContextProbe) error {
		closeCtx = ctx
		closeCtxErr = ctx.Err()
		closeCtxIsApp = ctx == app.Context()
		deadline, ok := ctx.Deadline()
		hasDeadline = ok
		remaining = time.Until(deadline)
		return nil
	})); err != nil {
		t.Fatalf("register close probe: %v", err)
	}

	if err := app.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	if closeCtx == nil {
		t.Fatal("expected facade closer to receive context")
	}
	if closeCtxIsApp {
		t.Fatal("Close should not pass the canceled application lifecycle context")
	}
	if closeCtxErr != nil {
		t.Fatalf("close context err = %v, want nil", closeCtxErr)
	}
	if !hasDeadline {
		t.Fatal("Close context should have a deadline")
	}
	if remaining <= 0 || remaining > DefaultCloseTimeout {
		t.Fatalf("close deadline remaining = %s, want within %s", remaining, DefaultCloseTimeout)
	}
}

func TestApplicationCloseAcceptsCustomTimeout(t *testing.T) {
	app := NewApplication()
	customTimeout := 120 * time.Millisecond
	var remaining time.Duration
	if err := app.Container().Instance("foundation.close.custom.probe", &closeContextProbe{}, container.WithContextCloser(func(ctx context.Context, _ *closeContextProbe) error {
		deadline, ok := ctx.Deadline()
		if !ok {
			t.Fatal("Close context should have a deadline")
		}
		remaining = time.Until(deadline)
		return nil
	})); err != nil {
		t.Fatalf("register close probe: %v", err)
	}

	if err := app.Close(customTimeout); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	if remaining <= 0 || remaining > customTimeout {
		t.Fatalf("close deadline remaining = %s, want within %s", remaining, customTimeout)
	}
}

func TestApplicationCloseFallsBackToDefaultTimeout(t *testing.T) {
	if got := closeTimeout(); got != DefaultCloseTimeout {
		t.Fatalf("closeTimeout() = %s, want %s", got, DefaultCloseTimeout)
	}
	if got := closeTimeout(0); got != DefaultCloseTimeout {
		t.Fatalf("closeTimeout(0) = %s, want %s", got, DefaultCloseTimeout)
	}
	if got := closeTimeout(-time.Second); got != DefaultCloseTimeout {
		t.Fatalf("closeTimeout(-1s) = %s, want %s", got, DefaultCloseTimeout)
	}
	if got := closeTimeout(time.Second); got != time.Second {
		t.Fatalf("closeTimeout(1s) = %s, want 1s", got)
	}
}

func TestApplicationCloseContextDispatchesEventsWithLiveContext(t *testing.T) {
	app := NewApplication()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var eventErr error
	bus := event.Resolve()
	if bus == nil {
		t.Fatal("resolve event bus returned nil")
	}
	bus.Listen(event.EventAppTerminating, event.ListenerFunc(func(ctx context.Context, _ event.Event) error {
		eventErr = ctx.Err()
		return nil
	}))

	if err := app.CloseContext(ctx); err != nil {
		t.Fatalf("CloseContext failed: %v", err)
	}
	if eventErr != nil {
		t.Fatalf("terminating event context err = %v, want nil", eventErr)
	}
}

func TestApplicationBootDispatchesProviderLifecycleEventsInOrder(t *testing.T) {
	app := NewApplication()
	provider := &testProvider{}
	if err := app.RegisterProvider(provider); err != nil {
		t.Fatalf("RegisterProvider error = %v", err)
	}
	wantName := "foundation.testProvider"

	var mu sync.Mutex
	var got []string
	bus := event.Resolve()
	bus.Listen(event.EventProviderRegistering, event.ListenerFunc(func(_ context.Context, ev event.Event) error {
		mu.Lock()
		defer mu.Unlock()
		payload, ok := ev.(event.ProviderRegistering)
		if !ok {
			t.Fatalf("unexpected event type: %T", ev)
		}
		if payload.Provider != wantName {
			return nil
		}
		got = append(got, "registering:"+payload.Provider)
		return nil
	}))
	bus.Listen(event.EventProviderRegistered, event.ListenerFunc(func(_ context.Context, ev event.Event) error {
		mu.Lock()
		defer mu.Unlock()
		payload, ok := ev.(event.ProviderRegistered)
		if !ok {
			t.Fatalf("unexpected event type: %T", ev)
		}
		if payload.Provider != wantName {
			return nil
		}
		got = append(got, "registered:"+payload.Provider)
		return nil
	}))
	bus.Listen(event.EventProviderBooting, event.ListenerFunc(func(_ context.Context, ev event.Event) error {
		mu.Lock()
		defer mu.Unlock()
		payload, ok := ev.(event.ProviderBooting)
		if !ok {
			t.Fatalf("unexpected event type: %T", ev)
		}
		if payload.Provider != wantName {
			return nil
		}
		got = append(got, "booting:"+payload.Provider)
		return nil
	}))
	bus.Listen(event.EventProviderBooted, event.ListenerFunc(func(_ context.Context, ev event.Event) error {
		mu.Lock()
		defer mu.Unlock()
		payload, ok := ev.(event.ProviderBooted)
		if !ok {
			t.Fatalf("unexpected event type: %T", ev)
		}
		if payload.Provider != wantName {
			return nil
		}
		got = append(got, "booted:"+payload.Provider)
		return nil
	}))

	if err := app.Boot(); err != nil {
		t.Fatalf("Boot failed: %v", err)
	}

	want := []string{
		"registering:" + wantName,
		"registered:" + wantName,
		"booting:" + wantName,
		"booted:" + wantName,
	}
	if len(got) != len(want) {
		t.Fatalf("event count = %d, want %d, got %v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("event[%d] = %q, want %q", i, got[i], want[i])
		}
	}
	if provider.calls == nil || len(provider.calls) != 2 || provider.calls[0] != "register" || provider.calls[1] != "boot" {
		t.Fatalf("provider calls = %v, want [register boot]", provider.calls)
	}
}

func TestApplicationCloseRunsCleanupsInReverseOrder(t *testing.T) {
	app := NewApplication()
	var calls []int
	app.RegisterCleanup(func(cleanupApp *Application) error {
		if cleanupApp != app {
			t.Fatal("cleanup should receive application instance")
		}
		calls = append(calls, 1)
		return nil
	})
	app.RegisterCleanup(func(cleanupApp *Application) error {
		if cleanupApp != app {
			t.Fatal("cleanup should receive application instance")
		}
		calls = append(calls, 2)
		return nil
	})
	app.RegisterCleanup(func(cleanupApp *Application) error {
		if cleanupApp != app {
			t.Fatal("cleanup should receive application instance")
		}
		calls = append(calls, 3)
		return nil
	})

	if err := app.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	want := []int{3, 2, 1}
	if len(calls) != len(want) {
		t.Fatalf("cleanup count = %d, want %d", len(calls), len(want))
	}
	for i := range want {
		if calls[i] != want[i] {
			t.Fatalf("cleanup[%d] = %d, want %d", i, calls[i], want[i])
		}
	}
}

func TestApplicationCloseDispatchesTerminatedError(t *testing.T) {
	app := NewApplication()
	wantErr := errors.New("cleanup failed")
	app.RegisterCleanup(func(_ *Application) error {
		return wantErr
	})
	bindApplicationCloseReporterForTest(t, app)

	var terminated event.AppTerminated
	bus := event.Resolve()
	bus.Listen(event.EventAppTerminated, event.ListenerFunc(func(_ context.Context, ev event.Event) error {
		terminated = ev.(event.AppTerminated)
		return nil
	}))

	if err := app.Close(); !errors.Is(err, wantErr) {
		t.Fatalf("Close error = %v, want %v", err, wantErr)
	}
	if terminated.Error != wantErr.Error() || terminated.Duration < 0 {
		t.Fatalf("terminated event = %+v, want error %q", terminated, wantErr.Error())
	}
}

func TestApplicationShutdownReasonDefaultsWhenActive(t *testing.T) {
	app := NewApplication()
	if got := app.shutdownReason(); got != ErrApplicationShutdown.Error() {
		t.Fatalf("active application shutdownReason = %q, want %q", got, ErrApplicationShutdown.Error())
	}
}

func TestApplicationNilAndFallbackBranches(t *testing.T) {
	app := NewApplication()
	app.RegisterCleanup(nil)
	if err := app.RegisterProvider(nil); err != nil {
		t.Fatalf("RegisterProvider error = %v", err)
	}

	if got := (*Application)(nil).Context(); got == nil {
		t.Fatal("nil application Context should return background context")
	}
	(*Application)(nil).Shutdown(errors.New("ignored"))
	(*Application)(nil).RegisterShutdownSignals()
	if got := (*Application)(nil).shutdownReason(); got != ErrApplicationShutdown.Error() {
		t.Fatalf("nil application shutdownReason = %q", got)
	}

	app.Shutdown(nil)
	if !errors.Is(context.Cause(app.Context()), ErrApplicationShutdown) {
		t.Fatalf("nil shutdown reason should use ErrApplicationShutdown, got %v", context.Cause(app.Context()))
	}
	if name := providerTypeName(nil); name != "<nil>" {
		t.Fatalf("providerTypeName(nil) = %q", name)
	}
}

func TestApplicationRegisterShutdownSignalsCancelsLifecycleContext(t *testing.T) {
	app := NewApplication()
	app.RegisterShutdownSignals()

	process, err := os.FindProcess(os.Getpid())
	if err != nil {
		t.Fatalf("FindProcess failed: %v", err)
	}
	if err := process.Signal(syscall.SIGTERM); err != nil {
		t.Skipf("send SIGTERM failed: %v", err)
	}

	deadline := time.After(2 * time.Second)
	for {
		if cause := context.Cause(app.Context()); cause != nil {
			if got := cause.Error(); got != "received signal terminated" && got != "received signal interrupt" {
				t.Fatalf("shutdown cause = %q, want signal shutdown message", got)
			}
			return
		}

		select {
		case <-deadline:
			t.Fatal("expected application context to be canceled by signal")
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestApplicationRegisterShutdownSignalsStopsWhenApplicationAlreadyClosed(t *testing.T) {
	app := NewApplication()
	app.RegisterShutdownSignals()

	want := errors.New("manual close")
	app.Shutdown(want)

	deadline := time.After(2 * time.Second)
	for {
		if cause := context.Cause(app.Context()); cause != nil {
			if !errors.Is(cause, want) {
				t.Fatalf("shutdown cause = %v, want %v", cause, want)
			}
			return
		}

		select {
		case <-deadline:
			t.Fatal("expected application context to be canceled")
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestFoundationConfigAndEventNames(t *testing.T) {
	app := NewApplication()
	t.Cleanup(func() { _ = app.Close() })
	cfg := configpkg.Resolve()
	if cfg == nil {
		t.Fatal("Resolve config returned nil")
	}

	if (event.AppTerminating{}).Name() != event.EventAppTerminating {
		t.Fatal("unexpected AppTerminating event name")
	}
	if (event.AppTerminated{}).Name() != event.EventAppTerminated {
		t.Fatal("unexpected AppTerminated event name")
	}
}

// TestAppTerminatedDurationFields 验证 AppTerminated 事件的 Duration 和 CloseDuration 字段语义正确
func TestAppTerminatedDurationFields(t *testing.T) {
	app := NewApplication()
	bindApplicationCloseReporterForTest(t, app)

	// 订阅 AppTerminated 事件
	var capturedEvent *event.AppTerminated
	bus := &mockEventDispatcher{handlers: make(map[string]func(any))}
	bus.handlers[event.EventAppTerminated] = func(payload any) {
		if e, ok := payload.(event.AppTerminated); ok {
			capturedEvent = &e
		}
	}
	if err := app.Container().Instance("event.dispatcher", bus); err != nil {
		t.Fatalf("bind event bus: %v", err)
	}

	// Boot 应用，记录启动时间
	if err := app.Boot(); err != nil {
		t.Fatalf("Boot failed: %v", err)
	}

	// 等待一小段时间，确保 Duration > 0
	time.Sleep(10 * time.Millisecond)

	// 关闭应用
	if err := app.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// 验证事件被捕获
	if capturedEvent == nil {
		t.Fatal("AppTerminated event not dispatched")
	}

	// Duration 应该 >= 10ms（应用生命周期总时长，包含了 sleep 的 10ms）
	if capturedEvent.Duration < 10*time.Millisecond {
		t.Errorf("Duration = %v, want >= 10ms (application lifetime)", capturedEvent.Duration)
	}

	// CloseDuration 应该 > 0（关闭流程实际执行了工作）
	if capturedEvent.CloseDuration <= 0 {
		t.Errorf("CloseDuration = %v, want > 0", capturedEvent.CloseDuration)
	}

	// Duration 应该 >= CloseDuration（总时长包含关闭时长）
	if capturedEvent.Duration < capturedEvent.CloseDuration {
		t.Errorf("Duration (%v) should be >= CloseDuration (%v)", capturedEvent.Duration, capturedEvent.CloseDuration)
	}
}

func TestConfigBaseProviderPreservesExplicitConfig(t *testing.T) {
	existing := configpkg.New()
	if err := existing.ReloadFromFile(filepath.Join(t.TempDir(), ".env")); err != nil {
		t.Fatalf("reload config failed: %v", err)
	}
	app := &Application{container: container.NewContainer()}
	if err := app.Container().Instance("config.default", existing); err != nil {
		t.Fatalf("seed config failed: %v", err)
	}
	if err := (configpkg.ServiceProvider{}).Register(app); err != nil {
		t.Fatalf("register config provider failed: %v", err)
	}

	raw, err := app.Make("config.default")
	if err != nil {
		t.Fatalf("Resolve config failed: %v", err)
	}
	got, ok := raw.(*configpkg.Config)
	if !ok {
		t.Fatalf("Resolve config type = %T, want *config.Config", raw)
	}
	if got != existing {
		t.Fatal("config base provider should preserve an explicitly registered config")
	}
}

func TestBuilderDeclaresFrameworkFacadeSlots(t *testing.T) {
	app := Configure().Create()
	t.Cleanup(func() { _ = app.Close() })
	if err := app.Boot(); err != nil {
		t.Fatalf("boot default providers: %v", err)
	}

	want := map[string]bool{
		"event.dispatcher":   false,
		"config.default":     false,
		"logger.manager":     false,
		"redis":              false,
		"redis.connection":   false,
		"cache.manager":      false,
		"cookie.queue":       false,
		"session.manager":    false,
		"filesystem.manager": false,
		"database.default":   false,
		"database.schema":    false,
	}
	for _, info := range container.List() {
		if _, ok := want[info.Key]; ok {
			want[info.Key] = true
		}
	}
	for key, ok := range want {
		if !ok {
			t.Fatalf("expected facade slot %q to be declared", key)
		}
	}
}

func TestNewApplicationDoesNotLoadFrameworkDefaultProviders(t *testing.T) {
	app := NewApplication()
	t.Cleanup(func() { _ = app.Close() })
	forbidden := map[string]bool{
		"redis":              true,
		"redis.connection":   true,
		"cache.manager":      true,
		"cookie.queue":       true,
		"session.manager":    true,
		"filesystem.manager": true,
		"database.default":   true,
		"database.schema":    true,
	}
	for _, info := range container.List() {
		if forbidden[info.Key] {
			t.Fatalf("NewApplication should not declare framework default slot %q", info.Key)
		}
	}
}

func TestConfigureWithBasePathUsesExplicitRoot(t *testing.T) {
	root := t.TempDir()

	app := Configure(root).Create()
	t.Cleanup(func() { _ = app.Close() })

	if got := app.BasePath(".env"); got != filepath.Join(root, ".env") {
		t.Fatalf("BasePath(.env) = %q, want %q", got, filepath.Join(root, ".env"))
	}
	if got := app.StoragePath("logs", "app.log"); got != filepath.Join(root, "storage", "logs", "app.log") {
		t.Fatalf("StoragePath(logs, app.log) = %q, want %q", got, filepath.Join(root, "storage", "logs", "app.log"))
	}
}

func TestInferBasePathRecognizesDeployedLayoutWithoutMarkers(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "storage"), 0o755); err != nil {
		t.Fatalf("mkdir storage: %v", err)
	}

	if got := pathutil.From(root, ""); got != root {
		t.Fatalf("path.From(deployed root) = %q, want %q", got, root)
	}
}

func TestInferBasePathUsesExecutableDirectoryAtProjectRoot(t *testing.T) {
	root := t.TempDir()
	workingDirectory := t.TempDir()

	if got := pathutil.From(workingDirectory, root); got != root {
		t.Fatalf("path.From(cwd, executable dir) = %q, want %q", got, root)
	}
}

func TestNilApplicationFacadesIsNil(t *testing.T) {
	var app *Application
	if got := app.Container(); got != nil {
		t.Fatalf("nil application Facades = %#v, want nil", got)
	}
}

func TestApplicationsOwnIndependentFacadeRegistries(t *testing.T) {
	container.SetProvider(nil)
	t.Cleanup(func() { container.SetProvider(nil) })

	var closed []string
	closeOption := container.WithCloser(func(p *applicationRegistryProbe) error {
		closed = append(closed, p.name)
		return nil
	})

	first := NewApplication()
	if first.Container() == nil {
		t.Fatal("first application should expose its facade registry")
	}
	if err := first.Container().Instance("foundation.application.registry.probe", &applicationRegistryProbe{name: "first"}, closeOption); err != nil {
		t.Fatalf("register first probe: %v", err)
	}

	second := NewApplication()
	if second.Container() == nil || second.Container() == first.Container() {
		t.Fatal("second application should own a different facade registry")
	}
	if second.Container().Resolved("foundation.application.registry.probe") {
		t.Fatal("second application reused first registry value")
	}
	if err := second.Container().Instance("foundation.application.registry.probe", &applicationRegistryProbe{name: "second"}, closeOption); err != nil {
		t.Fatalf("register second probe: %v", err)
	}

	if err := first.Close(); err != nil {
		t.Fatalf("close first application: %v", err)
	}
	if len(closed) != 1 || closed[0] != "first" {
		t.Fatalf("closed after first close = %v, want [first]", closed)
	}
	if got := container.Value[*applicationRegistryProbe]("foundation.application.registry.probe"); got == nil || got.name != "second" {
		t.Fatalf("current slot after first close = %#v, want second", got)
	}

	if err := second.Close(); err != nil {
		t.Fatalf("close second application: %v", err)
	}
	if len(closed) != 2 || closed[1] != "second" {
		t.Fatalf("closed after second close = %v, want [first second]", closed)
	}
	if _, err := container.Make[*applicationRegistryProbe]("foundation.application.registry.probe"); !errors.Is(err, container.ErrNoCurrentContainer) {
		t.Fatal("closing the current application should clear the current registry")
	}
}

func TestApplicationCloseRetryKeepsFailedFacadeResourcesObservable(t *testing.T) {
	container.SetProvider(nil)
	t.Cleanup(func() { container.SetProvider(nil) })

	app := NewApplication()
	closeErr := errors.New("facade close failed")
	var cleanupCalls int
	var closeCalls int
	var terminatingEvents int
	var terminatedEvents int

	app.RegisterCleanup(func(_ *Application) error {
		cleanupCalls++
		return nil
	})

	bus := event.Resolve()
	bus.Listen(event.EventAppTerminating, event.ListenerFunc(func(context.Context, event.Event) error {
		terminatingEvents++
		return nil
	}))
	bus.Listen(event.EventAppTerminated, event.ListenerFunc(func(context.Context, event.Event) error {
		terminatedEvents++
		return nil
	}))

	closeOption := container.WithCloser(func(*applicationCloseRetryProbe) error {
		closeCalls++
		if closeCalls == 1 {
			return closeErr
		}
		return nil
	})
	if err := app.Container().Instance("foundation.application.close.retry", &applicationCloseRetryProbe{}, closeOption); err != nil {
		t.Fatalf("register retry probe: %v", err)
	}

	if err := app.CloseContext(context.Background()); !errors.Is(err, closeErr) {
		t.Fatalf("first CloseContext error = %v, want %v", err, closeErr)
	}
	if cleanupCalls != 1 || closeCalls != 1 {
		t.Fatalf("after failed close cleanupCalls=%d closeCalls=%d, want 1/1", cleanupCalls, closeCalls)
	}
	if terminatingEvents != 1 || terminatedEvents != 0 {
		t.Fatalf("after failed close terminating=%d terminated=%d, want 1/0", terminatingEvents, terminatedEvents)
	}
	assertApplicationFacadeEntry(t, "foundation.application.close.retry", true)

	if err := app.Close(); err != nil {
		t.Fatalf("retry Close failed: %v", err)
	}
	if cleanupCalls != 1 || closeCalls != 2 {
		t.Fatalf("after retry cleanupCalls=%d closeCalls=%d, want 1/2", cleanupCalls, closeCalls)
	}
	if terminatingEvents != 1 || terminatedEvents != 1 {
		t.Fatalf("after retry terminating=%d terminated=%d, want 1/1", terminatingEvents, terminatedEvents)
	}
	if _, err := container.Make[*applicationCloseRetryProbe]("foundation.application.close.retry"); !errors.Is(err, container.ErrNoCurrentContainer) {
		t.Fatal("successful retry should clear the current registry")
	}
}

func TestApplicationFailedCloseAllowsPackageFacadeRetry(t *testing.T) {
	container.SetProvider(nil)
	t.Cleanup(func() { container.SetProvider(nil) })

	app := NewApplication()
	closeErr := errors.New("facade close failed")
	var closeCalls int
	closeOption := container.WithCloser(func(*applicationCloseRetryProbe) error {
		closeCalls++
		if closeCalls == 1 {
			return closeErr
		}
		return nil
	})
	if err := app.Container().Instance("foundation.application.package.retry", &applicationCloseRetryProbe{}, closeOption); err != nil {
		t.Fatalf("register retry probe: %v", err)
	}

	if err := app.CloseContext(context.Background()); !errors.Is(err, closeErr) {
		t.Fatalf("first CloseContext error = %v, want %v", err, closeErr)
	}
	if err := container.Close(context.Background()); err != nil {
		t.Fatalf("package facade retry failed: %v", err)
	}
	if closeCalls != 2 {
		t.Fatalf("closeCalls = %d, want 2", closeCalls)
	}
	assertApplicationFacadeEntry(t, "foundation.application.package.retry", false)
	if err := app.Close(); err != nil {
		t.Fatalf("final app Close failed: %v", err)
	}
	if _, err := container.Make[*applicationCloseRetryProbe]("foundation.application.package.retry"); !errors.Is(err, container.ErrNoCurrentContainer) {
		t.Fatal("final app Close should clear the current registry")
	}
}

type testProvider struct {
	calls []string
}

type closeContextProbe struct{}
type applicationRegistryProbe struct{ name string }
type applicationCloseRetryProbe struct{}

func withTestFacadeRegistry(t *testing.T) *container.Container {
	t.Helper()
	registry := container.NewContainer()
	container.SetProvider(func() *container.Container { return registry })
	t.Cleanup(func() { container.SetProvider(nil) })
	return registry
}

func (p *testProvider) Register(_ providercontract.Application) error {
	p.calls = append(p.calls, "register")
	return nil
}

func assertApplicationFacadeEntry(t *testing.T, key string, registered bool) {
	t.Helper()

	for _, info := range container.List() {
		if info.Key == key {
			if info.Registered != registered {
				t.Fatalf("entry %q registered=%v, want %v", key, info.Registered, registered)
			}
			return
		}
	}
	t.Fatalf("entry %q not found in container.List()", key)
}

func (p *testProvider) Boot(_ providercontract.Application) error {
	p.calls = append(p.calls, "boot")
	return nil
}
