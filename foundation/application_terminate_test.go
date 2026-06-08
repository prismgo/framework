package foundation

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prismgo/framework/container"
	providercontract "github.com/prismgo/framework/contracts/provider"
	"github.com/prismgo/framework/event"
	goexception "github.com/prismgo/framework/exception"
)

func TestApplicationCloseTerminatesProvidersBeforeCleanupAndFacade(t *testing.T) {
	withBaseProvidersForTest(t)
	app := NewApplication()
	var calls []string
	first := &terminableTestProvider{name: "first", calls: &calls}
	second := &terminableTestProvider{name: "second", calls: &calls}
	plain := repositoryProvider{name: "plain", callLog: &calls}

	if err := app.RegisterProvider(first); err != nil {
		t.Fatalf("register first: %v", err)
	}
	if err := app.RegisterProvider(plain); err != nil {
		t.Fatalf("register plain: %v", err)
	}
	if err := app.RegisterProvider(second); err != nil {
		t.Fatalf("register second: %v", err)
	}
	if err := app.Boot(); err != nil {
		t.Fatalf("boot app: %v", err)
	}
	app.RegisterCleanup(func(*Application) error {
		calls = append(calls, "cleanup")
		return nil
	})
	if err := app.Container().Instance("foundation.terminate.order", &closeContextProbe{}, container.WithCloser(func(*closeContextProbe) error {
		calls = append(calls, "facade.close")
		return nil
	})); err != nil {
		t.Fatalf("register facade probe: %v", err)
	}

	if err := app.CloseContext(context.Background()); err != nil {
		t.Fatalf("close app: %v", err)
	}

	assertSubsequence(t, calls, []string{
		"terminate:second",
		"terminate:first",
		"cleanup",
		"facade.close",
	})
}

func TestApplicationCloseTerminatesRegisteredProviderWhenBootFailed(t *testing.T) {
	withBaseProvidersForTest(t)
	app := NewApplication()
	bootErr := errors.New("boot failed")
	provider := &terminableTestProvider{name: "boot.failed", bootErr: bootErr}

	if err := app.RegisterProvider(provider); err != nil {
		t.Fatalf("register provider: %v", err)
	}
	if err := app.Boot(); !errors.Is(err, bootErr) {
		t.Fatalf("boot error = %v, want %v", err, bootErr)
	}
	if err := app.CloseContext(context.Background()); err != nil {
		t.Fatalf("close app: %v", err)
	}
	if provider.terminated != 1 {
		t.Fatalf("terminated count = %d, want 1", provider.terminated)
	}
}

func TestApplicationCloseSkipsNeverLoadedDeferredProvider(t *testing.T) {
	withBaseProvidersForTest(t)
	app := NewApplication()
	provider := &terminableDeferredProvider{
		deferredTestProvider: deferredTestProvider{name: "deferred.never.loaded", keys: []string{"deferred.never.loaded"}},
	}

	if err := app.RegisterProvider(provider); err != nil {
		t.Fatalf("register deferred provider: %v", err)
	}
	if err := app.Boot(); err != nil {
		t.Fatalf("boot app: %v", err)
	}
	if err := app.CloseContext(context.Background()); err != nil {
		t.Fatalf("close app: %v", err)
	}
	if provider.terminated != 0 {
		t.Fatalf("never-loaded deferred provider terminated %d times, want 0", provider.terminated)
	}
}

func TestApplicationCloseTerminatesLoadedDeferredProvider(t *testing.T) {
	withBaseProvidersForTest(t)
	app := NewApplication()
	provider := &terminableDeferredProvider{
		deferredTestProvider: deferredTestProvider{name: "deferred.loaded", keys: []string{"deferred.loaded"}},
	}

	if err := app.RegisterProvider(provider); err != nil {
		t.Fatalf("register deferred provider: %v", err)
	}
	if err := app.Boot(); err != nil {
		t.Fatalf("boot app: %v", err)
	}
	if _, err := container.Make[*deferredTestResource]("deferred.loaded"); err != nil {
		t.Fatalf("resolve deferred service: %v", err)
	}
	if err := app.CloseContext(context.Background()); err != nil {
		t.Fatalf("close app: %v", err)
	}
	if provider.terminated != 1 {
		t.Fatalf("loaded deferred provider terminated %d times, want 1", provider.terminated)
	}
}

func TestApplicationCloseRetryDoesNotRerunProviderTerminate(t *testing.T) {
	withBaseProvidersForTest(t)
	app := NewApplication()
	provider := &terminableTestProvider{name: "retry"}
	closeErr := errors.New("facade close failed")
	var closeCalls int

	if err := app.RegisterProvider(provider); err != nil {
		t.Fatalf("register provider: %v", err)
	}
	if err := app.Boot(); err != nil {
		t.Fatalf("boot app: %v", err)
	}
	if err := app.Container().Instance("foundation.terminate.retry", &closeContextProbe{}, container.WithCloser(func(*closeContextProbe) error {
		closeCalls++
		if closeCalls == 1 {
			return closeErr
		}
		return nil
	})); err != nil {
		t.Fatalf("register facade probe: %v", err)
	}

	if err := app.CloseContext(context.Background()); !errors.Is(err, closeErr) {
		t.Fatalf("first close error = %v, want %v", err, closeErr)
	}
	if err := app.CloseContext(context.Background()); err != nil {
		t.Fatalf("retry close: %v", err)
	}
	if provider.terminated != 1 || closeCalls != 2 {
		t.Fatalf("terminated=%d closeCalls=%d, want 1/2", provider.terminated, closeCalls)
	}
}

func TestApplicationCloseReportsCleanupErrorBeforeReportingFacadeClose(t *testing.T) {
	withBaseProvidersForTest(t)
	app := NewApplication()
	cleanupErr := errors.New("cleanup failed")
	var calls []string

	// 场景：cleanup 返回错误时，必须先通过仍存活的 reporting facade 上报，再关闭 reporting 资源。
	if err := app.Container().Instance("exception.handler", goexception.New(
		goexception.WithPanicStack(false),
		goexception.WithReporter(func(ctx any, err error, fields map[string]any) {
			if _, ok := ctx.(context.Context); !ok {
				t.Fatalf("report ctx = %T, want context.Context", ctx)
			}
			if !errors.Is(err, cleanupErr) {
				t.Fatalf("reported error = %v, want cleanup error", err)
			}
			if fields["phase"] != "application.close" {
				t.Fatalf("report phase = %v, want application.close", fields["phase"])
			}
			calls = append(calls, "report")
		}),
	), container.WithCloseGroup(container.CloseGroupReporting)); err != nil {
		t.Fatalf("register exception handler: %v", err)
	}
	if err := app.Container().Instance("foundation.reporting.close", &closeContextProbe{}, container.WithCloser(func(*closeContextProbe) error {
		calls = append(calls, "reporting.close")
		return nil
	}), container.WithCloseGroup(container.CloseGroupReporting)); err != nil {
		t.Fatalf("register reporting facade: %v", err)
	}
	app.RegisterCleanup(func(*Application) error {
		calls = append(calls, "cleanup")
		return cleanupErr
	})

	if err := app.CloseContext(context.Background()); !errors.Is(err, cleanupErr) {
		t.Fatalf("CloseContext error = %v, want cleanup error", err)
	}
	assertSubsequence(t, calls, []string{"cleanup", "report", "reporting.close"})
}

func TestApplicationCloseReportsProviderTerminateErrorBeforeReportingFacadeClose(t *testing.T) {
	withBaseProvidersForTest(t)
	app := NewApplication()
	terminateErr := errors.New("provider terminate failed")
	var calls []string

	// 场景：provider Terminate 错误属于普通关闭错误，reporting facade 关闭前应能收到该错误。
	if err := app.Container().Instance("exception.handler", goexception.New(
		goexception.WithPanicStack(false),
		goexception.WithReporter(func(ctx any, err error, fields map[string]any) {
			if _, ok := ctx.(context.Context); !ok {
				t.Fatalf("report ctx = %T, want context.Context", ctx)
			}
			if !errors.Is(err, terminateErr) {
				t.Fatalf("reported error = %v, want provider terminate error", err)
			}
			if fields["phase"] != "application.close" {
				t.Fatalf("report phase = %v, want application.close", fields["phase"])
			}
			calls = append(calls, "report")
		}),
	), container.WithCloseGroup(container.CloseGroupReporting)); err != nil {
		t.Fatalf("register exception handler: %v", err)
	}
	provider := &terminableTestProvider{
		name:  "terminate.error",
		calls: &calls,
		terminate: func(context.Context) error {
			return terminateErr
		},
	}
	if err := app.RegisterProvider(provider); err != nil {
		t.Fatalf("register provider: %v", err)
	}
	if err := app.Boot(); err != nil {
		t.Fatalf("boot app: %v", err)
	}
	if err := app.Container().Instance("foundation.reporting.after-terminate-error", &closeContextProbe{}, container.WithCloser(func(*closeContextProbe) error {
		calls = append(calls, "reporting.close")
		return nil
	}), container.WithCloseGroup(container.CloseGroupReporting)); err != nil {
		t.Fatalf("register reporting facade: %v", err)
	}

	err := app.CloseContext(context.Background())
	if !errors.Is(err, terminateErr) {
		t.Fatalf("CloseContext error = %v, want provider terminate error", err)
	}
	assertSubsequence(t, calls, []string{"terminate:terminate.error", "report", "reporting.close"})
}

func TestApplicationCloseReportsNormalFacadeErrorAndRetryDoesNotReportAgain(t *testing.T) {
	withBaseProvidersForTest(t)
	app := NewApplication()
	closeErr := errors.New("normal facade close failed")
	var calls []string
	var reportCalls int
	var closeCalls int

	// 场景：普通 facade 关闭失败只在首次关闭上报；重试只推进剩余资源，不重复上报旧错误。
	if err := app.Container().Instance("exception.handler", goexception.New(
		goexception.WithPanicStack(false),
		goexception.WithReporter(func(ctx any, err error, fields map[string]any) {
			reportCalls++
			if !errors.Is(err, closeErr) {
				t.Fatalf("reported error = %v, want normal facade error", err)
			}
			calls = append(calls, "report")
		}),
	), container.WithCloseGroup(container.CloseGroupReporting)); err != nil {
		t.Fatalf("register exception handler: %v", err)
	}
	if err := app.Container().Instance("foundation.normal.close.error", &closeContextProbe{}, container.WithCloser(func(*closeContextProbe) error {
		closeCalls++
		calls = append(calls, "normal.close")
		if closeCalls == 1 {
			return closeErr
		}
		return nil
	})); err != nil {
		t.Fatalf("register normal facade: %v", err)
	}
	if err := app.Container().Instance("foundation.reporting.after-normal-error", &closeContextProbe{}, container.WithCloser(func(*closeContextProbe) error {
		calls = append(calls, "reporting.close")
		return nil
	}), container.WithCloseGroup(container.CloseGroupReporting)); err != nil {
		t.Fatalf("register reporting facade: %v", err)
	}

	if err := app.CloseContext(context.Background()); !errors.Is(err, closeErr) {
		t.Fatalf("first CloseContext error = %v, want normal facade error", err)
	}
	assertSubsequence(t, calls, []string{"normal.close", "report", "reporting.close"})
	assertRegistryEntryRegistered(t, app.container, "foundation.normal.close.error", true)
	if reportCalls != 1 {
		t.Fatalf("reportCalls after first close = %d, want 1", reportCalls)
	}

	if err := app.CloseContext(context.Background()); err != nil {
		t.Fatalf("retry CloseContext failed: %v", err)
	}
	assertRegistryEntryRegistered(t, app.container, "foundation.normal.close.error", false)
	if reportCalls != 1 {
		t.Fatalf("reportCalls after retry = %d, want still 1", reportCalls)
	}
}

func TestApplicationCloseReporterPanicIsReturnedWithoutRecursiveReport(t *testing.T) {
	withBaseProvidersForTest(t)
	app := NewApplication()
	cleanupErr := errors.New("cleanup failed")
	var reportCalls int

	// 场景：reporter 自身 panic 进入 CloseContext 返回值，但不能再次递归触发异常上报。
	if err := app.Container().Instance("exception.handler", goexception.New(
		goexception.WithPanicStack(false),
		goexception.WithReporter(func(ctx any, err error, fields map[string]any) {
			reportCalls++
			panic("reporter failed")
		}),
	), container.WithCloseGroup(container.CloseGroupReporting)); err != nil {
		t.Fatalf("register exception handler: %v", err)
	}
	app.RegisterCleanup(func(*Application) error {
		return cleanupErr
	})

	err := app.CloseContext(context.Background())
	if !errors.Is(err, cleanupErr) || !strings.Contains(err.Error(), "report close error: reporter failed") {
		t.Fatalf("CloseContext error = %v, want cleanup and report panic errors", err)
	}
	if reportCalls != 1 {
		t.Fatalf("reportCalls = %d, want 1", reportCalls)
	}
}

func TestApplicationCloseDoesNotReportReportingFacadeCloseError(t *testing.T) {
	withBaseProvidersForTest(t)
	app := NewApplication()
	closeErr := errors.New("reporting facade close failed")
	var reportCalls int

	// 场景：reporting facade 自身关闭失败只作为返回错误，不递归调用 reporter 上报自己。
	if err := app.Container().Instance("exception.handler", goexception.New(
		goexception.WithPanicStack(false),
		goexception.WithReporter(func(ctx any, err error, fields map[string]any) {
			reportCalls++
		}),
	), container.WithCloseGroup(container.CloseGroupReporting)); err != nil {
		t.Fatalf("register exception handler: %v", err)
	}
	if err := app.Container().Instance("foundation.reporting.close.error", &closeContextProbe{}, container.WithCloser(func(*closeContextProbe) error {
		return closeErr
	}), container.WithCloseGroup(container.CloseGroupReporting)); err != nil {
		t.Fatalf("register reporting facade: %v", err)
	}

	err := app.CloseContext(context.Background())
	if !errors.Is(err, closeErr) {
		t.Fatalf("CloseContext error = %v, want reporting facade close error", err)
	}
	if reportCalls != 0 {
		t.Fatalf("reportCalls = %d, want 0", reportCalls)
	}
}

func TestApplicationCloseSerializesConcurrentCalls(t *testing.T) {
	withBaseProvidersForTest(t)
	app := NewApplication()
	entered := make(chan struct{})
	release := make(chan struct{})
	provider := &terminableTestProvider{
		name: "concurrent",
		terminate: func(context.Context) error {
			close(entered)
			<-release
			return nil
		},
	}
	var cleanupCalls int

	if err := app.RegisterProvider(provider); err != nil {
		t.Fatalf("register provider: %v", err)
	}
	if err := app.Boot(); err != nil {
		t.Fatalf("boot app: %v", err)
	}
	app.RegisterCleanup(func(*Application) error {
		cleanupCalls++
		return nil
	})

	errs := make(chan error, 2)
	go func() { errs <- app.CloseContext(context.Background()) }()
	<-entered
	go func() { errs <- app.CloseContext(context.Background()) }()
	close(release)

	if err := <-errs; err != nil {
		t.Fatalf("first close returned error: %v", err)
	}
	if err := <-errs; err != nil {
		t.Fatalf("second close returned error: %v", err)
	}
	if provider.terminated != 1 || cleanupCalls != 1 {
		t.Fatalf("terminated=%d cleanupCalls=%d, want 1/1", provider.terminated, cleanupCalls)
	}
}

func TestApplicationConcurrentCloseWaitsForActiveCloseAttempt(t *testing.T) {
	// 需求背景：两个外部 goroutine 同时关闭同一个 Application 时，第二个调用方不能提前观察到半关闭状态。
	// 逻辑说明：第一个 CloseContext 在 cleanup 中阻塞；第二个 CloseContext 必须等 release 后才能返回。
	tests := []struct {
		name         string
		secondCtx    func(*testing.T) context.Context
		waitBeforeOk time.Duration
	}{
		{
			name: "background context",
			secondCtx: func(*testing.T) context.Context {
				return context.Background()
			},
			waitBeforeOk: 50 * time.Millisecond,
		},
		{
			name: "canceled context",
			secondCtx: func(t *testing.T) context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				t.Cleanup(cancel)
				return ctx
			},
			waitBeforeOk: 50 * time.Millisecond,
		},
		{
			name: "deadline exceeded context",
			secondCtx: func(t *testing.T) context.Context {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
				t.Cleanup(cancel)
				return ctx
			},
			waitBeforeOk: 50 * time.Millisecond,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			withBaseProvidersForTest(t)
			app := NewApplication()
			entered := make(chan struct{})
			release := make(chan struct{})
			secondDone := make(chan error, 1)
			firstDone := make(chan error, 1)

			app.RegisterCleanup(func(*Application) error {
				close(entered)
				<-release
				return nil
			})

			go func() { firstDone <- app.CloseContext(context.Background()) }()
			<-entered
			go func() { secondDone <- app.CloseContext(tt.secondCtx(t)) }()

			select {
			case err := <-secondDone:
				t.Fatalf("second CloseContext returned before active close finished: %v", err)
			case <-time.After(tt.waitBeforeOk):
			}

			close(release)
			if err := <-firstDone; err != nil {
				t.Fatalf("first CloseContext error = %v", err)
			}
			if err := <-secondDone; err != nil {
				t.Fatalf("second CloseContext error = %v", err)
			}
		})
	}
}

func TestApplicationRegisterCleanupDuringCloseIsIgnored(t *testing.T) {
	// 需求背景：关闭链路开始后 cleanup 列表必须成为快照，避免并发追加导致 slice 竞争或本轮关闭语义漂移。
	// 行为断言：closing 期间注册的新 cleanup 不会执行；race 检测负责覆盖 RegisterCleanup 与 CloseContext 的并发读写。
	withBaseProvidersForTest(t)
	app := NewApplication()
	entered := make(chan struct{})
	release := make(chan struct{})
	registered := make(chan struct{})
	var lateCleanupRan bool

	app.RegisterCleanup(func(*Application) error {
		close(entered)
		<-release
		return nil
	})
	go func() {
		<-entered
		app.RegisterCleanup(func(*Application) error {
			lateCleanupRan = true
			return nil
		})
		close(registered)
	}()

	done := make(chan error, 1)
	go func() { done <- app.CloseContext(context.Background()) }()
	<-registered
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("CloseContext error = %v", err)
	}
	if lateCleanupRan {
		t.Fatal("cleanup registered after closing started should not run")
	}
}

func TestApplicationCloseRejectsWhileBootInProgress(t *testing.T) {
	// 设计思路：Boot 与 Close 都会改变 provider、事件和 container 生命周期状态，必须互斥。
	// 参数用途：entered/release 用来把 provider Boot 固定在进行中状态，从公开 CloseContext 入口验证错误边界。
	withBaseProvidersForTest(t)
	app := NewApplication()
	entered := make(chan struct{})
	release := make(chan struct{})
	provider := repositoryProvider{
		name: "slow.boot",
		boot: func(*Application) error {
			close(entered)
			<-release
			return nil
		},
	}
	if err := app.RegisterProvider(provider); err != nil {
		t.Fatalf("register provider: %v", err)
	}

	bootDone := make(chan error, 1)
	go func() { bootDone <- app.Boot() }()
	<-entered

	if err := app.CloseContext(context.Background()); err == nil || !strings.Contains(err.Error(), "application boot is in progress") {
		t.Fatalf("CloseContext during boot error = %v, want boot in progress error", err)
	}
	close(release)
	if err := <-bootDone; err != nil {
		t.Fatalf("Boot error = %v", err)
	}
	if err := app.CloseContext(context.Background()); err != nil {
		t.Fatalf("CloseContext after boot error = %v", err)
	}
}

func TestApplicationRejectsProviderWorkAfterClosingStarts(t *testing.T) {
	withBaseProvidersForTest(t)
	app := NewApplication()
	var registerErr error
	var resolveErr error
	provider := &terminableTestProvider{
		name: "closing.boundary",
		terminate: func(context.Context) error {
			registerErr = app.RegisterProvider(repositoryProvider{name: "late"})
			return nil
		},
	}
	deferred := &deferredTestProvider{name: "closing.deferred", keys: []string{"closing.deferred"}}

	if err := app.RegisterProvider(provider); err != nil {
		t.Fatalf("register provider: %v", err)
	}
	if err := app.RegisterProvider(deferred); err != nil {
		t.Fatalf("register deferred provider: %v", err)
	}
	if err := app.Boot(); err != nil {
		t.Fatalf("boot app: %v", err)
	}
	app.RegisterCleanup(func(*Application) error {
		_, resolveErr = container.Make[*deferredTestResource]("closing.deferred")
		return nil
	})

	if err := app.CloseContext(context.Background()); err != nil {
		t.Fatalf("close app: %v", err)
	}
	if registerErr == nil || !strings.Contains(registerErr.Error(), "application is closing") {
		t.Fatalf("register after closing error = %v, want closing error", registerErr)
	}
	if resolveErr == nil || !strings.Contains(resolveErr.Error(), "application is closing") {
		t.Fatalf("deferred load after closing error = %v, want closing error", resolveErr)
	}
	if err := app.RegisterProvider(repositoryProvider{name: "after.close"}); err == nil || !strings.Contains(err.Error(), "application is closing") {
		t.Fatalf("RegisterProvider after close error = %v, want closing error", err)
	}
}

func TestApplicationCloseUsesUncanceledContextWhenInputAlreadyCanceled(t *testing.T) {
	withBaseProvidersForTest(t)
	app := NewApplication()
	var providerCtxErr error
	var facadeCtxErr error
	provider := &terminableTestProvider{
		name: "canceled.input",
		terminate: func(ctx context.Context) error {
			providerCtxErr = ctx.Err()
			return nil
		},
	}

	if err := app.RegisterProvider(provider); err != nil {
		t.Fatalf("register provider: %v", err)
	}
	if err := app.Boot(); err != nil {
		t.Fatalf("boot app: %v", err)
	}
	if err := app.Container().Instance("foundation.terminate.canceled", &closeContextProbe{}, container.WithContextCloser(func(ctx context.Context, _ *closeContextProbe) error {
		facadeCtxErr = ctx.Err()
		return nil
	})); err != nil {
		t.Fatalf("register facade probe: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := app.CloseContext(ctx); err != nil {
		t.Fatalf("close app: %v", err)
	}
	if providerCtxErr != nil || facadeCtxErr != nil {
		t.Fatalf("close context errors provider=%v facade=%v, want nil/nil", providerCtxErr, facadeCtxErr)
	}
}

func TestApplicationCloseContinuesAfterMidShutdownCancelAndJoinsErrors(t *testing.T) {
	withBaseProvidersForTest(t)
	app := NewApplication()
	firstErr := errors.New("first terminate failed")
	secondErr := errors.New("second terminate failed")
	cleanupErr := errors.New("cleanup failed")
	var calls []string
	var facadeCtxErr error

	ctx, cancel := context.WithCancel(context.Background())
	first := &terminableTestProvider{
		name: "first",
		terminate: func(context.Context) error {
			calls = append(calls, "terminate:first")
			return firstErr
		},
	}
	second := &terminableTestProvider{
		name: "second",
		terminate: func(context.Context) error {
			calls = append(calls, "terminate:second")
			cancel()
			return secondErr
		},
	}

	if err := app.RegisterProvider(first); err != nil {
		t.Fatalf("register first: %v", err)
	}
	if err := app.RegisterProvider(second); err != nil {
		t.Fatalf("register second: %v", err)
	}
	if err := app.Boot(); err != nil {
		t.Fatalf("boot app: %v", err)
	}
	app.RegisterCleanup(func(*Application) error {
		calls = append(calls, "cleanup")
		return cleanupErr
	})
	if err := app.Container().Instance("foundation.terminate.mid-cancel", &closeContextProbe{}, container.WithContextCloser(func(ctx context.Context, _ *closeContextProbe) error {
		calls = append(calls, "facade.close")
		facadeCtxErr = ctx.Err()
		return nil
	})); err != nil {
		t.Fatalf("register facade probe: %v", err)
	}

	err := app.CloseContext(ctx)
	if !errors.Is(err, firstErr) || !errors.Is(err, secondErr) || !errors.Is(err, cleanupErr) {
		t.Fatalf("close error = %v, want all joined errors", err)
	}
	if facadeCtxErr != nil {
		t.Fatalf("facade close context error = %v, want nil", facadeCtxErr)
	}
	assertSubsequence(t, calls, []string{"terminate:second", "terminate:first", "cleanup", "facade.close"})
}

func TestApplicationCloseAllowsReentrantCloseFromTerminatedListener(t *testing.T) {
	app := NewApplication()
	bus, err := resolveEventDispatcher(app.container)
	if err != nil {
		t.Fatalf("resolve event bus: %v", err)
	}

	var reentrantErr error
	bus.Listen(event.EventAppTerminated, event.ListenerFunc(func(context.Context, event.Event) error {
		reentrantErr = app.CloseContext(context.Background())
		return nil
	}))

	done := make(chan error, 1)
	go func() {
		done <- app.CloseContext(context.Background())
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("CloseContext error = %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("CloseContext deadlocked when AppTerminated listener called CloseContext")
	}
	if reentrantErr != nil {
		t.Fatalf("reentrant CloseContext error = %v", reentrantErr)
	}
}

func TestApplicationCloseAllowsReentrantCloseFromTerminatingListener(t *testing.T) {
	app := NewApplication()
	bus, err := resolveEventDispatcher(app.container)
	if err != nil {
		t.Fatalf("resolve event bus: %v", err)
	}

	var reentrantErr error
	bus.Listen(event.EventAppTerminating, event.ListenerFunc(func(context.Context, event.Event) error {
		reentrantErr = app.CloseContext(context.Background())
		return nil
	}))

	done := make(chan error, 1)
	go func() {
		done <- app.CloseContext(context.Background())
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("CloseContext error = %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("CloseContext deadlocked when AppTerminating listener called CloseContext")
	}
	if reentrantErr != nil {
		t.Fatalf("reentrant CloseContext error = %v", reentrantErr)
	}
}

func TestApplicationCloseAllowsReentrantCloseFromCleanup(t *testing.T) {
	withBaseProvidersForTest(t)
	app := NewApplication()
	var cleanupCalls int
	var reentrantErr error
	app.RegisterCleanup(func(*Application) error {
		cleanupCalls++
		reentrantErr = app.CloseContext(context.Background())
		return nil
	})

	done := make(chan error, 1)
	go func() {
		done <- app.CloseContext(context.Background())
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("CloseContext error = %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("CloseContext deadlocked when cleanup called CloseContext")
	}
	if reentrantErr != nil {
		t.Fatalf("reentrant CloseContext error = %v", reentrantErr)
	}
	if cleanupCalls != 1 {
		t.Fatalf("cleanupCalls = %d, want 1", cleanupCalls)
	}
}

type terminableTestProvider struct {
	name       string
	calls      *[]string
	bootErr    error
	terminated int
	mu         sync.Mutex
	terminate  func(context.Context) error
}

func (p *terminableTestProvider) Name() string { return p.name }

func (p *terminableTestProvider) Register(providercontract.Application) error { return nil }

func (p *terminableTestProvider) Boot(providercontract.Application) error { return p.bootErr }

func (p *terminableTestProvider) Terminate(ctx context.Context) error {
	p.mu.Lock()
	p.terminated++
	p.mu.Unlock()
	if p.calls != nil {
		*p.calls = append(*p.calls, "terminate:"+p.name)
	}
	if p.terminate != nil {
		return p.terminate(ctx)
	}
	return nil
}

func assertRegistryEntryRegistered(t *testing.T, registry *container.Container, key string, registered bool) {
	t.Helper()

	for _, info := range registry.List() {
		if info.Key == key {
			if info.Registered != registered {
				t.Fatalf("entry %q registered=%v, want %v", key, info.Registered, registered)
			}
			return
		}
	}
	t.Fatalf("entry %q not found in application facade registry", key)
}

type terminableDeferredProvider struct {
	deferredTestProvider
	terminated int
}

func (p *terminableDeferredProvider) Terminate(context.Context) error {
	p.terminated++
	return nil
}
