package http

import (
	"context"
	"errors"
	"net"
	"net/http"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/prismgo/framework/event"
)

func TestListenAndServeGracefulContextCancelShutsDown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	bus := event.New()
	events := make(chan string, 4)
	bus.Listen("server.*", event.ListenerFunc(func(_ context.Context, ev event.Event) error {
		events <- ev.Name()
		return nil
	}))

	server := &http.Server{
		Addr:    "127.0.0.1:0",
		Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }),
	}

	done := make(chan error, 1)
	go func() {
		done <- ListenAndServeGracefulContext(ctx, server, time.Second, WithDispatcher(bus))
	}()

	waitForEvent(t, events, event.EventServerStarting)
	waitForEvent(t, events, event.EventServerStarted)
	cancel()

	select {
	case err := <-done:
		if err != http.ErrServerClosed {
			t.Fatalf("error = %v, want %v", err, http.ErrServerClosed)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("server did not shut down after context cancellation")
	}

	waitForEvent(t, events, event.EventServerStopping)
	waitForEvent(t, events, event.EventServerStopped)
}

func TestListenAndServeGracefulContextUsesCancelCauseAsStoppingReason(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	bus := event.New()
	stoppingReason := make(chan string, 1)
	server := &http.Server{
		Addr:    "127.0.0.1:0",
		Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }),
	}
	bus.ListenFunc(event.EventServerStarting, func(_ context.Context, _ event.Event) error {
		cancel(errors.New("app shutdown"))
		return nil
	})
	bus.ListenFunc(event.EventServerStopping, func(_ context.Context, ev event.Event) error {
		stoppingReason <- ev.(event.ServerStopping).Reason
		return nil
	})

	err := ListenAndServeGracefulContext(ctx, server, time.Second, WithDispatcher(bus))
	if err != http.ErrServerClosed {
		t.Fatalf("error = %v, want %v", err, http.ErrServerClosed)
	}
	select {
	case got := <-stoppingReason:
		if got != "app shutdown" {
			t.Fatalf("server stopping reason = %q, want %q", got, "app shutdown")
		}
	default:
		t.Fatal("expected server stopping reason to be recorded")
	}
}

func TestListenAndServeGracefulContextExternalShutdownDoesNotBlock(t *testing.T) {
	bus := event.New()
	events := make(chan string, 4)
	bus.Listen("server.*", event.ListenerFunc(func(_ context.Context, ev event.Event) error {
		events <- ev.Name()
		return nil
	}))

	server := &http.Server{
		Addr:    "127.0.0.1:0",
		Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }),
	}

	done := make(chan error, 1)
	go func() {
		done <- ListenAndServeGracefulContext(context.Background(), server, time.Second, WithDispatcher(bus))
	}()

	waitForEvent(t, events, event.EventServerStarting)
	waitForEvent(t, events, event.EventServerStarted)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("external shutdown failed: %v", err)
	}

	select {
	case err := <-done:
		if err != http.ErrServerClosed {
			t.Fatalf("error = %v, want %v", err, http.ErrServerClosed)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("ListenAndServeGracefulContext blocked after external shutdown")
	}

	waitForEvent(t, events, event.EventServerStopped)
}

func TestListenAndServeGracefulContextListenErrorReturnsImmediately(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port failed: %v", err)
	}
	defer func() {
		if err := listener.Close(); err != nil {
			t.Errorf("close listener: %v", err)
		}
	}()

	server := &http.Server{
		Addr:    listener.Addr().String(),
		Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }),
	}

	err = ListenAndServeGracefulContext(context.Background(), server, time.Second, WithDispatcher(event.New()))
	if err == nil {
		t.Fatal("expected listen error, got nil")
	}
	if errors.Is(err, http.ErrServerClosed) {
		t.Fatalf("error = %v, want listen error", err)
	}
}

func TestListenAndServeGracefulWrapperAllowsExplicitShutdown(t *testing.T) {
	registry := useHTTPTestContainer(t)
	if err := registry.Instance("event.dispatcher", event.New()); err != nil {
		t.Fatalf("bind event dispatcher: %v", err)
	}

	server := &http.Server{
		Addr:    "127.0.0.1:0",
		Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }),
	}

	done := make(chan error, 1)
	go func() {
		done <- ListenAndServeGraceful(server, time.Second)
	}()

	select {
	case err := <-done:
		t.Fatalf("server exited before explicit shutdown: %v", err)
	case <-time.After(150 * time.Millisecond):
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("server shutdown failed: %v", err)
	}

	select {
	case err := <-done:
		if err != http.ErrServerClosed {
			t.Fatalf("error = %v, want %v", err, http.ErrServerClosed)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("server did not stop after explicit shutdown")
	}
}

func TestListenAndServeGracefulContextDefaultDoesNotListenForProcessSignals(t *testing.T) {
	server := &http.Server{
		Addr:    "127.0.0.1:0",
		Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }),
	}

	done := make(chan error, 1)
	go func() {
		done <- ListenAndServeGracefulContext(context.Background(), server, time.Second, WithDispatcher(event.New()))
	}()

	select {
	case err := <-done:
		t.Fatalf("server exited before explicit shutdown: %v", err)
	case <-time.After(150 * time.Millisecond):
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("server shutdown failed: %v", err)
	}

	select {
	case err := <-done:
		if err != http.ErrServerClosed {
			t.Fatalf("error = %v, want %v", err, http.ErrServerClosed)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("server did not stop after explicit shutdown")
	}
}

func TestListenAndServeGracefulContextExplicitSignalAdapterStillWorks(t *testing.T) {
	server := &http.Server{
		Addr:    "127.0.0.1:0",
		Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }),
	}

	done := make(chan error, 1)
	go func() {
		done <- ListenAndServeGracefulContext(context.Background(), server, time.Second, WithDispatcher(event.New()), WithShutdownSignals(syscall.SIGTERM))
	}()

	select {
	case err := <-done:
		t.Fatalf("server exited before signal: %v", err)
	case <-time.After(150 * time.Millisecond):
	}

	process, err := os.FindProcess(os.Getpid())
	if err != nil {
		t.Fatalf("FindProcess failed: %v", err)
	}
	if err := process.Signal(syscall.SIGTERM); err != nil {
		t.Skipf("send SIGTERM failed: %v", err)
	}

	select {
	case err := <-done:
		if err != http.ErrServerClosed {
			t.Fatalf("error = %v, want %v", err, http.ErrServerClosed)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("server did not stop after SIGTERM")
	}
}

func TestWithShutdownSignalsDefaultsToTermAndInt(t *testing.T) {
	cfg := &serveConfig{}
	WithShutdownSignals()(cfg)
	if len(cfg.shutdownSignal) != 2 {
		t.Fatalf("signal count = %d, want 2", len(cfg.shutdownSignal))
	}
	if cfg.shutdownSignal[0] != syscall.SIGTERM || cfg.shutdownSignal[1] != syscall.SIGINT {
		t.Fatalf("shutdown signals = %v, want [%v %v]", cfg.shutdownSignal, syscall.SIGTERM, syscall.SIGINT)
	}
}

func TestListenAndServeGracefulContextRunsStartedHook(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hookRan := make(chan struct{}, 1)
	bus := event.New()
	bus.ListenFunc(event.EventServerStarted, func(_ context.Context, _ event.Event) error {
		select {
		case <-hookRan:
		default:
			t.Error("server started event dispatched before started hook")
		}
		cancel()
		return nil
	})

	server := &http.Server{
		Addr:    "127.0.0.1:0",
		Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }),
	}

	err := ListenAndServeGracefulContext(ctx, server, time.Second, WithDispatcher(bus), WithStartedHook(func() {
		hookRan <- struct{}{}
	}))
	if err != http.ErrServerClosed {
		t.Fatalf("error = %v, want %v", err, http.ErrServerClosed)
	}
}

type contractOnlyDispatcher struct{}

func (contractOnlyDispatcher) Listen(string, event.Listener)                               {}
func (contractOnlyDispatcher) ListenFunc(string, func(context.Context, event.Event) error) {}
func (contractOnlyDispatcher) Subscribe(event.Subscriber)                                 {}
func (contractOnlyDispatcher) Forget(string)                                              {}
func (contractOnlyDispatcher) Has(string) bool                                            { return false }
func (contractOnlyDispatcher) Dispatch(context.Context, event.Event)                      {}

func TestListenAndServeGracefulContextPanicsWithoutConcreteDispatcher(t *testing.T) {
	registry := useHTTPTestContainer(t)
	if err := registry.Instance("event.dispatcher", contractOnlyDispatcher{}); err != nil {
		t.Fatalf("bind event dispatcher: %v", err)
	}

	defer func() {
		if recovered := recover(); recovered == nil {
			t.Fatal("expected panic when resolved dispatcher is not concrete event dispatcher")
		}
	}()

	_ = ListenAndServeGracefulContext(context.Background(), &http.Server{}, time.Second)
}

func TestListenAndServeGracefulContextAcceptsNilContext(t *testing.T) {
	server := &http.Server{
		Addr:    "127.0.0.1:0",
		Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }),
	}

	err := ListenAndServeGracefulContext(nil, server, time.Second, WithDispatcher(event.New()), WithStartedHook(func() {
		go func() {
			_ = server.Shutdown(context.Background())
		}()
	}))
	if err != http.ErrServerClosed {
		t.Fatalf("error = %v, want %v", err, http.ErrServerClosed)
	}
}

func nilContext() context.Context { return nil }

func TestShutdownReasonFromContextBranches(t *testing.T) {
	if got := shutdownReasonFromContext(nilContext()); got != context.Canceled.Error() {
		t.Fatalf("nil context reason = %q, want %q", got, context.Canceled.Error())
	}

	ctxWithCause, cancelWithCause := context.WithCancelCause(context.Background())
	cancelWithCause(errors.New("cause reason"))
	if got := shutdownReasonFromContext(ctxWithCause); got != "cause reason" {
		t.Fatalf("context cause reason = %q, want %q", got, "cause reason")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if got := shutdownReasonFromContext(ctx); got != context.Canceled.Error() {
		t.Fatalf("canceled context reason = %q, want %q", got, context.Canceled.Error())
	}

	if got := shutdownReasonFromContext(context.Background()); got != context.Canceled.Error() {
		t.Fatalf("background context reason = %q, want %q", got, context.Canceled.Error())
	}
}

func TestNewServerAppliesDefaultTimeout(t *testing.T) {
	server := NewServer("127.0.0.1:8080", http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}), 0)
	if server.Addr != "127.0.0.1:8080" {
		t.Fatalf("addr = %q, want %q", server.Addr, "127.0.0.1:8080")
	}
	if server.ReadTimeout != 15*time.Second || server.WriteTimeout != 15*time.Second {
		t.Fatalf("timeouts = %s/%s, want 15s/15s", server.ReadTimeout, server.WriteTimeout)
	}
}

func TestShutdownServerReturnsServeError(t *testing.T) {
	bus := event.New()
	serveErr := make(chan error, 1)
	serveErr <- errors.New("serve failed")
	server := &http.Server{}

	err := shutdownServer(context.Background(), bus, server, time.Second, "manual shutdown", serveErr)
	if err == nil || err.Error() != "serve failed" {
		t.Fatalf("shutdownServer error = %v, want serve failed", err)
	}
}

func TestFindProcessByPortPlaceholder(t *testing.T) {
	pid, err := FindProcessByPort("8080")
	if err != nil {
		t.Fatalf("FindProcessByPort error = %v", err)
	}
	if pid != 0 {
		t.Fatalf("pid = %d, want 0", pid)
	}
}

func TestSendSignalAllowsSignalZero(t *testing.T) {
	err := SendSignal(os.Getpid(), syscall.Signal(0))
	if err != nil && err.Error() != "not supported by windows" {
		t.Fatalf("SendSignal error = %v", err)
	}
}

func TestShutdownServerTimeoutForcesClose(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	server := &http.Server{
		Addr: "127.0.0.1:0",
		Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			close(started)
			<-release
			w.WriteHeader(http.StatusOK)
		}),
	}
	listener, err := net.Listen("tcp", server.Addr)
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- server.Serve(listener)
	}()
	t.Cleanup(func() {
		close(release)
		_ = server.Close()
	})

	clientDone := make(chan error, 1)
	go func() {
		resp, err := http.Get("http://" + listener.Addr().String())
		if err == nil && resp != nil {
			_ = resp.Body.Close()
		}
		clientDone <- err
	}()

	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for request to enter handler")
	}

	err = shutdownServer(context.Background(), event.New(), server, time.Nanosecond, "timeout", serveErr)
	if err == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("shutdownServer error = %v, want %v", err, context.DeadlineExceeded)
	}

	select {
	case <-clientDone:
	case <-time.After(3 * time.Second):
		t.Fatal("client request did not finish after forced close")
	}
}

func TestDispatchServerStoppedIncludesErrorText(t *testing.T) {
	bus := event.New()
	wantErr := errors.New("shutdown failed")
	var captured event.ServerStopped
	bus.ListenFunc(event.EventServerStopped, func(_ context.Context, ev event.Event) error {
		captured = ev.(event.ServerStopped)
		return nil
	})

	dispatchServerStopped(context.Background(), bus, "127.0.0.1:0", time.Millisecond, wantErr)

	if captured.Error != wantErr.Error() || captured.Duration != time.Millisecond {
		t.Fatalf("server stopped event = %+v, want error %q", captured, wantErr.Error())
	}
}

func waitForEvent(t *testing.T, events <-chan string, want string) {
	t.Helper()
	select {
	case got := <-events:
		if got != want {
			t.Fatalf("event = %s, want %s", got, want)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for %s", want)
	}
}
