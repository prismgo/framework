package http

import (
	"context"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prismgo/framework/event"
)

// NewServer 构造标准 http.Server，预设读写超时，避免长连接占用进程。
// timeout <= 0 时使用 15s 默认值。
func NewServer(addr string, handler http.Handler, timeout time.Duration) *http.Server {
	if timeout <= 0 {
		timeout = 15 * time.Second
	}
	return &http.Server{
		Addr:         addr,
		Handler:      handler,
		ReadTimeout:  timeout,
		WriteTimeout: timeout,
	}
}

// NewServerWithConfig 根据 ServerConfig 构造标准 http.Server。
func NewServerWithConfig(handler http.Handler, cfg ServerConfig) *http.Server {
	maxHeaderBytes := cfg.MaxHeaderBytes
	if maxHeaderBytes <= 0 {
		maxHeaderBytes = defaultServerMaxHeaderBytes
	}
	return &http.Server{
		Addr:              cfg.Addr(),
		Handler:           handler,
		ReadTimeout:       cfg.ReadTimeout,
		ReadHeaderTimeout: cfg.ReadHeaderTimeout,
		WriteTimeout:      cfg.WriteTimeout,
		IdleTimeout:       cfg.IdleTimeout,
		MaxHeaderBytes:    maxHeaderBytes,
	}
}

// ServeOption 配置 ListenAndServeGraceful 的可选行为，采用函数式选项模式扩展。
type ServeOption func(*serveConfig)

type serveConfig struct {
	dispatcher     *event.Dispatcher
	shutdownSignal []os.Signal
	listener       net.Listener
	startedHook    func()
}

// WithDispatcher 指定用于派发服务器生命周期事件的 Dispatcher。
// 不传时回退到 event.Resolve()，全局未设置则使用独立空总线。
func WithDispatcher(d *event.Dispatcher) ServeOption {
	return func(cfg *serveConfig) {
		cfg.dispatcher = d
	}
}

// WithShutdownSignals 显式启用 HTTP 层进程信号监听。
//
// 用途：保留独立使用 http Server 时由 HTTP 层直接响应系统信号的能力。
// 设计说明：Application 场景下应由 foundation 统一接管系统信号，因此这里改为显式开启。
// 不传信号时默认监听 SIGTERM 和 SIGINT。
func WithShutdownSignals(signals ...os.Signal) ServeOption {
	return func(cfg *serveConfig) {
		if len(signals) == 0 {
			cfg.shutdownSignal = []os.Signal{syscall.SIGTERM, syscall.SIGINT}
			return
		}
		cfg.shutdownSignal = append([]os.Signal(nil), signals...)
	}
}

// WithListener 指定已创建好的监听器，供服务直接接管而不是重新监听地址。
//
// 用途：支持服务进程在平滑重载时继承旧进程传入的监听资源，避免重新 net.Listen 产生端口冲突。
func WithListener(listener net.Listener) ServeOption {
	return func(cfg *serveConfig) {
		cfg.listener = listener
	}
}

// WithStartedHook 注册服务开始接收请求前的回调。
//
// 用途：让调用方在新进程真正持有监听资源后再触发旧进程退出，保证 reload 切换顺序可控。
func WithStartedHook(hook func()) ServeOption {
	return func(cfg *serveConfig) {
		cfg.startedHook = hook
	}
}

// ListenAndServeGraceful 启动 HTTP 服务器，支持优雅关闭。
//
// 默认只响应传入 context 的取消或外部显式 Shutdown/Close；如需 HTTP 层直接监听
// SIGTERM/SIGINT，应显式传入 WithShutdownSignals()。
//
// 在以下时机派发事件（若提供了 Dispatcher）：
//   - 监听前：event.ServerStarting
//   - 监听协程已启动：event.ServerStarted
//   - 开始关闭：event.ServerStopping
//   - Shutdown 完成：event.ServerStopped
func ListenAndServeGraceful(server *http.Server, shutdownTimeout time.Duration, opts ...ServeOption) error {
	return ListenAndServeGracefulContext(context.Background(), server, shutdownTimeout, opts...)
}

// ListenAndServeGracefulContext 启动 HTTP 服务器，并允许调用方通过 ctx 主动触发优雅关闭。
func ListenAndServeGracefulContext(ctx context.Context, server *http.Server, shutdownTimeout time.Duration, opts ...ServeOption) error {
	if ctx == nil {
		ctx = context.Background()
	}

	cfg := &serveConfig{}
	for _, opt := range opts {
		opt(cfg)
	}
	bus := cfg.dispatcher
	if bus == nil {
		if resolved, ok := event.Resolve().(*event.Dispatcher); ok {
			bus = resolved
		}
		if bus == nil {
			panic("http: no event dispatcher available for server lifecycle events")
		}
	}

	pid := os.Getpid()
	bus.Dispatch(ctx, event.ServerStarting{Addr: server.Addr, PID: pid})

	addr := server.Addr
	if addr == "" {
		addr = ":http"
	}
	listener := cfg.listener
	if listener == nil {
		var err error
		listener, err = net.Listen("tcp", addr)
		if err != nil {
			return err
		}
	}

	var sigChan <-chan os.Signal
	var stopSignals func()
	if len(cfg.shutdownSignal) > 0 {
		registered := make(chan os.Signal, 1)
		signal.Notify(registered, cfg.shutdownSignal...)
		sigChan = registered
		stopSignals = func() {
			signal.Stop(registered)
		}
	}
	if stopSignals != nil {
		defer stopSignals()
	}

	serveErr := make(chan error, 1)
	go func() {
		serveErr <- server.Serve(listener)
	}()

	if cfg.startedHook != nil {
		cfg.startedHook()
	}
	bus.Dispatch(ctx, event.ServerStarted{Addr: server.Addr, PID: pid})

	select {
	case err := <-serveErr:
		if err == http.ErrServerClosed {
			dispatchServerStopped(ctx, bus, server.Addr, 0, nil)
		}
		return err
	case sig := <-sigChan:
		return shutdownServer(ctx, bus, server, shutdownTimeout, sig.String(), serveErr)
	case <-ctx.Done():
		return shutdownServer(ctx, bus, server, shutdownTimeout, shutdownReasonFromContext(ctx), serveErr)
	}
}

func shutdownServer(ctx context.Context, bus *event.Dispatcher, server *http.Server, shutdownTimeout time.Duration, reason string, serveErr <-chan error) error {
	eventCtx := context.WithoutCancel(ctx)
	bus.Dispatch(eventCtx, event.ServerStopping{Addr: server.Addr, Reason: reason})

	shutdownCtx, cancel := context.WithTimeout(eventCtx, shutdownTimeout)
	defer cancel()

	start := time.Now()
	shutdownErr := server.Shutdown(shutdownCtx)
	if shutdownErr != nil {
		_ = server.Close()
	}
	serveError := <-serveErr

	dispatchServerStopped(eventCtx, bus, server.Addr, time.Since(start), shutdownErr)
	if shutdownErr != nil {
		return shutdownErr
	}
	if serveError != nil && serveError != http.ErrServerClosed {
		return serveError
	}
	return http.ErrServerClosed
}

func dispatchServerStopped(ctx context.Context, bus *event.Dispatcher, addr string, duration time.Duration, err error) {
	bus.Dispatch(ctx, event.ServerStopped{
		Addr:     addr,
		Duration: duration,
		Error:    errorString(err),
	})
}

func shutdownReasonFromContext(ctx context.Context) string {
	if ctx == nil {
		return context.Canceled.Error()
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause.Error()
	}
	if err := ctx.Err(); err != nil {
		return err.Error()
	}
	return context.Canceled.Error()
}

func errorString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// SendSignal 向进程发送信号。
func SendSignal(pid int, sig syscall.Signal) error {
	process, err := os.FindProcess(pid)
	if err != nil {
		return err
	}
	return process.Signal(sig)
}
