package cmd

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/prismgo/framework/console"
	prismhttp "github.com/prismgo/framework/http"
)

// processManager 抽象 serve 命令所需的进程控制能力。
//
// 用途：把 PID 文件读写与进程启停逻辑从命令主体中隔离出来，便于测试和后续扩展实现。
type processManager interface {
	SavePID() error
	RemovePID() error
	ReadPID() (int, error)
	Kill(pid int) error
	Stop(pid int, shutdownTimeout time.Duration) error
	Reload(pid int, executable string, args []string, shutdownTimeout time.Duration) (int, error)
	Restart(pid int, executable string, args []string) (int, error)
}

// ServeCommand 启动和控制 HTTP 服务进程。
//
// 用途：统一承载启动服务、优雅停止、重载与重启等通用流程。
// 设计说明：命令只接收 Kernel 内部传入的 HTTP Server 工厂；HTTP Server 构建细节仍由
// prismgo/http 管理，不保留包级 fallback 注册入口，也不向 main.go 暴露 HTTP 配置。
type ServeCommand struct {
	newProcessManager func(pidFile string) processManager
	newHTTPServer     func(context.Context, string) (*http.Server, error)
	inheritedListener func() (net.Listener, error)
}

// NewServeCommand 创建通用 HTTP serve 命令。
func NewServeCommand(newHTTPServer func(context.Context, string) (*http.Server, error)) *ServeCommand {
	return &ServeCommand{
		newProcessManager: func(pidFile string) processManager { return prismhttp.NewProcessManager(pidFile) },
		newHTTPServer:     newHTTPServer,
		inheritedListener: prismhttp.InheritedListener,
	}
}

// Definition 返回命令定义。
func (c *ServeCommand) Definition() *console.Definition {
	return console.MustDefinition(
		"serve {--p|port= : HTTP listen port, overrides config} {--restart : Restart server} {--reload : Gracefully restart server} {--stop : Gracefully stop server} {--kill : Force stop server}",
		"Start HTTP API server",
	)
}

// Run 解析参数并启动 HTTP 服务或执行进程控制。
func (c *ServeCommand) Handle(commandCtx console.CommandContext) error {
	port := commandCtx.Input().Option("port")
	if port == "" {
		port = defaultHTTPPort()
	}
	if err := validateServePort(port); err != nil {
		return err
	}

	restart := commandCtx.Input().OptionBool("restart")
	reload := commandCtx.Input().OptionBool("reload")
	stop := commandCtx.Input().OptionBool("stop")
	kill := commandCtx.Input().OptionBool("kill")

	if restart || reload || stop || kill {
		return c.handleProcessControl(port, restart, reload, stop, kill, commandCtx.IO())
	}

	return c.startServer(commandCtx.Context(), port, commandCtx.IO())
}

// startServer 启动 HTTP 服务并维护当前进程的 PID 文件。
//
// 用途：执行普通 serve 命令时进入阻塞监听流程，并在退出时清理 PID 文件。
// 设计说明：ctx 来自命令上下文，最终由 Application 生命周期驱动取消，因此 HTTP 服务
// 可以和应用关闭信号保持一致。
func (c *ServeCommand) startServer(ctx context.Context, port string, io console.IO) error {
	if ctx == nil {
		ctx = context.Background()
	}

	if c.newHTTPServer == nil {
		return fmt.Errorf("http: HTTP routes registrar is not configured")
	}
	serverConfig := prismhttp.CurrentServerConfig()
	server, err := c.newHTTPServer(ctx, port)
	if err != nil {
		return err
	}

	pidFile := c.getPIDFile(port)
	pm := c.newProcessManager(pidFile)
	if err := pm.SavePID(); err != nil {
		return fmt.Errorf("save pid failed: %w", err)
	}
	defer pm.RemovePID()

	listenOptions := make([]prismhttp.ServeOption, 0, 2)
	if c.inheritedListener != nil {
		listener, err := c.inheritedListener()
		if err != nil {
			return fmt.Errorf("resolve inherited listener failed: %w", err)
		}
		if listener != nil {
			listenOptions = append(listenOptions, prismhttp.WithListener(listener))
			listenOptions = append(listenOptions, prismhttp.WithStartedHook(func() {
				_ = prismhttp.NotifyReloadParent()
			}))
		}
	}

	io.Success("api server listen on " + server.Addr)
	if err := prismhttp.ListenAndServeGracefulContext(ctx, server, serverConfig.ShutdownTimeout, listenOptions...); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("start api failed: %w", err)
	}
	return nil
}

// handleProcessControl handles restart, reload, stop and kill commands.
func (c *ServeCommand) handleProcessControl(port string, restart, reload, stop, kill bool, io console.IO) error {
	pidFile := c.getPIDFile(port)
	pm := c.newProcessManager(pidFile)

	pid, err := pm.ReadPID()
	if err != nil {
		return fmt.Errorf("read pid file failed: %w (is server running?)", err)
	}

	timeout := serverShutdownTimeout()

	switch {
	case kill:
		return c.killServer(pm, pid, io)
	case stop:
		return c.stopServer(pm, pid, timeout, io)
	case reload:
		return c.reloadServer(pm, port, pid, timeout, io)
	case restart:
		return c.restartServer(pm, port, pid, io)
	default:
		return nil
	}
}

// killServer force-stops the server.
func (c *ServeCommand) killServer(pm processManager, pid int, io console.IO) error {
	if err := pm.Kill(pid); err != nil {
		return fmt.Errorf("kill process failed: %w", err)
	}
	io.Success(fmt.Sprintf("server killed (pid: %d)", pid))
	return nil
}

// stopServer gracefully stops the server.
func (c *ServeCommand) stopServer(pm processManager, pid int, timeout time.Duration, io console.IO) error {
	io.Success(fmt.Sprintf("graceful shutdown initiated (pid: %d)", pid))
	if err := pm.Stop(pid, timeout); err != nil {
		return fmt.Errorf("graceful shutdown failed: %w", err)
	}
	io.Success("server shutdown completed")
	return nil
}

// reloadServer gracefully starts a new server before stopping the old one.
func (c *ServeCommand) reloadServer(pm processManager, port string, pid int, timeout time.Duration, io console.IO) error {
	args := []string{"serve", "--port", port}
	newPID, err := pm.Reload(pid, os.Args[0], args, timeout)
	if err != nil {
		return fmt.Errorf("reload failed: %w", err)
	}
	io.Success(fmt.Sprintf("new server started (pid: %d), old server gracefully shutting down", newPID))
	return nil
}

// restartServer kills the old server before starting a new one.
func (c *ServeCommand) restartServer(pm processManager, port string, pid int, io console.IO) error {
	args := []string{"serve", "--port", port}
	newPID, err := pm.Restart(pid, os.Args[0], args)
	if err != nil {
		return fmt.Errorf("restart failed: %w", err)
	}
	io.Success(fmt.Sprintf("old server killed (pid: %d), new server started (pid: %d)", pid, newPID))
	return nil
}

// getPIDFile returns the PID file path for a port-specific server process.
func (c *ServeCommand) getPIDFile(port string) string {
	tmpDir := os.TempDir()
	return filepath.Join(tmpDir, fmt.Sprintf("prismgo-serve-%s.pid", port))
}

func validateServePort(port string) error {
	if port == "" {
		return fmt.Errorf("serve port is required")
	}
	for _, ch := range port {
		if ch < '0' || ch > '9' {
			return fmt.Errorf("invalid serve port %q: only numeric ports are supported", port)
		}
	}
	return nil
}

func defaultHTTPPort() string {
	return prismhttp.CurrentServerConfig().Port
}

func serverShutdownTimeout() time.Duration {
	return prismhttp.CurrentServerConfig().ShutdownTimeout
}
