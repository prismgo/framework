//go:build !windows

package http

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/prismgo/framework/event"
	"github.com/prismgo/framework/internal/fmtx"
)

// ---------------------------------------------------------------------------
// TestMain：拦截 reload 子进程，避免运行测试套件
// ---------------------------------------------------------------------------

// TestMain 拦截 reload 子进程。
//
// 设计说明：跨进程集成测试通过 exec.Command 启动当前测试二进制文件作为 reload 子进程。
// 子进程通过 PRISMGO_TEST_RELOAD_CHILD 环境变量识别自身角色，执行 reload 子进程逻辑后直接退出，
// 不进入测试框架。父进程正常执行所有测试用例。
func TestMain(m *testing.M) {
	if marker := os.Getenv("PRISMGO_TEST_RELOAD_ENV_MARKER"); marker != "" {
		captureReloadEnv(marker)
		return
	}
	if os.Getenv("PRISMGO_TEST_RELOAD_CHILD") == "1" {
		captureReloadChildPID()
		runReloadChild()
		return
	}
	os.Exit(m.Run())
}

func captureReloadEnv(marker string) {
	content := strings.Join([]string{
		os.Getenv(listenerFDEnv),
		os.Getenv(reloadSignalEnv),
	}, "\n")
	if err := os.WriteFile(marker, []byte(content), 0644); err != nil {
		fmtx.Fprintf(os.Stderr, "reload env capture failed: %v\n", err)
		os.Exit(1)
	}
	os.Exit(0)
}

// captureReloadChildPID 将 reload 子进程 PID 写入测试专用文件，便于父测试确定性清理。
func captureReloadChildPID() {
	pidFile := os.Getenv("PRISMGO_TEST_RELOAD_CHILD_PID_FILE")
	if pidFile == "" {
		return
	}
	if err := os.WriteFile(pidFile, []byte(strconv.Itoa(os.Getpid())), 0644); err != nil {
		fmtx.Fprintf(os.Stderr, "reload child pid capture failed: %v\n", err)
		os.Exit(1)
	}
}

// runReloadChild 执行 reload 子进程的核心逻辑：
// 1. 通过 InheritedListener 接管父进程传入的监听器
// 2. 在继承的监听器上启动 HTTP 服务
// 3. 通知父进程开始优雅退出
// 4. 等待自身收到关闭信号后退出
func runReloadChild() {
	listener, err := InheritedListener()
	if err != nil || listener == nil {
		fmtx.Fprintf(os.Stderr, "reload child: inherited listener failed: %v\n", err)
		os.Exit(1)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("X-Served-By", "child")
		w.WriteHeader(http.StatusOK)
	})
	server := &http.Server{Handler: mux}

	// 在后台协程中启动 HTTP 服务
	go func() { _ = server.Serve(listener) }()

	// 通知父进程开始优雅退出（发送 SIGTERM）
	_ = NotifyReloadParent()

	// 等待关闭信号或检测父进程退出
	// 设计说明：测试场景下父进程可能不会向子进程发送 SIGTERM（仅通过
	// NotifyReloadParent 单向通知），因此同时轮询父进程存活状态，
	// 父进程退出后子进程主动发起关闭，避免孤儿进程残留。
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT)

	parentPID := 0
	if pidStr := os.Getenv(reloadSignalEnv); pidStr != "" {
		parentPID, _ = strconv.Atoi(pidStr)
	}

	go func() {
		if parentPID <= 0 {
			return
		}
		proc, err := os.FindProcess(parentPID)
		if err != nil {
			return
		}
		for {
			time.Sleep(200 * time.Millisecond)
			// Signal(0) 检查进程是否存在
			if err := proc.Signal(syscall.Signal(0)); err != nil {
				// 父进程已退出，向自身发送 SIGTERM 触发关闭
				_ = syscall.Kill(syscall.Getpid(), syscall.SIGTERM)
				return
			}
		}
	}()

	<-signals
	signal.Stop(signals)

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	_ = server.Shutdown(shutdownCtx)
}

// ---------------------------------------------------------------------------
// InheritedListener 错误分支测试
// ---------------------------------------------------------------------------

// TestInheritedListener_InvalidFDEnv 验证环境变量中非数字 FD 值返回解析错误。
func TestInheritedListenerInvalidFDEnv(t *testing.T) {
	t.Setenv(listenerFDEnv, "abc")
	_, err := InheritedListener()
	if err == nil || !strings.Contains(err.Error(), "parse inherited listener fd") {
		t.Fatalf("InheritedListener error = %v, want parse error", err)
	}
}

// TestInheritedListener_NegativeFD 验证负数 FD 返回无效 FD 错误。
func TestInheritedListenerNegativeFD(t *testing.T) {
	t.Setenv(listenerFDEnv, "-1")
	_, err := InheritedListener()
	if err == nil || !strings.Contains(err.Error(), "invalid inherited listener fd") {
		t.Fatalf("InheritedListener error = %v, want invalid fd error", err)
	}
}

// TestInheritedListener_UnusableFD 验证指向无效资源的 FD 返回继承失败错误。
//
// 设计说明：FD 999 在测试进程中大概率未被占用，os.NewFile 会成功但 net.FileListener 会失败，
// 覆盖 "inherit listener failed" 错误分支。
func TestInheritedListenerUnusableFD(t *testing.T) {
	t.Setenv(listenerFDEnv, "999")
	_, err := InheritedListener()
	if err == nil || !strings.Contains(err.Error(), "inherit listener failed") {
		t.Fatalf("InheritedListener error = %v, want inherit listener failed", err)
	}
}

func TestInheritedListenerReturnsListenerFromEnvFD(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}
	defer func() {
		if err := listener.Close(); err != nil {
			t.Errorf("close listener: %v", err)
		}
	}()

	file, err := listener.(*net.TCPListener).File()
	if err != nil {
		t.Fatalf("listener file failed: %v", err)
	}

	t.Setenv(listenerFDEnv, strconv.Itoa(int(file.Fd())))
	inherited, err := InheritedListener()
	if err != nil {
		t.Fatalf("InheritedListener returned error: %v", err)
	}
	if inherited == nil {
		t.Fatal("InheritedListener returned nil listener")
	}
	if err := inherited.Close(); err != nil {
		t.Fatalf("close inherited listener: %v", err)
	}
}

// ---------------------------------------------------------------------------
// NotifyReloadParent 错误分支测试
// ---------------------------------------------------------------------------

// TestNotifyReloadParent_InvalidPIDEnv 验证非数字 PID 环境变量返回解析错误。
func TestNotifyReloadParentInvalidPIDEnv(t *testing.T) {
	t.Setenv(reloadSignalEnv, "abc")
	err := NotifyReloadParent()
	if err == nil || !strings.Contains(err.Error(), "parse reload parent pid") {
		t.Fatalf("NotifyReloadParent error = %v, want parse error", err)
	}
}

// TestNotifyReloadParent_DeadProcess 验证指向不存在进程 PID 的通知返回错误。
//
// 设计说明：PID 999999 在系统中极大概率不存在，用于覆盖 "notify reload parent failed" 分支。
func TestNotifyReloadParentDeadProcess(t *testing.T) {
	t.Setenv(reloadSignalEnv, "999999")
	err := NotifyReloadParent()
	if err == nil || !strings.Contains(err.Error(), "notify reload parent failed") {
		t.Fatalf("NotifyReloadParent error = %v, want notify failed", err)
	}
}

// ---------------------------------------------------------------------------
// spawnReloadChild 单元测试
// ---------------------------------------------------------------------------

// nonTCPListener 模拟非 TCP 类型的 listener，用于覆盖 spawnReloadChild 的类型断言失败分支。
type nonTCPListener struct{}

func (nonTCPListener) Accept() (net.Conn, error) { return nil, fmt.Errorf("not implemented") }
func (nonTCPListener) Close() error              { return nil }
func (nonTCPListener) Addr() net.Addr            { return &net.UnixAddr{Name: "/dev/null", Net: "unix"} }

// compile-time assertion: nonTCPListener implements net.Listener
var _ net.Listener = nonTCPListener{}

// TestSpawnReloadChild_RejectsNonTCPListener 验证非 TCP 监听器被拒绝。
func TestSpawnReloadChildRejectsNonTCPListener(t *testing.T) {
	err := spawnReloadChild(nonTCPListener{}, "", nil, nil)
	if err == nil || !strings.Contains(err.Error(), "reload requires TCP listener") {
		t.Fatalf("spawnReloadChild error = %v, want 'reload requires TCP listener'", err)
	}
}

func TestSpawnReloadChildStartError(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}
	defer func() {
		if err := listener.Close(); err != nil {
			t.Errorf("close listener: %v", err)
		}
	}()

	err = spawnReloadChild(listener, filepath.Join(t.TempDir(), "missing-executable"), nil, nil)
	if err == nil || !strings.Contains(err.Error(), "start reload child failed") {
		t.Fatalf("spawnReloadChild error = %v, want start reload child failed", err)
	}
}

func TestSpawnReloadChildIncludesReadyEnv(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}
	defer func() {
		if err := listener.Close(); err != nil {
			t.Errorf("close listener: %v", err)
		}
	}()

	marker := filepath.Join(t.TempDir(), "reload-env.txt")
	t.Setenv("PRISMGO_TEST_RELOAD_ENV_MARKER", marker)

	err = spawnReloadChild(listener, os.Args[0], nil, func() {})
	if err != nil {
		t.Fatalf("spawnReloadChild error = %v, want nil", err)
	}

	var lines []string
	for i := 0; i < 20; i++ {
		var data []byte
		data, err = os.ReadFile(marker)
		if err == nil {
			lines = strings.Split(strings.TrimSpace(string(data)), "\n")
		}
		if len(lines) == 2 && lines[0] != "" && lines[1] != "" {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("read reload env marker: %v", err)
	}
	if len(lines) != 2 {
		t.Fatalf("reload env marker = %v, want listener and parent pid", lines)
	}
	if lines[0] != strconv.Itoa(inheritedFD) {
		t.Fatalf("%s = %q, want %d", listenerFDEnv, lines[0], inheritedFD)
	}
	if lines[1] != strconv.Itoa(os.Getpid()) {
		t.Fatalf("%s = %q, want %d", reloadSignalEnv, lines[1], os.Getpid())
	}
}

// ---------------------------------------------------------------------------
// WatchReloadSignal 单元测试
// ---------------------------------------------------------------------------

// TestWatchReloadSignal_NilListenerReturnsNoop 验证 nil listener 返回空操作清理函数。
//
// 设计说明：当服务未启动或不需要 reload 时，传入 nil listener 应安全跳过信号注册。
func TestWatchReloadSignalNilListenerReturnsNoop(t *testing.T) {
	cleanup := WatchReloadSignal(context.Background(), nil, "", nil, nil)
	if cleanup == nil {
		t.Fatal("WatchReloadSignal returned nil cleanup")
	}
	cleanup() // 不应 panic
}

func TestWatchReloadSignalDefaultsExecutableAndArgs(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}
	defer func() {
		if err := listener.Close(); err != nil {
			t.Errorf("close listener: %v", err)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	cleanup := WatchReloadSignal(ctx, listener, "", nil, nil)
	cancel()
	cleanup()
}

// TestWatchReloadSignal_ContextCancellationCleansUp 验证取消 context 后清理函数正常返回。
//
// 设计说明：确保 goroutine 在 ctx 取消后退出，cleanup 不会永久阻塞。
func TestWatchReloadSignalContextCancellationCleansUp(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}
	defer func() {
		if err := listener.Close(); err != nil {
			t.Errorf("close listener: %v", err)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	cleanup := WatchReloadSignal(ctx, listener, trueExecutable(t), nil, nil)

	cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		cleanup()
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("cleanup did not return after context cancellation")
	}
}

// TestWatchReloadSignal_ReloadSignalSpawnsChild 验证 SIGUSR2 触发 WatchReloadSignal 派生子进程。
//
// 设计说明：直接向当前进程发送 SIGUSR2，验证 WatchReloadSignal 能正确捕获信号并调用
// spawnReloadChild 启动子进程。通过标记文件验证子进程确实被执行。
func TestWatchReloadSignalReloadSignalSpawnsChild(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}
	defer func() {
		if err := listener.Close(); err != nil {
			t.Errorf("close listener: %v", err)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())

	// 使用标记文件验证子进程被实际执行（spawnReloadChild 内部并不调用 onReady，
	// 它仅设置环境变量让子进程自行通知父进程，因此用标记文件替代验证）
	markerFile := fmt.Sprintf("%s/watch-reload-marker-%d", t.TempDir(), os.Getpid())
	markerScript := fmt.Sprintf("#!/bin/sh\ntouch %q\n", markerFile)
	scriptPath := fmt.Sprintf("%s/spawn-marker.sh", t.TempDir())
	if err := os.WriteFile(scriptPath, []byte(markerScript), 0755); err != nil {
		t.Fatalf("write marker script: %v", err)
	}

	// defer 顺序（LIFO）：cleanup 先注册（后执行），cancel 后注册（先执行）
	// cancel 必须先于 cleanup，否则 cleanup 等待 goroutine 退出时会死锁
	cleanup := WatchReloadSignal(ctx, listener, scriptPath, nil, nil)
	defer cleanup()
	defer cancel()

	// 等待信号处理器注册完成
	time.Sleep(100 * time.Millisecond)

	if err := syscall.Kill(syscall.Getpid(), syscall.SIGUSR2); err != nil {
		t.Skipf("SIGUSR2 not supported: %v", err)
	}

	// 等待标记文件出现（证明子进程被派生并执行）
	for i := 0; i < 20; i++ {
		time.Sleep(250 * time.Millisecond)
		if _, err := os.Stat(markerFile); err == nil {
			return // 子进程已派生并执行
		}
	}
	t.Fatal("SIGUSR2 did not trigger child spawn within timeout")
}

// ---------------------------------------------------------------------------
// 跨进程端到端集成测试
// ---------------------------------------------------------------------------

// TestSpawnReloadChild_Integration 验证 spawnReloadChild 跨进程监听器继承的完整流程。
//
// 测试流程：
//  1. 父进程创建 TCP 监听器并启动 HTTP 服务
//  2. 验证父进程能正常响应 HTTP 请求
//  3. 调用 spawnReloadChild 派生子进程，传递监听器 FD
//  4. 子进程（测试二进制文件）通过 InheritedListener 接管监听器并启动 HTTP 服务
//  5. 轮询等待子进程在继承的监听器上响应 HTTP 请求
//  6. 关闭父进程，验证子进程独立提供服务
//
// 设计说明：此测试不经过 SIGUSR2 信号触发路径，直接调用 spawnReloadChild，
// 隔离验证"监听器 FD 继承 + 子进程 HTTP 服务"这一核心能力。
func TestSpawnReloadChildIntegration(t *testing.T) {
	// 设置子进程标识环境变量，spawnReloadChild 通过 os.Environ() 传递给子进程
	t.Setenv("PRISMGO_TEST_RELOAD_CHILD", "1")
	registerReloadChildCleanup(t)
	// 1. 父进程创建监听器
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}
	addr := listener.Addr().String()

	// 2. 父进程启动 HTTP 服务
	parentCtx, parentCancel := context.WithCancel(context.Background())
	defer parentCancel()

	bus := event.New()
	parentServer := &http.Server{
		Addr: addr,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("X-Served-By", "parent")
			w.WriteHeader(http.StatusOK)
		}),
	}

	parentDone := make(chan error, 1)
	go func() {
		parentDone <- ListenAndServeGracefulContext(parentCtx, parentServer, time.Second, WithDispatcher(bus), WithListener(listener))
	}()
	time.Sleep(200 * time.Millisecond)

	// 3. 验证父进程可用
	resp, err := http.Get("http://" + addr + "/health")
	if err != nil {
		t.Fatalf("parent request failed: %v", err)
	}
	_ = resp.Body.Close()
	if resp.Header.Get("X-Served-By") != "parent" {
		t.Fatalf("parent X-Served-By = %q, want 'parent'", resp.Header.Get("X-Served-By"))
	}

	// 4. 派生 reload 子进程（子进程为当前测试二进制文件）
	// onReady 传入 nil：避免设置 PRISMGO_HTTP_RELOAD_SIGNAL_PID，
	// 否则子进程 NotifyReloadParent() 会向测试进程发送 SIGTERM 导致测试进程被杀。
	// 子进程通过 PRISMGO_TEST_RELOAD_CHILD（t.Setenv 已设置）识别自身角色，
	// 并通过轮询父进程 PID 存活状态在父进程退出后自行关闭。
	if err := spawnReloadChild(listener, os.Args[0], nil, nil); err != nil {
		t.Fatalf("spawnReloadChild failed: %v", err)
	}

	// 测试结束后清理残留子进程
	t.Cleanup(func() {
		parentCancel()
		killProcessOnPort(addr)
	})

	// 5. 等待子进程启动后主动关闭父进程
	// 由于 onReady=nil，子进程不会发送 SIGTERM 通知父进程退出，
	// 因此需要手动取消父进程 context，否则父子进程同时在同一 socket 上 accept，
	// 请求会被内核负载均衡到父进程，导致无法验证子进程是否正常工作。
	time.Sleep(2 * time.Second) // 等待子进程初始化 Go 运行时并启动 HTTP 服务
	parentCancel()
	select {
	case <-parentDone:
	case <-time.After(3 * time.Second):
		t.Fatal("parent did not shut down within timeout")
	}

	// 6. 验证子进程独立提供服务（父进程已退出，所有请求应由子进程处理）
	var childServing bool
	for i := 0; i < 20; i++ {
		time.Sleep(500 * time.Millisecond)
		r, err := http.Get("http://" + addr + "/health")
		if err != nil {
			continue
		}
		servedBy := r.Header.Get("X-Served-By")
		_ = r.Body.Close()
		if servedBy == "child" {
			childServing = true
			break
		}
	}

	if !childServing {
		t.Fatal("child never responded on inherited listener")
	}

	// 7. 验证子进程稳定提供服务
	resp, err = http.Get("http://" + addr + "/health")
	if err != nil {
		t.Fatalf("post-reload child request failed: %v", err)
	}
	_ = resp.Body.Close()
	if resp.Header.Get("X-Served-By") != "child" {
		t.Fatalf("child X-Served-By = %q, want 'child'", resp.Header.Get("X-Served-By"))
	}
}

// TestReloadCrossProcess_EndToEnd 验证完整的跨进程平滑重载流程（信号触发 → 监听器继承 → 进程切换）。
//
// 测试流程：
//  1. 父进程启动 HTTP 服务 + WatchReloadSignal 监听 SIGUSR2
//  2. 验证父进程能正常响应 HTTP 请求
//  3. 向父进程发送 SIGUSR2 触发 reload
//  4. WatchReloadSignal 捕获信号，派生子进程并传递监听器 FD
//  5. 子进程通过 InheritedListener 接管监听器，启动 HTTP 服务
//  6. 等待子进程就绪后取消父进程 context（模拟真实场景中 NotifyReloadParent 发送的 SIGTERM）
//  7. 父进程优雅关闭，子进程独占监听器
//  8. 验证子进程在继承的监听器上独立提供服务
//
// 设计说明：
//   - 真实场景中子进程通过 NotifyReloadParent 向父进程发送 SIGTERM 触发优雅关闭。
//     但在测试进程中注册 SIGTERM _shutdown_ 信号会导致测试进程自身被杀，
//     因此用 context 取消替代 SIGTERM，其余流程（SIGUSR2 → spawn → FD 继承 → HTTP 服务）完全一致。
//   - 这是最高层级的集成测试，覆盖了从信号触发到进程切换的完整链路，
//     验证 serve --reload 命令背后的核心基础设施在真实跨进程场景下的正确性。
func TestReloadCrossProcessEndToEnd(t *testing.T) {
	t.Setenv("PRISMGO_TEST_RELOAD_CHILD", "1")
	registerReloadChildCleanup(t)
	// 1. 创建 TCP 监听器
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}
	addr := listener.Addr().String()

	// 2. 启动父进程 HTTP 服务
	ctx, cancel := context.WithCancel(context.Background())

	bus := event.New()
	server := &http.Server{
		Addr: addr,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("X-Served-By", "parent")
			w.WriteHeader(http.StatusOK)
		}),
	}

	// defer 顺序（LIFO）：stopWatch 先注册（后执行），cancel 后注册（先执行）
	// cancel 必须先于 stopWatch，否则 stopWatch 等待 goroutine 退出时死锁
	stopWatch := WatchReloadSignal(ctx, listener, os.Args[0], nil, nil)
	defer stopWatch()
	defer cancel()

	// 启动 HTTP 服务（不使用 WithShutdownSignals，避免 SIGTERM 杀死测试进程）
	serverDone := make(chan error, 1)
	go func() {
		serverDone <- ListenAndServeGracefulContext(
			ctx, server, 5*time.Second,
			WithDispatcher(bus),
			WithListener(listener),
		)
	}()

	// 等待服务就绪
	time.Sleep(200 * time.Millisecond)

	// 3. 验证父进程可用
	resp, err := http.Get("http://" + addr + "/health")
	if err != nil {
		t.Fatalf("parent request failed: %v", err)
	}
	_ = resp.Body.Close()
	if resp.Header.Get("X-Served-By") != "parent" {
		t.Fatalf("parent X-Served-By = %q, want 'parent'", resp.Header.Get("X-Served-By"))
	}

	// 4. 发送 SIGUSR2 触发 reload（等待信号处理器注册完成）
	time.Sleep(100 * time.Millisecond)
	if err := syscall.Kill(syscall.Getpid(), syscall.SIGUSR2); err != nil {
		t.Skipf("SIGUSR2 not supported: %v", err)
	}

	// 5. 等待子进程启动后取消父进程 context（模拟 NotifyReloadParent 的 SIGTERM 效果）
	time.Sleep(2 * time.Second)
	cancel()

	select {
	case srvErr := <-serverDone:
		if srvErr != http.ErrServerClosed {
			t.Fatalf("server error = %v, want ErrServerClosed", srvErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("parent server did not shut down within timeout")
	}

	// 6. 验证子进程独立提供服务
	var childResponded bool
	for i := 0; i < 20; i++ {
		time.Sleep(500 * time.Millisecond)
		r, err := http.Get("http://" + addr + "/health")
		if err != nil {
			continue
		}
		servedBy := r.Header.Get("X-Served-By")
		_ = r.Body.Close()
		if servedBy == "child" {
			childResponded = true
			break
		}
	}

	if !childResponded {
		t.Fatal("child never responded on inherited listener after SIGUSR2 reload")
	}

	// 7. 验证子进程稳定提供服务
	resp, err = http.Get("http://" + addr + "/health")
	if err != nil {
		t.Fatalf("post-reload request failed: %v", err)
	}
	_ = resp.Body.Close()
	if resp.Header.Get("X-Served-By") != "child" {
		t.Fatalf("post-reload X-Served-By = %q, want 'child'", resp.Header.Get("X-Served-By"))
	}

	// 清理可能残留的子进程
	killProcessOnPort(addr)
}

// TestReloadCrossProcess_RequestContinuity 验证 reload 切换期间 HTTP 请求不中断。
//
// 测试流程：
//  1. 父进程启动 HTTP 服务 + WatchReloadSignal
//  2. 触发 SIGUSR2 reload
//  3. 在 reload 切换过程中持续发送 HTTP 请求
//  4. 等待子进程就绪后取消父进程 context
//  5. 验证绝大部分请求都能成功响应（允许进程切换瞬间少量失败）
//
// 设计说明：平滑重载的核心目标是零停机，此测试验证在进程切换窗口内请求不会大量丢失。
// 同 EndToEnd 测试，使用 context 取消替代 SIGTERM 以避免杀死测试进程。
func TestReloadCrossProcessRequestContinuity(t *testing.T) {
	t.Setenv("PRISMGO_TEST_RELOAD_CHILD", "1")
	registerReloadChildCleanup(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}
	addr := listener.Addr().String()

	ctx, cancel := context.WithCancel(context.Background())

	bus := event.New()
	server := &http.Server{
		Addr: addr,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("X-Served-By", "parent")
			w.WriteHeader(http.StatusOK)
		}),
	}

	// defer 顺序（LIFO）：stopWatch 先注册（后执行），cancel 后注册（先执行）
	stopWatch := WatchReloadSignal(ctx, listener, os.Args[0], nil, nil)
	defer stopWatch()
	defer cancel()

	serverDone := make(chan error, 1)
	go func() {
		serverDone <- ListenAndServeGracefulContext(
			ctx, server, 5*time.Second,
			WithDispatcher(bus),
			WithListener(listener),
		)
	}()
	time.Sleep(200 * time.Millisecond)

	// 触发 reload（等待信号处理器注册完成）
	time.Sleep(100 * time.Millisecond)
	if err := syscall.Kill(syscall.Getpid(), syscall.SIGUSR2); err != nil {
		t.Skipf("SIGUSR2 not supported: %v", err)
	}

	// 等待子进程启动后取消父进程 context（模拟 NotifyReloadParent 的 SIGTERM 效果）
	time.Sleep(2 * time.Second)
	cancel()

	// 在 reload 切换过程中持续发送请求
	var (
		totalRequests  int
		failedRequests int
		mu             sync.Mutex
	)

	client := &http.Client{Timeout: 2 * time.Second}
	deadline := time.After(8 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

loop:
	for {
		select {
		case <-deadline:
			break loop
		case <-ticker.C:
			mu.Lock()
			totalRequests++
			r, err := client.Get("http://" + addr + "/health")
			if err != nil {
				failedRequests++
			} else {
				_ = r.Body.Close()
			}
			mu.Unlock()
		}
	}

	select {
	case <-serverDone:
	case <-time.After(5 * time.Second):
	}

	mu.Lock()
	total := totalRequests
	failed := failedRequests
	mu.Unlock()

	// 允许少量请求在进程切换瞬间失败（连接被重置），但失败率不能超过 20%
	if total > 0 && float64(failed)/float64(total) > 0.20 {
		t.Fatalf("request continuity: %d/%d requests failed (%.1f%%), want < 20%%",
			failed, total, float64(failed)/float64(total)*100)
	}
	if total < 10 {
		t.Fatalf("expected at least 10 requests during reload window, got %d", total)
	}

	killProcessOnPort(addr)
}

// ---------------------------------------------------------------------------
// 辅助函数
// ---------------------------------------------------------------------------

func registerReloadChildCleanup(t *testing.T) {
	t.Helper()
	pidFile := filepath.Join(t.TempDir(), "reload-child.pid")
	t.Setenv("PRISMGO_TEST_RELOAD_CHILD_PID_FILE", pidFile)
	t.Cleanup(func() {
		killProcessFromPIDFile(t, pidFile)
	})
}

// killProcessFromPIDFile 读取测试子进程 PID 并发送 SIGTERM，降低跨测试残留概率。
func killProcessFromPIDFile(t *testing.T, pidFile string) {
	t.Helper()
	data, err := os.ReadFile(pidFile)
	if err != nil {
		return
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		t.Logf("parse reload child pid file %s failed: %v", pidFile, err)
		return
	}
	if pid <= 0 {
		t.Logf("reload child pid file %s contains invalid pid %d", pidFile, pid)
		return
	}
	process, err := os.FindProcess(pid)
	if err != nil {
		t.Logf("find reload child process %d failed: %v", pid, err)
		return
	}
	if err := process.Signal(syscall.SIGTERM); err != nil {
		t.Logf("signal reload child process %d failed: %v", pid, err)
	}
}

// killProcessOnPort 尝试杀死占用指定地址的残留进程，用于测试清理。
//
// 设计说明：跨进程测试可能残留子进程，此函数通过 fuser 命令尝试杀死占用端口的进程。
// 如果 fuser 不可用或端口已释放，则静默跳过。
func killProcessOnPort(addr string) {
	// 先检查端口是否仍被占用
	conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
	if err != nil {
		return // 端口已释放，无需清理
	}
	_ = conn.Close()

	// 提取端口号（addr 格式为 "host:port"）
	parts := strings.Split(addr, ":")
	if len(parts) >= 2 {
		port := parts[len(parts)-1]
		// 使用 fuser 杀死占用端口的进程（仅限 Linux，忽略错误）
		cmd := exec.Command("fuser", "-k", port+"/tcp")
		_ = cmd.Run()
	}
}
