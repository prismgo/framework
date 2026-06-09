package horizon

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prismgo/framework/container"
	queuecontract "github.com/prismgo/framework/contracts/queue"
	goexception "github.com/prismgo/framework/exception"
	"github.com/prismgo/framework/logger"
	"github.com/prismgo/framework/queue"
)

// panickingManagedProcess 是测试专用 ManagedProcess，其 Wait() 方法会主动 panic。
//
// 需求背景：验证 watchProcess、waitProcessesWithHeartbeat 和 startListenProcess
// 中 goroutine 的 panic 恢复能力，确保 process.Wait() 意外 panic 不会导致整个进程崩溃。
type panickingManagedProcess struct {
	pid int
	msg string // panic 时携带的消息
}

func (p panickingManagedProcess) PID() int { return p.pid }

func (p panickingManagedProcess) Wait() error {
	panic(p.msg)
}

// panickingProcessRunner 是测试专用 ProcessRunner，其 Start() 方法返回 panickingManagedProcess。
//
// 需求背景：startListenProcess 通过 ProcessRunner.Start() 获取子进程，需要注入可 panic 的
// fake runner 来验证其内部 goroutine 的 panic 恢复。
type panickingProcessRunner struct {
	pid int
	msg string
}

func (r *panickingProcessRunner) Start(_ context.Context, _ ProcessSpec) (ManagedProcess, error) {
	return panickingManagedProcess{pid: r.pid, msg: r.msg}, nil
}

type reportedException struct {
	err    error
	fields map[string]any
}

func captureReportedExceptions(t *testing.T) <-chan reportedException {
	t.Helper()
	registry := useHorizonTestContainer(t)
	reports := make(chan reportedException, 4)
	handler := goexception.New()
	handler.Reporters = append(handler.Reporters, func(_ any, err error, fields map[string]any) {
		copied := make(map[string]any, len(fields))
		for key, value := range fields {
			copied[key] = value
		}
		select {
		case reports <- reportedException{err: err, fields: copied}:
		default:
		}
	})
	if err := registry.Instance("exception.handler", handler, container.WithCloseGroup(container.CloseGroupReporting)); err != nil {
		t.Fatalf("bind exception handler: %v", err)
	}
	bindHorizonPanicLogger(t, registry)
	return reports
}

func bindHorizonPanicReporter(t *testing.T) {
	t.Helper()
	registry := useHorizonTestContainer(t)
	if err := registry.Instance("exception.handler", goexception.New(goexception.WithPanicStack(false)), container.WithCloseGroup(container.CloseGroupReporting)); err != nil {
		t.Fatalf("bind exception handler: %v", err)
	}
	bindHorizonPanicLogger(t, registry)
}

func bindHorizonPanicLogger(t *testing.T, registry *container.Container) {
	t.Helper()
	manager, err := logger.NewManager(logger.Config{
		Default:  "null",
		Channels: map[string]logger.ChannelOptions{"null": {Driver: "null", Level: "debug"}},
	})
	if err != nil {
		t.Fatalf("new logger manager: %v", err)
	}
	if err := registry.Instance("logger.manager", manager, container.WithCloseGroup(container.CloseGroupReporting)); err != nil {
		t.Fatalf("bind logger manager: %v", err)
	}
}

type panickingQueueManager struct {
	message string
}

func (m panickingQueueManager) Queue(string) (queuecontract.Queue, error) {
	return panickingQueueConnection(m), nil
}

func (m panickingQueueManager) Failed() queue.FailedStore {
	return nil
}

func (m panickingQueueManager) RequestRestart(context.Context) error {
	return nil
}

type panickingQueueConnection struct {
	message string
}

func (c panickingQueueConnection) Size(context.Context, string) (int64, error) {
	panic(c.message)
}

func (c panickingQueueConnection) Clear(context.Context, string) error {
	return nil
}

func (c panickingQueueConnection) Push(context.Context, string, queuecontract.Payload) error {
	panic(c.message)
}

func (c panickingQueueConnection) Later(context.Context, string, queuecontract.Payload, time.Duration) error {
	panic(c.message)
}

func (c panickingQueueConnection) Bulk(context.Context, string, []queuecontract.Payload) (queuecontract.BulkResult, error) {
	panic(c.message)
}

func (c panickingQueueConnection) Pop(context.Context, []string, ...queuecontract.PopWaitMode) (queuecontract.ReservedJob, error) {
	panic(c.message)
}

func (c panickingQueueConnection) Close() error {
	return nil
}

// TestWatchProcessRecoversFromPanic 验证 watchProcess goroutine 能捕获 process.Wait() panic 并将其转换为错误事件。
//
// 需求背景：supervisor runtime loop 通过 watchProcess 监听 worker 子进程退出。
// process.Wait() 意外 panic 时如果传播到 goroutine 外部，会导致整个 supervisor 进程崩溃。
func TestWatchProcessRecoversFromPanic(t *testing.T) {
	bindHorizonPanicReporter(t)
	exits := make(chan processExit, 1)
	watchProcess(0, panickingManagedProcess{pid: 1, msg: "test panic in Wait"}, exits)

	select {
	case exit := <-exits:
		if exit.index != 0 {
			t.Fatalf("expected index 0, got %d", exit.index)
		}
		if exit.err == nil {
			t.Fatal("expected error from panic recovery, got nil")
		}
		if !strings.Contains(exit.err.Error(), "watchProcess panic") {
			t.Fatalf("expected panic error, got: %v", exit.err)
		}
		if !strings.Contains(exit.err.Error(), "test panic in Wait") {
			t.Fatalf("expected error to contain original panic message, got: %v", exit.err)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for exit event")
	}
}

// TestWatchProcessNilProcess 验证 watchProcess 对 nil process 的处理不受 panic recovery 影响。
func TestWatchProcessNilProcess(t *testing.T) {
	exits := make(chan processExit, 1)
	watchProcess(5, nil, exits)

	select {
	case exit := <-exits:
		if exit.index != 5 {
			t.Fatalf("expected index 5, got %d", exit.index)
		}
		if exit.err != nil {
			t.Fatalf("expected nil error for nil process, got: %v", exit.err)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for exit event")
	}
}

// TestWatchProcessNormalExit 验证 watchProcess 对正常退出的 process 行为不变。
func TestWatchProcessNormalExit(t *testing.T) {
	exits := make(chan processExit, 1)
	watchProcess(3, fakeManagedProcess{pid: 42}, exits)

	select {
	case exit := <-exits:
		if exit.index != 3 {
			t.Fatalf("expected index 3, got %d", exit.index)
		}
		if exit.err != nil {
			t.Fatalf("expected nil error for normal exit, got: %v", exit.err)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for exit event")
	}
}

// TestWatchProcessErrorExit 验证 watchProcess 对带错误退出的 process 能正常透传错误。
func TestWatchProcessErrorExit(t *testing.T) {
	expectedErr := errors.New("worker failed")
	exits := make(chan processExit, 1)
	watchProcess(7, fakeManagedProcess{pid: 99, err: expectedErr}, exits)

	select {
	case exit := <-exits:
		if exit.index != 7 {
			t.Fatalf("expected index 7, got %d", exit.index)
		}
		if !errors.Is(exit.err, expectedErr) {
			t.Fatalf("expected %v, got: %v", expectedErr, exit.err)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for exit event")
	}
}

func TestSupervisorRuntimeLoopReportsWorkloadSamplePanic(t *testing.T) {
	// 需求背景：workload 采样在 supervisor 控制循环外的 goroutine 中执行。
	// 队列后端异常 panic 时必须通过 prismgo/exception 上报，并且不能让 supervisor 进程崩溃。
	reports := captureReportedExceptions(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wakeC := make(chan struct{}, 1)
	previousWake := newRuntimeControlWake
	newRuntimeControlWake = func(context.Context) runtimeControlWake {
		return runtimeControlWake{C: wakeC}
	}
	defer func() { newRuntimeControlWake = previousWake }()

	store := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})
	manager, err := NewManager(Config{Store: "memory", Environment: "local", LoopInterval: time.Hour}, WithStoreFactory(staticStoreResolver{store: store}), WithQueueManager(panickingQueueManager{message: "workload size panic"}))
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	runtime := &runtimeCommandAdapter{manager: manager, store: store}
	supervisor := SupervisorConfig{Name: "panic-supervisor", Connection: "redis", Queues: []string{"default"}, Balance: BalanceFalse, MinProcesses: 0, MaxProcesses: 1}
	state := SupervisorState{Name: "panic-supervisor", Status: SupervisorRunning, LastHeartbeatAt: time.Now().UTC()}

	done := make(chan error, 1)
	go func() {
		done <- runtime.supervisorRuntimeLoop(ctx, supervisor, "local", state, nil, nil)
	}()
	wakeC <- struct{}{}

	select {
	case report := <-reports:
		if report.err == nil || !strings.Contains(report.err.Error(), "workload size panic") {
			t.Fatalf("expected reported workload panic, got %v", report.err)
		}
		if report.fields["subsystem"] != "supervisor" {
			t.Fatalf("expected supervisor subsystem field, got %#v", report.fields)
		}
		if report.fields["supervisor"] != "panic-supervisor" {
			t.Fatalf("expected supervisor name field, got %#v", report.fields)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for reported workload panic")
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("supervisor returned error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for supervisor shutdown")
	}
}

// TestWaitProcessesWithHeartbeatRecoversFromPanic 验证 waitProcessesWithHeartbeat 内的 goroutine
// 能捕获 p.Wait() panic 并上报到 exception.Report，不让整个 master 进程崩溃。
//
// 需求背景：对齐 Laravel Horizon — master 等待 supervisor 退出时，任一 goroutine panic
// 应上报错误并继续监控其他 supervisor，不崩溃 master。
func TestWaitProcessesWithHeartbeatRecoversFromPanic(t *testing.T) {
	bindHorizonPanicReporter(t)
	ctx := context.Background()
	processes := []ManagedProcess{
		fakeManagedProcess{pid: 1},                                    // 正常退出的进程
		panickingManagedProcess{pid: 2, msg: "supervisor wait panic"}, // panic 的进程
		fakeManagedProcess{pid: 3},                                    // 另一个正常进程
	}

	// 对齐 Laravel Horizon：panic 被 recover 并上报，master 继续运行直到所有进程退出
	err := waitProcessesWithHeartbeat(ctx, processes, 10*time.Millisecond, nil)

	if err != nil {
		t.Fatalf("expected nil error (panic was reported not returned), got: %v", err)
	}
}

// TestWaitProcessesWithHeartbeatNormalExit 验证 panic recovery 不影响正常退出行为。
func TestWaitProcessesWithHeartbeatNormalExit(t *testing.T) {
	ctx := context.Background()
	processes := []ManagedProcess{
		fakeManagedProcess{pid: 1},
		fakeManagedProcess{pid: 2},
		fakeManagedProcess{pid: 3},
	}

	err := waitProcessesWithHeartbeat(ctx, processes, 10*time.Millisecond, nil)

	if err != nil {
		t.Fatalf("expected no error for normal exit, got: %v", err)
	}
}

// TestWaitProcessesWithHeartbeatErrorExit 验证子进程退出错误被上报而不是传播。
//
// 需求背景：对齐 Laravel Horizon — supervisor 异常退出时 master 上报错误并继续监控其他 supervisor。
func TestWaitProcessesWithHeartbeatErrorExit(t *testing.T) {
	bindHorizonPanicReporter(t)
	ctx := context.Background()
	processes := []ManagedProcess{
		fakeManagedProcess{pid: 1},
		fakeManagedProcess{pid: 2, err: errors.New("supervisor failed")},
	}

	// 对齐 Laravel Horizon：错误被上报，master 继续运行直到所有进程退出
	err := waitProcessesWithHeartbeat(ctx, processes, 10*time.Millisecond, nil)

	if err != nil {
		t.Fatalf("expected nil error (error was reported not returned), got: %v", err)
	}
}

// TestWaitProcessesWithHeartbeatHeartbeatCallback 验证 panic recovery 不影响 heartbeat 回调行为。
func TestWaitProcessesWithHeartbeatHeartbeatCallback(t *testing.T) {
	ctx := context.Background()
	var heartbeats int64
	heartbeat := func(_ time.Time) error {
		atomic.AddInt64(&heartbeats, 1)
		return nil
	}
	// 使用阻塞进程确保 ticker 有机会在进程退出前触发
	blockCtx, blockCancel := context.WithCancel(context.Background())
	defer blockCancel()
	processes := []ManagedProcess{
		blockingManagedProcess{pid: 1, ctx: blockCtx},
	}

	done := make(chan error, 1)
	go func() {
		done <- waitProcessesWithHeartbeat(ctx, processes, 10*time.Millisecond, heartbeat)
	}()

	// 等待足够时间让 ticker 触发几次
	time.Sleep(100 * time.Millisecond)

	if atomic.LoadInt64(&heartbeats) == 0 {
		t.Fatal("expected heartbeat callback to be called at least once")
	}

	// 释放阻塞进程让其退出
	blockCancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for waitProcessesWithHeartbeat to complete")
	}
}

// TestWaitProcessesWithHeartbeatContextCancellation 验证 context 取消后进入 graceful shutdown 等待，
// 而不是立刻丢下仍在运行的子进程返回。
func TestWaitProcessesWithHeartbeatContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	// 使用 blockingManagedProcess 让 goroutine 阻塞在 Wait() 上
	blockCtx, blockCancel := context.WithCancel(context.Background())
	processes := []ManagedProcess{
		blockingManagedProcess{pid: 1, ctx: blockCtx},
	}

	done := make(chan error, 1)
	go func() {
		done <- waitProcessesWithHeartbeat(ctx, processes, 10*time.Millisecond, nil)
	}()

	// 确保 goroutine 已启动
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		t.Fatalf("wait returned before child process exited: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	// 释放阻塞的进程
	blockCancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("expected nil after child process exit, got: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for graceful cancellation")
	}
}

// TestStartListenProcessRecoversFromPanic 验证 startListenProcess 内的 goroutine 能捕获 process.Wait() panic。
//
// 需求背景：horizon:listen 命令通过 startListenProcess 启动 horizon 子进程。
// process.Wait() 意外 panic 会导致 listen 进程崩溃，开发者需要手动重启。
func TestStartListenProcessRecoversFromPanic(t *testing.T) {
	bindHorizonPanicReporter(t)
	cfg := Config{Environment: "testing", Watch: []string{"missing-path"}}
	manager, err := NewManager(cfg,
		WithProcessRunner(&panickingProcessRunner{pid: 9001, msg: "listen process panic"}),
	)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	adapter := &runtimeCommandAdapter{manager: manager}
	_, cancel, exits, err := adapter.startListenProcess(context.Background(), "testing")
	if err != nil {
		t.Fatalf("startListenProcess: %v", err)
	}
	defer func() {
		if cancel != nil {
			cancel()
		}
	}()

	if exits == nil {
		t.Fatal("expected non-nil exits channel")
	}

	select {
	case exitErr := <-exits:
		if exitErr == nil {
			t.Fatal("expected panic error from exits channel, got nil")
		}
		if !strings.Contains(exitErr.Error(), "startListenProcess panic") {
			t.Fatalf("expected panic error, got: %v", exitErr)
		}
		if !strings.Contains(exitErr.Error(), "listen process panic") {
			t.Fatalf("expected original panic message, got: %v", exitErr)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for exit event from panicking process")
	}
}

// failingStartProcessRunner 是测试专用 ProcessRunner，其 Start() 方法返回错误。
//
// 需求背景：验证 startListenProcess 在 ProcessRunner.Start() 失败时能正确返回错误并清理 cancel。
type failingStartProcessRunner struct {
	err error
}

func (r *failingStartProcessRunner) Start(_ context.Context, _ ProcessSpec) (ManagedProcess, error) {
	return nil, r.err
}

// TestStartListenProcessStartError 验证 ProcessRunner.Start() 失败时 startListenProcess 返回错误。
func TestStartListenProcessStartError(t *testing.T) {
	cfg := Config{Environment: "testing", Watch: []string{"missing-path"}}
	expectedErr := errFakeProcessWait
	manager, err := NewManager(cfg,
		WithProcessRunner(&failingStartProcessRunner{err: expectedErr}),
	)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	adapter := &runtimeCommandAdapter{manager: manager}
	process, cancel, exits, err := adapter.startListenProcess(context.Background(), "testing")
	if err == nil {
		t.Fatal("expected error from startListenProcess, got nil")
	}
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected %v, got: %v", expectedErr, err)
	}
	if process != nil {
		t.Fatal("expected nil process on start error")
	}
	if cancel != nil {
		// cancel 应在错误路径中被调用
		t.Fatal("expected cancel to be nil (already called) on start error")
	}
	if exits != nil {
		t.Fatal("expected nil exits channel on start error")
	}
}

// TestStartListenProcessNormalExit 验证 panic recovery 不影响 startListenProcess 正常退出行为。
func TestStartListenProcessNormalExit(t *testing.T) {
	cfg := Config{Environment: "testing", Watch: []string{"missing-path"}}
	manager, err := NewManager(cfg,
		WithProcessRunner(&fakeProcessRunner{}),
	)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	adapter := &runtimeCommandAdapter{manager: manager}
	process, cancel, exits, err := adapter.startListenProcess(context.Background(), "testing")
	if err != nil {
		t.Fatalf("startListenProcess: %v", err)
	}
	defer func() {
		if cancel != nil {
			cancel()
		}
	}()

	if process == nil {
		t.Fatal("expected non-nil process")
	}
	if exits == nil {
		t.Fatal("expected non-nil exits channel")
	}

	// 正常 fake process 的 Wait() 返回 nil，gourtine 应正常发送 nil error
	select {
	case exitErr := <-exits:
		if exitErr != nil {
			t.Fatalf("expected nil error for normal exit, got: %v", exitErr)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for exit event")
	}
}
