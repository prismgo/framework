package horizon

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	horizoncmd "github.com/prismgo/framework/horizon/cmd"
	goprocess "github.com/prismgo/framework/process"
	"github.com/prismgo/framework/queue"
	"github.com/prismgo/framework/queue/payload"
)

// fakeFailingStore 在 HeartbeatWorker/HeartbeatSupervisor 写方法上立即失败并计数，
// 用于防回归验证队列事件热路径不会同步写 Store。
//
// 需求背景（historical scenario 51）：任何 queue event listener 不得直接写 Horizon Store；
// 队列事件需要持久化的观测数据必须先进入 collector，再由 async flusher 落盘。
// 其他 Store 方法委托给嵌入的 MemoryStore，避免每次 Store 接口变更都需要同步更新签名。
type fakeFailingStore struct {
	MemoryStore
	writeCalled atomic.Int64
}

func (s *fakeFailingStore) HeartbeatWorker(_ context.Context, _ WorkerState) error {
	s.writeCalled.Add(1)
	return errors.New("store write banned on hot path")
}

func (s *fakeFailingStore) HeartbeatSupervisor(_ context.Context, _ SupervisorState) error {
	s.writeCalled.Add(1)
	return errors.New("store write banned on hot path")
}

// TestQueueEventHotPathDoesNotWriteStore 防回归：使用会在任意 Store 写入时失败的 fake Store
// 触发队列事件，证明事件监听热路径不会同步调用 Store。
func TestQueueEventHotPathDoesNotWriteStore(t *testing.T) {
	ctx := context.Background()
	store := &fakeFailingStore{
		MemoryStore: *NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute}),
	}
	recorder := &workerEventRecorder{
		store: store,
		state: WorkerState{ID: "no-write-worker", Supervisor: "test", Status: WorkerIdle},
	}

	// 触发各种队列事件，确保 record() 不会调用任何 Store 方法
	events := []queue.Event{
		queue.JobProcessing{JobID: "j1", JobName: "TestJob", Queue: "default"},
		queue.JobProcessed{JobID: "j1", JobName: "TestJob", Queue: "default"},
		queue.JobFailed{FailedJob: payload.FailedJob{JobID: "j2", JobName: "FailingJob", Queue: "default"}},
		queue.JobReleased{JobID: "j3", JobName: "ReleasedJob", Queue: "default"},
	}
	for _, ev := range events {
		recorder.record(ctx, ev)
	}

	if store.writeCalled.Load() > 0 {
		t.Fatalf("queue event hot path must not call Store writes, got %d writes", store.writeCalled.Load())
	}
}

// TestHeartbeatNotFiredPerQueueEvent 验证 heartbeat 写入次数受 interval 约束，
// 而不是随 queue event 数量线性增长。
func TestHeartbeatNotFiredPerQueueEvent(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})

	// 先写入一次初始 heartbeat
	if err := store.HeartbeatWorker(ctx, WorkerState{
		ID:              "hb-constrained",
		Supervisor:      "test",
		Status:          WorkerIdle,
		LastHeartbeatAt: time.Now(),
	}); err != nil {
		t.Fatalf("initial heartbeat: %v", err)
	}

	initial, _, _ := store.Worker(ctx, "hb-constrained", time.Now())
	initialAt := initial.LastHeartbeatAt

	// record() 不应触发 heartbeat — 事件只设置 sawJob
	recorder := &workerEventRecorder{
		store: store,
		state: WorkerState{ID: "hb-constrained", Status: WorkerIdle, LastHeartbeatAt: initialAt},
	}
	for i := 0; i < 100; i++ {
		recorder.record(ctx, queue.JobProcessing{JobID: fmt.Sprintf("j-%d", i), JobName: "TestJob"})
		recorder.record(ctx, queue.JobProcessed{JobID: fmt.Sprintf("j-%d", i), JobName: "TestJob"})
	}

	// Store 中的 LastHeartbeatAt 不应因 200 个事件而更新
	current, _, _ := store.Worker(ctx, "hb-constrained", time.Now())
	if !current.LastHeartbeatAt.Equal(initialAt) {
		t.Fatalf("heartbeat should not update per queue event: initial=%v current=%v", initialAt, current.LastHeartbeatAt)
	}
}

func TestWorkerHeartbeatPersistsCollectorMemoryMetric(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})
	cfg := observabilityPresetConfigOrFull()
	cfg.BufferSize = 7
	recorder := &workerEventRecorder{
		store:     store,
		state:     WorkerState{ID: "collector-memory-worker", Supervisor: "test", Status: WorkerIdle, LastHeartbeatAt: time.Now()},
		collector: newCollector(cfg),
	}

	recorder.heartbeat(ctx)

	state, found, err := store.Worker(ctx, "collector-memory-worker", time.Now())
	if err != nil || !found {
		t.Fatalf("worker after heartbeat: found=%v err=%v", found, err)
	}
	if state.CollectorMemoryBytes.Status != goprocess.StatusAvailable || state.CollectorMemoryBytes.Unit != goprocess.UnitBytes {
		t.Fatalf("collector memory metric = %#v", state.CollectorMemoryBytes)
	}
	value, ok := state.CollectorMemoryBytes.Value.(int64)
	if !ok || value <= 0 {
		t.Fatalf("collector memory value = %#v", state.CollectorMemoryBytes.Value)
	}
}

// TestWorkerPeriodicHeartbeatDuringLongTask 验证长任务期间 worker 持续刷新 heartbeat，
// 不会因为没有队列事件而被读成 stale。
func TestWorkerPeriodicHeartbeatDuringLongTask(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	interval := 20 * time.Millisecond
	store := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})
	recorder := &workerEventRecorder{
		store: store,
		state: WorkerState{ID: "long-task-worker", Supervisor: "test", Status: WorkerIdle, LastHeartbeatAt: time.Now()},
	}

	// 先写入初始 heartbeat
	recorder.heartbeat(ctx)

	// 启动周期性 heartbeat
	stop := recorder.startPeriodicHeartbeat(ctx, interval)
	defer stop()

	// 等待足够多次 heartbeat tick
	time.Sleep(interval*3 + 10*time.Millisecond)

	state, found, err := store.Worker(ctx, "long-task-worker", time.Now())
	if err != nil || !found {
		t.Fatalf("worker after periodic heartbeat: found=%v err=%v", found, err)
	}
	if state.Status != WorkerIdle {
		t.Fatalf("periodic heartbeat should keep worker idle: %s", state.Status)
	}
	// LastHeartbeatAt 应已更新（至少晚于初始时间）
	if !state.LastHeartbeatAt.After(recorder.state.StartedAt) {
		t.Fatal("periodic heartbeat should update LastHeartbeatAt")
	}
}

// TestWorkerHeartbeatWriteErrorDiagnostics 验证 heartbeat 写入失败时仍保留诊断信息，
// 不取消正在执行的 job，也不泄露 payload 或凭据。
func TestWorkerHeartbeatWriteErrorDiagnostics(t *testing.T) {
	ctx := context.Background()
	failing := &fakeFailingStore{
		MemoryStore: *NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute}),
	}
	recorder := &workerEventRecorder{
		store: failing,
		state: WorkerState{ID: "diag-worker", Status: WorkerIdle, LastHeartbeatAt: time.Now()},
	}

	// 触发 heartbeat，应记录错误
	recorder.heartbeat(ctx)

	// 验证 heartbeatError 已设置
	if recorder.heartbeatError.Code != "heartbeat_write_failed" {
		t.Fatalf("expected heartbeat_write_failed, got %s", recorder.heartbeatError.Code)
	}
	if recorder.heartbeatError.Message != "Worker heartbeat write failed." {
		t.Fatalf("unexpected heartbeat error message: %s", recorder.heartbeatError.Message)
	}
	if recorder.heartbeatError.FailedAt.IsZero() {
		t.Fatal("heartbeat error should record FailedAt timestamp")
	}

	// result(nil) 应返回 heartbeat 诊断错误
	err := recorder.result(nil)
	if err == nil || !strings.Contains(err.Error(), "heartbeat_write_failed") {
		t.Fatalf("result should expose heartbeat diagnostic: %v", err)
	}

	// 错误消息不得包含 payload、Store 凭据或内部状态
	errStr := err.Error()
	for _, forbidden := range []string{"redis://", "password", "payload", "token", "secret", "JobPayload"} {
		if strings.Contains(strings.ToLower(errStr), strings.ToLower(forbidden)) {
			t.Fatalf("heartbeat error must not leak %q: %s", forbidden, errStr)
		}
	}
}

// TestWorkerLifecycleStatesPersistWithoutEventHeartbeat 验证 pause、terminate、
// runner error、StopWhenEmpty 等生命周期路径仍能写入最终状态。
func TestWorkerLifecycleStatesPersistWithoutEventHeartbeat(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})

	// idle 方法仅更新内存状态，不写 Store
	recorder := &workerEventRecorder{
		store: store,
		state: WorkerState{ID: "lifecycle-worker", Status: WorkerIdle, LastHeartbeatAt: time.Now()},
	}
	recorder.idle()
	_, found, _ := store.Worker(ctx, "lifecycle-worker", time.Now())
	if found {
		t.Fatal("idle() must not write to Store")
	}

	// paused 方法应持久化 paused 状态到 Store
	recorder.paused(ctx)
	state, found, err := store.Worker(ctx, "lifecycle-worker", time.Now())
	if err != nil || !found {
		t.Fatalf("paused worker: found=%v err=%v", found, err)
	}
	if state.Status != WorkerPaused {
		t.Fatalf("paused() should persist WorkerPaused, got %s", state.Status)
	}

	// terminating 方法应持久化 terminating 状态到 Store
	recorder.terminating(ctx)
	state, found, err = store.Worker(ctx, "lifecycle-worker", time.Now())
	if err != nil || !found {
		t.Fatalf("terminating worker: found=%v err=%v", found, err)
	}
	if state.Status != WorkerTerminating {
		t.Fatalf("terminating() should persist WorkerTerminating, got %s", state.Status)
	}
}

// TestWorkerStoreNoJobSurface 验证 WorkerState 不再包含 CurrentJob、Processed、Failed 字段。
func TestWorkerStoreNoJobSurface(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})

	// 写入 heartbeat，确保不会因为旧字段残留而导致序列化错误
	if err := store.HeartbeatWorker(ctx, WorkerState{
		ID:               "no-job-surface",
		Supervisor:       "test",
		Status:           WorkerIdle,
		LastHeartbeatAt:  time.Now(),
		ConfiguredQueues: []string{"default"},
	}); err != nil {
		t.Fatalf("heartbeat worker: %v", err)
	}

	state, found, err := store.Worker(ctx, "no-job-surface", time.Now())
	if err != nil || !found {
		t.Fatalf("read worker: found=%v err=%v", found, err)
	}

	// 确认 ConfiguredQueues 仍然可用（描述 worker 启动消费范围）
	if len(state.ConfiguredQueues) != 1 || state.ConfiguredQueues[0] != "default" {
		t.Fatalf("ConfiguredQueues should persist: %#v", state.ConfiguredQueues)
	}
}

// TestCollectWorkerEventDoesNotWriteStore 验证 collectWorkerEvent 只调用
// collector（非阻塞），不直接写 Store。
func TestCollectWorkerEventDoesNotWriteStore(t *testing.T) {
	store := &fakeFailingStore{
		MemoryStore: *NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute}),
	}
	obs := observabilityPresetConfigOrFull()
	obs.EventMetrics = true
	obs.EventMetricsSampleRate = 1
	obs.QueuedWaitsMax = 0

	manager, _ := NewManager(Config{
		Store:         "memory",
		Observability: obs,
	}, WithStoreFactory(staticStoreResolver{store: store}))
	manager.coll.Start(context.Background())
	defer manager.coll.Stop()

	runtime := &runtimeCommandAdapter{manager: manager, store: store}
	runtime.collectWorkerEvent(context.Background(), queue.JobProcessing{
		JobID: "j1", JobName: "TestJob", Connection: "redis", Queue: "default",
	}, "test-supervisor")

	// collector.Collect 是非阻塞的，不应触发 Store 写
	if store.writeCalled.Load() > 0 {
		t.Fatalf("collectWorkerEvent must not trigger Store writes, got %d", store.writeCalled.Load())
	}
}

// TestRunWorkerInitialHeartbeat 验证 RunWorker 启动后立即写入一次 worker heartbeat。
func TestRunWorkerInitialHeartbeat(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})
	worker := &fakeWorkerRunner{
		hook: func(ctx context.Context) {
			// 不触发任何事件，直接返回
		},
	}
	manager, _ := NewManager(Config{
		Store:         "memory",
		LoopInterval:  50 * time.Millisecond,
		Observability: observabilityPresetConfigOrFull(),
	}, WithStoreFactory(staticStoreResolver{store: store}), WithWorkerRunner(worker))

	runtime := &runtimeCommandAdapter{manager: manager, store: store}

	// 启动 worker 并在短暂延迟后取消（模拟 StopWhenEmpty 立即退出）
	done := make(chan error, 1)
	go func() {
		done <- runtime.RunWorker(ctx, horizoncmd.WorkerOptions{
			Name:          "init-hb-worker",
			Supervisor:    "test",
			StopWhenEmpty: true,
			Sleep:         0,
		})
	}()

	// 等待 worker 退出
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("worker exited with error: %v", err)
		}
	case <-time.After(2 * time.Second):
		cancel()
		<-done
		t.Fatal("worker did not exit in time")
	}

	// 验证 worker heartbeat 已在 Store 中
	state, found, err := store.Worker(ctx, "init-hb-worker", time.Now())
	if err != nil || !found {
		t.Fatalf("worker must be in store after initial heartbeat: found=%v err=%v", found, err)
	}
	if state.Status != WorkerTerminating {
		t.Fatalf("worker should be terminating after StopWhenEmpty: %s", state.Status)
	}
}

// TestWorkerHeartbeatTickerFiresAtInterval 验证独立 ticker 按 runtimeLoopInterval 周期性刷新 heartbeat，
// 且 ticker 不修改 worker 状态为 working，始终保持 WorkerIdle。
func TestWorkerHeartbeatTickerFiresAtInterval(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})
	recorder := &workerEventRecorder{
		store: store,
		state: WorkerState{ID: "ticker-worker", Supervisor: "test", Status: WorkerIdle, LastHeartbeatAt: time.Now()},
	}

	// 初始 heartbeat
	recorder.heartbeat(ctx)
	initial, _, _ := store.Worker(ctx, "ticker-worker", time.Now())
	initialAt := initial.LastHeartbeatAt

	// 启动 ticker，间隔 20ms
	interval := 20 * time.Millisecond
	stop := recorder.startPeriodicHeartbeat(ctx, interval)

	// 等待足够多次 tick
	time.Sleep(interval*3 + 10*time.Millisecond)

	// 验证 LastHeartbeatAt 已向后推进
	state, found, err := store.Worker(ctx, "ticker-worker", time.Now())
	if err != nil || !found {
		t.Fatalf("worker not found: found=%v err=%v", found, err)
	}
	if !state.LastHeartbeatAt.After(initialAt) {
		t.Fatal("periodic heartbeat ticker should advance LastHeartbeatAt")
	}

	// status 应保持 WorkerIdle（ticker 不设置 working）
	if state.Status != WorkerIdle {
		t.Fatalf("ticker should not change status, expected idle got %s", state.Status)
	}

	// 自省字段应已填充
	if state.GoroutineCount.Status != goprocess.StatusAvailable {
		t.Fatal("periodic ticker should apply self-observation")
	}

	// 停止 ticker 并等待 goroutine 退出
	stop()
	lastBeforeStop := state.LastHeartbeatAt
	time.Sleep(interval * 2)
	stateAfter, _, _ := store.Worker(ctx, "ticker-worker", time.Now())
	if stateAfter.LastHeartbeatAt.After(lastBeforeStop) {
		t.Fatal("heartbeat should stop after stop() is called")
	}
}

// panicStore 在 HeartbeatWorker 上 panic，用于验证 ticker goroutine 的 recover 机制。
type panicStore struct {
	MemoryStore
}

func (s *panicStore) HeartbeatWorker(_ context.Context, _ WorkerState) error {
	panic("injected heartbeat panic for test coverage")
}

// TestWorkerHeartbeatTickerRecoversFromPanic 验证 ticker goroutine 内部 panic 被 recover 捕获，
// goroutine 安全退出且 heartbeatError 被记录，不会导致整个进程崩溃。
func TestWorkerHeartbeatTickerRecoversFromPanic(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := &panicStore{MemoryStore: *NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})}
	recorder := &workerEventRecorder{
		store: store,
		state: WorkerState{ID: "panic-worker", Supervisor: "test", Status: WorkerIdle, LastHeartbeatAt: time.Now()},
	}

	// 写入初始 heartbeat（用嵌入的 MemoryStore 直写绕过 panic store 覆盖）
	store.MemoryStore.HeartbeatWorker(ctx, recorder.state)

	// 启动 ticker，第一次 tick 时 heartbeat 会 panic
	interval := 10 * time.Millisecond
	stop := recorder.startPeriodicHeartbeat(ctx, interval)

	// 等待 ticker 触发并 panic
	time.Sleep(interval * 2)

	// stop 应正常返回（goroutine 已通过 recover 安全退出）
	stop()

	// heartbeatError 应被记录，且消息包含 panic 值
	heartbeatError := recorder.currentHeartbeatError()
	if heartbeatError.Code != "heartbeat_write_failed" {
		t.Fatalf("panic recovery should record heartbeat error, got code=%s", heartbeatError.Code)
	}
	if !strings.Contains(heartbeatError.Message, "injected heartbeat panic") {
		t.Fatalf("panic error message should contain panic value, got %q", heartbeatError.Message)
	}
}
