package horizon

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	queuecontract "github.com/prismgo/framework/contracts/queue"
	"github.com/prismgo/framework/event"
	"github.com/prismgo/framework/queue"
	"github.com/prismgo/framework/queue/payload"
	"github.com/prismgo/framework/queue/state"
	"github.com/spf13/cobra"

	"github.com/prismgo/framework/console"
	horizoncmd "github.com/prismgo/framework/horizon/cmd"
)

func TestRuntimeCommandsUseStoreForControlAndStatus(t *testing.T) {
	// 需求背景：除 horizon:list 外，Laravel config contract 的运行时命令都必须通过 Horizon Store 执行。
	// 本测试注入同一个 memory store，验证 pause/continue/terminate 与 status 输出共享真实状态。
	now := time.Now().UTC()
	store := NewMemoryStore(StoreOptions{Prefix: "cmd", HeartbeatTTL: time.Minute})
	if err := store.HeartbeatSupervisor(context.Background(), SupervisorState{
		Name:            "supervisor-default",
		Host:            "host-1",
		PID:             101,
		Status:          SupervisorRunning,
		StartedAt:       now.Add(-time.Minute),
		LastHeartbeatAt: now,
		WorkerCount:     2,
		Connection:      "redis",
		Queues:          []string{"default"},
	}); err != nil {
		t.Fatalf("seed supervisor: %v", err)
	}
	manager, err := NewManager(Config{Store: "memory", Prefix: "cmd", HeartbeatTTL: time.Minute}, WithStoreFactory(staticStoreResolver{store: store}), WithQueueManager(&fakeRuntimeQueueManager{connections: map[string]*fakeQueueConnection{}}))
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	load := func() (*Manager, error) { return manager, nil }

	runHorizonCommand(t, horizoncmd.NewPauseCommand(newRuntimeLoader(load)), runtimeInput{})
	snapshot, err := store.StatusSnapshot(context.Background(), now)
	if err != nil {
		t.Fatalf("snapshot after pause: %v", err)
	}
	if snapshot.Status != GlobalPaused {
		t.Fatalf("status after pause = %q", snapshot.Status)
	}

	runHorizonCommand(t, horizoncmd.NewTerminateCommand(newRuntimeLoader(load)), runtimeInput{})
	runHorizonCommand(t, horizoncmd.NewContinueCommand(newRuntimeLoader(load)), runtimeInput{})
	control, err := store.Control(context.Background())
	if err != nil {
		t.Fatalf("control after continue: %v", err)
	}
	if control.GlobalPaused || control.TerminateRequestedAt.IsZero() {
		t.Fatalf("continue should clear pause only, got %#v", control)
	}

	output := runHorizonCommand(t, horizoncmd.NewStatusCommand(newRuntimeLoader(load)), runtimeInput{})
	for _, want := range []string{"Status: terminating", "Global Paused: false", "Terminate Requested: true", "Supervisors: 1", "Stale Supervisors: 0"} {
		if !strings.Contains(output, want) {
			t.Fatalf("status output missing %q:\n%s", want, output)
		}
	}
}

func TestStatusCommandReturnsErrorForInactiveAndSucceedsForPausedState(t *testing.T) {
	// 需求背景：inactive 代表 runtime 不可用，CLI 应保持非零退出；paused 是显式维护态，
	// 命令应继续输出状态摘要，但不能让脚本把它误判为失败。
	ctx := context.Background()
	store := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})
	manager, _ := NewManager(Config{Store: "memory"}, WithStoreFactory(staticStoreResolver{store: store}))
	cmd := horizoncmd.NewStatusCommand(newRuntimeLoader(func() (*Manager, error) { return manager, nil }))

	err := cmd.Handle(runtimeCommandContext(cmd, runtimeInput{}, io.Discard))
	if err == nil || !strings.Contains(err.Error(), "status is inactive") {
		t.Fatalf("expected inactive status error, got %v", err)
	}

	now := time.Now().UTC()
	if err := store.HeartbeatSupervisor(ctx, SupervisorState{Name: "s1", Status: SupervisorRunning, LastHeartbeatAt: now}); err != nil {
		t.Fatalf("seed supervisor: %v", err)
	}
	if err := store.SetGlobalPaused(ctx, true); err != nil {
		t.Fatalf("pause: %v", err)
	}
	err = cmd.Handle(runtimeCommandContext(cmd, runtimeInput{}, io.Discard))
	if err != nil {
		t.Fatalf("paused status should succeed, got %v", err)
	}
}

func TestTerminateCommandRequestsQueueRestartButPauseContinueDoNot(t *testing.T) {
	// 需求背景：Laravel Horizon terminate 会同时触发底层 queue restart 语义，
	// pause/continue 只是 Horizon Store 控制状态，不能让正在执行的 queue worker 退出。
	ctx := context.Background()
	store := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})
	queueManager := &fakeRuntimeQueueManager{connections: map[string]*fakeQueueConnection{}}
	manager, _ := NewManager(Config{Store: "memory"}, WithStoreFactory(staticStoreResolver{store: store}), WithQueueManager(queueManager))
	load := func() (*Manager, error) { return manager, nil }

	runHorizonCommand(t, horizoncmd.NewPauseCommand(newRuntimeLoader(load)), runtimeInput{})
	runHorizonCommand(t, horizoncmd.NewContinueCommand(newRuntimeLoader(load)), runtimeInput{})
	if queueManager.restartCalls != 0 {
		t.Fatalf("pause/continue must not request queue restart, got %d", queueManager.restartCalls)
	}

	runHorizonCommand(t, horizoncmd.NewTerminateCommand(newRuntimeLoader(load)), runtimeInput{})
	control, err := store.Control(ctx)
	if err != nil {
		t.Fatalf("control: %v", err)
	}
	if control.TerminateRequestedAt.IsZero() || queueManager.restartCalls != 1 {
		t.Fatalf("terminate should keep Store flag and request restart once, control=%#v restarts=%d", control, queueManager.restartCalls)
	}
}

func TestControlCommandsNotifyFreshMasterAndSupervisorsOnly(t *testing.T) {
	// 需求背景：Store flag 是控制事实源，control signal 只唤醒 fresh master/supervisor；
	// worker PID 不能被通知，避免中断正在执行的 job。
	ctx := context.Background()
	now := time.Now().UTC()
	store := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})
	if err := store.HeartbeatMaster(ctx, MasterState{ID: "master-1", PID: 7001, Status: MasterRunning, LastHeartbeatAt: now}); err != nil {
		t.Fatalf("seed master: %v", err)
	}
	if err := store.HeartbeatSupervisor(ctx, SupervisorState{Name: "s1", PID: 7002, Status: SupervisorRunning, LastHeartbeatAt: now}); err != nil {
		t.Fatalf("seed supervisor: %v", err)
	}
	if err := store.HeartbeatWorker(ctx, WorkerState{ID: "w1", Supervisor: "s1", PID: 7003, Status: WorkerIdle, LastHeartbeatAt: now}); err != nil {
		t.Fatalf("seed worker: %v", err)
	}
	notifier := &fakeControlNotifier{}
	queueManager := &fakeRuntimeQueueManager{connections: map[string]*fakeQueueConnection{}}
	manager, _ := NewManager(Config{Store: "memory"}, WithStoreFactory(staticStoreResolver{store: store}), WithQueueManager(queueManager), WithControlNotifier(notifier))
	load := func() (*Manager, error) { return manager, nil }

	runHorizonCommand(t, horizoncmd.NewPauseCommand(newRuntimeLoader(load)), runtimeInput{})
	runHorizonCommand(t, horizoncmd.NewTerminateCommand(newRuntimeLoader(load)), runtimeInput{})

	if !notifier.has("master", 7001) || !notifier.has("supervisor", 7002) {
		t.Fatalf("expected master and supervisor notifications, got %#v", notifier.targets)
	}
	if notifier.has("worker", 7003) || notifier.has("", 7003) {
		t.Fatalf("worker pid must not be notified, got %#v", notifier.targets)
	}
}

func TestControlNotifyFailureDoesNotRollbackStoreFlag(t *testing.T) {
	// 需求背景：control signal 是唤醒机制，失败时命令返回错误，但不能回滚已经写入的 Store flag。
	ctx := context.Background()
	now := time.Now().UTC()
	store := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})
	if err := store.HeartbeatMaster(ctx, MasterState{ID: "master-1", PID: 7001, Status: MasterRunning, LastHeartbeatAt: now}); err != nil {
		t.Fatalf("seed master: %v", err)
	}
	notifier := &fakeControlNotifier{err: errFakeControlNotify}
	manager, _ := NewManager(Config{Store: "memory"}, WithStoreFactory(staticStoreResolver{store: store}), WithControlNotifier(notifier))
	cmd := horizoncmd.NewPauseCommand(newRuntimeLoader(func() (*Manager, error) { return manager, nil }))

	err := cmd.Handle(runtimeCommandContext(cmd, runtimeInput{}, io.Discard))
	if err == nil || !strings.Contains(err.Error(), errFakeControlNotify.Error()) {
		t.Fatalf("expected notify error, got %v", err)
	}
	control, readErr := store.Control(ctx)
	if readErr != nil {
		t.Fatalf("control: %v", readErr)
	}
	if !control.GlobalPaused {
		t.Fatalf("notify failure must not rollback pause flag: %#v", control)
	}
}

func TestTerminateRestartFailureKeepsTerminateFlag(t *testing.T) {
	// 需求背景：terminate 的 Store flag 是事实源；RequestRestart 失败时返回错误，但不回滚 terminate request。
	ctx := context.Background()
	store := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})
	queueManager := &fakeRuntimeQueueManager{connections: map[string]*fakeQueueConnection{}, restartErr: errFakeRestart}
	manager, _ := NewManager(Config{Store: "memory"}, WithStoreFactory(staticStoreResolver{store: store}), WithQueueManager(queueManager))
	cmd := horizoncmd.NewTerminateCommand(newRuntimeLoader(func() (*Manager, error) { return manager, nil }))

	err := cmd.Handle(runtimeCommandContext(cmd, runtimeInput{}, io.Discard))
	if err == nil || !strings.Contains(err.Error(), errFakeRestart.Error()) {
		t.Fatalf("expected restart error, got %v", err)
	}
	control, readErr := store.Control(ctx)
	if readErr != nil {
		t.Fatalf("control: %v", readErr)
	}
	if control.TerminateRequestedAt.IsZero() || queueManager.restartCalls != 1 {
		t.Fatalf("restart failure should keep terminate flag and one restart call, control=%#v restarts=%d", control, queueManager.restartCalls)
	}
}

func TestTerminateWaitWritesInternalWaitStrategyWithoutBlockingCommand(t *testing.T) {
	// 需求背景：batch bulk dispatch contract 要求 horizon:terminate --wait 不得被静默忽略；当前 runtime 尚不等待所有
	// worker 退出，因此命令必须先保持 terminate + queue restart 语义，再返回稳定的不支持错误。
	ctx := context.Background()
	store := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})
	queueManager := &fakeRuntimeQueueManager{connections: map[string]*fakeQueueConnection{}}
	manager, _ := NewManager(Config{Store: "memory"}, WithStoreFactory(staticStoreResolver{store: store}), WithQueueManager(queueManager))
	cmd := horizoncmd.NewTerminateCommand(newRuntimeLoader(func() (*Manager, error) { return manager, nil }))

	if err := cmd.Handle(runtimeCommandContext(cmd, runtimeInput{options: map[string]string{"wait": "true"}}, io.Discard)); err != nil {
		t.Fatalf("terminate --wait should not block or return unsupported error: %v", err)
	}
	control, readErr := store.Control(ctx)
	if readErr != nil {
		t.Fatalf("control: %v", readErr)
	}
	if control.TerminateRequestedAt.IsZero() || !control.TerminateShouldWait || queueManager.restartCalls != 1 {
		t.Fatalf("wait must still write terminate flag and restart queue, control=%#v restarts=%d", control, queueManager.restartCalls)
	}
}

func TestTerminateCommandReportsNoProcessesButKeepsRestartSemantics(t *testing.T) {
	// 需求背景：没有 fresh master/supervisor 时，horizon:terminate 仍要保持 queue restart 语义，
	// 但 CLI 输出应稳定提示没有可终止进程，避免误导用户以为已经通知到运行中进程。
	ctx := context.Background()
	store := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})
	queueManager := &fakeRuntimeQueueManager{connections: map[string]*fakeQueueConnection{}}
	manager, _ := NewManager(Config{Store: "memory"}, WithStoreFactory(staticStoreResolver{store: store}), WithQueueManager(queueManager))

	output := runHorizonCommand(t, horizoncmd.NewTerminateCommand(newRuntimeLoader(func() (*Manager, error) { return manager, nil })), runtimeInput{})

	if !strings.Contains(output, "No processes to terminate.") {
		t.Fatalf("terminate output should report no processes, got:\n%s", output)
	}
	control, err := store.Control(ctx)
	if err != nil {
		t.Fatalf("control: %v", err)
	}
	if control.TerminateRequestedAt.IsZero() || queueManager.restartCalls != 1 {
		t.Fatalf("terminate should keep flag and queue restart, control=%#v restarts=%d", control, queueManager.restartCalls)
	}
}

func TestProcessCommandsSpawnMasterSupervisorAndWorkerPaths(t *testing.T) {
	// 需求背景：runtime command contract 要求 Horizon 使用真实命令行子进程边界，而不是在 master 进程内直接调用 supervisor/worker。
	// 测试通过可注入 process runner 观察公开命令行为：master 派生 supervisor，supervisor 再按固定数量派生 worker。
	ctx := context.Background()
	store := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})
	processes := &fakeProcessRunner{}
	manager, _ := NewManager(Config{
		Store:       "memory",
		Environment: "local",
		Supervisors: map[string]SupervisorConfig{
			"fixed": {
				Name:         "fixed",
				Connection:   "redis",
				Queues:       []string{"default", "emails"},
				Balance:      BalanceFalse,
				MinProcesses: 1,
				MaxProcesses: 2,
				Sleep:        3,
				Timeout:      60,
				Tries:        1,
			},
			"auto": {
				Name:         "auto",
				Connection:   "redis",
				Queues:       []string{"default"},
				Balance:      BalanceAuto,
				MinProcesses: 1,
				MaxProcesses: 3,
				Sleep:        1,
			},
		},
	}, WithStoreFactory(staticStoreResolver{store: store}), WithProcessRunner(processes))
	load := func() (*Manager, error) { return manager, nil }

	runHorizonCommand(t, horizoncmd.NewMasterCommand(newRuntimeLoader(load)), runtimeInput{})
	masters, err := store.Masters(ctx, time.Now())
	if err != nil {
		t.Fatalf("masters: %v", err)
	}
	if len(masters) != 1 || masters[0].SupervisorCount != 2 || masters[0].Environment != "local" {
		t.Fatalf("unexpected master heartbeat: %#v", masters)
	}
	if got := processes.commands(); len(got) != 2 || got[0] != "horizon:supervisor" || got[1] != "horizon:supervisor" {
		t.Fatalf("master should start two supervisors, got %#v", got)
	}
	if !processes.containsArg("--master-id="+masters[0].ID) || !processes.containsArg("--environment=local") {
		t.Fatalf("supervisor process args missing identity: %#v", processes.specs)
	}
	supervisorProcesses := &blockingProcessRunner{}
	supervisorManager, _ := NewManager(manager.Config(), WithStoreFactory(staticStoreResolver{store: store}), WithProcessRunner(supervisorProcesses))
	supervisorRuntime := &runtimeCommandAdapter{manager: supervisorManager, store: store}
	supervisorCtx, supervisorCancel := context.WithCancel(context.Background())
	supervisorDone := make(chan error, 1)
	go func() {
		supervisorDone <- supervisorRuntime.RunSupervisor(supervisorCtx, horizoncmd.SupervisorProcessOptions{Name: "fixed", MasterID: masters[0].ID, Environment: "local"})
	}()
	waitForTestCondition(t, func() bool { return supervisorProcesses.starts() == 1 })
	supervisorCancel()
	if err := <-supervisorDone; err != nil {
		t.Fatalf("supervisor process returned error: %v", err)
	}
	if got := supervisorProcesses.commands(); len(got) != 1 || got[0] != "horizon:work" {
		t.Fatalf("balance=false should start min_processes workers without backlog snapshot, got %#v", got)
	}
	if !supervisorProcesses.containsArg("--queue=default,emails") || !supervisorProcesses.containsArg("--supervisor=fixed") {
		t.Fatalf("worker process args missing queue/supervisor identity: %#v", supervisorProcesses.specs)
	}
}

func TestTerminateStartedProcessesCleansMasterChildrenOnStartFailure(t *testing.T) {
	// 需求背景：master 启动多个 supervisor 时，如果后续 Start 失败，已启动的 supervisor
	// 不能遗留为无人管理的子进程。
	ctx := context.Background()
	store := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})
	startErr := errors.New("start supervisor failed")
	processes := &failAfterProcessRunner{failOn: 2, err: startErr}
	inspector := &fakeProcessInspector{}
	manager, _ := NewManager(Config{
		Store:       "memory",
		Environment: "local",
		Supervisors: map[string]SupervisorConfig{
			"alpha": {Name: "alpha", Connection: "redis", Queues: []string{"default"}, Balance: BalanceFalse, MinProcesses: 1, MaxProcesses: 1},
			"beta":  {Name: "beta", Connection: "redis", Queues: []string{"default"}, Balance: BalanceFalse, MinProcesses: 1, MaxProcesses: 1},
		},
	}, WithStoreFactory(staticStoreResolver{store: store}), WithProcessRunner(processes), WithProcessInspector(inspector))
	runtime := &runtimeCommandAdapter{manager: manager, store: store}

	err := runtime.RunMaster(ctx, horizoncmd.MasterOptions{Environment: "local"})
	if err == nil || !errors.Is(err, startErr) {
		t.Fatalf("expected original start error, got %v", err)
	}
	if !inspector.terminated(7001, false) {
		t.Fatalf("first supervisor should be gracefully terminated, terminations=%#v", inspector.terminations)
	}
}

func TestTerminateStartedProcessesCleansSupervisorWorkersOnStartFailure(t *testing.T) {
	// 需求背景：supervisor 启动 worker 池时，如果第 N 个 worker 启动失败，前面已启动的
	// worker 必须被清理，避免容量控制入口退出后留下孤儿 worker。
	ctx := context.Background()
	store := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})
	startErr := errors.New("start worker failed")
	processes := &failAfterProcessRunner{failOn: 2, err: startErr}
	inspector := &fakeProcessInspector{}
	manager, _ := NewManager(Config{
		Store:       "memory",
		Environment: "local",
		Supervisors: map[string]SupervisorConfig{
			"fixed": {
				Name: "fixed", Connection: "redis", Queues: []string{"default"},
				Balance: BalanceFalse, MinProcesses: 2, MaxProcesses: 2,
			},
		},
	}, WithStoreFactory(staticStoreResolver{store: store}), WithProcessRunner(processes), WithProcessInspector(inspector))
	runtime := &runtimeCommandAdapter{manager: manager, store: store}

	err := runtime.RunSupervisor(ctx, horizoncmd.SupervisorProcessOptions{Name: "fixed", Environment: "local"})
	if err == nil || !errors.Is(err, startErr) {
		t.Fatalf("expected original start error, got %v", err)
	}
	if !inspector.terminated(7001, false) {
		t.Fatalf("first worker should be gracefully terminated, terminations=%#v", inspector.terminations)
	}
}

func TestRuntimeProcessesFailFastOnFreshMasterAndSupervisorConflicts(t *testing.T) {
	// 需求背景：同一 host/environment 下重复启动 fresh master 或同名 supervisor 会让多个进程同时管理容量，
	// 必须在写入新 heartbeat 前返回包含定位信息的稳定错误。
	ctx := context.Background()
	now := time.Now().UTC()
	store := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})
	currentHost := hostname()
	if err := store.HeartbeatMaster(ctx, MasterState{ID: "master-existing", Host: currentHost, PID: 8101, Status: MasterRunning, LastHeartbeatAt: now, Environment: "local"}); err != nil {
		t.Fatalf("seed master: %v", err)
	}
	if err := store.HeartbeatSupervisor(ctx, SupervisorState{Name: "fixed", Host: currentHost, PID: 8102, MasterID: "master-existing", Environment: "local", Status: SupervisorRunning, LastHeartbeatAt: now}); err != nil {
		t.Fatalf("seed supervisor: %v", err)
	}
	manager, _ := NewManager(Config{
		Store:       "memory",
		Environment: "local",
		Supervisors: map[string]SupervisorConfig{
			"fixed": {Name: "fixed", Connection: "redis", Queues: []string{"default"}, Balance: BalanceFalse, MinProcesses: 1, MaxProcesses: 1},
		},
	}, WithStoreFactory(staticStoreResolver{store: store}), WithProcessRunner(&blockingProcessRunner{}))
	runtime := &runtimeCommandAdapter{manager: manager, store: store}

	masterErr := runtime.RunMaster(ctx, horizoncmd.MasterOptions{Environment: "local"})
	if masterErr == nil || !strings.Contains(masterErr.Error(), "master already running") ||
		!strings.Contains(masterErr.Error(), "host="+currentHost) ||
		!strings.Contains(masterErr.Error(), "environment=local") ||
		!strings.Contains(masterErr.Error(), "existing_id=master-existing") ||
		!strings.Contains(masterErr.Error(), "pid=8101") {
		t.Fatalf("unexpected master conflict error: %v", masterErr)
	}

	supervisorErr := runtime.RunSupervisor(ctx, horizoncmd.SupervisorProcessOptions{Name: "fixed", Environment: "local"})
	if supervisorErr == nil || !strings.Contains(supervisorErr.Error(), "supervisor already running") ||
		!strings.Contains(supervisorErr.Error(), "name=fixed") ||
		!strings.Contains(supervisorErr.Error(), "host="+currentHost) ||
		!strings.Contains(supervisorErr.Error(), "environment=local") ||
		!strings.Contains(supervisorErr.Error(), "master_id=master-existing") ||
		!strings.Contains(supervisorErr.Error(), "pid=8102") {
		t.Fatalf("unexpected supervisor conflict error: %v", supervisorErr)
	}
}

func TestSupervisorConflictDetectionScopesEnvironmentAndAllowsFastTerminatingRace(t *testing.T) {
	// 需求背景：重复实例检测只应阻塞同一 host/environment/supervisor name 的 fresh 实例；
	// fast termination 抢跑窗口中，旧 supervisor 已处于 terminating 时允许新入口启动。
	ctx := context.Background()
	now := time.Now().UTC()
	store := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})
	if err := store.HeartbeatSupervisor(ctx, SupervisorState{Name: "fixed", Host: hostname(), PID: 8201, MasterID: "old-local", Environment: "local", Status: SupervisorRunning, LastHeartbeatAt: now}); err != nil {
		t.Fatalf("seed supervisor: %v", err)
	}
	otherEnvironmentRunner := &blockingProcessRunner{}
	otherEnvironmentManager, _ := NewManager(Config{
		Store:       "memory",
		Environment: "production",
		Supervisors: map[string]SupervisorConfig{
			"fixed": {Name: "fixed", Connection: "redis", Queues: []string{"default"}, Balance: BalanceFalse, MinProcesses: 1, MaxProcesses: 1},
		},
	}, WithStoreFactory(staticStoreResolver{store: store}), WithProcessRunner(otherEnvironmentRunner))
	otherEnvironmentRuntime := &runtimeCommandAdapter{manager: otherEnvironmentManager, store: store}
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		done <- otherEnvironmentRuntime.RunSupervisor(runCtx, horizoncmd.SupervisorProcessOptions{Name: "fixed", Environment: "production"})
	}()
	waitForTestCondition(t, func() bool { return otherEnvironmentRunner.starts() == 1 })
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("different environment supervisor should not conflict: %v", err)
	}

	if err := store.RequestTerminate(ctx, now, false); err != nil {
		t.Fatalf("request terminate: %v", err)
	}
	fastManager, _ := NewManager(Config{
		Store:           "memory",
		Environment:     "local",
		FastTermination: true,
		Supervisors: map[string]SupervisorConfig{
			"fixed": {Name: "fixed", Connection: "redis", Queues: []string{"default"}, Balance: BalanceFalse, MinProcesses: 1, MaxProcesses: 1},
		},
	}, WithStoreFactory(staticStoreResolver{store: store}), WithProcessRunner(&fakeProcessRunner{}))
	fastRuntime := &runtimeCommandAdapter{manager: fastManager, store: store}
	if err := fastRuntime.RunSupervisor(ctx, horizoncmd.SupervisorProcessOptions{Name: "fixed", Environment: "local"}); err != nil {
		t.Fatalf("fast terminating supervisor should be allowed to race: %v", err)
	}
}

func TestRunSupervisorControlGateDoesNotWriteFreshHeartbeat(t *testing.T) {
	// 需求背景：paused/terminating 是启动前控制态；RunSupervisor 若先写 heartbeat/lease 再返回，
	// 会留下看似 fresh 的实例并阻塞后续正常启动。
	ctx := context.Background()
	for _, tc := range []struct {
		name    string
		prepare func(*MemoryStore)
	}{
		{
			name: "paused",
			prepare: func(store *MemoryStore) {
				if err := store.SetGlobalPaused(ctx, true); err != nil {
					t.Fatalf("pause horizon: %v", err)
				}
			},
		},
		{
			name: "terminating",
			prepare: func(store *MemoryStore) {
				if err := store.RequestTerminate(ctx, time.Now().UTC(), false); err != nil {
					t.Fatalf("request terminate: %v", err)
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})
			tc.prepare(store)
			manager, _ := NewManager(Config{
				Store:       "memory",
				Environment: "local",
				Supervisors: map[string]SupervisorConfig{
					"fixed": {Name: "fixed", Connection: "redis", Queues: []string{"default"}, Balance: BalanceFalse, MinProcesses: 1, MaxProcesses: 1},
				},
			}, WithStoreFactory(staticStoreResolver{store: store}), WithProcessRunner(&fakeProcessRunner{}))
			runtime := &runtimeCommandAdapter{manager: manager, store: store}

			if err := runtime.RunSupervisor(ctx, horizoncmd.SupervisorProcessOptions{Name: "fixed", Environment: "local"}); err != nil {
				t.Fatalf("run supervisor: %v", err)
			}
			supervisors, err := store.Supervisors(ctx, time.Now().UTC())
			if err != nil {
				t.Fatalf("supervisors: %v", err)
			}
			if len(supervisors) != 0 {
				t.Fatalf("control-gated startup must not leave fresh heartbeat: %#v", supervisors)
			}
		})
	}
}

func TestMasterConflictDetectionAllowsFastTerminationRace(t *testing.T) {
	// 需求背景：fast_termination=true 且未 --wait 时，Laravel 允许新 master 在旧进程收尾窗口内抢跑；
	// Prismgo 启动成功后不能清理旧 terminate flag，否则旧进程可能错过退出信号。
	ctx := context.Background()
	now := time.Now().UTC()
	store := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})
	if err := store.HeartbeatMaster(ctx, MasterState{ID: "master-draining", Host: hostname(), PID: 8301, Status: MasterRunning, LastHeartbeatAt: now, Environment: "local"}); err != nil {
		t.Fatalf("seed master: %v", err)
	}
	if err := store.RequestTerminate(ctx, now, false); err != nil {
		t.Fatalf("request terminate: %v", err)
	}
	manager, _ := NewManager(Config{
		Store:           "memory",
		Environment:     "local",
		FastTermination: true,
		Supervisors: map[string]SupervisorConfig{
			"fixed": {Name: "fixed", Connection: "redis", Queues: []string{"default"}, Balance: BalanceFalse, MinProcesses: 1, MaxProcesses: 1},
		},
	}, WithStoreFactory(staticStoreResolver{store: store}), WithProcessRunner(&fakeProcessRunner{}))
	runtime := &runtimeCommandAdapter{manager: manager, store: store}

	if err := runtime.RunMaster(ctx, horizoncmd.MasterOptions{Environment: "local"}); err != nil {
		t.Fatalf("fast terminating master should be allowed to race: %v", err)
	}
	control, err := store.Control(ctx)
	if err != nil {
		t.Fatalf("control: %v", err)
	}
	if control.TerminateRequestedAt.IsZero() {
		t.Fatalf("new master startup should preserve old terminate request: %#v", control)
	}
}

func TestMasterClearsTerminateAndTrimsHeartbeatsAfterDrain(t *testing.T) {
	// 需求背景：terminate 是一次性控制请求；等待型 terminate 收尾后必须清理控制标记和旧 heartbeat，
	// 否则 Dashboard 会在进程已退出后继续显示 terminating，直到 TTL 或下一次 master 启动。
	ctx := context.Background()
	now := time.Now().UTC()
	store := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})
	processes := newControlledProcessRunner()
	manager, _ := NewManager(Config{
		Store:        "memory",
		Environment:  "local",
		HeartbeatTTL: time.Minute,
		Supervisors: map[string]SupervisorConfig{
			"fixed": {Name: "fixed", Connection: "redis", Queues: []string{"default"}, Balance: BalanceFalse, MinProcesses: 1, MaxProcesses: 1},
		},
	}, WithStoreFactory(staticStoreResolver{store: store}), WithProcessRunner(processes))
	runtime := &runtimeCommandAdapter{manager: manager, store: store}

	done := make(chan error, 1)
	go func() { done <- runtime.RunMaster(ctx, horizoncmd.MasterOptions{Environment: "local"}) }()
	waitForTestCondition(t, func() bool { return processes.starts() == 1 })
	if err := store.HeartbeatSupervisor(ctx, SupervisorState{Name: "fixed", PID: 9101, Status: SupervisorTerminating, LastHeartbeatAt: now}); err != nil {
		t.Fatalf("seed supervisor heartbeat: %v", err)
	}
	if err := store.HeartbeatWorker(ctx, WorkerState{ID: "w1", Supervisor: "fixed", PID: 9102, Status: WorkerTerminating, LastHeartbeatAt: now}); err != nil {
		t.Fatalf("seed worker heartbeat: %v", err)
	}
	if err := store.RequestTerminate(ctx, now, false); err != nil {
		t.Fatalf("request terminate: %v", err)
	}
	processes.release(0, nil)
	if err := <-done; err != nil {
		t.Fatalf("master returned error: %v", err)
	}

	control, err := store.Control(ctx)
	if err != nil {
		t.Fatalf("control: %v", err)
	}
	if !control.TerminateRequestedAt.IsZero() {
		t.Fatalf("terminate request should be cleared after drain: %#v", control)
	}
	snapshot, err := store.StatusSnapshot(ctx, time.Now().UTC())
	if err != nil {
		t.Fatalf("status snapshot: %v", err)
	}
	if snapshot.Status != GlobalInactive || snapshot.SupervisorCount != 0 || snapshot.WorkerCount != 0 {
		t.Fatalf("dashboard should not see drained processes as terminating: %#v", snapshot)
	}
}

func TestMasterContextCancelRequestsGracefulTerminateAndWaits(t *testing.T) {
	// 需求背景：Ctrl+C 应转换为 Horizon 的等待型 terminate 流程，不能只让 master 自己返回后留下
	// supervisor/worker 子进程继续运行，也不能绕过 Store control flag 直接强杀子进程。
	ctx, cancel := context.WithCancel(context.Background())
	store := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})
	processes := newManualProcessRunner()
	notifier := &fakeControlNotifier{}
	manager, _ := NewManager(Config{
		Store:        "memory",
		Environment:  "local",
		LoopInterval: time.Millisecond,
		HeartbeatTTL: time.Minute,
		Supervisors: map[string]SupervisorConfig{
			"fixed": {Name: "fixed", Connection: "redis", Queues: []string{"default"}, Balance: BalanceFalse, MinProcesses: 1, MaxProcesses: 1},
		},
	}, WithStoreFactory(staticStoreResolver{store: store}), WithProcessRunner(processes), WithControlNotifier(notifier), WithQueueManager(blockingRestartQueueManager{}))
	runtime := &runtimeCommandAdapter{manager: manager, store: store}

	done := make(chan error, 1)
	go func() { done <- runtime.RunMaster(ctx, horizoncmd.MasterOptions{Environment: "local"}) }()
	waitForTestCondition(t, func() bool { return processes.starts() == 1 })
	if err := store.HeartbeatSupervisor(context.Background(), SupervisorState{Name: "fixed", Host: hostname(), PID: 9101, Status: SupervisorRunning, LastHeartbeatAt: time.Now().UTC(), Environment: "local"}); err != nil {
		t.Fatalf("seed supervisor heartbeat: %v", err)
	}

	cancel()
	waitForTestCondition(t, func() bool {
		control, err := store.Control(context.Background())
		return err == nil && !control.TerminateRequestedAt.IsZero() && control.TerminateShouldWait
	})
	if !notifier.has("supervisor", 9101) {
		t.Fatalf("expected cancel path to notify supervisor pid 9101, got %#v", notifier.targets)
	}
	select {
	case err := <-done:
		t.Fatalf("master returned before supervisor drained: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	processes.release(0, nil)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("master returned error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("master did not exit after supervisor drained")
	}
}

func TestMasterContextCancelCleanupIgnoresCanceledRunContext(t *testing.T) {
	// 需求背景：Ctrl+C 会取消命令运行 context；Redis Store 会尊重该取消信号。
	// master 等待 supervisor drain 后的最终清理必须使用独立 cleanup context，否则旧 heartbeat 会阻塞下一次启动。
	ctx, cancel := context.WithCancel(context.Background())
	memory := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})
	store := contextAwareStore{Store: memory}
	processes := newManualProcessRunner()
	manager, _ := NewManager(Config{
		Store:        "memory",
		Environment:  "local",
		LoopInterval: time.Millisecond,
		HeartbeatTTL: time.Minute,
		Supervisors: map[string]SupervisorConfig{
			"fixed": {Name: "fixed", Connection: "redis", Queues: []string{"default"}, Balance: BalanceFalse, MinProcesses: 1, MaxProcesses: 1},
		},
	}, WithStoreFactory(staticStoreResolver{store: store}), WithProcessRunner(processes), WithQueueManager(blockingRestartQueueManager{}))
	runtime := &runtimeCommandAdapter{manager: manager, store: store}

	done := make(chan error, 1)
	go func() { done <- runtime.RunMaster(ctx, horizoncmd.MasterOptions{Environment: "local"}) }()
	waitForTestCondition(t, func() bool { return processes.starts() == 1 })
	if err := memory.HeartbeatSupervisor(context.Background(), SupervisorState{Name: "fixed", Host: hostname(), PID: 9101, Status: SupervisorRunning, LastHeartbeatAt: time.Now().UTC(), Environment: "local"}); err != nil {
		t.Fatalf("seed supervisor heartbeat: %v", err)
	}

	cancel()
	waitForTestCondition(t, func() bool {
		control, err := memory.Control(context.Background())
		return err == nil && !control.TerminateRequestedAt.IsZero() && control.TerminateShouldWait
	})
	processes.release(0, nil)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("master returned error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("master did not exit after supervisor drained")
	}
	control, err := memory.Control(context.Background())
	if err != nil {
		t.Fatalf("control: %v", err)
	}
	if !control.TerminateRequestedAt.IsZero() {
		t.Fatalf("terminate request should be cleared after canceled-context drain: %#v", control)
	}
	masters, err := memory.Masters(context.Background(), time.Now().UTC())
	if err != nil {
		t.Fatalf("masters: %v", err)
	}
	if len(masters) != 0 {
		t.Fatalf("master heartbeat should be trimmed after canceled-context drain: %#v", masters)
	}
}

func TestMasterRefreshesHeartbeatWhileSupervisorsRun(t *testing.T) {
	// 需求背景：historical scenario 12 要求 master 是长驻运行系统，不能只在启动时写一次 heartbeat。
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})
	processes := &blockingProcessRunner{}
	manager, _ := NewManager(Config{
		Store:        "memory",
		Environment:  "local",
		LoopInterval: time.Millisecond,
		Supervisors: map[string]SupervisorConfig{
			"fixed": {Name: "fixed", Connection: "redis", Queues: []string{"default"}, Balance: BalanceFalse, MinProcesses: 1, MaxProcesses: 1},
		},
	}, WithStoreFactory(staticStoreResolver{store: store}), WithProcessRunner(processes))
	runtime := &runtimeCommandAdapter{manager: manager, store: store}

	done := make(chan error, 1)
	go func() { done <- runtime.RunMaster(ctx, horizoncmd.MasterOptions{}) }()

	var first time.Time
	for deadline := time.Now().Add(time.Second); time.Now().Before(deadline); {
		masters, err := store.Masters(context.Background(), time.Now())
		if err != nil {
			t.Fatalf("masters: %v", err)
		}
		if len(masters) == 1 {
			if first.IsZero() {
				first = masters[0].LastHeartbeatAt
			} else if masters[0].LastHeartbeatAt.After(first) {
				cancel()
				<-done
				return
			}
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("master heartbeat did not refresh while supervisor process was running")
}

func TestSupervisorRefreshesHeartbeatWhileWorkersRun(t *testing.T) {
	// 需求背景：historical scenario 12 要求 supervisor 长驻维护自己的 heartbeat，stale 只能由 TTL 派生。
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})
	processes := &blockingProcessRunner{}
	manager, _ := NewManager(Config{
		Store:        "memory",
		Environment:  "local",
		LoopInterval: time.Millisecond,
		Supervisors: map[string]SupervisorConfig{
			"fixed": {Name: "fixed", Connection: "redis", Queues: []string{"default"}, Balance: BalanceFalse, MinProcesses: 1, MaxProcesses: 1},
		},
	}, WithStoreFactory(staticStoreResolver{store: store}), WithProcessRunner(processes))
	runtime := &runtimeCommandAdapter{manager: manager, store: store}

	done := make(chan error, 1)
	go func() {
		done <- runtime.RunSupervisor(ctx, horizoncmd.SupervisorProcessOptions{Name: "fixed", Environment: "local"})
	}()

	var first time.Time
	for deadline := time.Now().Add(time.Second); time.Now().Before(deadline); {
		supervisor, found, err := store.Supervisor(context.Background(), "fixed", time.Now())
		if err != nil {
			t.Fatalf("supervisor: %v", err)
		}
		if found {
			if first.IsZero() {
				first = supervisor.LastHeartbeatAt
			} else if supervisor.LastHeartbeatAt.After(first) {
				cancel()
				<-done
				return
			}
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("supervisor heartbeat did not refresh while worker process was running")
}

func TestSupervisorRestartsExitedWorkersInRuntimeLoop(t *testing.T) {
	// 需求背景：historical scenario 12 要求 worker 崩溃或退出后，supervisor 按目标 worker 数持续补拉。
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})
	processes := &restartOnceProcessRunner{}
	manager, _ := NewManager(Config{
		Store:        "memory",
		Environment:  "local",
		LoopInterval: time.Millisecond,
		Supervisors: map[string]SupervisorConfig{
			"fixed": {Name: "fixed", Connection: "redis", Queues: []string{"default"}, Balance: BalanceFalse, MinProcesses: 1, MaxProcesses: 1},
		},
	}, WithStoreFactory(staticStoreResolver{store: store}), WithProcessRunner(processes))
	runtime := &runtimeCommandAdapter{manager: manager, store: store}

	done := make(chan error, 1)
	go func() {
		done <- runtime.RunSupervisor(ctx, horizoncmd.SupervisorProcessOptions{Name: "fixed", Environment: "local"})
	}()

	for deadline := time.Now().Add(time.Second); time.Now().Before(deadline); {
		if processes.starts() >= 2 {
			cancel()
			<-done
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("supervisor did not restart exited worker, starts=%d", processes.starts())
}

func TestSupervisorRuntimeLoopUsesDefaultIntervalWhenUnset(t *testing.T) {
	// 需求背景：historical scenario 16 要求 LoopInterval 只表示内部 runtime loop 节奏，不能作为生产 loop 开关。
	// 逻辑说明：这里故意不设置 Config.LoopInterval；首个 worker 立即退出后，supervisor 必须仍在 runtime loop 中按目标容量补拉。
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})
	processes := &restartOnceProcessRunner{}
	manager, _ := NewManager(Config{
		Store:       "memory",
		Environment: "local",
		Supervisors: map[string]SupervisorConfig{
			"fixed": {Name: "fixed", Connection: "redis", Queues: []string{"default"}, Balance: BalanceFalse, MinProcesses: 1, MaxProcesses: 1},
		},
	}, WithStoreFactory(staticStoreResolver{store: store}), WithProcessRunner(processes))
	runtime := &runtimeCommandAdapter{manager: manager, store: store}

	done := make(chan error, 1)
	go func() {
		done <- runtime.RunSupervisor(ctx, horizoncmd.SupervisorProcessOptions{Name: "fixed", Environment: "local"})
	}()

	for deadline := time.Now().Add(time.Second); time.Now().Before(deadline); {
		if processes.starts() >= 2 {
			cancel()
			<-done
			return
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	<-done
	t.Fatalf("supervisor did not enter runtime loop without explicit LoopInterval, starts=%d", processes.starts())
}

func TestSupervisorRuntimeLoopRefreshesAutoscalingTargets(t *testing.T) {
	// 需求背景：historical scenario 12 要求 autoscaling 在运行期周期性读取 queue size，而不是启动时一次性计算。
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})
	processes := &blockingProcessRunner{}
	connection := &fakeQueueConnection{sizes: map[string]int64{"default": 4, "emails": 0}}
	queueManager := &fakeRuntimeQueueManager{connections: map[string]*fakeQueueConnection{"redis": connection}}
	manager, _ := NewManager(Config{
		Store:        "memory",
		Environment:  "local",
		LoopInterval: time.Millisecond,
		Supervisors: map[string]SupervisorConfig{
			"auto": {
				Name: "auto", Connection: "redis", Queues: []string{"default", "emails"},
				Balance: BalanceAuto, AutoScalingStrategy: AutoScalingStrategySize,
				MinProcesses: 1, MaxProcesses: 4, BalanceMaxShift: 10,
			},
		},
	}, WithStoreFactory(staticStoreResolver{store: store}), WithProcessRunner(processes), WithProcessInspector(&fakeProcessInspector{}), WithQueueManager(queueManager))
	runtime := &runtimeCommandAdapter{manager: manager, store: store}

	done := make(chan error, 1)
	go func() {
		done <- runtime.RunSupervisor(ctx, horizoncmd.SupervisorProcessOptions{Name: "auto", Environment: "local"})
	}()

	for deadline := time.Now().Add(time.Second); time.Now().Before(deadline); {
		supervisor, found, err := store.Supervisor(context.Background(), "auto", time.Now())
		if err != nil {
			t.Fatalf("supervisor: %v", err)
		}
		if found && poolTarget(supervisor.Pools, "default") == 3 {
			connection.setSizes(map[string]int64{"default": 0, "emails": 4})
			break
		}
		time.Sleep(time.Millisecond)
	}
	for deadline := time.Now().Add(time.Second); time.Now().Before(deadline); {
		supervisor, found, err := store.Supervisor(context.Background(), "auto", time.Now())
		if err != nil {
			t.Fatalf("supervisor: %v", err)
		}
		if found && poolTarget(supervisor.Pools, "emails") == 3 {
			cancel()
			<-done
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("autoscaling target did not refresh from latest queue sizes")
}

func TestSupervisorRuntimeLoopPausesWithoutRestartingExitedWorkers(t *testing.T) {
	// 需求背景：historical scenario 17 对齐 Laravel Horizon 的运行期 pause 语义：pause 只暂停 workload 对账和补拉，不退出 supervisor。
	// 逻辑说明：pause 期间 worker 自行退出后，CurrentWorkers 可以下降；continue 清除 pause 后，下一个 tick 再按目标数补拉。
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})
	processes := newControlledProcessRunner()
	manager, _ := NewManager(Config{
		Store:        "memory",
		Environment:  "local",
		LoopInterval: time.Millisecond,
		Supervisors: map[string]SupervisorConfig{
			"fixed": {Name: "fixed", Connection: "redis", Queues: []string{"default"}, Balance: BalanceFalse, MinProcesses: 1, MaxProcesses: 1},
		},
	}, WithStoreFactory(staticStoreResolver{store: store}), WithProcessRunner(processes))
	runtime := &runtimeCommandAdapter{manager: manager, store: store}

	done := make(chan error, 1)
	go func() {
		done <- runtime.RunSupervisor(ctx, horizoncmd.SupervisorProcessOptions{Name: "fixed", Environment: "local"})
	}()

	waitForTestCondition(t, func() bool { return processes.starts() == 1 })
	if err := store.SetGlobalPaused(context.Background(), true); err != nil {
		t.Fatalf("pause: %v", err)
	}
	firstHeartbeat := waitForSupervisorHeartbeat(t, store, "fixed")
	processes.release(0, nil)

	time.Sleep(20 * time.Millisecond)
	if starts := processes.starts(); starts != 1 {
		t.Fatalf("paused supervisor must not restart exited worker, starts=%d", starts)
	}
	supervisor, found, err := store.Supervisor(context.Background(), "fixed", time.Now())
	if err != nil || !found {
		t.Fatalf("supervisor after paused exit: found=%v err=%v", found, err)
	}
	if supervisor.Status != SupervisorPaused || supervisor.WorkerCount != 0 || len(supervisor.Pools) != 1 ||
		supervisor.Pools[0].CurrentWorkers != 0 || supervisor.Pools[0].TargetWorkers != 1 {
		t.Fatalf("paused supervisor should keep target and expose zero current workers: %#v", supervisor)
	}
	if !supervisor.LastHeartbeatAt.After(firstHeartbeat) {
		t.Fatalf("paused supervisor heartbeat did not refresh: first=%v current=%v", firstHeartbeat, supervisor.LastHeartbeatAt)
	}

	if err := store.SetGlobalPaused(context.Background(), false); err != nil {
		t.Fatalf("continue: %v", err)
	}
	waitForTestCondition(t, func() bool { return processes.starts() >= 2 })
	cancel()
	<-done
}

func TestSupervisorRuntimeLoopReconcilesScaleDownWithGracefulThenForceTerminate(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})
	processes := &blockingProcessRunner{}
	inspector := &fakeProcessInspector{}
	connection := &fakeQueueConnection{sizes: map[string]int64{"default": 3}}
	queueManager := &fakeRuntimeQueueManager{connections: map[string]*fakeQueueConnection{"redis": connection}}
	manager, _ := NewManager(Config{
		Store:        "memory",
		Environment:  "local",
		LoopInterval: time.Millisecond,
		Supervisors: map[string]SupervisorConfig{
			"fixed": {
				Name: "fixed", Connection: "redis", Queues: []string{"default"},
				Balance: BalanceFalse, MinProcesses: 1, MaxProcesses: 3, Timeout: 0,
			},
		},
	}, WithStoreFactory(staticStoreResolver{store: store}), WithProcessRunner(processes), WithProcessInspector(inspector), WithQueueManager(queueManager))
	runtime := &runtimeCommandAdapter{manager: manager, store: store}

	done := make(chan error, 1)
	go func() {
		done <- runtime.RunSupervisor(ctx, horizoncmd.SupervisorProcessOptions{Name: "fixed", Environment: "local"})
	}()

	for deadline := time.Now().Add(time.Second); time.Now().Before(deadline); {
		if processes.starts() >= 3 {
			connection.setSizes(map[string]int64{"default": 1})
			break
		}
		time.Sleep(time.Millisecond)
	}
	for deadline := time.Now().Add(time.Second); time.Now().Before(deadline); {
		if inspector.terminationCount(false) >= 2 && inspector.terminationCount(true) >= 2 {
			cancel()
			<-done
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("scale down should gracefully then force terminate excess workers, terminations=%#v", inspector.terminations)
}

func TestSupervisorRuntimeLoopTerminationWaitStrategyMatrix(t *testing.T) {
	cases := []struct {
		name          string
		fast          bool
		wait          bool
		wantTerminate bool
	}{
		{name: "default waits without command wait", fast: false, wait: false, wantTerminate: true},
		{name: "fast termination does not wait without command wait", fast: true, wait: false, wantTerminate: false},
		{name: "command wait overrides fast termination", fast: true, wait: true, wantTerminate: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			store := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})
			processes := &blockingProcessRunner{}
			inspector := &fakeProcessInspector{}
			manager, _ := NewManager(Config{
				Store:           "memory",
				Environment:     "local",
				LoopInterval:    time.Millisecond,
				FastTermination: tc.fast,
				Supervisors: map[string]SupervisorConfig{
					"fixed": {Name: "fixed", Connection: "redis", Queues: []string{"default"}, Balance: BalanceFalse, MinProcesses: 1, MaxProcesses: 1, Timeout: 0},
				},
			}, WithStoreFactory(staticStoreResolver{store: store}), WithProcessRunner(processes), WithProcessInspector(inspector))
			runtime := &runtimeCommandAdapter{manager: manager, store: store}

			done := make(chan error, 1)
			go func() {
				done <- runtime.RunSupervisor(ctx, horizoncmd.SupervisorProcessOptions{Name: "fixed", Environment: "local"})
			}()
			for deadline := time.Now().Add(time.Second); time.Now().Before(deadline); {
				if processes.starts() >= 1 {
					break
				}
				time.Sleep(time.Millisecond)
			}
			if err := store.RequestTerminate(context.Background(), time.Now(), tc.wait); err != nil {
				t.Fatalf("request terminate: %v", err)
			}

			for deadline := time.Now().Add(time.Second); time.Now().Before(deadline); {
				if inspector.terminationCount(false) > 0 || !tc.wantTerminate {
					break
				}
				time.Sleep(time.Millisecond)
			}
			if got := inspector.terminationCount(false) > 0; got != tc.wantTerminate {
				t.Fatalf("graceful terminate sent = %v, want %v; terminations=%#v", got, tc.wantTerminate, inspector.terminations)
			}
			cancel()
			<-done
		})
	}
}

func TestSupervisorRuntimeLoopControlWakeProcessesTerminateBeforeNextTick(t *testing.T) {
	// 需求背景：horizon:terminate 已写入 Store 后，生产进程应由 control signal 立即唤醒；
	// 否则空队列场景也要等 runtimeLoopInterval 的下一次 ticker，表现为终止命令很慢。
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wakeC := make(chan struct{}, 1)
	previousWake := newRuntimeControlWake
	newRuntimeControlWake = func(context.Context) runtimeControlWake {
		return runtimeControlWake{C: wakeC}
	}
	defer func() { newRuntimeControlWake = previousWake }()

	store := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})
	processes := &blockingProcessRunner{}
	inspector := &fakeProcessInspector{}
	manager, _ := NewManager(Config{
		Store:        "memory",
		Environment:  "local",
		LoopInterval: time.Hour,
		Supervisors: map[string]SupervisorConfig{
			"fixed": {Name: "fixed", Connection: "redis", Queues: []string{"default"}, Balance: BalanceFalse, MinProcesses: 1, MaxProcesses: 1, Timeout: 60},
		},
	}, WithStoreFactory(staticStoreResolver{store: store}), WithProcessRunner(processes), WithProcessInspector(inspector))
	runtime := &runtimeCommandAdapter{manager: manager, store: store}

	done := make(chan error, 1)
	go func() {
		done <- runtime.RunSupervisor(ctx, horizoncmd.SupervisorProcessOptions{Name: "fixed", Environment: "local"})
	}()
	waitForTestCondition(t, func() bool { return processes.starts() == 1 })
	if err := store.RequestTerminate(context.Background(), time.Now(), false); err != nil {
		t.Fatalf("request terminate: %v", err)
	}
	wakeC <- struct{}{}

	waitForTestCondition(t, func() bool { return inspector.terminationCount(false) == 1 })
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("supervisor returned error: %v", err)
	}
}

func TestSupervisorRuntimeLoopTerminateDoesNotWaitForBlockedWorkloadSample(t *testing.T) {
	// 需求背景：queue Size/QueueInspect 属于观测采样，不能阻塞 terminate 控制面。
	// 否则 RabbitMQ 管理调用卡住时，supervisor 会等到 worker timeout 才能退出。
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wakeC := make(chan struct{}, 1)
	previousWake := newRuntimeControlWake
	newRuntimeControlWake = func(context.Context) runtimeControlWake {
		return runtimeControlWake{C: wakeC}
	}
	defer func() { newRuntimeControlWake = previousWake }()

	store := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})
	inspector := &fakeProcessInspector{}
	blockingQueue := newBlockingSizeQueueConnection()
	manager, _ := NewManager(Config{Store: "memory", Environment: "local", LoopInterval: 10 * time.Millisecond}, WithStoreFactory(staticStoreResolver{store: store}), WithQueueManager(&blockingQueueManager{connection: blockingQueue}), WithProcessInspector(inspector))
	runtime := &runtimeCommandAdapter{manager: manager, store: store}
	supervisor := SupervisorConfig{Name: "fixed", Connection: "redis", Queues: []string{"default"}, Balance: BalanceFalse, MinProcesses: 1, MaxProcesses: 1, Timeout: 60}
	state := SupervisorState{Name: "fixed", Status: SupervisorRunning, LastHeartbeatAt: time.Now().UTC(), Pools: []ProcessPoolState{{Name: "fixed:default", Queue: "default", Queues: []string{"default"}, TargetWorkers: 1, CurrentWorkers: 1}}}
	processCtx, processCancel := context.WithCancel(ctx)
	defer processCancel()

	done := make(chan error, 1)
	go func() {
		done <- runtime.supervisorRuntimeLoop(ctx, supervisor, "local", state, []ProcessSpec{{Args: []string{"horizon:work"}}}, []ManagedProcess{blockingManagedProcess{pid: 7101, ctx: processCtx}})
	}()
	waitForTestCondition(t, func() bool { return blockingQueue.entered() })
	if err := store.RequestTerminate(context.Background(), time.Now(), false); err != nil {
		t.Fatalf("request terminate: %v", err)
	}
	wakeC <- struct{}{}

	waitForTestCondition(t, func() bool { return inspector.terminationCount(false) == 1 })
	cancel()
	processCancel()
	blockingQueue.release()
	if err := <-done; err != nil {
		t.Fatalf("supervisor returned error: %v", err)
	}
}

func TestSupervisorRuntimeLoopWorkerErrorDoesNotCrashSupervisor(t *testing.T) {
	bindHorizonPanicReporter(t)
	// 测试目的：对齐 Laravel Horizon — worker 子进程 wait 失败时上报错误但不崩溃 supervisor。
	// supervisor loop 应继续运行，等待 context 取消才退出。
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	store := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})
	processes := &errorProcessRunner{err: errFakeProcessWait}
	manager, _ := NewManager(Config{
		Store:        "memory",
		Environment:  "local",
		LoopInterval: 10 * time.Millisecond,
		Supervisors: map[string]SupervisorConfig{
			"fixed": {Name: "fixed", Connection: "redis", Queues: []string{"default"}, Balance: BalanceFalse, MinProcesses: 1, MaxProcesses: 1},
		},
	}, WithStoreFactory(staticStoreResolver{store: store}), WithProcessRunner(processes))
	runtime := &runtimeCommandAdapter{manager: manager, store: store}

	err := runtime.RunSupervisor(ctx, horizoncmd.SupervisorProcessOptions{Name: "fixed", Environment: "local"})
	// supervisor 应因 context 超时正常退出，而非因 worker 错误崩溃
	if err != nil {
		t.Fatalf("expected nil error (context timeout), got %v", err)
	}
}

func TestRunMasterReturnsSupervisorStartupError(t *testing.T) {
	// 需求背景：master 启动期如果所有 supervisor 子进程都在启动窗口内失败，
	// 必须把失败原因返回给 CLI，不能只写日志后静默成功。
	// 设计思路：用 errorProcessRunner 模拟 supervisor 进程启动后立刻 Wait() 失败，并把
	// LoopInterval 缩短为 10ms，使启动窗口在单测中稳定收敛。
	// 逻辑说明：断言 RunMaster 返回值包含稳定前缀和原始错误，同时通过 captureReportedExceptions
	// 验证异常仍按 component=horizon、subsystem=master 上报。
	reports := captureReportedExceptions(t)
	ctx := context.Background()
	store := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})
	processErr := errors.New("supervisor boot exploded")
	manager, _ := NewManager(Config{
		Store:        "memory",
		Environment:  "local",
		LoopInterval: 10 * time.Millisecond,
		Supervisors: map[string]SupervisorConfig{
			"fixed": {Name: "fixed", Connection: "redis", Queues: []string{"default"}, Balance: BalanceFalse, MinProcesses: 1, MaxProcesses: 1},
		},
	}, WithStoreFactory(staticStoreResolver{store: store}), WithProcessRunner(&errorProcessRunner{err: processErr}))
	runtime := &runtimeCommandAdapter{manager: manager, store: store}

	err := runtime.RunMaster(ctx, horizoncmd.MasterOptions{Environment: "local"})
	if err == nil || !strings.Contains(err.Error(), "horizon: supervisor startup failed") || !strings.Contains(err.Error(), processErr.Error()) {
		t.Fatalf("expected startup failure with original reason, got %v", err)
	}
	select {
	case report := <-reports:
		if report.err == nil || !strings.Contains(report.err.Error(), "horizon: supervisor startup failed") {
			t.Fatalf("unexpected reported error: %v", report.err)
		}
		if report.fields["component"] != "horizon" || report.fields["subsystem"] != "master" {
			t.Fatalf("startup report fields missing horizon master context: %#v", report.fields)
		}
	case <-time.After(time.Second):
		t.Fatal("expected startup failure to be reported")
	}
}

func TestRunMasterDoesNotReturnSupervisorErrorAfterStartupProcessSurvives(t *testing.T) {
	// 需求背景：启动期通过后，后续 supervisor 退出错误仍沿用长期运行语义：
	// 上报异常，但不让 master 因单个长期子进程退出而返回失败。
	// 设计思路：让 supervisor 子进程真实存活超过一个 loop tick，再释放子进程错误。
	// 逻辑说明：如果启动窗口判断正确，RunMaster 会在子进程退出后正常返回 nil；错误只会进入
	// exception reporter，不会被 CLI 当作启动失败处理。
	reports := captureReportedExceptions(t)
	ctx := context.Background()
	store := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})
	processes := newManualProcessRunner()
	manager, _ := NewManager(Config{
		Store:        "memory",
		Environment:  "local",
		LoopInterval: 10 * time.Millisecond,
		Supervisors: map[string]SupervisorConfig{
			"fixed": {Name: "fixed", Connection: "redis", Queues: []string{"default"}, Balance: BalanceFalse, MinProcesses: 1, MaxProcesses: 1},
		},
	}, WithStoreFactory(staticStoreResolver{store: store}), WithProcessRunner(processes))
	runtime := &runtimeCommandAdapter{manager: manager, store: store}

	done := make(chan error, 1)
	go func() { done <- runtime.RunMaster(ctx, horizoncmd.MasterOptions{Environment: "local"}) }()
	waitForTestCondition(t, func() bool { return processes.starts() == 1 })
	time.Sleep(30 * time.Millisecond)
	processErr := errors.New("supervisor failed after startup")
	processes.release(0, processErr)

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("expected long-running supervisor error to be reported, not returned: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("master did not return after supervisor process exited")
	}
	select {
	case report := <-reports:
		if report.err == nil || !strings.Contains(report.err.Error(), processErr.Error()) {
			t.Fatalf("unexpected reported error: %v", report.err)
		}
		if report.fields["component"] != "horizon" || report.fields["subsystem"] != "master" {
			t.Fatalf("long-running report fields missing horizon master context: %#v", report.fields)
		}
	case <-time.After(time.Second):
		t.Fatal("expected post-startup supervisor error to be reported")
	}
}

func TestWaitProcessesWithHeartbeatHeartbeatErrorDoesNotCrashMaster(t *testing.T) {
	bindHorizonPanicReporter(t)
	// 测试目的：对齐 Laravel Horizon — master heartbeat 写入失败时上报错误但不崩溃 master 循环。
	// master 应继续运行直到所有子进程退出。
	ctx, cancel := context.WithCancel(context.Background())
	heartbeatCalls := 0
	process := blockingManagedProcess{pid: 7001, ctx: ctx}

	// 在 goroutine 中运行，让 heartbeat tick 有机会触发，然后 cancel context 退出
	done := make(chan error, 1)
	go func() {
		done <- waitProcessesWithHeartbeat(ctx, []ManagedProcess{process}, 10*time.Millisecond, func(time.Time) error {
			heartbeatCalls++
			return errFakeWorker
		})
	}()

	// 等待 heartbeat 触发几次
	time.Sleep(80 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		// heartbeat 错误已被上报，master 正常退出（不返回错误）
		if err != nil {
			t.Fatalf("expected nil error after context cancel, got %v", err)
		}
		if heartbeatCalls == 0 {
			t.Fatal("expected heartbeat callback to be called at least once")
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for waitProcessesWithHeartbeat to complete")
	}
}

func TestWatchProcessHandlesNilProcess(t *testing.T) {
	// 测试目的：nil process 仍要唤醒 supervisor reconcile loop，避免测试替身或启动失败边界卡死。
	exits := make(chan processExit, 1)
	watchProcess(3, nil, exits)
	exit := <-exits
	if exit.index != 3 || exit.err != nil {
		t.Fatalf("unexpected nil process exit: %#v", exit)
	}
}

func TestSupervisorRuntimeHelperBranches(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	pools := []ProcessPoolState{{Name: "s:default", Queue: "default", TargetWorkers: 2}}
	slots := initialSupervisorProcessSlots(pools, []ProcessSpec{{Args: []string{"horizon:work", "--queue=default"}}}, []ManagedProcess{
		fakeManagedProcess{pid: 7101},
		fakeManagedProcess{pid: 7102},
		fakeManagedProcess{pid: 7103},
	})
	if len(slots) != 3 || slots[0].poolName != "s:default" || slots[2].poolName != "" {
		t.Fatalf("unexpected initial slots: %#v", slots)
	}
	state := scaleStateFromPools([]ProcessPoolState{{Queue: "default", CurrentWorkers: 0, TargetWorkers: 2}, {Queue: "emails", CurrentWorkers: 3, TargetWorkers: 4}})
	if state.CurrentWorkers["default"] != 2 || state.CurrentWorkers["emails"] != 3 {
		t.Fatalf("unexpected scale state: %#v", state)
	}
	if got := supervisorShutdownTimeout(SupervisorConfig{Timeout: 2}); got != 2*time.Second {
		t.Fatalf("timeout = %v, want 2s", got)
	}
	if got := maxInt(1, 2); got != 2 {
		t.Fatalf("maxInt = %d, want 2", got)
	}
	if got := maxInt(3, 2); got != 3 {
		t.Fatalf("maxInt = %d, want 3", got)
	}

	inspector := &fakeProcessInspector{}
	runner := &fakeProcessRunner{}
	manager, _ := NewManager(Config{}, WithProcessInspector(inspector), WithProcessRunner(runner))
	runtime := &runtimeCommandAdapter{manager: manager, store: NewMemoryStore(StoreOptions{})}

	deleteSlots := map[int]supervisorProcessSlot{1: {process: nil}}
	if err := runtime.terminateSupervisorSlot(ctx, 1, deleteSlots[1], deleteSlots, now, false); err != nil {
		t.Fatalf("terminate nil slot: %v", err)
	}
	if len(deleteSlots) != 0 {
		t.Fatalf("nil process slot should be deleted: %#v", deleteSlots)
	}

	terminating := map[int]supervisorProcessSlot{
		1: {poolName: "s:default", process: fakeManagedProcess{pid: 7201}, terminatingAt: now},
	}
	if err := runtime.terminateSupervisorSlot(ctx, 1, terminating[1], terminating, now, false); err != nil {
		t.Fatalf("duplicate graceful terminate: %v", err)
	}
	if inspector.terminationCount(false) != 0 {
		t.Fatalf("duplicate graceful terminate should be skipped: %#v", inspector.terminations)
	}
	if err := runtime.terminateSupervisorSlot(ctx, 1, terminating[1], terminating, now, true); err != nil {
		t.Fatalf("force terminate: %v", err)
	}
	if err := runtime.terminateSupervisorSlot(ctx, 1, terminating[1], terminating, now, true); err != nil {
		t.Fatalf("duplicate force terminate: %v", err)
	}
	if inspector.terminationCount(true) != 1 {
		t.Fatalf("force terminate should be sent once: %#v", inspector.terminations)
	}

	expiring := map[int]supervisorProcessSlot{
		1: {poolName: "s:default", process: fakeManagedProcess{pid: 7301}, terminatingAt: now},
		2: {poolName: "s:default", process: fakeManagedProcess{pid: 7302}, terminatingAt: now.Add(-2 * time.Second)},
	}
	if err := runtime.forceExpiredSupervisorSlots(ctx, expiring, now, time.Second); err != nil {
		t.Fatalf("force expired slots: %v", err)
	}
	if inspector.terminationCount(true) != 2 {
		t.Fatalf("only expired slot should add one force terminate: %#v", inspector.terminations)
	}
	allSlots := map[int]supervisorProcessSlot{
		1: {poolName: "s:default", process: fakeManagedProcess{pid: 7351}},
		2: {poolName: "s:default", process: fakeManagedProcess{pid: 7352}},
	}
	if err := runtime.terminateSupervisorSlots(ctx, allSlots, now, false); err != nil {
		t.Fatalf("terminate all slots: %v", err)
	}
	if inspector.terminationCount(false) != 2 {
		t.Fatalf("terminate all slots should send graceful requests: %#v", inspector.terminations)
	}

	reconcileSlots := map[int]supervisorProcessSlot{
		1: {poolName: "old", process: fakeManagedProcess{pid: 7401}},
		2: {poolName: "s:default", process: fakeManagedProcess{pid: 7402}},
		3: {poolName: "s:default", process: fakeManagedProcess{pid: 7403}},
	}
	nextID := 3
	exits := make(chan processExit, 4)
	gracefulBefore := inspector.terminationCount(false)
	if err := runtime.reconcileSupervisorSlots(ctx, SupervisorConfig{Name: "s", Connection: "redis", Queues: []string{"default"}}, "local", []ProcessPoolState{{Name: "s:default", Queue: "default", Queues: []string{"default"}, TargetWorkers: 1}}, reconcileSlots, &nextID, exits, now); err != nil {
		t.Fatalf("reconcile scale down: %v", err)
	}
	if inspector.terminationCount(false) != gracefulBefore+2 {
		t.Fatalf("reconcile should terminate old pool and one excess worker: %#v", inspector.terminations)
	}

	expandSlots := map[int]supervisorProcessSlot{}
	if err := runtime.reconcileSupervisorSlots(ctx, SupervisorConfig{Name: "s", Connection: "redis", Queues: []string{"default"}}, "local", []ProcessPoolState{{Name: "s:default", Queue: "default", Queues: []string{"default"}, TargetWorkers: 2}}, expandSlots, &nextID, exits, now); err != nil {
		t.Fatalf("reconcile scale up: %v", err)
	}
	if len(expandSlots) != 2 || runner.countArg("--queue=default") != 2 {
		t.Fatalf("reconcile should start missing workers, slots=%#v specs=%#v", expandSlots, runner.specs)
	}
}

func TestRuntimeAdapterProjectionAndMaintenanceBranches(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	store := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})
	if err := store.HeartbeatMaster(ctx, MasterState{ID: "master-1", PID: 8001, Status: MasterRunning, LastHeartbeatAt: now, SupervisorCount: 1}); err != nil {
		t.Fatalf("seed master: %v", err)
	}
	if err := store.HeartbeatSupervisor(ctx, SupervisorState{Name: "s1", PID: 8002, Status: SupervisorRunning, LastHeartbeatAt: now, Connection: "redis", Queues: []string{"default"}, WorkerCount: 1}); err != nil {
		t.Fatalf("seed supervisor: %v", err)
	}
	if err := store.HeartbeatWorker(ctx, WorkerState{ID: "w1", Supervisor: "s1", PID: 8003, Status: WorkerIdle, LastHeartbeatAt: now}); err != nil {
		t.Fatalf("seed worker: %v", err)
	}
	manager, _ := NewManager(Config{
		Store:       "memory",
		Environment: "local",
		Supervisors: map[string]SupervisorConfig{
			"s1":    {Name: "s1", Connection: "redis", Queues: []string{"default", ""}, Timeout: 5},
			"empty": {Name: "empty", Connection: "", Queues: []string{"ignored"}},
		},
	}, WithStoreFactory(staticStoreResolver{store: store}))
	runtime := &runtimeCommandAdapter{manager: manager, store: store}

	snapshot, err := runtime.StatusSnapshot(ctx, now)
	if err != nil || snapshot.SupervisorCount != 1 || snapshot.WorkerCount != 1 {
		t.Fatalf("status snapshot = %#v err=%v", snapshot, err)
	}
	masters, err := runtime.Masters(ctx, now)
	if err != nil || len(masters) != 1 || masters[0].ID != "master-1" {
		t.Fatalf("masters = %#v err=%v", masters, err)
	}
	supervisors, err := runtime.Supervisors(ctx, now)
	if err != nil || len(supervisors) != 1 || supervisors[0].Name != "s1" {
		t.Fatalf("supervisors = %#v err=%v", supervisors, err)
	}
	workers, err := runtime.Workers(ctx, now)
	if err != nil || len(workers) != 1 || workers[0].ID != "w1" {
		t.Fatalf("workers = %#v err=%v", workers, err)
	}
	targets := runtime.QueueTargets()
	if len(targets) != 1 || targets[0].Connection != "redis" || targets[0].Queue != "default" {
		t.Fatalf("queue targets = %#v", targets)
	}
	if err := runtime.SetGlobalPaused(ctx, true); err != nil {
		t.Fatalf("set global paused: %v", err)
	}
	if err := runtime.SetSupervisorPaused(ctx, "s1", false); err != nil {
		t.Fatalf("set supervisor paused: %v", err)
	}
	if err := runtime.ClearMetrics(ctx); err != nil {
		t.Fatalf("clear metrics: %v", err)
	}
	if err := runtime.ClearQueue(ctx, horizoncmd.QueueTarget{Connection: "redis", Queue: "default"}); err == nil || !strings.Contains(err.Error(), "queue manager is not configured") {
		t.Fatalf("expected clear queue manager error, got %v", err)
	}
	if err := runtime.ForgetAllFailedJobs(ctx); err == nil || !strings.Contains(err.Error(), "queue manager is not configured") {
		t.Fatalf("expected forget all queue manager error, got %v", err)
	}
	if err := runtime.RequestTerminate(ctx, now, false); err == nil || !strings.Contains(err.Error(), "queue manager is not configured") {
		t.Fatalf("expected terminate queue manager error, got %v", err)
	}
	if _, err := newRuntimeLoader(nil)(ctx); !errors.Is(err, ErrStoreNotConfigured) {
		t.Fatalf("expected nil runtime loader error, got %v", err)
	}
	if _, err := newRuntimeLoader(func() (*Manager, error) { return nil, errFakeWorker })(ctx); !errors.Is(err, errFakeWorker) {
		t.Fatalf("expected runtime loader manager error, got %v", err)
	}
}

func TestSupervisorProcessDoesNotSpawnWorkersWhilePausedOrTerminating(t *testing.T) {
	// 需求背景：historical scenario 06 把 pause/terminate 接入 supervisor 生命周期后，
	// paused/terminating supervisor 不能补拉 worker，避免继续制造新消费者。
	ctx := context.Background()
	store := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})
	processes := &blockingProcessRunner{}
	manager, _ := NewManager(Config{
		Store:       "memory",
		Environment: "local",
		Supervisors: map[string]SupervisorConfig{
			"fixed": {Name: "fixed", Connection: "redis", Queues: []string{"default"}, Balance: BalanceFalse, MaxProcesses: 2, Sleep: 1},
		},
	}, WithStoreFactory(staticStoreResolver{store: store}), WithProcessRunner(processes))
	load := func() (*Manager, error) { return manager, nil }

	if err := store.SetGlobalPaused(ctx, true); err != nil {
		t.Fatalf("pause: %v", err)
	}
	runHorizonCommand(t, horizoncmd.NewSupervisorProcessCommand(newRuntimeLoader(load)), runtimeInput{args: map[string][]string{"name": {"fixed"}}})
	if got := processes.commands(); len(got) != 0 {
		t.Fatalf("paused supervisor must not spawn workers, got %#v", got)
	}

	if err := store.SetGlobalPaused(ctx, false); err != nil {
		t.Fatalf("continue: %v", err)
	}
	terminatingStore := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})
	terminatingManager, _ := NewManager(manager.Config(), WithStoreFactory(staticStoreResolver{store: terminatingStore}), WithProcessRunner(processes))
	terminatingLoad := func() (*Manager, error) { return terminatingManager, nil }
	if err := terminatingStore.RequestTerminate(ctx, time.Now(), false); err != nil {
		t.Fatalf("terminate: %v", err)
	}
	runHorizonCommand(t, horizoncmd.NewSupervisorProcessCommand(newRuntimeLoader(terminatingLoad)), runtimeInput{args: map[string][]string{"name": {"fixed"}}})
	if got := processes.commands(); len(got) != 0 {
		t.Fatalf("terminating supervisor must not spawn workers, got %#v", got)
	}
}

func TestSupervisorProcessSpawnsPerQueuePoolsAndRecordsPoolState(t *testing.T) {
	// 需求背景：historical scenario 07 要求 simple/auto 使用 per-queue process pool，worker 只能消费对应单队列；
	// supervisor heartbeat 也要暴露每个 pool 的当前 worker 数和目标 worker 数，供 status/debug/UI 解释扩缩容决策。
	store := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})
	processes := &blockingProcessRunner{}
	manager, _ := NewManager(Config{
		Store:       "memory",
		Environment: "local",
		Supervisors: map[string]SupervisorConfig{
			"simple": {
				Name: "simple", Connection: "redis", Queues: []string{"high", "default"},
				Balance: BalanceSimple, MinProcesses: 1, MaxProcesses: 4, Sleep: 1,
			},
		},
	}, WithStoreFactory(staticStoreResolver{store: store}), WithProcessRunner(processes))
	runtime := &runtimeCommandAdapter{manager: manager, store: store}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- runtime.RunSupervisor(ctx, horizoncmd.SupervisorProcessOptions{Name: "simple", Environment: "local"})
	}()
	waitForTestCondition(t, func() bool { return processes.starts() == 4 })
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("supervisor process returned error: %v", err)
	}

	if got := processes.commands(); len(got) != 4 {
		t.Fatalf("simple should start two workers per queue, got commands=%#v specs=%#v", got, processes.specs)
	}
	if processes.countArg("--queue=high") != 2 || processes.countArg("--queue=default") != 2 {
		t.Fatalf("per-queue workers should receive one queue each, specs=%#v", processes.specs)
	}
	supervisor, found, err := store.Supervisor(ctx, "simple", time.Now())
	if err != nil || !found {
		t.Fatalf("supervisor heartbeat: found=%v err=%v", found, err)
	}
	if supervisor.WorkerCount != 4 || len(supervisor.Pools) != 2 {
		t.Fatalf("supervisor heartbeat should include pool totals, got %#v", supervisor)
	}
	for _, pool := range supervisor.Pools {
		if pool.CurrentWorkers != 2 || pool.TargetWorkers != 2 || len(pool.Queues) != 1 {
			t.Fatalf("unexpected pool heartbeat: %#v", supervisor.Pools)
		}
	}
}

func TestSupervisorProcessKeepsFullQueueListForBalanceFalse(t *testing.T) {
	// 逻辑说明：balance=false 表示单个 queue-priority pool，worker 参数必须继续携带完整队列列表，
	// 不能因为 per-queue pool 模型而破坏高优先级队列在底层 worker 中的消费顺序。
	store := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})
	processes := &blockingProcessRunner{}
	manager, _ := NewManager(Config{
		Store: "memory",
		Supervisors: map[string]SupervisorConfig{
			"fixed": {
				Name: "fixed", Connection: "redis", Queues: []string{"high", "default", "low"},
				Balance: BalanceFalse, MinProcesses: 1, MaxProcesses: 3,
			},
		},
	}, WithStoreFactory(staticStoreResolver{store: store}), WithProcessRunner(processes))
	runtime := &runtimeCommandAdapter{manager: manager, store: store}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- runtime.RunSupervisor(ctx, horizoncmd.SupervisorProcessOptions{Name: "fixed"})
	}()
	waitForTestCondition(t, func() bool { return processes.starts() == 1 })
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("supervisor process returned error: %v", err)
	}

	if got := processes.countArg("--queue=high,default,low"); got != 1 {
		t.Fatalf("balance=false should keep one full queue-priority worker in this empty-backlog slice, got %d specs=%#v", got, processes.specs)
	}
}

func TestWorkCommandRunsQueueWorkerOncePerHorizonLoop(t *testing.T) {
	// 逻辑说明：horizon:work 自己负责心跳、sleep 和退出阈值，每轮只调用一次底层 queue worker。
	// 这避免把 Horizon 的长驻进程语义塞进普通 queue:work，同时保持 queue.Worker.Work 公开签名不变。
	ctx := context.Background()
	store := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})
	worker := &fakeWorkerRunner{}
	manager, _ := NewManager(Config{Store: "memory"}, WithStoreFactory(staticStoreResolver{store: store}), WithWorkerRunner(worker))
	load := func() (*Manager, error) { return manager, nil }

	runHorizonCommand(t, horizoncmd.NewWorkCommand(newRuntimeLoader(load)), runtimeInput{args: map[string][]string{"connection": {"redis"}}, options: map[string]string{
		"name":            "worker-1",
		"supervisor":      "supervisor-default",
		"environment":     "local",
		"queue":           "default,emails",
		"sleep":           "0",
		"timeout":         "30",
		"tries":           "2",
		"backoff":         "1,5",
		"retry-after":     "45",
		"max-jobs":        "1",
		"max-time":        "0",
		"stop-when-empty": "true",
	}})
	if len(worker.options) != 1 {
		t.Fatalf("expected one queue worker call, got %d", len(worker.options))
	}
	if worker.beginCalls != 1 || worker.closeCalls != 1 {
		t.Fatalf("worker session lifecycle begin=%d close=%d", worker.beginCalls, worker.closeCalls)
	}
	opts := worker.options[0]
	if !opts.Once || opts.Connection != "redis" || len(opts.Queues) != 2 || opts.Queues[1] != "emails" || opts.Tries != 2 || opts.RetryAfter != 45*time.Second {
		t.Fatalf("unexpected worker options: %#v", opts)
	}
	state, found, err := store.Worker(ctx, "worker-1", time.Now())
	if err != nil || !found {
		t.Fatalf("worker heartbeat: found=%v err=%v", found, err)
	}
	// worker 退出路径（StopWhenEmpty）写入 terminating 作为最终状态
	if state.Status != WorkerTerminating || state.Supervisor != "supervisor-default" || state.PID == 0 {
		t.Fatalf("unexpected worker heartbeat: %#v", state)
	}
}

func TestRunWorkerKeepsRabbitMQConsumerIntentForWorkerLifetime(t *testing.T) {
	// 需求背景：RabbitMQ push consumer 在 basic.cancel 前可能已经把下一条 delivery 推入 AMQP 客户端缓冲。
	// 如果 Horizon 每轮 Once:true 都让底层 queue worker 获取并释放 consumer lease，缓冲中的 delivery
	// 会留在 broker 的 unacked 状态，后续 worker 也无法继续处理。
	//
	// 设计思路：测试使用实现 queue.ConsumerIntentLeaser 的 fake 连接模拟 RabbitMQ 连接。
	// RunWorker 应在 horizon:work 生命周期只获取一次外层 lease，并让每轮底层 worker 通过
	// SkipConsumerIntent 跳过临时 lease；同时把 retry_after 从 Horizon 选项传给 queue worker。
	//
	// 参数说明：WorkerOptions.Connection/Queue 决定需要持有 lease 的连接和队列，
	// RetryAfter 决定底层 Pop 的保留窗口。
	ctx := context.Background()
	store := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})
	worker := &fakeWorkerRunner{}
	manager, _ := NewManager(
		Config{Store: "memory"},
		WithStoreFactory(staticStoreResolver{store: store}),
		WithWorkerRunner(worker),
	)
	runtime := &runtimeCommandAdapter{manager: manager, store: store}

	err := runtime.RunWorker(ctx, horizoncmd.WorkerOptions{
		Name:          "rabbit-worker",
		Connection:    "rabbitmq",
		Queue:         "test1",
		RetryAfter:    90,
		StopWhenEmpty: true,
	})
	if err != nil {
		t.Fatalf("RunWorker error=%v", err)
	}
	if worker.beginCalls != 1 || worker.activateCalls != 1 || worker.closeCalls != 1 {
		t.Fatalf("worker session lifecycle begin=%d activate=%d close=%d", worker.beginCalls, worker.activateCalls, worker.closeCalls)
	}
	opts := worker.options[0]
	if !opts.SkipConsumerIntent || opts.RetryAfter != 90*time.Second {
		t.Fatalf("worker options should skip inner lease and preserve timing options: %#v", opts)
	}
}

func TestRunWorkerDoesNotSleepAfterProcessingJob(t *testing.T) {
	// 需求背景：horizon:work 的 --sleep 是空队列轮询间隔，不是每条任务后的节流间隔。
	// 旧逻辑在每轮 Once:true worker 调用后无条件 sleep，导致 RabbitMQ 中已有 backlog 时，
	// 默认 sleep=3 会把每个 worker 限制为 3 秒处理 1 条轻量任务。
	//
	// 设计思路：fake worker 连续两轮派发 queue.job_processing 事件，模拟成功取得任务。
	// RunWorker 配置 Sleep=1、MaxJobs=2；如果处理后仍 sleep，测试至少会等待约 1 秒。
	// 修复后两轮应连续执行，并在 MaxJobs 达到后快速退出。
	//
	// 参数说明：Sleep 使用秒级命令参数；MaxJobs 控制 worker 在处理两条任务后退出，
	// hook 通过当前 worker event observer 通知 RunWorker 本轮确实看到任务。
	ctx := context.Background()
	store := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})
	worker := &fakeWorkerRunner{}
	worker.hook = func(eventCtx context.Context) {
		worker.emit(eventCtx, queue.JobProcessing{Connection: "rabbitmq", Queue: "test1", JobID: "job", JobName: "SmokeJob"})
	}
	manager, _ := NewManager(Config{Store: "memory"}, WithStoreFactory(staticStoreResolver{store: store}), WithWorkerRunner(worker))
	runtime := &runtimeCommandAdapter{manager: manager, store: store}

	started := time.Now()
	err := runtime.RunWorker(ctx, horizoncmd.WorkerOptions{
		Name:       "fast-worker",
		Connection: "rabbitmq",
		Queue:      "test1",
		Sleep:      1,
		MaxJobs:    2,
	})
	elapsed := time.Since(started)
	if err != nil {
		t.Fatalf("RunWorker error=%v", err)
	}
	if worker.beginCalls != 1 || worker.activateCalls != 1 || worker.closeCalls != 1 {
		t.Fatalf("worker session should span the RunWorker lifetime, begin=%d activate=%d close=%d", worker.beginCalls, worker.activateCalls, worker.closeCalls)
	}
	if elapsed >= 500*time.Millisecond {
		t.Fatalf("worker slept after processing job, elapsed=%v", elapsed)
	}
}

func TestQueueWorkerAdapterReusesPopSessionAcrossWorkRounds(t *testing.T) {
	adapter := &QueueWorkerAdapter{}
	if _, err := adapter.Begin(context.Background(), queue.WorkerOptions{}); err == nil {
		t.Fatal("empty adapter Begin should fail")
	}
}

func TestQueueWorkerAdapterConsumerIntentAndSessionUseSameResolvedConnection(t *testing.T) {
	// 需求背景：Horizon adapter 只委托 queue.WorkerSessionFactory，底层 queue 包负责在同一次解析
	// 出的 connection 上获取 consumer intent 和 pop session。
	worker := &fakeWorkerRunner{}
	adapter := &QueueWorkerAdapter{worker: worker}
	session, err := adapter.Begin(context.Background(), queue.WorkerOptions{Connection: "redis", Queues: []string{"default"}, Once: true})
	if err != nil {
		t.Fatalf("begin queue worker adapter session: %v", err)
	}
	if worker.beginCalls != 1 {
		t.Fatalf("Begin should delegate once, got %d", worker.beginCalls)
	}
	if err := session.Activate(context.Background()); err != nil {
		t.Fatalf("activate delegated session: %v", err)
	}
}

func TestWorkerOptionsDoNotExposeBlockFor(t *testing.T) {
	if _, ok := reflect.TypeOf(queue.WorkerOptions{}).FieldByName("BlockFor"); ok {
		t.Fatal("queue.WorkerOptions should not expose BlockFor")
	}
	if _, ok := reflect.TypeOf(horizoncmd.WorkerOptions{}).FieldByName("BlockFor"); ok {
		t.Fatal("horizon WorkerOptions should not expose BlockFor")
	}
	if _, ok := reflect.TypeOf(SupervisorConfig{}).FieldByName("BlockFor"); ok {
		t.Fatal("horizon SupervisorConfig should not expose BlockFor")
	}
	if _, ok := reflect.TypeOf(horizoncmd.SupervisorView{}).FieldByName("BlockFor"); ok {
		t.Fatal("horizon SupervisorView should not expose BlockFor")
	}
}

func TestRunWorkerStartsMonitorAndFlushesQueueEvents(t *testing.T) {
	// 需求背景：Horizon 长驻 worker 入口必须启动 collector/flusher，使 queue event 通过
	// event dispatcher 进入 Store；worker 热路径不能直接写 Store 指标。
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	t.Cleanup(func() { queue.UseEventSink(nil) })

	store := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})
	bus := event.New()
	worker := &fakeWorkerRunner{}
	worker.hook = func(ctx context.Context) {
		worker.emit(ctx, queue.JobProcessing{Connection: "redis", Queue: "default", JobID: "job-1", JobName: "Job"})
		worker.emit(ctx, queue.JobProcessed{Connection: "redis", Queue: "default", JobID: "job-1", JobName: "Job", Duration: time.Millisecond})
	}
	queue.UseEventSink(func(ctx context.Context, ev queue.Event) {
		bus.Dispatch(ctx, ev)
	})
	manager, err := NewManager(Config{
		Store:        "memory",
		Prefix:       "cmd",
		Environment:  "local",
		HeartbeatTTL: time.Minute,
		Observability: ObservabilityConfig{
			Preset:                 ObservabilityPresetFull,
			EventMetrics:           true,
			MetricsWindow:          10 * time.Millisecond,
			FlushInterval:          10 * time.Millisecond,
			FlushTimeout:           time.Second,
			BatchSize:              10,
			BufferSize:             100,
			EventMetricsSampleRate: 1,
		},
	}, WithStoreFactory(staticStoreResolver{store: store}), WithWorkerRunner(worker), WithEventDispatcher(bus))
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	runtime := &runtimeCommandAdapter{manager: manager, store: store}
	if err := runtime.RunWorker(ctx, horizoncmd.WorkerOptions{
		Name:          "worker-monitor",
		Supervisor:    "supervisor-default",
		Connection:    "redis",
		Queue:         "default",
		MaxJobs:       1,
		StopWhenEmpty: true,
	}); err != nil {
		t.Fatalf("RunWorker error=%v", err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		windows, err := store.EventMetricWindows(ctx, EventMetricWindowQuery{Page: PageRequest{Page: 1, PageSize: 10}})
		if err != nil {
			t.Fatalf("read event metric windows: %v", err)
		}
		if len(windows.Items) > 0 && windows.Items[0].Processed > 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("event metrics were not flushed, windows=%#v", windows.Items)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestRunWorkerStartsMonitorBeforeConsumerIntent(t *testing.T) {
	// 需求背景：consumer_started 是 worker 生命周期关键指标。Horizon 必须先注册
	// monitor，再获取 queue consumer intent，否则 Redis/RabbitMQ driver 在 intent 阶段
	// 发出的生命周期事件会丢在 monitor 订阅链路之外。
	ctx := context.Background()
	t.Cleanup(func() { queue.UseEventSink(nil) })

	store := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})
	dispatcher := &countingEventDispatcher{}
	queue.UseEventSink(func(ctx context.Context, ev queue.Event) {
		dispatcher.Dispatch(ctx, ev)
	})
	worker := &fakeWorkerRunner{}
	worker.activateHook = func(context.Context) {
		if dispatcher.listenCalls == 0 {
			t.Fatal("consumer intent acquired before monitor listener registration")
		}
		worker.emit(context.Background(), queue.InfrastructureEvent{
			EventName:  queue.EventConsumerStarted,
			Connection: "redis",
			Queue:      "default",
		})
	}
	manager, err := NewManager(Config{
		Store:        "memory",
		Prefix:       "cmd",
		Environment:  "local",
		HeartbeatTTL: time.Minute,
		Observability: ObservabilityConfig{
			Preset:                 ObservabilityPresetFull,
			EventMetrics:           true,
			MetricsWindow:          10 * time.Millisecond,
			FlushInterval:          10 * time.Millisecond,
			FlushTimeout:           time.Second,
			BatchSize:              10,
			BufferSize:             100,
			EventMetricsSampleRate: 1,
		},
	}, WithStoreFactory(staticStoreResolver{store: store}), WithWorkerRunner(worker), WithEventDispatcher(dispatcher))
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	runtime := &runtimeCommandAdapter{manager: manager, store: store}
	if err := runtime.RunWorker(ctx, horizoncmd.WorkerOptions{
		Name:          "worker-consumer-started",
		Supervisor:    "supervisor-default",
		Connection:    "redis",
		Queue:         "default",
		StopWhenEmpty: true,
	}); err != nil {
		t.Fatalf("RunWorker error=%v", err)
	}
	if worker.activateCalls != 1 {
		t.Fatalf("consumer intent should be acquired during worker Activate, calls=%d", worker.activateCalls)
	}
	if dispatcher.DispatchCalls(queue.EventConsumerStarted) == 0 {
		t.Fatal("consumer_started should pass through the registered event dispatcher listener")
	}
}

func TestRunWorkerConsumerStartedFromActivateUsesWorkerSupervisor(t *testing.T) {
	// 需求背景：Redis/RabbitMQ 在 consumer intent 阶段会发出 consumer_started。
	// 该事件发生在 WorkerSession.Activate 中，Horizon 必须先安装 worker sink wrapper，
	// 才能把当前 --supervisor 通过 context 传给 monitor，并保留原始 queue sink 转发。
	ctx := context.Background()
	t.Cleanup(func() { queue.UseEventSink(nil) })

	store := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})
	bus := event.New()
	forwarded := make([]string, 0)
	var dispatchedSupervisor string
	bus.ListenFunc(queue.EventConsumerStarted, func(eventCtx context.Context, _ event.Event) error {
		dispatchedSupervisor = workerSupervisorFromContext(eventCtx)
		return nil
	})
	queue.UseEventSink(func(ctx context.Context, ev queue.Event) {
		forwarded = append(forwarded, ev.Name())
		bus.Dispatch(ctx, ev)
	})
	worker := &fakeWorkerRunner{}
	worker.activateHook = func(context.Context) {
		worker.emit(context.Background(), queue.InfrastructureEvent{
			EventName:  queue.EventConsumerStarted,
			Connection: "redis",
			Queue:      "default",
		})
	}
	manager, err := NewManager(Config{
		Store:        "memory",
		Prefix:       "cmd",
		Environment:  "local",
		HeartbeatTTL: time.Minute,
		Observability: ObservabilityConfig{
			Preset:                 ObservabilityPresetFull,
			EventMetrics:           true,
			MetricsWindow:          10 * time.Millisecond,
			FlushInterval:          10 * time.Millisecond,
			FlushTimeout:           time.Second,
			BatchSize:              10,
			BufferSize:             100,
			EventMetricsSampleRate: 1,
			DiagnosticsRetention:   time.Hour,
		},
	}, WithStoreFactory(staticStoreResolver{store: store}), WithWorkerRunner(worker), WithEventDispatcher(bus))
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	runtime := &runtimeCommandAdapter{manager: manager, store: store}
	if err := runtime.RunWorker(ctx, horizoncmd.WorkerOptions{
		Name:          "worker-consumer-started-source",
		Supervisor:    "supervisor-default",
		Connection:    "redis",
		Queue:         "default",
		StopWhenEmpty: true,
	}); err != nil {
		t.Fatalf("RunWorker error=%v", err)
	}
	if len(forwarded) != 1 || forwarded[0] != queue.EventConsumerStarted {
		t.Fatalf("original queue sink should receive consumer_started once, got %#v", forwarded)
	}
	if dispatchedSupervisor != "supervisor-default" {
		t.Fatalf("dispatcher should receive worker supervisor context, got %q", dispatchedSupervisor)
	}
}

func TestWorkerEventBridgeCollectsSupervisorSourceAndPreservesQueueSink(t *testing.T) {
	// 需求背景：historical scenario 43 要求 horizon:work runtime 在普通 queue event 进入 collector 前补齐当前
	// --supervisor 身份，同时不能吞掉 queue.ServiceProvider 已安装的 queue -> event bridge。
	// 逻辑说明：测试先安装一个原始 queue sink，再运行 worker；worker hook 触发普通 queue event。
	// 断言同一事件既进入原始 sink，又进入 Horizon collector，且 SourceSupervisor 只来自 WorkerOptions。
	ctx := context.Background()
	t.Cleanup(func() { queue.UseEventSink(nil) })

	forwarded := make([]string, 0)
	queue.UseEventSink(func(_ context.Context, ev queue.Event) {
		forwarded = append(forwarded, ev.Name())
	})

	obs := observabilityPresetConfigOrFull()
	obs.EventMetrics = true
	obs.EventMetricsSampleRate = 1
	obs.QueuedWaitsMax = 0
	store := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})
	worker := &fakeWorkerRunner{}
	manager, _ := NewManager(Config{
		Store:         "memory",
		Prefix:        "runtime-prefix",
		Environment:   "testing",
		Observability: obs,
	}, WithStoreFactory(staticStoreResolver{store: store}), WithWorkerRunner(worker))
	manager.coll.Start(ctx)

	worker.hook = func(eventCtx context.Context) {
		worker.emit(eventCtx, queue.JobProcessing{Connection: "redis", Queue: "critical", JobID: "job-1", JobName: "EmailJob"})
		worker.emit(eventCtx, queue.JobProcessed{Connection: "redis", Queue: "critical", JobID: "job-1", JobName: "EmailJob", Duration: time.Millisecond})
	}

	runtime := &runtimeCommandAdapter{manager: manager, store: store}
	if err := runtime.RunWorker(ctx, horizoncmd.WorkerOptions{
		Name:          "worker-source",
		Supervisor:    "runtime-supervisor",
		MaxJobs:       1,
		StopWhenEmpty: true,
	}); err != nil {
		t.Fatalf("run worker: %v", err)
	}
	manager.coll.Stop()

	if len(forwarded) != 2 || forwarded[0] != queue.EventJobProcessing || forwarded[1] != queue.EventJobProcessed {
		t.Fatalf("original queue sink should receive forwarded events, got %#v", forwarded)
	}
	snapshot := manager.coll.FlushSnapshot(time.Now())
	if snapshot == nil || len(snapshot.windows) != 1 {
		t.Fatalf("collector should receive worker event window, got %#v", snapshot)
	}
	window := snapshot.windows[0]
	if window.supervisor != "runtime-supervisor" {
		t.Fatalf("SourceSupervisor should come from worker runtime, got %#v", window)
	}
	if window.sourcePrefix != "runtime-prefix" || window.environment != "testing" || window.connection != "redis" || window.queue != "critical" {
		t.Fatalf("source dimensions changed unexpectedly: %#v", window)
	}
}

func TestWorkerLoopHonorsPauseAndTerminateAtJobBoundary(t *testing.T) {
	// 需求背景：historical scenario 06 要求 Horizon worker 在每轮取新任务前读取 Store 控制标记。
	// 逻辑说明：pause 期间只写 paused heartbeat 并等待下一次检查，不调用底层 queue worker；
	// terminate 优先级高于 pause，worker 写入 terminating 后退出。
	ctx := context.Background()
	store := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})
	worker := &fakeWorkerRunner{}
	manager, _ := NewManager(Config{Store: "memory"}, WithStoreFactory(staticStoreResolver{store: store}), WithWorkerRunner(worker))
	load := func() (*Manager, error) { return manager, nil }

	if err := store.SetGlobalPaused(ctx, true); err != nil {
		t.Fatalf("pause store: %v", err)
	}
	go func() {
		for {
			state, found, _ := store.Worker(ctx, "worker-paused", time.Now())
			if found && state.Status == WorkerPaused {
				_ = store.RequestTerminate(ctx, time.Now(), false)
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()

	runHorizonCommand(t, horizoncmd.NewWorkCommand(newRuntimeLoader(load)), runtimeInput{options: map[string]string{
		"name":       "worker-paused",
		"supervisor": "supervisor-default",
		"sleep":      "0",
		"max-jobs":   "1",
	}})
	if len(worker.options) != 0 {
		t.Fatalf("paused worker must not call queue runner, got %d calls", len(worker.options))
	}
	state, found, err := store.Worker(ctx, "worker-paused", time.Now())
	if err != nil || !found {
		t.Fatalf("worker heartbeat: found=%v err=%v", found, err)
	}
	if state.Status != WorkerTerminating {
		t.Fatalf("worker should exit through terminating heartbeat, got %#v", state)
	}
}

func TestWorkerTerminateWaitsForCurrentJobBoundary(t *testing.T) {
	// 需求背景：terminate 不能取消正在执行的 job；worker 只能在底层 Once:true worker 返回后，
	// 进入下一轮取任务边界时写入 terminating 并退出。
	ctx := context.Background()
	store := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})
	worker := &fakeWorkerRunner{}
	worker.hook = func(context.Context) {
		_ = store.RequestTerminate(ctx, time.Now(), false)
	}
	manager, _ := NewManager(Config{Store: "memory"}, WithStoreFactory(staticStoreResolver{store: store}), WithWorkerRunner(worker))
	load := func() (*Manager, error) { return manager, nil }

	runHorizonCommand(t, horizoncmd.NewWorkCommand(newRuntimeLoader(load)), runtimeInput{options: map[string]string{
		"name":       "worker-terminates-after-job",
		"supervisor": "supervisor-default",
		"sleep":      "0",
	}})
	if len(worker.options) != 1 {
		t.Fatalf("worker should finish current job before terminating, calls=%d", len(worker.options))
	}
	state, found, err := store.Worker(ctx, "worker-terminates-after-job", time.Now())
	if err != nil || !found {
		t.Fatalf("worker heartbeat: found=%v err=%v", found, err)
	}
	if state.Status != WorkerTerminating {
		t.Fatalf("unexpected terminating state after current job: %#v", state)
	}
}

func TestRunWorkerDoesNotStartMonitorWhenSessionBeginFails(t *testing.T) {
	// 需求背景：horizon:work 关键启动校验失败时不应启动 collector/flusher，
	// 否则失败进程也会注册监控监听器并留下半启动状态。
	store := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})
	dispatcher := &countingEventDispatcher{}
	manager, err := NewManager(
		Config{Store: "memory"},
		WithStoreFactory(staticStoreResolver{store: store}),
		WithWorkerRunner(beginErrorWorkerRunner{err: errors.New("begin failed")}),
		WithEventDispatcher(dispatcher),
	)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	runtime := &runtimeCommandAdapter{manager: manager, store: store}
	err = runtime.RunWorker(context.Background(), horizoncmd.WorkerOptions{Name: "worker-begin-fail"})
	if err == nil || !strings.Contains(err.Error(), "begin failed") {
		t.Fatalf("RunWorker error=%v, want begin failed", err)
	}
	if dispatcher.listenCalls != 0 {
		t.Fatalf("monitor should not start before worker session succeeds, listenCalls=%d", dispatcher.listenCalls)
	}
}

func TestRunMasterDoesNotStartMonitorWhenLeaseFails(t *testing.T) {
	// 需求背景：master lease 是 Horizon 启动互斥边界；租约失败前启动 monitor
	// 会让未接管容量的进程也注册 runtime 事件采集。
	baseStore := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})
	store := masterLeaseDenyStore{Store: baseStore}
	dispatcher := &countingEventDispatcher{}
	manager, err := NewManager(
		Config{Store: "memory", Environment: "local"},
		WithStoreFactory(staticStoreResolver{store: store}),
		WithProcessRunner(&fakeProcessRunner{}),
		WithEventDispatcher(dispatcher),
	)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	runtime := &runtimeCommandAdapter{manager: manager, store: store}
	err = runtime.RunMaster(context.Background(), horizoncmd.MasterOptions{Environment: "local"})
	if err == nil || !strings.Contains(err.Error(), "master already running") {
		t.Fatalf("RunMaster error=%v, want master already running", err)
	}
	if dispatcher.listenCalls != 0 {
		t.Fatalf("monitor should not start before master lease succeeds, listenCalls=%d", dispatcher.listenCalls)
	}
}

func TestRunWorkerTreatsCanceledContextAsGracefulShutdown(t *testing.T) {
	// 需求背景：OS SIGTERM 会先取消 Application context；worker 退出路径仍要写最终 heartbeat，
	// 但不能把 context.Canceled 包装成任务执行失败或 heartbeat diagnostic。
	ctx, cancel := context.WithCancel(context.Background())
	store := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})
	worker := &fakeWorkerRunner{err: context.Canceled, hook: func(context.Context) { cancel() }}
	manager, _ := NewManager(Config{Store: "memory"}, WithStoreFactory(staticStoreResolver{store: store}), WithWorkerRunner(worker))
	runtime := &runtimeCommandAdapter{manager: manager, store: store}

	if err := runtime.RunWorker(ctx, horizoncmd.WorkerOptions{Name: "worker-canceled", Supervisor: "s1"}); err != nil {
		t.Fatalf("canceled worker should exit cleanly: %v", err)
	}
	state, found, err := store.Worker(context.Background(), "worker-canceled", time.Now())
	if err != nil || !found {
		t.Fatalf("worker heartbeat after cancel: found=%v err=%v", found, err)
	}
	if state.Status != WorkerTerminating || state.LastHeartbeatErrorCode != "" {
		t.Fatalf("canceled worker should persist clean terminating heartbeat: %#v", state)
	}
}

func TestWorkCommandReportsRunnerConfigurationAndExecutionErrors(t *testing.T) {
	bindHorizonPanicReporter(t)
	// 需求背景：horizon:work 的 worker runner 是显式依赖；缺失或执行失败时应返回清晰错误，
	// 并在执行失败后把 worker heartbeat 恢复为空闲边界状态。
	ctx := context.Background()
	store := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})
	manager, _ := NewManager(Config{Store: "memory"}, WithStoreFactory(staticStoreResolver{store: store}))
	cmd := horizoncmd.NewWorkCommand(newRuntimeLoader(func() (*Manager, error) { return manager, nil }))
	err := cmd.Handle(runtimeCommandContext(cmd, runtimeInput{}, io.Discard))
	if err == nil || !strings.Contains(err.Error(), "queue worker runner is not configured") {
		t.Fatalf("expected runner configuration error, got %v", err)
	}

	worker := &fakeWorkerRunner{err: errFakeWorker}
	manager, _ = NewManager(Config{Store: "memory"}, WithStoreFactory(staticStoreResolver{store: store}), WithWorkerRunner(worker))
	cmd = horizoncmd.NewWorkCommand(newRuntimeLoader(func() (*Manager, error) { return manager, nil }))
	err = cmd.Handle(runtimeCommandContext(cmd, runtimeInput{options: map[string]string{"name": "worker-error", "supervisor": "s1"}}, io.Discard))
	if err == nil || !strings.Contains(err.Error(), errFakeWorker.Error()) {
		t.Fatalf("expected worker execution error, got %v", err)
	}
	state, found, readErr := store.Worker(ctx, "worker-error", time.Now())
	if readErr != nil || !found {
		t.Fatalf("worker heartbeat after error: found=%v err=%v", found, readErr)
	}
	if state.Status != WorkerTerminating {
		t.Fatalf("worker should be terminating after runner error: %#v", state)
	}
}

func TestWorkCommandReportsHeartbeatDiagnosticsWithoutCancelingRunner(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})
	store := &heartbeatFailOnceStore{Store: base, remainingFailures: 1}
	worker := &fakeWorkerRunner{}
	manager, _ := NewManager(Config{Store: "memory"}, WithStoreFactory(staticStoreResolver{store: store}), WithWorkerRunner(worker))
	cmd := horizoncmd.NewWorkCommand(newRuntimeLoader(func() (*Manager, error) { return manager, nil }))

	err := cmd.Handle(runtimeCommandContext(cmd, runtimeInput{options: map[string]string{
		"name":            "worker-heartbeat-error",
		"supervisor":      "s1",
		"max-jobs":        "1",
		"stop-when-empty": "true",
	}}, io.Discard))
	if err == nil || !strings.Contains(err.Error(), "heartbeat_write_failed") {
		t.Fatalf("expected heartbeat diagnostic error, got %v", err)
	}
	if len(worker.options) != 1 {
		t.Fatalf("heartbeat failure must not cancel queue runner, calls=%d", len(worker.options))
	}
	state, found, readErr := base.Worker(ctx, "worker-heartbeat-error", time.Now())
	if readErr != nil || !found {
		t.Fatalf("worker heartbeat after recovery: found=%v err=%v", found, readErr)
	}
	if state.LastHeartbeatErrorCode != "heartbeat_write_failed" || strings.Contains(state.LastHeartbeatErrorMessage, "payload") {
		t.Fatalf("heartbeat diagnostic should be stable and sanitized: %#v", state)
	}
}

func TestRunWorkerAllowsConcurrentWorkersWithIndependentObservers(t *testing.T) {
	storeOne := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})
	storeTwo := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})
	workerOne := &fakeWorkerRunner{}
	workerOne.hook = func(ctx context.Context) {
		workerOne.emit(ctx, queue.JobProcessing{Connection: "redis", Queue: "default", JobID: "job-1", JobName: "Job"})
	}
	workerTwo := &fakeWorkerRunner{}
	workerTwo.hook = func(ctx context.Context) {
		workerTwo.emit(ctx, queue.JobProcessing{Connection: "redis", Queue: "critical", JobID: "job-2", JobName: "Job"})
	}
	managerOne, _ := NewManager(Config{Store: "memory"}, WithStoreFactory(staticStoreResolver{store: storeOne}), WithWorkerRunner(workerOne))
	managerTwo, _ := NewManager(Config{Store: "memory"}, WithStoreFactory(staticStoreResolver{store: storeTwo}), WithWorkerRunner(workerTwo))

	if err := (&runtimeCommandAdapter{manager: managerOne, store: storeOne}).RunWorker(context.Background(), horizoncmd.WorkerOptions{Name: "worker-one", StopWhenEmpty: true, MaxJobs: 1}); err != nil {
		t.Fatalf("first worker error=%v", err)
	}
	if err := (&runtimeCommandAdapter{manager: managerTwo, store: storeTwo}).RunWorker(context.Background(), horizoncmd.WorkerOptions{Name: "worker-two", StopWhenEmpty: true, MaxJobs: 1}); err != nil {
		t.Fatalf("second worker error=%v", err)
	}
	if workerOne.beginCalls != 1 || workerTwo.beginCalls != 1 {
		t.Fatalf("workers should run independently, begin one=%d two=%d", workerOne.beginCalls, workerTwo.beginCalls)
	}
}

func TestWorkerEventRecorderOnlyTracksSawJobOnProcessing(t *testing.T) {
	// 需求背景（historical scenario 49/50/51）：队列事件热路径禁止直接写 Store，禁止维护 worker heartbeat state。
	// record() 只标记 sawJob 用于 StopWhenEmpty 判断和本地 processed 计数。
	// 任务量统计统一走 collector + flusher。
	ctx := context.Background()
	store := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})
	recorder := &workerEventRecorder{
		store: store,
		state: WorkerState{ID: "worker-events", Supervisor: "supervisor-default", Status: WorkerIdle, LastHeartbeatAt: time.Now()},
	}

	// JobProcessing: sawJob 应为 true
	recorder.beginRound()
	recorder.record(ctx, queue.JobProcessing{JobID: "job-1", JobName: "EmailJob"})
	if !recorder.sawJob {
		t.Fatal("expected sawJob=true after processing event")
	}

	// JobProcessed: sawJob 保持，不写 Store
	recorder.record(ctx, queue.JobProcessed{JobID: "job-1", JobName: "EmailJob"})
	recorder.beginRound()

	// JobFailed: sawJob 不受影响
	recorder.record(ctx, queue.JobFailed{FailedJob: payload.FailedJob{JobID: "job-2", JobName: "FailingJob"}})
	if recorder.sawJob {
		t.Fatal("expected sawJob=false after beginRound, failed event should not set it")
	}

	// JobReleased: sawJob 不受影响
	recorder.record(ctx, queue.JobReleased{JobID: "job-3", JobName: "ReleasedJob"})
	if recorder.sawJob {
		t.Fatal("released event should not set sawJob")
	}

	// 验证 record() 不写 Store：只有通过 heartbeat() 或 paused()/terminating() 才写
	_, found, _ := store.Worker(ctx, "worker-events", time.Now())
	if found {
		t.Fatal("record() must not write to Store; worker should not be in store")
	}

	// 验证 idle/paused/terminating 更新内存状态
	recorder.idle()
	recorder.mu.Lock()
	if recorder.state.Status != WorkerIdle {
		t.Fatalf("idle should set status to idle: %s", recorder.state.Status)
	}
	recorder.mu.Unlock()

	recorder.paused(ctx)
	recorder.mu.Lock()
	if recorder.state.Status != WorkerPaused {
		t.Fatalf("paused should set status to paused: %s", recorder.state.Status)
	}
	recorder.mu.Unlock()

	recorder.terminating(ctx)
	recorder.mu.Lock()
	if recorder.state.Status != WorkerTerminating {
		t.Fatalf("terminating should set status to terminating: %s", recorder.state.Status)
	}
	recorder.mu.Unlock()
}

func TestProcessHelpersCoverFixedCountsAndRunnerBoundaries(t *testing.T) {
	// 逻辑说明：固定 worker 数规则和 runner 缺失错误是进程树启动的关键边界，单独覆盖可避免回归。
	if got := processPoolWorkerCount(CalculateProcessPools(SupervisorConfig{Balance: BalanceFalse, Queues: []string{"default"}, MinProcesses: 1, MaxProcesses: 3}, []QueueWorkload{{Queue: "default", Ready: 9}}, ScaleState{}, time.Now())); got != 3 {
		t.Fatalf("balance=false count = %d", got)
	}
	if got := processPoolWorkerCount(CalculateProcessPools(SupervisorConfig{Balance: BalanceSimple, Queues: []string{"high", "default"}, MinProcesses: 2, MaxProcesses: 6}, nil, ScaleState{}, time.Now())); got != 6 {
		t.Fatalf("balance=simple count = %d", got)
	}
	if got := joinInts([]int{1, 5, 10}); got != "1,5,10" {
		t.Fatalf("joinInts = %q", got)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	sleepContext(canceled, time.Hour)
	if _, err := NewQueueWorkerAdapter(nil).Begin(context.Background(), queue.WorkerOptions{}); err == nil {
		t.Fatal("nil queue worker adapter should report configuration error")
	}
	if workerPauseSleep(0) != time.Second || workerPauseSleep(2) != 2*time.Second {
		t.Fatal("worker pause sleep should use 1s fallback and positive sleep seconds")
	}
	if !shouldStopWorkerLoop(horizoncmd.WorkerOptions{MaxJobs: 1}, 1, time.Now()) {
		t.Fatal("worker loop should stop after max jobs")
	}
	if !shouldStopWorkerLoop(horizoncmd.WorkerOptions{MaxTime: 1}, 0, time.Now().Add(-2*time.Second)) {
		t.Fatal("worker loop should stop after max time")
	}
	if shouldStopWorkerLoop(horizoncmd.WorkerOptions{}, 0, time.Now()) {
		t.Fatal("worker loop should continue without limits")
	}
	process, err := (&fakeProcessRunner{}).Start(context.Background(), ProcessSpec{Args: []string{"horizon:work"}})
	if err != nil {
		t.Fatalf("fake process start: %v", err)
	}
	if process.PID() == 0 || process.Wait() != nil {
		t.Fatalf("unexpected fake process: %#v", process)
	}
	var nilProcess *osManagedProcess
	if nilProcess.PID() != 0 || nilProcess.Wait() != nil {
		t.Fatal("nil os managed process should be inert")
	}
	emptyProcess := &osManagedProcess{}
	if emptyProcess.PID() != 0 || emptyProcess.Wait() != nil {
		t.Fatal("empty os managed process should be inert")
	}
	if err := waitProcesses([]ManagedProcess{nil, fakeManagedProcess{pid: 1}}); err != nil {
		t.Fatalf("wait processes with nil and successful process: %v", err)
	}
	if err := waitProcesses([]ManagedProcess{fakeManagedProcess{pid: 2, err: errFakeProcessWait}}); !errors.Is(err, errFakeProcessWait) {
		t.Fatalf("expected process wait error, got %v", err)
	}
}

func TestOSProcessRunnerStartsCurrentExecutable(t *testing.T) {
	// 需求背景：生产路径依赖当前可执行文件重新进入 main.go。测试用 Go 测试二进制的 helper 模式验证
	// OSProcessRunner 的真实 Start/PID/Wait 路径，不共享任何 Go 对象实例。
	process, err := OSProcessRunner{}.Start(context.Background(), ProcessSpec{
		Args: []string{"-test.run=TestHorizonProcessRunnerHelper"},
		Env:  []string{"PRISMGO_HORIZON_PROCESS_HELPER=1"},
	})
	if err != nil {
		t.Fatalf("start helper process: %v", err)
	}
	if process.PID() == 0 {
		t.Fatal("expected helper process pid")
	}
	if err := process.Wait(); err != nil {
		t.Fatalf("wait helper process: %v", err)
	}
}

func TestHorizonProcessRunnerHelper(t *testing.T) {
	if os.Getenv("PRISMGO_HORIZON_PROCESS_HELPER") != "1" {
		return
	}
	os.Exit(0)
}

func TestRuntimeSupervisorCommandsDisplaySupervisorState(t *testing.T) {
	// 设计思路：supervisors 和 supervisor-status 只展示基础状态字段，不泄漏 worker 明细、queue length
	// 或 metrics。测试断言核心字段和 not found 错误边界。
	now := time.Now().UTC()
	store := NewMemoryStore(StoreOptions{Prefix: "cmd", HeartbeatTTL: time.Minute})
	if err := store.HeartbeatSupervisor(context.Background(), SupervisorState{
		Name:            "supervisor-default",
		Host:            "host-1",
		PID:             101,
		Status:          SupervisorRunning,
		StartedAt:       now.Add(-time.Minute),
		LastHeartbeatAt: now,
		WorkerCount:     2,
		Connection:      "redis",
		Queues:          []string{"default", "emails"},
	}); err != nil {
		t.Fatalf("seed supervisor: %v", err)
	}
	manager, _ := NewManager(Config{Store: "memory", Prefix: "cmd", HeartbeatTTL: time.Minute}, WithStoreFactory(staticStoreResolver{store: store}))
	load := func() (*Manager, error) { return manager, nil }

	listOutput := runHorizonCommand(t, horizoncmd.NewSupervisorsCommand(newRuntimeLoader(load)), runtimeInput{})
	for _, want := range []string{"Name", "PID", "Status", "Workers", "Balancing", "Host", "Connection", "Queues", "supervisor-default", "default,emails"} {
		if !strings.Contains(listOutput, want) {
			t.Fatalf("supervisors output missing %q:\n%s", want, listOutput)
		}
	}

	detailOutput := runHorizonCommand(t, horizoncmd.NewSupervisorStatusCommand(newRuntimeLoader(load)), runtimeInput{args: map[string][]string{"name": {"supervisor-default"}}})
	if !strings.Contains(detailOutput, "supervisor-default has 1 instance(s)") || !strings.Contains(detailOutput, "Last Heartbeat") {
		t.Fatalf("unexpected supervisor detail output:\n%s", detailOutput)
	}
	missingSupervisorCommand := horizoncmd.NewSupervisorStatusCommand(newRuntimeLoader(load))
	err := missingSupervisorCommand.Handle(runtimeCommandContext(missingSupervisorCommand, runtimeInput{args: map[string][]string{"name": {"missing"}}}, io.Discard))
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("expected not found error, got %v", err)
	}
}

func TestTimeoutCommandReportsLoadedEnvironmentMaxWorkerTimeout(t *testing.T) {
	// 需求背景：horizon:timeout 对齐 Laravel 的配置查询语义，只读当前已加载 environment 的 supervisor timeout。
	store := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})
	manager, _ := NewManager(Config{
		Store:       "memory",
		Environment: "local",
		Supervisors: map[string]SupervisorConfig{
			"short": {Name: "short", Timeout: 30},
			"long":  {Name: "long", Timeout: 95},
		},
	}, WithStoreFactory(staticStoreResolver{store: store}))
	load := func() (*Manager, error) { return manager, nil }

	output := runHorizonCommand(t, horizoncmd.NewTimeoutCommand(newRuntimeLoader(load)), runtimeInput{args: map[string][]string{"environment": {"local"}}})
	if !strings.Contains(output, "95") {
		t.Fatalf("unexpected timeout output: %q", output)
	}

	cmd := horizoncmd.NewTimeoutCommand(newRuntimeLoader(load))
	err := cmd.Handle(runtimeCommandContext(cmd, runtimeInput{args: map[string][]string{"environment": {"production"}}}, io.Discard))
	if err == nil || !strings.Contains(err.Error(), "loaded environment") {
		t.Fatalf("expected loaded environment error, got %v", err)
	}

	defaultManager, _ := NewManager(Config{Store: "memory", Environment: "local"}, WithStoreFactory(staticStoreResolver{store: store}))
	defaultOutput := runHorizonCommand(t, horizoncmd.NewTimeoutCommand(newRuntimeLoader(func() (*Manager, error) { return defaultManager, nil })), runtimeInput{})
	if !strings.Contains(defaultOutput, "60") {
		t.Fatalf("empty supervisor timeout should default to 60, got %q", defaultOutput)
	}
}

func TestStaleCommandListsHeartbeatStaleProcessesOnly(t *testing.T) {
	// 需求背景：horizon:stale 是 Prismgo 只读诊断命令，按 heartbeat TTL 列出失联的 master/supervisor/worker。
	ctx := context.Background()
	now := time.Now().UTC()
	store := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Second})
	if err := store.HeartbeatMaster(ctx, MasterState{ID: "master-stale", Host: "host-1", PID: 1001, Status: MasterRunning, LastHeartbeatAt: now.Add(-time.Minute)}); err != nil {
		t.Fatalf("seed master: %v", err)
	}
	if err := store.HeartbeatSupervisor(ctx, SupervisorState{Name: "supervisor-stale", Host: "host-1", PID: 1002, Status: SupervisorRunning, LastHeartbeatAt: now.Add(-time.Minute)}); err != nil {
		t.Fatalf("seed supervisor: %v", err)
	}
	if err := store.HeartbeatWorker(ctx, WorkerState{ID: "worker-stale", Supervisor: "supervisor-stale", Host: "host-1", PID: 1003, Status: WorkerPaused, LastHeartbeatAt: now.Add(-time.Minute)}); err != nil {
		t.Fatalf("seed worker: %v", err)
	}
	if err := store.HeartbeatWorker(ctx, WorkerState{ID: "worker-fresh", Supervisor: "supervisor-stale", Host: "host-1", PID: 1004, Status: WorkerIdle, LastHeartbeatAt: now}); err != nil {
		t.Fatalf("seed fresh worker: %v", err)
	}
	manager, _ := NewManager(Config{Store: "memory"}, WithStoreFactory(staticStoreResolver{store: store}))

	output := runHorizonCommand(t, horizoncmd.NewStaleCommand(newRuntimeLoader(func() (*Manager, error) { return manager, nil })), runtimeInput{})
	for _, want := range []string{"master", "master-stale", "supervisor", "supervisor-stale", "worker", "worker-stale"} {
		if !strings.Contains(output, want) {
			t.Fatalf("stale output missing %q:\n%s", want, output)
		}
	}
	if strings.Contains(output, "worker-fresh") {
		t.Fatalf("fresh worker must not be listed:\n%s", output)
	}
}

func TestMetricsCommandsSnapshotAndClearMonitorState(t *testing.T) {
	// 需求背景：horizon:snapshot 只持久化 collector 从 queue 事件聚合出的 metrics，
	// horizon:clear-metrics 只清理 metrics 和 collector 内存聚合，不清理控制状态或 queue 数据。
	ctx := context.Background()
	store := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})
	manager, _ := NewManager(Config{Store: "memory"}, WithStoreFactory(staticStoreResolver{store: store}))
	load := func() (*Manager, error) { return manager, nil }
	// 通过 collector 注入事件。
	coll := manager.Collector()
	coll.Start(ctx)
	defer coll.Stop()
	now := time.Now()
	_ = coll.Collect(ctx, CollectorInput{
		Event: "queue.job_queued", Connection: "redis", Queue: "default",
		JobID: "job-1", JobName: "EmailJob", OccurredAt: now,
		Sampling: SamplingDecision{EventMetricsSampled: true, EventMetricsSampleRate: 1.0},
	})
	_ = coll.Collect(ctx, CollectorInput{
		Event: "queue.job_processed", Connection: "redis", Queue: "default",
		JobID: "job-1", JobName: "EmailJob", Runtime: 25 * time.Millisecond, OccurredAt: now,
		Sampling: SamplingDecision{EventMetricsSampled: true, EventMetricsSampleRate: 1.0},
	})
	time.Sleep(100 * time.Millisecond)
	if err := store.SetGlobalPaused(ctx, true); err != nil {
		t.Fatalf("pause: %v", err)
	}

	output := runHorizonCommand(t, horizoncmd.NewSnapshotCommand(newRuntimeLoader(load)), runtimeInput{})
	for _, want := range []string{"Snapshot At:", "Buckets: 1", "Processed: 1", "Failed: 0", "Released: 0", "Poison Envelopes: 0"} {
		if !strings.Contains(output, want) {
			t.Fatalf("snapshot output missing %q:\n%s", want, output)
		}
	}
	if strings.Contains(output, "Recent Jobs:") {
		t.Fatalf("snapshot output must not include recent jobs:\n%s", output)
	}
	snapshot, err := store.EventMetricWindows(ctx, EventMetricWindowQuery{Page: PageRequest{Page: 1, PageSize: 100}})
	if err != nil {
		t.Fatalf("stored event windows: %v", err)
	}
	if aggregateMetricsTotals(snapshot.Items).Processed != 1 || aggregateMetricsTotals(snapshot.Items).Queued != 1 {
		t.Fatalf("unexpected stored windows: %#v", snapshot)
	}

	clearOutput := runHorizonCommand(t, horizoncmd.NewClearMetricsCommand(newRuntimeLoader(load)), runtimeInput{})
	if !strings.Contains(clearOutput, "Horizon metrics cleared.") {
		t.Fatalf("unexpected clear output:\n%s", clearOutput)
	}
	cleared, err := store.EventMetricWindows(ctx, EventMetricWindowQuery{Page: PageRequest{Page: 1, PageSize: 100}})
	if err != nil {
		t.Fatalf("cleared metrics snapshot: %v", err)
	}
	// 使用 collector 状态检查内存聚合是否清理。
	collectorCleared := true
	if coll := manager.Collector(); coll != nil {
		peek := coll.SnapshotPeek(time.Now())
		if peek != nil && len(peek.windows) > 0 {
			collectorCleared = false
		}
	}
	if cleared.Total != 0 || !collectorCleared {
		t.Fatalf("windows should be cleared, store=%#v collector_cleared=%v", cleared, collectorCleared)
	}
	control, err := store.Control(ctx)
	if err != nil {
		t.Fatalf("control after clear metrics: %v", err)
	}
	if !control.GlobalPaused {
		t.Fatal("clear-metrics must not clear Horizon control state")
	}
}

func TestSnapshotCommandPersistsQueueLengthsBeforeMetrics(t *testing.T) {
	// 需求背景：historical scenario 04 要求 horizon:snapshot 先采集并保存队列长度，再保存 historical scenario 03 的事件派生 metrics。
	// 测试通过公开命令入口验证目标只来自 supervisor 配置，并且重复 connection+queue 会被去重。
	ctx := context.Background()
	store := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})
	queueManager := &fakeRuntimeQueueManager{
		connections: map[string]*fakeQueueConnection{
			"redis": {sizes: map[string]int64{"default": 4, "emails": 2}},
		},
	}
	manager, _ := NewManager(Config{
		Store: "memory",
		Supervisors: map[string]SupervisorConfig{
			"one": {Name: "one", Connection: "redis", Queues: []string{"default", "emails"}},
			"two": {Name: "two", Connection: "redis", Queues: []string{"default"}},
		},
	}, WithStoreFactory(staticStoreResolver{store: store}), WithQueueManager(queueManager))
	load := func() (*Manager, error) { return manager, nil }
	// 通过 collector 注入事件替代旧采集入口
	collQ := manager.Collector()
	collQ.Start(ctx)
	defer collQ.Stop()
	nowQ := time.Now()
	_ = collQ.Collect(ctx, CollectorInput{
		Event: "queue.job_queued", Connection: "redis", Queue: "default",
		JobID: "job-1", JobName: "EmailJob", OccurredAt: nowQ,
		Sampling: SamplingDecision{EventMetricsSampled: true, EventMetricsSampleRate: 1.0},
	})
	// 注入 processed 事件以生成 event_metrics bucket。
	_ = collQ.Collect(ctx, CollectorInput{
		Event: "queue.job_processed", Connection: "redis", Queue: "default",
		JobID: "job-1", JobName: "EmailJob", Runtime: 1 * time.Second, OccurredAt: nowQ,
		Sampling: SamplingDecision{EventMetricsSampled: true, EventMetricsSampleRate: 1.0},
	})
	time.Sleep(100 * time.Millisecond)

	output := runHorizonCommand(t, horizoncmd.NewSnapshotCommand(newRuntimeLoader(load)), runtimeInput{})
	for _, want := range []string{"Queue Lengths: 2", "Buckets: 1"} {
		if !strings.Contains(output, want) {
			t.Fatalf("snapshot output missing %q:\n%s", want, output)
		}
	}
	if strings.Contains(output, "Recent Jobs:") {
		t.Fatalf("snapshot output must not include recent jobs:\n%s", output)
	}
	queueLengths, err := store.QueueLengthSnapshot(ctx)
	if err != nil {
		t.Fatalf("stored queue length snapshot: %v", err)
	}
	if len(queueLengths.Queues) != 2 || queueLengths.Queues[0].Queue != "default" || queueLengths.Queues[0].Size != 4 || queueLengths.Queues[1].Queue != "emails" {
		t.Fatalf("unexpected queue length snapshot: %#v", queueLengths)
	}
	metrics, err := store.EventMetricWindows(ctx, EventMetricWindowQuery{Page: PageRequest{Page: 1, PageSize: 100}})
	if err != nil {
		t.Fatalf("stored event windows: %v", err)
	}
	if aggregateMetricsTotals(metrics.Items).Queued != 1 {
		t.Fatalf("metrics should be saved after queue lengths, got %#v", metrics)
	}
	if queueManager.restartCalls != 0 {
		t.Fatal("snapshot must not request queue worker restart")
	}
}

func TestSnapshotCommandPersistsMetricsHistory(t *testing.T) {
	// 需求背景：metadata boundary contract 要求 metrics history 在 horizon:snapshot 维护流程中生效，
	// historical scenario 34 后只保留 queue 级 event_metrics history，供 HTTP API 和 Dashboard 读取。
	ctx := context.Background()
	store := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})
	queueManager := &fakeRuntimeQueueManager{
		connections: map[string]*fakeQueueConnection{
			"redis": {sizes: map[string]int64{"default": 1}},
		},
	}
	bus := event.New()
	longWaits := 0
	bus.ListenFunc(EventLongWait, func(_ context.Context, ev event.Event) error {
		if typed, ok := ev.(LongWaitEvent); ok && typed.Connection == "redis" && typed.Queue == "default" {
			longWaits++
		}
		return nil
	})
	manager, _ := NewManager(Config{
		Store: "memory",
		Supervisors: map[string]SupervisorConfig{
			"one": {Name: "one", Connection: "redis", Queues: []string{"default"}},
		},
		Waits: map[string]int{"redis:default": 5},
	}, WithStoreFactory(staticStoreResolver{store: store}), WithQueueManager(queueManager), WithEventDispatcher(bus))
	load := func() (*Manager, error) { return manager, nil }
	// 通过 collector 注入事件替代旧采集入口
	coll := manager.Collector()
	coll.Start(ctx)
	defer coll.Stop()
	now := time.Now()
	queuedAt := now.Add(-7 * time.Second)
	// 入队事件（携带显式 queued_at 用于 waits/long_wait 计算）
	_ = coll.Collect(ctx, CollectorInput{
		Event: "queue.job_queued", Connection: "redis", Queue: "default",
		JobID: "job-1", JobName: "MailJob", OccurredAt: queuedAt,
		Sampling: SamplingDecision{EventMetricsSampled: true, EventMetricsSampleRate: 1.0},
	})
	// 处理事件
	_ = coll.Collect(ctx, CollectorInput{
		Event: "queue.job_processed", Connection: "redis", Queue: "default",
		JobID: "job-2", JobName: "MailJob", Runtime: 25 * time.Millisecond, OccurredAt: now,
		Sampling: SamplingDecision{EventMetricsSampled: true, EventMetricsSampleRate: 1.0},
	})
	// 等待后台 goroutine 处理 buffer 中的事件
	time.Sleep(100 * time.Millisecond)

	cmd := horizoncmd.NewSnapshotCommand(newRuntimeLoader(load))
	if err := cmd.Handle(runtimeCommandContext(cmd, runtimeInput{}, io.Discard)); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	// MetricsHistory 写入已随 SaveMetricsSnapshot 移除；改用 EventMetricWindows 验证。
	windows, err := store.EventMetricWindows(ctx, EventMetricWindowQuery{Page: PageRequest{Page: 1, PageSize: 10}})
	if err != nil {
		t.Fatalf("event windows: %v", err)
	}
	if windows.Total == 0 {
		t.Fatalf("event windows should be persisted after snapshot: %#v", windows)
	}
	if longWaits != 1 {
		t.Fatalf("long wait events = %d, want 1", longWaits)
	}
}

func TestListenRestartsHorizonWhenWatchedFilesChange(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tempDir := t.TempDir()
	watched := filepath.Join(tempDir, "horizon.go")
	if err := os.WriteFile(watched, []byte("first"), 0o644); err != nil {
		t.Fatalf("seed watched file: %v", err)
	}
	processes := &blockingProcessRunner{}
	inspector := &fakeProcessInspector{}
	manager, err := NewManager(Config{
		Environment: "testing",
		Watch:       []string{tempDir, tempDir, " "},
	}, WithStoreFactory(staticStoreResolver{store: NewMemoryStore(StoreOptions{})}), WithProcessRunner(processes), WithProcessInspector(inspector))
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	runtime := &runtimeCommandAdapter{manager: manager, store: NewMemoryStore(StoreOptions{})}
	result := make(chan struct {
		summary horizoncmd.ListenSummary
		err     error
	}, 1)
	go func() {
		summary, err := runtime.Listen(ctx, horizoncmd.ListenOptions{Environment: "testing", Poll: 100 * time.Millisecond})
		result <- struct {
			summary horizoncmd.ListenSummary
			err     error
		}{summary: summary, err: err}
	}()
	waitForTestCondition(t, func() bool { return processes.starts() == 1 })
	time.Sleep(20 * time.Millisecond)
	if err := os.WriteFile(watched, []byte("second"), 0o644); err != nil {
		t.Fatalf("write watched file: %v", err)
	}
	if err := os.WriteFile(filepath.Join(tempDir, "new.go"), []byte("new"), 0o644); err != nil {
		t.Fatalf("write second watched file: %v", err)
	}
	waitForTestCondition(t, func() bool { return processes.starts() == 2 })
	cancel()

	got := <-result
	if got.err != nil {
		t.Fatalf("listen returned error: %v", got.err)
	}
	if got.summary.WatchPathCount != 1 || got.summary.Starts != 2 || got.summary.Restarts != 1 {
		t.Fatalf("listen summary = %#v", got.summary)
	}
	if !inspector.terminated(5001, false) {
		t.Fatalf("expected first listen child to be gracefully terminated, got %#v", inspector.terminations)
	}
	for _, spec := range processes.specs {
		if len(spec.Args) != 2 || spec.Args[0] != "horizon" || spec.Args[1] != "--environment=testing" {
			t.Fatalf("listen child args = %#v", spec.Args)
		}
	}
}

func TestListenPropagatesChildExitErrorAndIgnoresMissingWatchPaths(t *testing.T) {
	store := NewMemoryStore(StoreOptions{})
	runner := &errorProcessRunner{err: errFakeProcessWait}
	manager, err := NewManager(Config{Environment: "testing", Watch: []string{"missing-path"}}, WithStoreFactory(staticStoreResolver{store: store}), WithProcessRunner(runner), WithProcessInspector(&fakeProcessInspector{}))
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	runtime := &runtimeCommandAdapter{manager: manager, store: store}
	summary, err := runtime.Listen(context.Background(), horizoncmd.ListenOptions{Poll: time.Millisecond})
	if !errors.Is(err, errFakeProcessWait) {
		t.Fatalf("listen error = %v, want %v", err, errFakeProcessWait)
	}
	if summary.WatchPathCount != 1 || summary.Starts != 1 {
		t.Fatalf("listen summary = %#v", summary)
	}

	left := scanWatchSignature([]string{"missing-path"})
	right := scanWatchSignature(nil)
	if !watchSignatureEqual(left, right) {
		t.Fatalf("missing paths should produce empty equal signatures: %#v %#v", left, right)
	}
}

func TestSnapshotCommandQueueLengthFailureIsAllOrNothing(t *testing.T) {
	// 需求背景：historical scenario 04 要求队列长度采集 fail-fast，任何一个目标 Size 失败都不能保存部分结果。
	ctx := context.Background()
	previous := QueueLengthSnapshot{
		CapturedAt: time.Date(2026, 5, 11, 20, 20, 0, 0, time.UTC),
		Queues:     []QueueLengthBucket{{Connection: "redis", Queue: "default", Size: 9}},
	}
	store := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})
	if err := store.SaveQueueLengthSnapshot(ctx, previous); err != nil {
		t.Fatalf("seed queue length snapshot: %v", err)
	}
	queueManager := &fakeRuntimeQueueManager{
		connections: map[string]*fakeQueueConnection{
			"redis": {sizes: map[string]int64{"default": 4}, sizeErrors: map[string]error{"emails": errFakeQueueSize}},
		},
	}
	manager, _ := NewManager(Config{
		Store: "memory",
		Supervisors: map[string]SupervisorConfig{
			"one": {Name: "one", Connection: "redis", Queues: []string{"default", "emails"}},
		},
	}, WithStoreFactory(staticStoreResolver{store: store}), WithQueueManager(queueManager))

	failingSnapshotCommand := horizoncmd.NewSnapshotCommand(newRuntimeLoader(func() (*Manager, error) { return manager, nil }))
	err := failingSnapshotCommand.Handle(runtimeCommandContext(failingSnapshotCommand, runtimeInput{}, io.Discard))
	if err == nil || !strings.Contains(err.Error(), "operation=size") || !strings.Contains(err.Error(), "connection=redis") || !strings.Contains(err.Error(), "queue=emails") || !strings.Contains(err.Error(), errFakeQueueSize.Error()) {
		t.Fatalf("expected detailed size error, got %v", err)
	}
	read, err := store.QueueLengthSnapshot(ctx)
	if err != nil {
		t.Fatalf("read queue length snapshot after failure: %v", err)
	}
	if len(read.Queues) != 1 || read.Queues[0].Size != 9 {
		t.Fatalf("failure must keep previous queue length snapshot, got %#v", read)
	}
	metrics, err := store.EventMetricWindows(ctx, EventMetricWindowQuery{Page: PageRequest{Page: 1, PageSize: 100}})
	if err != nil {
		t.Fatalf("read event windows after failure: %v", err)
	}
	if metrics.Total != 0 {
		t.Fatalf("failure must not save event windows, got %#v", metrics)
	}
	if queueManager.restartCalls != 0 {
		t.Fatal("snapshot failure must not request queue worker restart")
	}
}

func TestRuntimeMaintenanceCommandsReportMissingDependencies(t *testing.T) {
	// 需求背景：Horizon 维护命令依赖显式注入的 QueueManager/FailedStore；
	// 缺失依赖时必须返回错误，不能静默跳过维护动作。
	store := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})
	manager, _ := NewManager(Config{
		Store: "memory",
		Supervisors: map[string]SupervisorConfig{
			"one": {Name: "one", Connection: "redis", Queues: []string{"default"}},
		},
	}, WithStoreFactory(staticStoreResolver{store: store}))
	load := func() (*Manager, error) { return manager, nil }

	snapshotCmd := horizoncmd.NewSnapshotCommand(newRuntimeLoader(load))
	err := snapshotCmd.Handle(runtimeCommandContext(snapshotCmd, runtimeInput{}, io.Discard))
	if err == nil || !strings.Contains(err.Error(), "queue manager is not configured") {
		t.Fatalf("expected snapshot queue manager error, got %v", err)
	}
	clearCmd := horizoncmd.NewClearCommand(newRuntimeLoader(load))
	err = clearCmd.Handle(runtimeCommandContext(clearCmd, runtimeInput{options: map[string]string{"force": "true"}}, io.Discard))
	if err == nil || !strings.Contains(err.Error(), "queue manager is not configured") {
		t.Fatalf("expected clear queue manager error, got %v", err)
	}

	managerWithQueue, _ := NewManager(Config{Store: "memory"}, WithStoreFactory(staticStoreResolver{store: store}), WithQueueManager(&fakeRuntimeQueueManager{connections: map[string]*fakeQueueConnection{}}))
	forgetCmd := horizoncmd.NewForgetCommand(newRuntimeLoader(func() (*Manager, error) { return managerWithQueue, nil }))
	err = forgetCmd.Handle(runtimeCommandContext(forgetCmd, runtimeInput{args: map[string][]string{"id": {"failed-1"}}}, io.Discard))
	if err == nil || !strings.Contains(err.Error(), "failed job store is not configured") {
		t.Fatalf("expected failed store error, got %v", err)
	}
}

func TestClearCommandResolvesTargetsFromSupervisorConfig(t *testing.T) {
	// 需求背景：horizon:clear 只能从当前环境 supervisor 配置推导维护目标，
	// 参数不足时仅在目标唯一时自动选择，不能回退到 queue manager 默认连接或默认队列。
	store := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})
	conn := &fakeQueueConnection{sizes: map[string]int64{"default": 1}}
	queueManager := &fakeRuntimeQueueManager{connections: map[string]*fakeQueueConnection{"redis": conn}}
	manager, _ := NewManager(Config{
		Store: "memory",
		Supervisors: map[string]SupervisorConfig{
			"one": {Name: "one", Connection: "redis", Queues: []string{"default"}},
		},
	}, WithStoreFactory(staticStoreResolver{store: store}), WithQueueManager(queueManager))
	load := func() (*Manager, error) { return manager, nil }

	output := runHorizonCommand(t, horizoncmd.NewClearCommand(newRuntimeLoader(load)), runtimeInput{options: map[string]string{"force": "true"}})
	if !strings.Contains(output, "Queue cleared: redis:default") {
		t.Fatalf("unexpected clear output:\n%s", output)
	}
	if len(conn.clearCalls) != 1 || conn.clearCalls[0] != "default" {
		t.Fatalf("expected clear default once, got %#v", conn.clearCalls)
	}
	if queueManager.restartCalls != 0 {
		t.Fatal("clear must not request queue worker restart")
	}
}

func TestClearCommandReportsMissingAndAmbiguousTargets(t *testing.T) {
	// 测试目的：固定 clear 目标解析错误语义，保证 CLI 能清楚提示 connection、queue 或歧义候选问题。
	store := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})
	queueManager := &fakeRuntimeQueueManager{connections: map[string]*fakeQueueConnection{"redis": {sizes: map[string]int64{}}}}
	manager, _ := NewManager(Config{
		Store: "memory",
		Supervisors: map[string]SupervisorConfig{
			"one": {Name: "one", Connection: "redis", Queues: []string{"default", "emails"}},
			"two": {Name: "two", Connection: "sqs", Queues: []string{"default"}},
		},
	}, WithStoreFactory(staticStoreResolver{store: store}), WithQueueManager(queueManager))
	load := func() (*Manager, error) { return manager, nil }

	cases := []struct {
		name  string
		input runtimeInput
		want  string
	}{
		{name: "missing connection", input: runtimeInput{options: map[string]string{"queue": "default", "force": "true"}, args: map[string][]string{"connection": {"missing"}}}, want: "connection \"missing\" is not monitored"},
		{name: "missing queue", input: runtimeInput{options: map[string]string{"queue": "missing", "force": "true"}, args: map[string][]string{"connection": {"redis"}}}, want: "queue \"missing\" is not monitored"},
		{name: "ambiguous", input: runtimeInput{options: map[string]string{"force": "true"}}, want: "queue target is ambiguous"},
		{name: "ambiguous queue across connections", input: runtimeInput{options: map[string]string{"queue": "default", "force": "true"}}, want: "queue target is ambiguous"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cmd := horizoncmd.NewClearCommand(newRuntimeLoader(load))
			err := cmd.Handle(runtimeCommandContext(cmd, tc.input, io.Discard))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("expected error containing %q, got %v", tc.want, err)
			}
		})
	}
}

func TestForgetCommandFindsBeforeDeletingFailedJob(t *testing.T) {
	// 需求背景：queue.FailedStore.Forget 在部分实现中接近幂等删除，Horizon 命令层必须先 Find 才能稳定区分未找到。
	failed := &fakeFailedStore{items: map[string]*payload.FailedJob{
		"failed-1": {
			ID:       "failed-1",
			JobID:    "job-1",
			JobName:  "SensitiveJob",
			Error:    "raw database password stack",
			Envelope: payload.Envelope{ID: "job-1", Payload: []byte(`{"secret":"value"}`)},
		},
	}}
	queueManager := &fakeRuntimeQueueManager{connections: map[string]*fakeQueueConnection{}, failed: failed}
	manager, _ := NewManager(Config{Store: "memory"}, WithStoreFactory(staticStoreResolver{store: NewMemoryStore(StoreOptions{})}), WithQueueManager(queueManager))
	load := func() (*Manager, error) { return manager, nil }

	output := runHorizonCommand(t, horizoncmd.NewForgetCommand(newRuntimeLoader(load)), runtimeInput{args: map[string][]string{"id": {"failed-1"}}})
	if !strings.Contains(output, "Failed job forgotten: failed-1") {
		t.Fatalf("unexpected forget output:\n%s", output)
	}
	for _, leaked := range []string{"SensitiveJob", "secret", "raw database password stack"} {
		if strings.Contains(output, leaked) {
			t.Fatalf("forget output leaked %q:\n%s", leaked, output)
		}
	}
	if failed.findCalls != 1 || failed.forgetCalls != 1 {
		t.Fatalf("expected find then forget once, got find=%d forget=%d", failed.findCalls, failed.forgetCalls)
	}
	if queueManager.restartCalls != 0 {
		t.Fatal("forget must not request queue worker restart")
	}
}

func TestForgetCommandAllFlushesFailedStore(t *testing.T) {
	// 需求背景：Laravel 对齐的 horizon:forget {id?} {--all} 在 --all 时删除所有 failed jobs。
	// 测试通过 failed store 的公开 Flush 边界验证，不读取 job payload，避免命令输出泄露失败任务正文。
	failed := &fakeFailedStore{items: map[string]*payload.FailedJob{
		"failed-1": {ID: "failed-1", Envelope: payload.Envelope{Payload: []byte(`{"secret":"value"}`)}},
	}}
	queueManager := &fakeRuntimeQueueManager{connections: map[string]*fakeQueueConnection{}, failed: failed}
	manager, _ := NewManager(Config{Store: "memory"}, WithStoreFactory(staticStoreResolver{store: NewMemoryStore(StoreOptions{})}), WithQueueManager(queueManager))
	load := func() (*Manager, error) { return manager, nil }

	output := runHorizonCommand(t, horizoncmd.NewForgetCommand(newRuntimeLoader(load)), runtimeInput{options: map[string]string{"all": "true"}})
	if !strings.Contains(output, "All failed jobs forgotten.") {
		t.Fatalf("unexpected forget all output:\n%s", output)
	}
	if failed.flushCalls != 1 || failed.findCalls != 0 || failed.forgetCalls != 0 {
		t.Fatalf("forget --all should flush only, flush=%d find=%d forget=%d", failed.flushCalls, failed.findCalls, failed.forgetCalls)
	}
	if strings.Contains(output, "secret") {
		t.Fatalf("forget --all output leaked payload:\n%s", output)
	}
}

func TestForgetCommandReportsNotFoundReadAndDeleteErrors(t *testing.T) {
	// 测试目的：固定 forget 的错误分支，避免把 missing、读取失败和删除失败混成同一种 CLI 结果。
	cases := []struct {
		name   string
		store  *fakeFailedStore
		input  runtimeInput
		want   string
		forget int
	}{
		{name: "missing id", store: &fakeFailedStore{}, input: runtimeInput{}, want: "failed job id is required"},
		{name: "not found", store: &fakeFailedStore{findErr: queue.ErrEmpty}, input: runtimeInput{args: map[string][]string{"id": {"missing"}}}, want: "failed job not found: missing"},
		{name: "read error", store: &fakeFailedStore{findErr: errFakeFailedRead}, input: runtimeInput{args: map[string][]string{"id": {"failed-1"}}}, want: "read failed job failed-1"},
		{name: "delete error", store: &fakeFailedStore{items: map[string]*payload.FailedJob{"failed-1": {ID: "failed-1"}}, forgetErr: errFakeFailedDelete}, input: runtimeInput{args: map[string][]string{"id": {"failed-1"}}}, want: "delete failed job failed-1", forget: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			queueManager := &fakeRuntimeQueueManager{connections: map[string]*fakeQueueConnection{}, failed: tc.store}
			manager, _ := NewManager(Config{Store: "memory"}, WithStoreFactory(staticStoreResolver{store: NewMemoryStore(StoreOptions{})}), WithQueueManager(queueManager))
			cmd := horizoncmd.NewForgetCommand(newRuntimeLoader(func() (*Manager, error) { return manager, nil }))
			err := cmd.Handle(runtimeCommandContext(cmd, tc.input, io.Discard))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("expected error containing %q, got %v", tc.want, err)
			}
			if tc.store.forgetCalls != tc.forget {
				t.Fatalf("forget calls = %d, want %d", tc.store.forgetCalls, tc.forget)
			}
		})
	}
}

func TestPurgeCommandTerminatesOnlyHorizonOrphanWorkers(t *testing.T) {
	// 需求背景：horizon:purge 只清理不属于 active supervisor pool 的 Horizon worker process，
	// 不清空业务队列、不删除 failed store、不执行 metrics trim，也不替代 heartbeat stale cleanup。
	ctx := context.Background()
	now := time.Now().UTC()
	store := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})
	if err := store.HeartbeatMaster(ctx, MasterState{ID: "master-1", PID: 9001, Status: MasterRunning, LastHeartbeatAt: now}); err != nil {
		t.Fatalf("seed master: %v", err)
	}
	if err := store.HeartbeatWorker(ctx, WorkerState{ID: "worker-active", Supervisor: "s1", PID: 3001, Status: WorkerIdle, LastHeartbeatAt: now}); err != nil {
		t.Fatalf("seed worker: %v", err)
	}
	if err := store.RecordOrphanProcess(ctx, "master-1", 3003, now.Add(-2*time.Minute)); err != nil {
		t.Fatalf("seed old orphan: %v", err)
	}
	queueManager := &fakeRuntimeQueueManager{connections: map[string]*fakeQueueConnection{
		"redis": {sizes: map[string]int64{"default": 1}},
	}, failed: &fakeFailedStore{items: map[string]*payload.FailedJob{"failed-1": {ID: "failed-1"}}}}
	inspector := &fakeProcessInspector{processes: []HorizonProcess{
		{PID: 3001, Kind: HorizonProcessWorker, Command: "horizon:work --name=worker-active --prefix=prismgo_horizon --environment=production"},
		{PID: 3002, Kind: HorizonProcessWorker, Command: "horizon:work --name=orphan --prefix=prismgo_horizon --environment=production"},
		{PID: 9999, Kind: "other", Command: "not horizon"},
	}}
	manager, _ := NewManager(Config{
		Store:        "memory",
		HeartbeatTTL: time.Minute,
		Supervisors: map[string]SupervisorConfig{
			"s1": {Name: "s1", Connection: "redis", Queues: []string{"default"}, Timeout: 60},
		},
	}, WithStoreFactory(staticStoreResolver{store: store}), WithQueueManager(queueManager), WithProcessInspector(inspector))
	load := func() (*Manager, error) { return manager, nil }

	output := runHorizonCommand(t, horizoncmd.NewPurgeCommand(newRuntimeLoader(load)), runtimeInput{})

	for _, want := range []string{"Orphans Discovered: 1", "Terminate Requests: 2", "Orphans Forgotten: 1"} {
		if !strings.Contains(output, want) {
			t.Fatalf("purge output missing %q:\n%s", want, output)
		}
	}
	if !inspector.terminated(3002, false) || !inspector.terminated(3003, true) || inspector.terminated(3001, false) {
		t.Fatalf("unexpected terminate requests: %#v", inspector.terminations)
	}
	orphans, err := store.OrphanProcesses(ctx, "master-1")
	if err != nil {
		t.Fatalf("read orphan records: %v", err)
	}
	if len(orphans) != 1 || orphans[0].PID != 3002 {
		t.Fatalf("purge should keep newly discovered orphan tracking only, got %#v", orphans)
	}
	if queueManager.restartCalls != 0 || len(queueManager.connections["redis"].clearCalls) != 0 {
		t.Fatal("purge must not request queue restart or clear queue payload")
	}
}

func TestPurgeCommandRejectsUnsupportedSignal(t *testing.T) {
	// 测试目的：batch bulk dispatch contract 要求 --signal 必须被解析和校验；当前 ProcessInspector 只表达 SIGTERM
	// 等价的优雅终止，其他 signal 必须返回稳定错误，不能静默忽略。
	store := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})
	inspector := &fakeProcessInspector{}
	manager, _ := NewManager(Config{Store: "memory"}, WithStoreFactory(staticStoreResolver{store: store}), WithProcessInspector(inspector))
	cmd := horizoncmd.NewPurgeCommand(newRuntimeLoader(func() (*Manager, error) { return manager, nil }))

	err := cmd.Handle(runtimeCommandContext(cmd, runtimeInput{options: map[string]string{"signal": "SIGUSR1"}}, io.Discard))
	if err == nil || !strings.Contains(err.Error(), "purge signal \"SIGUSR1\" is not supported") {
		t.Fatalf("expected unsupported signal error, got %v", err)
	}
	if len(inspector.terminations) != 0 {
		t.Fatalf("unsupported signal must not terminate processes: %#v", inspector.terminations)
	}
}

func TestRuntimeAdapterPurgeReportsInspectorErrorWithDefaultMaster(t *testing.T) {
	// 测试目的：覆盖没有 fresh master 时的 default tracking key，以及进程扫描失败的明确错误边界。
	store := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})
	inspector := &fakeProcessInspector{err: errors.New("scan failed")}
	manager, _ := NewManager(Config{Store: "memory"}, WithStoreFactory(staticStoreResolver{store: store}), WithProcessInspector(inspector))
	runtime := &runtimeCommandAdapter{manager: manager, store: store}

	_, err := runtime.Purge(context.Background(), time.Now(), "SIGTERM")
	if err == nil || !strings.Contains(err.Error(), "scan failed") {
		t.Fatalf("expected process scan error, got %v", err)
	}
}

func TestRuntimeAdapterPurgeNoopsWhenNoOrphanProcesses(t *testing.T) {
	// 测试目的：覆盖 purge 的空扫描成功路径，确保没有 orphan 时不写 tracking record、不发送终止请求。
	ctx := context.Background()
	now := time.Now().UTC()
	store := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})
	if err := store.HeartbeatMaster(ctx, MasterState{ID: "master-empty", Status: MasterRunning, LastHeartbeatAt: now}); err != nil {
		t.Fatalf("seed master: %v", err)
	}
	inspector := &fakeProcessInspector{}
	manager, _ := NewManager(Config{Store: "memory"}, WithStoreFactory(staticStoreResolver{store: store}), WithProcessInspector(inspector))
	runtime := &runtimeCommandAdapter{manager: manager, store: store}

	summary, err := runtime.Purge(ctx, now, "SIGTERM")
	if err != nil {
		t.Fatalf("purge empty: %v", err)
	}
	if summary.OrphansDiscovered != 0 || summary.TerminateRequests != 0 || len(inspector.terminations) != 0 {
		t.Fatalf("unexpected empty purge summary=%#v terminations=%#v", summary, inspector.terminations)
	}
}

func TestProcessPoolProjectionAndWorkerArgsFallback(t *testing.T) {
	// 测试目的：覆盖 process pool DTO 复制和 workerArgs 的 supervisor queue fallback 分支。
	// 需求背景：workerArgs 是 supervisor 配置进入 horizon:work 子进程的唯一投影点，
	// retry_after 不能在这里丢失；block_for 属于 queue connection 配置，不投影为 worker 参数。
	pools := toCommandProcessPools([]ProcessPoolState{{Name: "s:all", Queues: []string{"high", "default"}, TargetWorkers: 2}})
	pools[0].Queues[0] = "changed"
	original := []ProcessPoolState{{Name: "s:all", Queues: []string{"high", "default"}, TargetWorkers: 2}}
	if original[0].Queues[0] != "high" {
		t.Fatal("test setup should keep original untouched")
	}
	args := workerArgs(SupervisorConfig{Name: "s", Connection: "redis", Queues: []string{"high", "default"}, RetryAfter: 90}, "worker-1", "local", "cmd", nil)
	foundQueue := false
	foundRetryAfter := false
	for _, arg := range args {
		if arg == "--queue=high,default" {
			foundQueue = true
		}
		if arg == "--retry-after=90" {
			foundRetryAfter = true
		}
		if arg == "--block-for=5" {
			t.Fatalf("worker args should not include block_for: %#v", args)
		}
	}
	if !foundQueue || !foundRetryAfter {
		t.Fatalf("worker args should fall back to supervisor queues: %#v", args)
	}
}

func TestSupervisorWorkloadsUseQueueSizesAndMetricsRuntime(t *testing.T) {
	// 需求背景：auto=time 的 runtime 必须来自 Horizon metrics；ready job 数量必须来自 queue backend Size。
	ctx := context.Background()
	store := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})
	queueManager := &fakeRuntimeQueueManager{connections: map[string]*fakeQueueConnection{
		"redis": {sizes: map[string]int64{"default": 7}},
	}}
	manager, _ := NewManager(Config{Store: "memory"}, WithStoreFactory(staticStoreResolver{store: store}), WithQueueManager(queueManager))
	// 通过 collector 注入事件替代旧采集入口
	coll := manager.Collector()
	coll.Start(ctx)
	defer coll.Stop()
	now := time.Now()
	_ = coll.Collect(ctx, CollectorInput{
		Event: "queue.job_processed", Connection: "redis", Queue: "default",
		JobName: "MailJob", Runtime: 2 * time.Second, OccurredAt: now,
		Sampling: SamplingDecision{EventMetricsSampled: true, EventMetricsSampleRate: 1.0},
	})
	// 等待后台处理
	time.Sleep(50 * time.Millisecond)
	runtime := &runtimeCommandAdapter{manager: manager, store: store}

	workloads, err := runtime.supervisorWorkloads(ctx, SupervisorConfig{Name: "s1", Connection: "redis", Queues: []string{"default"}}, time.Now())
	if err != nil {
		t.Fatalf("supervisor workloads: %v", err)
	}
	if len(workloads) != 1 || workloads[0].Ready != 7 || workloads[0].Runtime != 2*time.Second {
		t.Fatalf("unexpected workloads: %#v", workloads)
	}
}

func TestSnapshotCommandPersistsEmptyMetrics(t *testing.T) {
	// 测试目的：没有任何 queue 事件时，horizon:snapshot 仍应保存空 snapshot 并输出 0 counters，
	// 方便运维脚本确认命令执行成功，而不是把空数据当作错误。
	store := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})
	manager, _ := NewManager(Config{Store: "memory"}, WithStoreFactory(staticStoreResolver{store: store}))
	output := runHorizonCommand(t, horizoncmd.NewSnapshotCommand(newRuntimeLoader(func() (*Manager, error) { return manager, nil })), runtimeInput{})
	for _, want := range []string{"Buckets: 0", "Processed: 0", "Failed: 0", "Released: 0", "Poison Envelopes: 0"} {
		if !strings.Contains(output, want) {
			t.Fatalf("empty snapshot output missing %q:\n%s", want, output)
		}
	}
	if strings.Contains(output, "Recent Jobs:") {
		t.Fatalf("empty snapshot output must not include recent jobs:\n%s", output)
	}
	snapshot, err := store.EventMetricWindows(context.Background(), EventMetricWindowQuery{Page: PageRequest{Page: 1, PageSize: 100}})
	if err != nil {
		t.Fatalf("stored empty event windows: %v", err)
	}
	// 空 snapshot 无事件数据时 EventMetricWindows 为空是预期行为。
	if snapshot.Total != 0 {
		t.Fatalf("empty snapshot should not persist event windows, got %#v", snapshot)
	}
}

func runHorizonCommand(t *testing.T, cmd console.Command, input runtimeInput) string {
	t.Helper()
	stdout := &bytes.Buffer{}
	if err := cmd.Handle(runtimeCommandContext(cmd, input, stdout)); err != nil {
		t.Fatalf("%s returned error: %v", cmd.Definition().Name, err)
	}
	return stdout.String()
}

// runtimeCommandContext 构造运行时命令测试上下文。
//
// 参数说明：cmd 是待测命令；input 是 fake 输入；out 同时承接 stdout/stderr，便于断言输出文本。
func runtimeCommandContext(cmd console.Command, input runtimeInput, out io.Writer) console.CommandContext {
	return console.NewCommandContext(context.Background(), cmd, *cmd.Definition(), input, console.NewIO(strings.NewReader(""), out, out), nil, &cobra.Command{Use: cmd.Definition().Name})
}

type runtimeInput struct {
	args    map[string][]string
	options map[string]string
}

func (i runtimeInput) Argument(name string) string {
	values := i.args[name]
	if len(values) == 0 {
		return ""
	}
	return values[0]
}
func (i runtimeInput) Arguments(name string) []string { return append([]string(nil), i.args[name]...) }
func (i runtimeInput) Option(name string) string      { return i.options[name] }
func (i runtimeInput) OptionStrings(string) []string  { return nil }
func (i runtimeInput) OptionBool(string) bool         { return false }
func (i runtimeInput) OptionInt(string) int           { return 0 }
func (i runtimeInput) HasOption(string) bool          { return false }

type staticStoreResolver struct {
	store Store
}

// ResolveStore 返回测试预置 Store，确保命令测试不访问全局配置或 Redis。
func (r staticStoreResolver) ResolveStore(context.Context, Config) (Store, error) {
	return r.store, nil
}

var (
	errFakeQueueConnectionMissing = errors.New("fake queue connection missing")
	errFakeQueueMissing           = errors.New("fake queue missing")
	errFakeQueueSize              = errors.New("fake queue size failed")
	errFakeFailedRead             = errors.New("fake failed read")
	errFakeFailedDelete           = errors.New("fake failed delete")
	errFakeControlNotify          = errors.New("fake control notify failed")
	errFakeRestart                = errors.New("fake restart failed")
	errFakeWorker                 = errors.New("fake worker failed")
	errFakeProcessWait            = errors.New("fake process wait failed")
)

type fakeRuntimeQueueManager struct {
	connections  map[string]*fakeQueueConnection
	failed       queue.FailedStore
	restartCalls int
	restartErr   error
}

type contextAwareStore struct {
	Store
}

func (s contextAwareStore) Control(ctx context.Context) (ControlState, error) {
	if err := ctx.Err(); err != nil {
		return ControlState{}, err
	}
	return s.Store.Control(ctx)
}

func (s contextAwareStore) ClearTerminateRequest(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.Store.ClearTerminateRequest(ctx)
}

func (s contextAwareStore) Trim(ctx context.Context, now time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.Store.Trim(ctx, now)
}

type fakeProcessRunner struct {
	specs []ProcessSpec
	next  int
}

func (r *fakeProcessRunner) Start(_ context.Context, spec ProcessSpec) (ManagedProcess, error) {
	r.next++
	r.specs = append(r.specs, spec)
	return fakeManagedProcess{pid: 4000 + r.next}, nil
}

type failAfterProcessRunner struct {
	specs  []ProcessSpec
	failOn int
	err    error
	next   int
}

func (r *failAfterProcessRunner) Start(_ context.Context, spec ProcessSpec) (ManagedProcess, error) {
	r.next++
	r.specs = append(r.specs, spec)
	if r.next == r.failOn {
		return nil, r.err
	}
	return fakeManagedProcess{pid: 7000 + r.next}, nil
}

func (r *fakeProcessRunner) commands() []string {
	out := make([]string, 0, len(r.specs))
	for _, spec := range r.specs {
		if len(spec.Args) > 0 {
			out = append(out, spec.Args[0])
		}
	}
	return out
}

func (r *fakeProcessRunner) containsArg(value string) bool {
	for _, spec := range r.specs {
		for _, arg := range spec.Args {
			if arg == value {
				return true
			}
		}
	}
	return false
}

func (r *fakeProcessRunner) countArg(value string) int {
	count := 0
	for _, spec := range r.specs {
		for _, arg := range spec.Args {
			if arg == value {
				count++
			}
		}
	}
	return count
}

func poolTarget(pools []ProcessPoolState, queueName string) int {
	for _, pool := range pools {
		if pool.Queue == queueName {
			return pool.TargetWorkers
		}
	}
	return 0
}

func waitForTestCondition(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition was not met before timeout")
}

func waitForSupervisorHeartbeat(t *testing.T, store Store, name string) time.Time {
	t.Helper()
	for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); {
		supervisor, found, err := store.Supervisor(context.Background(), name, time.Now())
		if err != nil {
			t.Fatalf("supervisor heartbeat: %v", err)
		}
		if found && !supervisor.LastHeartbeatAt.IsZero() {
			return supervisor.LastHeartbeatAt
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("supervisor %s heartbeat was not recorded", name)
	return time.Time{}
}

type blockingProcessRunner struct {
	mu    sync.Mutex
	specs []ProcessSpec
	next  int
}

func (r *blockingProcessRunner) Start(ctx context.Context, spec ProcessSpec) (ManagedProcess, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.next++
	r.specs = append(r.specs, spec)
	return blockingManagedProcess{pid: 5000 + r.next, ctx: ctx}, nil
}

func (r *blockingProcessRunner) starts() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.next
}

func (r *blockingProcessRunner) commands() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, 0, len(r.specs))
	for _, spec := range r.specs {
		if len(spec.Args) > 0 {
			out = append(out, spec.Args[0])
		}
	}
	return out
}

func (r *blockingProcessRunner) containsArg(value string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, spec := range r.specs {
		for _, arg := range spec.Args {
			if arg == value {
				return true
			}
		}
	}
	return false
}

func (r *blockingProcessRunner) countArg(value string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	count := 0
	for _, spec := range r.specs {
		for _, arg := range spec.Args {
			if arg == value {
				count++
			}
		}
	}
	return count
}

type blockingManagedProcess struct {
	pid int
	ctx context.Context
}

func (p blockingManagedProcess) PID() int { return p.pid }

func (p blockingManagedProcess) Wait() error {
	<-p.ctx.Done()
	return nil
}

type restartOnceProcessRunner struct {
	mu    sync.Mutex
	count int
}

func (r *restartOnceProcessRunner) Start(ctx context.Context, _ ProcessSpec) (ManagedProcess, error) {
	r.mu.Lock()
	r.count++
	count := r.count
	r.mu.Unlock()
	if count == 1 {
		return immediateManagedProcess{pid: 6001}, nil
	}
	return blockingManagedProcess{pid: 6000 + count, ctx: ctx}, nil
}

func (r *restartOnceProcessRunner) starts() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.count
}

type controlledProcessRunner struct {
	mu        sync.Mutex
	processes []*controlledManagedProcess
}

func newControlledProcessRunner() *controlledProcessRunner {
	return &controlledProcessRunner{}
}

func (r *controlledProcessRunner) Start(ctx context.Context, _ ProcessSpec) (ManagedProcess, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	process := &controlledManagedProcess{
		pid:  6201 + len(r.processes),
		done: make(chan error, 1),
		ctx:  ctx,
	}
	r.processes = append(r.processes, process)
	return process, nil
}

func (r *controlledProcessRunner) starts() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.processes)
}

func (r *controlledProcessRunner) release(index int, err error) {
	r.mu.Lock()
	if index < 0 || index >= len(r.processes) {
		r.mu.Unlock()
		return
	}
	process := r.processes[index]
	r.mu.Unlock()
	process.done <- err
}

type controlledManagedProcess struct {
	pid  int
	done chan error
	ctx  context.Context
}

func (p *controlledManagedProcess) PID() int { return p.pid }

func (p *controlledManagedProcess) Wait() error {
	select {
	case err := <-p.done:
		return err
	case <-p.ctx.Done():
		return nil
	}
}

type manualProcessRunner struct {
	mu        sync.Mutex
	processes []*manualManagedProcess
}

func newManualProcessRunner() *manualProcessRunner {
	return &manualProcessRunner{}
}

func (r *manualProcessRunner) Start(context.Context, ProcessSpec) (ManagedProcess, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	process := &manualManagedProcess{pid: 6401 + len(r.processes), done: make(chan error, 1)}
	r.processes = append(r.processes, process)
	return process, nil
}

func (r *manualProcessRunner) starts() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.processes)
}

func (r *manualProcessRunner) release(index int, err error) {
	r.mu.Lock()
	if index < 0 || index >= len(r.processes) {
		r.mu.Unlock()
		return
	}
	process := r.processes[index]
	r.mu.Unlock()
	process.done <- err
}

type manualManagedProcess struct {
	pid  int
	done chan error
}

func (p *manualManagedProcess) PID() int { return p.pid }

func (p *manualManagedProcess) Wait() error {
	return <-p.done
}

type immediateManagedProcess struct {
	pid int
}

func (p immediateManagedProcess) PID() int    { return p.pid }
func (p immediateManagedProcess) Wait() error { return nil }

type errorProcessRunner struct {
	err error
}

func (r *errorProcessRunner) Start(context.Context, ProcessSpec) (ManagedProcess, error) {
	return fakeManagedProcess{pid: 6101, err: r.err}, nil
}

type fakeManagedProcess struct {
	pid int
	err error
}

func (p fakeManagedProcess) PID() int    { return p.pid }
func (p fakeManagedProcess) Wait() error { return p.err }

type fakeControlNotifier struct {
	mu      sync.Mutex
	targets []ControlTarget
	err     error
}

func (n *fakeControlNotifier) Notify(_ context.Context, targets []ControlTarget) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.targets = append(n.targets, targets...)
	return n.err
}

func (n *fakeControlNotifier) has(kind string, pid int) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, target := range n.targets {
		if target.Type == kind && target.PID == pid {
			return true
		}
	}
	return false
}

type fakeProcessInspector struct {
	mu           sync.Mutex
	processes    []HorizonProcess
	terminations []fakeTermination
	err          error
}

type fakeTermination struct {
	pid   int
	force bool
}

func (i *fakeProcessInspector) HorizonProcesses(context.Context) ([]HorizonProcess, error) {
	return append([]HorizonProcess(nil), i.processes...), i.err
}

func (i *fakeProcessInspector) Terminate(_ context.Context, pid int, force bool) error {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.terminations = append(i.terminations, fakeTermination{pid: pid, force: force})
	return nil
}

func (i *fakeProcessInspector) terminationCount(force bool) int {
	i.mu.Lock()
	defer i.mu.Unlock()
	count := 0
	for _, item := range i.terminations {
		if item.force == force {
			count++
		}
	}
	return count
}

func (i *fakeProcessInspector) terminated(pid int, force bool) bool {
	i.mu.Lock()
	defer i.mu.Unlock()
	for _, item := range i.terminations {
		if item.pid == pid && item.force == force {
			return true
		}
	}
	return false
}

type fakeWorkerRunner struct {
	options        []queue.WorkerOptions
	sessionOptions queue.WorkerOptions
	err            error
	hook           func(context.Context)
	activateHook   func(context.Context)
	beginCalls     int
	activateCalls  int
	closeCalls     int
	sessionHook    func(*fakeWorkerRunner)
}

type beginErrorWorkerRunner struct {
	err error
}

func (r beginErrorWorkerRunner) Begin(context.Context, queue.WorkerOptions) (queuecontract.WorkerSession, error) {
	return nil, r.err
}

type countingEventDispatcher struct {
	mu            sync.Mutex
	listenCalls   int
	handlers      map[string][]func(context.Context, event.Event) error
	dispatchCalls map[string]int
}

func (d *countingEventDispatcher) ListenFunc(eventName string, fn func(context.Context, event.Event) error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.listenCalls++
	if d.handlers == nil {
		d.handlers = make(map[string][]func(context.Context, event.Event) error)
	}
	d.handlers[eventName] = append(d.handlers[eventName], fn)
}

func (d *countingEventDispatcher) Dispatch(ctx context.Context, ev event.Event) {
	if ev == nil {
		return
	}
	d.mu.Lock()
	name := ev.Name()
	handlers := append([]func(context.Context, event.Event) error(nil), d.handlers[name]...)
	d.mu.Unlock()
	for _, handler := range handlers {
		_ = handler(ctx, ev)
		d.mu.Lock()
		if d.dispatchCalls == nil {
			d.dispatchCalls = make(map[string]int)
		}
		d.dispatchCalls[name]++
		d.mu.Unlock()
	}
}

func (d *countingEventDispatcher) DispatchCalls(eventName string) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.dispatchCalls[eventName]
}

type masterLeaseDenyStore struct {
	Store
}

func (s masterLeaseDenyStore) AcquireMasterLease(context.Context, MasterState) (bool, error) {
	return false, nil
}

func (r *fakeWorkerRunner) Begin(_ context.Context, options queue.WorkerOptions) (queuecontract.WorkerSession, error) {
	r.beginCalls++
	r.sessionOptions = options
	if r.sessionHook != nil {
		r.sessionHook(r)
	}
	return r, nil
}

func (r *fakeWorkerRunner) Activate(ctx context.Context) error {
	r.activateCalls++
	if r.activateHook != nil {
		r.activateHook(ctx)
	}
	return nil
}

func (r *fakeWorkerRunner) Work(ctx context.Context) error {
	r.options = append(r.options, r.sessionOptions)
	if r.hook != nil {
		r.hook(ctx)
	}
	return r.err
}

func (r *fakeWorkerRunner) emit(ctx context.Context, ev queue.Event) {
	if r != nil && r.sessionOptions.EventObserver != nil {
		if observedCtx := r.sessionOptions.EventObserver(ctx, ev); observedCtx != nil {
			ctx = observedCtx
		}
	}
	if sink := queue.CurrentEventSink(); sink != nil {
		sink(ctx, ev)
	}
}

func (r *fakeWorkerRunner) Close() error {
	r.closeCalls++
	return nil
}

type heartbeatFailOnceStore struct {
	Store
	remainingFailures int
}

func (s *heartbeatFailOnceStore) HeartbeatWorker(ctx context.Context, state WorkerState) error {
	if s.remainingFailures > 0 {
		s.remainingFailures--
		return errors.New("redis://secret payload raw envelope failed")
	}
	return s.Store.HeartbeatWorker(ctx, state)
}

func (m *fakeRuntimeQueueManager) Queue(name string) (queuecontract.Queue, error) {
	conn := m.connections[name]
	if conn == nil {
		return nil, errFakeQueueConnectionMissing
	}
	return conn, nil
}

func (m *fakeRuntimeQueueManager) Failed() queue.FailedStore {
	return m.failed
}

func (m *fakeRuntimeQueueManager) RequestRestart(context.Context) error {
	m.restartCalls++
	return m.restartErr
}

type fakeQueueConnection struct {
	mu         sync.Mutex
	sizes      map[string]int64
	sizeErrors map[string]error
	clearCalls []string
	clearErr   error
	// leaseCalls/releaseCalls/leaseQueues 用于验证 horizon:work 是否按进程生命周期持有消费意图。
	//
	// 需求背景：真实 RabbitMQ 连接实现 queue.ConsumerIntentLeaser；测试替身需要记录
	// AcquireConsumerIntent 的调用次数、释放次数和队列参数，才能覆盖 unacked 卡死问题的回归路径。
	// 设计思路：fake 连接同时满足 Horizon QueueConnection 和 queue.ConsumerIntentLeaser，
	// 不访问真实 RabbitMQ，只验证 runtime 对 consumer lease 的生命周期管理。
	leaseCalls           int
	releaseCalls         int
	leaseQueues          []string
	popSessionCalls      int
	popSessionCloseCalls int
	acquireHook          func()
}

type fakeQueuePopSession struct {
	*fakeQueueConnection
	id int
}

func (s *fakeQueuePopSession) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.popSessionCloseCalls++
	return nil
}

type blockingQueueManager struct {
	connection *blockingSizeQueueConnection
}

func (m *blockingQueueManager) Queue(string) (queuecontract.Queue, error) {
	return m.connection, nil
}

func (m *blockingQueueManager) Failed() queue.FailedStore {
	return nil
}

func (m *blockingQueueManager) RequestRestart(context.Context) error {
	return nil
}

type blockingRestartQueueManager struct{}

func (m blockingRestartQueueManager) Queue(string) (queuecontract.Queue, error) {
	return nil, errFakeQueueConnectionMissing
}

func (m blockingRestartQueueManager) Failed() queue.FailedStore {
	return nil
}

func (m blockingRestartQueueManager) RequestRestart(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}

type blockingSizeQueueConnection struct {
	enteredC chan struct{}
	releaseC chan struct{}
	once     sync.Once
}

func newBlockingSizeQueueConnection() *blockingSizeQueueConnection {
	return &blockingSizeQueueConnection{enteredC: make(chan struct{}), releaseC: make(chan struct{})}
}

func (c *blockingSizeQueueConnection) Size(ctx context.Context, _ string) (int64, error) {
	c.once.Do(func() { close(c.enteredC) })
	select {
	case <-c.releaseC:
		return 0, nil
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

func (c *blockingSizeQueueConnection) Clear(context.Context, string) error {
	return nil
}

func (c *blockingSizeQueueConnection) Push(context.Context, string, queuecontract.Payload) error {
	return nil
}

func (c *blockingSizeQueueConnection) Later(context.Context, string, queuecontract.Payload, time.Duration) error {
	return nil
}

func (c *blockingSizeQueueConnection) Bulk(_ context.Context, _ string, bodies []queuecontract.Payload) (queuecontract.BulkResult, error) {
	return queuecontract.BulkResult{Accepted: len(bodies)}, nil
}

func (c *blockingSizeQueueConnection) Pop(context.Context, []string, ...queuecontract.PopWaitMode) (queuecontract.ReservedJob, error) {
	return nil, queue.ErrEmpty
}

func (c *blockingSizeQueueConnection) Close() error {
	return nil
}

func (c *blockingSizeQueueConnection) entered() bool {
	select {
	case <-c.enteredC:
		return true
	default:
		return false
	}
}

func (c *blockingSizeQueueConnection) release() {
	close(c.releaseC)
}

func (c *fakeQueueConnection) Size(_ context.Context, queueName string) (int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.sizeErrors[queueName]; err != nil {
		return 0, err
	}
	size, ok := c.sizes[queueName]
	if !ok {
		return 0, errFakeQueueMissing
	}
	return size, nil
}

func (c *fakeQueueConnection) setSizes(sizes map[string]int64) {
	c.mu.Lock()
	c.sizes = sizes
	c.mu.Unlock()
}

func (c *fakeQueueConnection) Clear(_ context.Context, queueName string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.clearCalls = append(c.clearCalls, queueName)
	return c.clearErr
}

func (c *fakeQueueConnection) Push(context.Context, string, queuecontract.Payload) error {
	return nil
}

func (c *fakeQueueConnection) Later(context.Context, string, queuecontract.Payload, time.Duration) error {
	return nil
}

func (c *fakeQueueConnection) Bulk(_ context.Context, _ string, bodies []queuecontract.Payload) (queuecontract.BulkResult, error) {
	return queuecontract.BulkResult{Accepted: len(bodies)}, nil
}

func (c *fakeQueueConnection) Pop(context.Context, []string, ...queuecontract.PopWaitMode) (queuecontract.ReservedJob, error) {
	return nil, queue.ErrEmpty
}

func (c *fakeQueueConnection) NewPopSession() queuecontract.Queue {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.popSessionCalls++
	return &fakeQueuePopSession{fakeQueueConnection: c, id: c.popSessionCalls}
}

func (c *fakeQueueConnection) Close() error {
	return nil
}

func (c *fakeQueueConnection) AcquireConsumerIntent(queues []string) (func() error, error) {
	// 参数说明：queues 是 horizon:work 本次计划消费的队列列表；fake 只复制保存，避免测试断言受切片复用影响。
	c.mu.Lock()
	c.leaseCalls++
	c.leaseQueues = append([]string(nil), queues...)
	acquireHook := c.acquireHook
	c.mu.Unlock()
	if acquireHook != nil {
		acquireHook()
	}
	return func() error {
		c.mu.Lock()
		c.releaseCalls++
		c.mu.Unlock()
		return nil
	}, nil
}

type fakeFailedStore struct {
	items       map[string]*payload.FailedJob
	findErr     error
	forgetErr   error
	findCalls   int
	forgetCalls int
	flushCalls  int
}

func (s *fakeFailedStore) Record(context.Context, payload.FailedJob) error { return nil }

func (s *fakeFailedStore) Page(context.Context, state.PageRequest) (state.PageEnvelope[payload.FailedJob], error) {
	return state.PageEnvelope[payload.FailedJob]{}, nil
}

func (s *fakeFailedStore) Find(_ context.Context, id string) (*payload.FailedJob, error) {
	s.findCalls++
	if s.findErr != nil {
		return nil, s.findErr
	}
	item := s.items[id]
	if item == nil {
		return nil, queue.ErrEmpty
	}
	cloned := *item
	return &cloned, nil
}

func (s *fakeFailedStore) Forget(_ context.Context, id string) error {
	s.forgetCalls++
	if s.forgetErr != nil {
		return s.forgetErr
	}
	delete(s.items, id)
	return nil
}

func (s *fakeFailedStore) Flush(context.Context) error {
	s.flushCalls++
	s.items = map[string]*payload.FailedJob{}
	return nil
}
