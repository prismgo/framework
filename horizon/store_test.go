package horizon

import (
	"context"
	"reflect"
	"testing"
	"time"
)

func TestMemoryStoreDerivesStatusFromHeartbeatAndControl(t *testing.T) {
	// 需求背景：memory store 是本地/测试实现，但状态语义必须与 Redis Store 一致。该测试按一次
	// supervisor/worker heartbeat 到 status snapshot 的完整路径验证 running、paused、terminating 和 stale。
	ctx := context.Background()
	now := time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC)
	store := NewMemoryStore(StoreOptions{Prefix: "test", HeartbeatTTL: 30 * time.Second})

	if err := store.HeartbeatSupervisor(ctx, SupervisorState{
		Name:            "supervisor-default",
		Host:            "worker-1",
		PID:             101,
		Status:          SupervisorRunning,
		StartedAt:       now.Add(-time.Minute),
		LastHeartbeatAt: now,
		WorkerCount:     2,
		Connection:      "redis",
		Queues:          []string{"default", "emails"},
	}); err != nil {
		t.Fatalf("heartbeat supervisor: %v", err)
	}
	if err := store.HeartbeatWorker(ctx, WorkerState{
		ID:              "worker-1",
		Supervisor:      "supervisor-default",
		Host:            "worker-1",
		PID:             201,
		Status:          WorkerIdle,
		StartedAt:       now.Add(-time.Minute),
		LastHeartbeatAt: now,
	}); err != nil {
		t.Fatalf("heartbeat worker: %v", err)
	}

	snapshot, err := store.StatusSnapshot(ctx, now.Add(10*time.Second))
	if err != nil {
		t.Fatalf("status snapshot: %v", err)
	}
	if snapshot.Status != GlobalRunning || snapshot.SupervisorCount != 1 || snapshot.WorkerCount != 1 {
		t.Fatalf("unexpected running snapshot: %#v", snapshot)
	}

	if err := store.SetGlobalPaused(ctx, true); err != nil {
		t.Fatalf("pause horizon: %v", err)
	}
	snapshot, err = store.StatusSnapshot(ctx, now.Add(10*time.Second))
	if err != nil {
		t.Fatalf("paused snapshot: %v", err)
	}
	if snapshot.Status != GlobalPaused || !snapshot.GlobalPaused {
		t.Fatalf("unexpected paused snapshot: %#v", snapshot)
	}

	if err := store.RequestTerminate(ctx, now.Add(20*time.Second), false); err != nil {
		t.Fatalf("request terminate: %v", err)
	}
	snapshot, err = store.StatusSnapshot(ctx, now.Add(20*time.Second))
	if err != nil {
		t.Fatalf("terminating snapshot: %v", err)
	}
	if snapshot.Status != GlobalTerminating || !snapshot.TerminateRequested {
		t.Fatalf("unexpected terminating snapshot: %#v", snapshot)
	}

	if err := store.ClearTerminateRequest(ctx); err != nil {
		t.Fatalf("clear terminate: %v", err)
	}
	if err := store.SetGlobalPaused(ctx, false); err != nil {
		t.Fatalf("continue horizon: %v", err)
	}
	stale, found, err := store.Supervisor(ctx, "supervisor-default", now.Add(time.Minute))
	if err != nil || !found {
		t.Fatalf("read stale supervisor: found=%v err=%v", found, err)
	}
	if stale.Status != SupervisorStale {
		t.Fatalf("supervisor status = %q, want stale", stale.Status)
	}
	snapshot, err = store.StatusSnapshot(ctx, now.Add(time.Minute))
	if err != nil {
		t.Fatalf("stale snapshot: %v", err)
	}
	if snapshot.Status != GlobalInactive || snapshot.StaleSupervisorCount != 1 || snapshot.StaleWorkerCount != 1 {
		t.Fatalf("unexpected stale snapshot: %#v", snapshot)
	}
}

func TestMemoryStoreStatusSnapshotInactiveWhenNoFreshSupervisor(t *testing.T) {
	// 需求背景：Horizon 全局状态由 Store 读取路径派生；没有 fresh supervisor 时应明确返回 inactive。
	store := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})
	snapshot, err := store.StatusSnapshot(context.Background(), time.Now())
	if err != nil {
		t.Fatalf("status snapshot: %v", err)
	}
	if snapshot.Status != GlobalInactive || snapshot.SupervisorCount != 0 || snapshot.WorkerCount != 0 {
		t.Fatalf("unexpected inactive snapshot: %#v", snapshot)
	}
}

func TestMemoryStoreKeepsSameNameSupervisorInstancesByHostEnvironment(t *testing.T) {
	// 需求背景：同一个 Horizon 集群可能有多台机器运行同名 supervisor；memory store 作为测试实现
	// 也必须保留每个 host/environment 实例，并让旧 Supervisor(name) 接口稳定返回最新 heartbeat。
	ctx := context.Background()
	now := time.Date(2026, 5, 19, 10, 0, 0, 0, time.UTC)
	store := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})

	states := []SupervisorState{
		{Name: "fixed", Host: "host-a", Environment: "local", PID: 101, Status: SupervisorRunning, LastHeartbeatAt: now},
		{Name: "fixed", Host: "host-b", Environment: "local", PID: 202, Status: SupervisorRunning, LastHeartbeatAt: now.Add(time.Second)},
	}
	for _, state := range states {
		if err := store.HeartbeatSupervisor(ctx, state); err != nil {
			t.Fatalf("heartbeat supervisor: %v", err)
		}
	}

	supervisors, err := store.Supervisors(ctx, now.Add(2*time.Second))
	if err != nil {
		t.Fatalf("supervisors: %v", err)
	}
	if len(supervisors) != 2 || supervisors[0].Host != "host-b" || supervisors[1].Host != "host-a" {
		t.Fatalf("same-name supervisors should remain distinct and heartbeat-sorted: %#v", supervisors)
	}
	latest, ok, err := store.Supervisor(ctx, "fixed", now.Add(2*time.Second))
	if err != nil || !ok {
		t.Fatalf("latest supervisor: ok=%v err=%v", ok, err)
	}
	if latest.Host != "host-b" || latest.PID != 202 {
		t.Fatalf("Supervisor(name) should return latest heartbeat instance, got %#v", latest)
	}
}

func TestMemoryStoreKeepsSameWorkerSlotByHostEnvironment(t *testing.T) {
	// 需求背景：worker slot 名只在单个 supervisor 实例内唯一；跨机器复用同一 slot 名时，
	// Workers() 不能覆盖旧实例，控制态派生也要同时作用于所有实例。
	ctx := context.Background()
	now := time.Date(2026, 5, 19, 10, 30, 0, 0, time.UTC)
	store := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})

	for _, state := range []WorkerState{
		{ID: "fixed-1", Supervisor: "fixed", Host: "host-a", Environment: "local", PID: 301, Status: WorkerIdle, LastHeartbeatAt: now},
		{ID: "fixed-1", Supervisor: "fixed", Host: "host-b", Environment: "local", PID: 302, Status: WorkerIdle, LastHeartbeatAt: now.Add(time.Second)},
	} {
		if err := store.HeartbeatWorker(ctx, state); err != nil {
			t.Fatalf("heartbeat worker: %v", err)
		}
	}
	if err := store.RequestTerminate(ctx, now.Add(2*time.Second), false); err != nil {
		t.Fatalf("request terminate: %v", err)
	}

	workers, err := store.Workers(ctx, now.Add(3*time.Second))
	if err != nil {
		t.Fatalf("workers: %v", err)
	}
	if len(workers) != 2 || workers[0].Host != "host-b" || workers[1].Host != "host-a" {
		t.Fatalf("same worker slot should remain distinct and heartbeat-sorted: %#v", workers)
	}
	for _, worker := range workers {
		if worker.Status != WorkerTerminating {
			t.Fatalf("worker should derive terminating status, got %#v", worker)
		}
	}
}

func TestStatusSnapshotIgnoresOrphanTerminateFlagWithoutFreshProcesses(t *testing.T) {
	// 需求背景：旧版本可能留下 terminate control flag；没有 fresh supervisor/worker 时，
	// Dashboard 不应因为这个孤立标记永久显示 terminating。
	store := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})
	if err := store.RequestTerminate(context.Background(), time.Now(), false); err != nil {
		t.Fatalf("request terminate: %v", err)
	}

	snapshot, err := store.StatusSnapshot(context.Background(), time.Now())
	if err != nil {
		t.Fatalf("status snapshot: %v", err)
	}
	if snapshot.Status != GlobalInactive || snapshot.TerminateRequested {
		t.Fatalf("orphan terminate flag should not keep global status terminating: %#v", snapshot)
	}
}

func TestMemoryStoreDerivesMasterStatusFromHeartbeat(t *testing.T) {
	// 需求背景：runtime command contract 引入真实 master/supervisor/worker 进程树后，Store 需要单独表达 horizon master 进程，
	// 不能把 master 混入 SupervisorState。该测试通过公开 Store API 验证 master heartbeat、列表和 stale 派生语义。
	ctx := context.Background()
	now := time.Date(2026, 5, 12, 9, 0, 0, 0, time.UTC)
	store := NewMemoryStore(StoreOptions{HeartbeatTTL: 30 * time.Second})

	if err := store.HeartbeatMaster(ctx, MasterState{
		ID:              "master-1",
		Host:            "host-1",
		PID:             1001,
		Status:          MasterRunning,
		StartedAt:       now.Add(-time.Minute),
		LastHeartbeatAt: now,
		SupervisorCount: 2,
		Environment:     "local",
	}); err != nil {
		t.Fatalf("heartbeat master: %v", err)
	}
	read, found, err := store.Master(ctx, "master-1", now.Add(10*time.Second))
	if err != nil || !found {
		t.Fatalf("read master: found=%v err=%v", found, err)
	}
	if read.Status != MasterRunning || read.SupervisorCount != 2 || read.Environment != "local" {
		t.Fatalf("unexpected master state: %#v", read)
	}
	masters, err := store.Masters(ctx, now.Add(10*time.Second))
	if err != nil {
		t.Fatalf("list masters: %v", err)
	}
	if len(masters) != 1 || masters[0].ID != "master-1" {
		t.Fatalf("unexpected masters: %#v", masters)
	}
	stale, found, err := store.Master(ctx, "master-1", now.Add(time.Minute))
	if err != nil || !found {
		t.Fatalf("read stale master: found=%v err=%v", found, err)
	}
	if stale.Status != MasterStale {
		t.Fatalf("master status = %q, want stale", stale.Status)
	}
}

func TestMemoryStoreMetricsSnapshotAndClearMetricsBoundary(t *testing.T) {
	// 需求背景：historical scenario 03 的 clear-metrics 只清理事件派生 metrics，不能清理 Laravel config contract 已有的
	// supervisor/worker heartbeat 与控制标记；该测试固定 memory store 的同一语义。
	ctx := context.Background()
	now := time.Date(2026, 5, 11, 19, 0, 0, 0, time.UTC)
	store := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})
	if err := store.HeartbeatSupervisor(ctx, SupervisorState{Name: "s1", Status: SupervisorRunning, LastHeartbeatAt: now}); err != nil {
		t.Fatalf("heartbeat supervisor: %v", err)
	}
	if err := store.SetGlobalPaused(ctx, true); err != nil {
		t.Fatalf("pause: %v", err)
	}
	eventWindow := EventMetricWindow{
		WindowStart: now.Truncate(time.Minute),
		WindowEnd:   now.Truncate(time.Minute).Add(time.Minute),
		FlushAt:     now,
		Connection:  "redis",
		Queue:       "default",
		Processed:   2,
		Quality:     EventMetricQualityExact,
	}
	if err := store.AppendEventMetricWindows(ctx, []EventMetricWindow{eventWindow}, 24*time.Hour); err != nil {
		t.Fatalf("append event windows: %v", err)
	}
	read, err := store.EventMetricWindows(ctx, EventMetricWindowQuery{Page: PageRequest{Page: 1, PageSize: 10}})
	if err != nil {
		t.Fatalf("read event windows: %v", err)
	}
	if read.Total != 1 || read.Items[0].Processed != 2 {
		t.Fatalf("unexpected event windows: %#v", read)
	}
	read.Items[0].Processed = 99
	again, err := store.EventMetricWindows(ctx, EventMetricWindowQuery{Page: PageRequest{Page: 1, PageSize: 10}})
	if err != nil {
		t.Fatalf("read event windows again: %v", err)
	}
	if again.Items[0].Processed != 2 {
		t.Fatal("event windows should be cloned on read")
	}

	if err := store.ClearMetrics(ctx); err != nil {
		t.Fatalf("clear metrics: %v", err)
	}
	cleared, err := store.EventMetricWindows(ctx, EventMetricWindowQuery{Page: PageRequest{Page: 1, PageSize: 10}})
	if err != nil {
		t.Fatalf("read cleared windows: %v", err)
	}
	if cleared.Total != 0 {
		t.Fatalf("windows should be cleared, got %#v", cleared)
	}
	control, err := store.Control(ctx)
	if err != nil {
		t.Fatalf("control after clear metrics: %v", err)
	}
	if !control.GlobalPaused {
		t.Fatal("clear metrics must not clear control flags")
	}
	if _, found, err := store.Supervisor(ctx, "s1", now); err != nil || !found {
		t.Fatalf("clear metrics must not clear supervisor heartbeat: found=%v err=%v", found, err)
	}
}

func TestMemoryStoreQueueLengthSnapshotIsIndependentFromMetrics(t *testing.T) {
	// 需求背景：historical scenario 04 要求队列长度使用独立模型和 Store API，不能塞进事件派生的 MetricsSnapshot。
	// 本测试通过 memory store 的公开接口验证读写克隆和 clear-metrics 边界，避免后续实现误共享切片或清理范围。
	ctx := context.Background()
	now := time.Date(2026, 5, 11, 20, 0, 0, 0, time.UTC)
	store := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})
	snapshot := QueueLengthSnapshot{
		CapturedAt: now,
		Queues: []QueueLengthBucket{
			{Connection: "redis", Queue: "default", Size: 3},
			{Connection: "redis", Queue: "emails", Size: 7},
		},
	}

	if err := store.SaveQueueLengthSnapshot(ctx, snapshot); err != nil {
		t.Fatalf("save queue length snapshot: %v", err)
	}
	read, err := store.QueueLengthSnapshot(ctx)
	if err != nil {
		t.Fatalf("read queue length snapshot: %v", err)
	}
	if len(read.Queues) != 2 || read.Queues[0].Size != 3 || read.Queues[1].Size != 7 {
		t.Fatalf("unexpected queue length snapshot: %#v", read)
	}
	read.Queues[0].Size = 99
	again, err := store.QueueLengthSnapshot(ctx)
	if err != nil {
		t.Fatalf("read queue length snapshot again: %v", err)
	}
	if again.Queues[0].Size != 3 {
		t.Fatal("queue length snapshot should be cloned on read/write")
	}

	if err := store.ClearMetrics(ctx); err != nil {
		t.Fatalf("clear metrics: %v", err)
	}
	stillStored, err := store.QueueLengthSnapshot(ctx)
	if err != nil {
		t.Fatalf("read queue length snapshot after clear metrics: %v", err)
	}
	if len(stillStored.Queues) != 2 {
		t.Fatalf("clear metrics must not clear queue length snapshot: %#v", stillStored)
	}
}

func TestMemoryStoreHighValueDetailsAndDiagnostics(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore(StoreOptions{})
	now := time.Now().UTC()
	details := []HighValueJobDetail{
		{ID: "detail-old", Kind: HighValueDetailFailed, JobID: "old", OccurredAt: now.Add(-2 * time.Hour)},
		{ID: "detail-new", Kind: HighValueDetailFailed, JobID: "new", OccurredAt: now},
		{ID: "detail-slow", Kind: HighValueDetailSlowJob, JobID: "slow", OccurredAt: now.Add(time.Minute)},
	}
	if err := store.SaveHighValueDetails(ctx, details, time.Hour); err != nil {
		t.Fatalf("save high-value details: %v", err)
	}
	page, err := store.HighValueDetails(ctx, HighValueDetailQuery{Kind: HighValueDetailFailed, Page: PageRequest{Page: 1, PageSize: 10}})
	if err != nil {
		t.Fatalf("read high-value details: %v", err)
	}
	if page.Total != 1 || len(page.Items) != 1 || page.Items[0].ID != "detail-new" {
		t.Fatalf("unexpected high-value detail page: %#v", page)
	}
	page.Items[0].ID = "changed"
	detail, found, err := store.HighValueDetail(ctx, "detail-new")
	if err != nil || !found || detail.ID != "detail-new" {
		t.Fatalf("detail should be cloned and readable: found=%v detail=%#v err=%v", found, detail, err)
	}

	diagnostics := []ObservabilityDiagnostic{
		{Reason: "skip", Count: 0, ObservedAt: now},
		{Reason: MemoryDropBufferFull, Count: 2, ObservedAt: now},
	}
	if err := store.SaveObservabilityDiagnostics(ctx, diagnostics, time.Hour); err != nil {
		t.Fatalf("save diagnostics: %v", err)
	}
	diagPage, err := store.ObservabilityDiagnostics(ctx, PageRequest{Page: 1, PageSize: 10})
	if err != nil {
		t.Fatalf("read diagnostics: %v", err)
	}
	if diagPage.Total != 1 || len(diagPage.Items) != 1 || diagPage.Items[0].Reason != MemoryDropBufferFull {
		t.Fatalf("unexpected diagnostics page: %#v", diagPage)
	}
}

func TestMemoryStoreTracksOrphanProcessesByAge(t *testing.T) {
	// 需求背景：historical scenario 07 要求 Store 提供 Laravel ProcessRepository 风格的 orphan PID tracking，
	// purge 命令只维护 Horizon orphan worker 记录，不碰业务队列、failed store 或 metrics。
	ctx := context.Background()
	now := time.Date(2026, 5, 12, 11, 0, 0, 0, time.UTC)
	store := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})

	if err := store.RecordOrphanProcess(ctx, "master-1", 1001, now.Add(-10*time.Minute)); err != nil {
		t.Fatalf("record old orphan: %v", err)
	}
	if err := store.RecordOrphanProcess(ctx, "master-1", 1002, now.Add(-time.Minute)); err != nil {
		t.Fatalf("record fresh orphan: %v", err)
	}
	all, err := store.OrphanProcesses(ctx, "master-1")
	if err != nil {
		t.Fatalf("list orphan processes: %v", err)
	}
	if len(all) != 2 || all[0].PID != 1001 || all[1].PID != 1002 {
		t.Fatalf("unexpected orphan list: %#v", all)
	}
	old, err := store.OrphanProcessesOlderThan(ctx, "master-1", 5*time.Minute, now)
	if err != nil {
		t.Fatalf("list old orphan processes: %v", err)
	}
	if len(old) != 1 || old[0].PID != 1001 {
		t.Fatalf("expected only old orphan, got %#v", old)
	}
	if err := store.ForgetOrphanProcess(ctx, "master-1", 1001); err != nil {
		t.Fatalf("forget orphan: %v", err)
	}
	remaining, err := store.OrphanProcesses(ctx, "master-1")
	if err != nil {
		t.Fatalf("list remaining orphan processes: %v", err)
	}
	if len(remaining) != 1 || remaining[0].PID != 1002 {
		t.Fatalf("unexpected remaining orphan list: %#v", remaining)
	}
}

func TestMemoryStoreRejectsInvalidOrphanProcessesAndKeepsFirstSeen(t *testing.T) {
	// 测试目的：orphan tracking 只接受明确的 master 和正 PID，并且重复发现同一 PID 时保留首次发现时间。
	ctx := context.Background()
	now := time.Now().UTC().Add(-time.Hour)
	store := NewMemoryStore(StoreOptions{HeartbeatTTL: time.Minute})

	if err := store.RecordOrphanProcess(ctx, " ", 1001, now); err == nil {
		t.Fatal("expected empty master id error")
	}
	if err := store.RecordOrphanProcess(ctx, "master-1", 0, now); err == nil {
		t.Fatal("expected non-positive pid error")
	}
	if err := store.RecordOrphanProcess(ctx, " master-1 ", 1001, now); err != nil {
		t.Fatalf("record orphan: %v", err)
	}
	if err := store.RecordOrphanProcess(ctx, "master-1", 1001, now.Add(time.Minute)); err != nil {
		t.Fatalf("record duplicate orphan: %v", err)
	}
	all, err := store.OrphanProcesses(ctx, "master-1")
	if err != nil {
		t.Fatalf("list orphan processes: %v", err)
	}
	if len(all) != 1 || !all[0].FirstSeenAt.Equal(now) {
		t.Fatalf("duplicate record should keep first seen time, got %#v", all)
	}
	old, err := store.OrphanProcessesOlderThan(ctx, "master-1", time.Nanosecond, time.Time{})
	if err != nil {
		t.Fatalf("list old orphan processes with implicit now: %v", err)
	}
	if len(old) != 1 {
		t.Fatalf("implicit now should treat old record as old, got %#v", old)
	}
	if err := store.ForgetOrphanProcess(ctx, "master-1", 1001); err != nil {
		t.Fatalf("forget orphan: %v", err)
	}
	remaining, err := store.OrphanProcesses(ctx, "master-1")
	if err != nil {
		t.Fatalf("list remaining orphan processes: %v", err)
	}
	if len(remaining) != 0 {
		t.Fatalf("last orphan should remove master bucket, got %#v", remaining)
	}
}

func TestStoreStateSortingTieBreakers(t *testing.T) {
	// 逻辑说明：状态列表用于 CLI/API 展示，排序必须在 heartbeat 相同或不同的情况下保持稳定可预测。
	now := time.Now().UTC()
	supervisors := []SupervisorState{
		{Name: "beta", Host: "host-b", Environment: "prod", PID: 3, LastHeartbeatAt: now},
		{Name: "alpha", Host: "host-b", Environment: "prod", PID: 2, LastHeartbeatAt: now},
		{Name: "alpha", Host: "host-a", Environment: "prod", PID: 4, LastHeartbeatAt: now},
		{Name: "alpha", Host: "host-a", Environment: "local", PID: 5, LastHeartbeatAt: now},
		{Name: "alpha", Host: "host-a", Environment: "local", PID: 1, LastHeartbeatAt: now.Add(time.Second)},
	}
	sortSupervisorStates(supervisors)
	if got := []int{supervisors[0].PID, supervisors[1].PID, supervisors[2].PID, supervisors[3].PID, supervisors[4].PID}; !reflect.DeepEqual(got, []int{1, 5, 4, 2, 3}) {
		t.Fatalf("supervisor order = %v", got)
	}

	workers := []WorkerState{
		{ID: "worker-b", Host: "host-b", Environment: "prod", Supervisor: "s2", LastHeartbeatAt: now},
		{ID: "worker-a", Host: "host-b", Environment: "prod", Supervisor: "s2", LastHeartbeatAt: now},
		{ID: "worker-a", Host: "host-a", Environment: "prod", Supervisor: "s2", LastHeartbeatAt: now},
		{ID: "worker-a", Host: "host-a", Environment: "local", Supervisor: "s2", LastHeartbeatAt: now},
		{ID: "worker-a", Host: "host-a", Environment: "local", Supervisor: "s1", LastHeartbeatAt: now.Add(time.Second)},
	}
	sortWorkerStates(workers)
	if got := []string{workers[0].Supervisor, workers[1].Supervisor, workers[2].Environment, workers[3].Host, workers[4].ID}; !reflect.DeepEqual(got, []string{"s1", "s2", "prod", "host-b", "worker-b"}) {
		t.Fatalf("worker order = %v", got)
	}
}
