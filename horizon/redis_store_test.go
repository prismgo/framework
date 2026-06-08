package horizon

import (
	"context"
	"net"
	"strconv"
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

type redisCommandRecorder struct {
	commands [][]interface{}
}

func (r *redisCommandRecorder) DialHook(next redis.DialHook) redis.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		return next(ctx, network, addr)
	}
}

func (r *redisCommandRecorder) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		return next(ctx, cmd)
	}
}

func (r *redisCommandRecorder) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		for _, cmd := range cmds {
			args := append([]interface{}(nil), cmd.Args()...)
			r.commands = append(r.commands, args)
		}
		return next(ctx, cmds)
	}
}

func TestRedisStorePersistsHeartbeatControlAndTrimsStaleIndexes(t *testing.T) {
	// 需求背景：Laravel config contract 要求 Redis Store 使用固定 key schema、heartbeat TTL、control flags 和
	// stale/trim 语义。该测试通过 miniredis 验证完整状态读写链路，不连接真实生产 Redis。
	ctx := context.Background()
	now := time.Date(2026, 5, 11, 13, 0, 0, 0, time.UTC)
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	store := NewRedisStoreFromClient(client, StoreOptions{Prefix: ":demo_horizon:", HeartbeatTTL: time.Minute})

	if err := store.HeartbeatMaster(ctx, MasterState{
		ID:              "master-1",
		Host:            "host-1",
		PID:             10,
		Status:          MasterRunning,
		StartedAt:       now.Add(-time.Minute),
		LastHeartbeatAt: now,
		SupervisorCount: 1,
		Environment:     "local",
	}); err != nil {
		t.Fatalf("heartbeat master: %v", err)
	}
	readMaster, foundMaster, err := store.Master(ctx, "master-1", now.Add(10*time.Second))
	if err != nil || !foundMaster {
		t.Fatalf("read master: found=%v err=%v", foundMaster, err)
	}
	if readMaster.Status != MasterRunning || readMaster.SupervisorCount != 1 || readMaster.Environment != "local" {
		t.Fatalf("unexpected master state: %#v", readMaster)
	}
	if err := store.HeartbeatSupervisor(ctx, SupervisorState{
		Name:            "supervisor-default",
		Host:            "host-1",
		PID:             11,
		Status:          SupervisorRunning,
		StartedAt:       now.Add(-time.Minute),
		LastHeartbeatAt: now,
		WorkerCount:     3,
		Connection:      "redis",
		Queues:          []string{"default"},
	}); err != nil {
		t.Fatalf("heartbeat supervisor: %v", err)
	}
	if err := store.HeartbeatWorker(ctx, WorkerState{
		ID:              "worker-1",
		Supervisor:      "supervisor-default",
		Status:          WorkerIdle,
		StartedAt:       now.Add(-time.Minute),
		LastHeartbeatAt: now,
	}); err != nil {
		t.Fatalf("heartbeat worker: %v", err)
	}

	if !server.Exists("demo_horizon:supervisors") || !server.Exists("demo_horizon:supervisor:supervisor-default") {
		t.Fatal("expected fixed supervisor keys to exist")
	}
	if !server.Exists("demo_horizon:workers") || !server.Exists("demo_horizon:worker:worker-1") {
		t.Fatal("expected fixed worker keys to exist")
	}
	if !server.Exists("demo_horizon:masters") || !server.Exists("demo_horizon:master:master-1") {
		t.Fatal("expected fixed master keys to exist")
	}
	if ttl, err := client.TTL(ctx, "demo_horizon:supervisor:supervisor-default").Result(); err != nil || ttl <= 0 {
		t.Fatalf("expected supervisor entity ttl, got %v, %v", ttl, err)
	}
	if ttl, err := client.TTL(ctx, "demo_horizon:master:master-1").Result(); err != nil || ttl <= 0 {
		t.Fatalf("expected master entity ttl, got %v, %v", ttl, err)
	}

	if err := store.SetGlobalPaused(ctx, true); err != nil {
		t.Fatalf("pause globally: %v", err)
	}
	if err := store.SetSupervisorPaused(ctx, "supervisor-default", true); err != nil {
		t.Fatalf("pause supervisor: %v", err)
	}
	if err := store.RequestTerminate(ctx, now.Add(5*time.Second), false); err != nil {
		t.Fatalf("terminate: %v", err)
	}
	control, err := store.Control(ctx)
	if err != nil {
		t.Fatalf("control: %v", err)
	}
	if !control.GlobalPaused || !control.PausedSupervisors["supervisor-default"] || control.TerminateRequestedAt.IsZero() {
		t.Fatalf("unexpected control state: %#v", control)
	}
	if err := store.SetGlobalPaused(ctx, false); err != nil {
		t.Fatalf("continue globally: %v", err)
	}
	control, err = store.Control(ctx)
	if err != nil {
		t.Fatalf("control after continue: %v", err)
	}
	if control.GlobalPaused || control.TerminateRequestedAt.IsZero() {
		t.Fatalf("continue should not clear terminate request: %#v", control)
	}

	if err := store.ClearTerminateRequest(ctx); err != nil {
		t.Fatalf("clear terminate: %v", err)
	}
	snapshot, err := store.StatusSnapshot(ctx, now.Add(2*time.Minute))
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if snapshot.Status != GlobalInactive || snapshot.StaleSupervisorCount != 1 || snapshot.StaleWorkerCount != 1 {
		t.Fatalf("unexpected stale snapshot: %#v", snapshot)
	}
	if err := store.Trim(ctx, now.Add(2*time.Minute)); err != nil {
		t.Fatalf("trim: %v", err)
	}
	masters, err := store.Masters(ctx, now.Add(2*time.Minute))
	if err != nil {
		t.Fatalf("masters after trim: %v", err)
	}
	masterStillIndexed, err := client.SIsMember(ctx, "demo_horizon:masters", "master-1").Result()
	if err != nil {
		t.Fatalf("check master index: %v", err)
	}
	if len(masters) != 0 || masterStillIndexed {
		t.Fatalf("expected trim to remove stale master index, got %#v", masters)
	}
	supervisors, err := store.Supervisors(ctx, now.Add(2*time.Minute))
	if err != nil {
		t.Fatalf("supervisors after trim: %v", err)
	}
	stillIndexed, err := client.SIsMember(ctx, "demo_horizon:supervisors", "supervisor-default").Result()
	if err != nil {
		t.Fatalf("check supervisor index: %v", err)
	}
	if len(supervisors) != 0 || stillIndexed {
		t.Fatalf("expected trim to remove stale supervisor index, got %#v", supervisors)
	}
}

func TestRedisStoreTrimRemovesHeartbeatLeases(t *testing.T) {
	// 需求背景：Ctrl+C 等待型 terminate 收尾后会通过 Trim 清理旧 heartbeat。
	// Redis 租约 key 若未同步删除，会在 heartbeat 已不可见时继续阻塞下一次 horizon 启动，
	// 使错误退化为 existing_id= pid=0。
	ctx := context.Background()
	now := time.Date(2026, 5, 19, 10, 0, 0, 0, time.UTC)
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	store := NewRedisStoreFromClient(client, StoreOptions{Prefix: "demo_horizon", HeartbeatTTL: time.Minute})

	oldMaster := MasterState{
		ID:              "master-old",
		Host:            "host-1",
		PID:             10,
		Status:          MasterRunning,
		StartedAt:       now.Add(-2 * time.Minute),
		LastHeartbeatAt: now.Add(-2 * time.Minute),
		Environment:     "local",
	}
	if acquired, err := store.AcquireMasterLease(ctx, oldMaster); err != nil || !acquired {
		t.Fatalf("acquire old master lease: acquired=%v err=%v", acquired, err)
	}
	oldSupervisor := SupervisorState{
		Name:            "supervisor-default",
		Host:            "host-1",
		PID:             11,
		MasterID:        "master-old",
		Environment:     "local",
		Status:          SupervisorRunning,
		StartedAt:       now.Add(-2 * time.Minute),
		LastHeartbeatAt: now.Add(-2 * time.Minute),
	}
	if acquired, err := store.AcquireSupervisorLease(ctx, oldSupervisor); err != nil || !acquired {
		t.Fatalf("acquire old supervisor lease: acquired=%v err=%v", acquired, err)
	}

	if err := store.Trim(ctx, now); err != nil {
		t.Fatalf("trim stale heartbeats: %v", err)
	}

	newMaster := oldMaster
	newMaster.ID = "master-new"
	newMaster.PID = 20
	newMaster.StartedAt = now
	newMaster.LastHeartbeatAt = now
	if acquired, err := store.AcquireMasterLease(ctx, newMaster); err != nil || !acquired {
		t.Fatalf("expected stale master lease to be reusable after trim, acquired=%v err=%v", acquired, err)
	}
	newSupervisor := oldSupervisor
	newSupervisor.PID = 21
	newSupervisor.MasterID = "master-new"
	newSupervisor.StartedAt = now
	newSupervisor.LastHeartbeatAt = now
	if acquired, err := store.AcquireSupervisorLease(ctx, newSupervisor); err != nil || !acquired {
		t.Fatalf("expected stale supervisor lease to be reusable after trim, acquired=%v err=%v", acquired, err)
	}
}

func TestRedisStoreSupervisorLeaseKeepsIdentityAcrossHeartbeat(t *testing.T) {
	// 需求背景：supervisor lease 既承担互斥，也承担排障时的实例归属信息。
	// 同一 lease key 的 value 必须在 acquire 与 heartbeat 阶段保持同一 identity 语义。
	ctx := context.Background()
	now := time.Date(2026, 5, 30, 9, 0, 0, 0, time.UTC)
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	store := NewRedisStoreFromClient(client, StoreOptions{Prefix: "demo_horizon", HeartbeatTTL: time.Minute})
	state := SupervisorState{
		Name:            "supervisor-default",
		Host:            "host-1",
		PID:             11,
		MasterID:        "master-1",
		Environment:     "local",
		Status:          SupervisorRunning,
		StartedAt:       now,
		LastHeartbeatAt: now,
	}
	identity := supervisorInstanceID(state)
	leaseKey := store.supervisorLeaseKey(state.Name, state.Host, state.Environment)

	acquired, err := store.AcquireSupervisorLease(ctx, state)
	if err != nil || !acquired {
		t.Fatalf("acquire supervisor lease: acquired=%v err=%v", acquired, err)
	}
	leaseValue, err := client.Get(ctx, leaseKey).Result()
	if err != nil {
		t.Fatalf("read lease after acquire: %v", err)
	}
	if leaseValue != identity {
		t.Fatalf("lease value after acquire = %q, want %q", leaseValue, identity)
	}

	state.LastHeartbeatAt = now.Add(5 * time.Second)
	if err := store.HeartbeatSupervisor(ctx, state); err != nil {
		t.Fatalf("heartbeat supervisor: %v", err)
	}
	leaseValue, err = client.Get(ctx, leaseKey).Result()
	if err != nil {
		t.Fatalf("read lease after heartbeat: %v", err)
	}
	if leaseValue != identity {
		t.Fatalf("lease value after heartbeat = %q, want %q", leaseValue, identity)
	}
}

func TestRedisStoreRequestTerminateUsesRedis3CompatibleHSet(t *testing.T) {
	// 需求背景：Redis 4 之前的 HSET 只支持单个 field/value。terminate 需要写两个 control 字段，
	// 但不能发出 HSET key field value field value，否则旧 Redis 会返回 wrong number of arguments。
	ctx := context.Background()
	now := time.Date(2026, 5, 14, 10, 40, 0, 0, time.UTC)
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	recorder := &redisCommandRecorder{}
	client.AddHook(recorder)
	store := NewRedisStoreFromClient(client, StoreOptions{Prefix: "demo_horizon", HeartbeatTTL: time.Minute})

	if err := store.RequestTerminate(ctx, now, true); err != nil {
		t.Fatalf("request terminate: %v", err)
	}
	control, err := store.Control(ctx)
	if err != nil {
		t.Fatalf("control: %v", err)
	}
	if control.TerminateRequestedAt.IsZero() || !control.TerminateShouldWait {
		t.Fatalf("terminate control not persisted: %#v", control)
	}

	fields := map[string]bool{}
	for _, args := range recorder.commands {
		if len(args) == 0 || args[0] != "hset" {
			continue
		}
		if len(args) != 4 {
			t.Fatalf("terminate must use Redis 3 compatible single-field HSET, got args %#v", args)
		}
		field, _ := args[2].(string)
		fields[field] = true
	}
	if !fields["terminate_requested_at"] || !fields["terminate_should_wait"] {
		t.Fatalf("terminate should write both fields with single-field HSET commands, got %#v", recorder.commands)
	}
}

func TestRedisStoreKeepsSameNameSupervisorAndWorkerInstances(t *testing.T) {
	// 需求背景：Redis Store 是生产跨进程状态源，同名 supervisor 和同 slot worker 必须按
	// host/environment 隔离保存；旧单实体读取接口只用于兼容最新实例视图。
	ctx := context.Background()
	now := time.Date(2026, 5, 19, 11, 0, 0, 0, time.UTC)
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	store := NewRedisStoreFromClient(client, StoreOptions{Prefix: "demo_horizon", HeartbeatTTL: time.Minute})

	for _, state := range []SupervisorState{
		{Name: "fixed", Host: "host-a", Environment: "local", PID: 101, Status: SupervisorRunning, LastHeartbeatAt: now},
		{Name: "fixed", Host: "host-b", Environment: "local", PID: 202, Status: SupervisorRunning, LastHeartbeatAt: now.Add(time.Second)},
	} {
		if err := store.HeartbeatSupervisor(ctx, state); err != nil {
			t.Fatalf("heartbeat supervisor: %v", err)
		}
	}
	for _, state := range []WorkerState{
		{ID: "fixed-1", Supervisor: "fixed", Host: "host-a", Environment: "local", PID: 301, Status: WorkerIdle, LastHeartbeatAt: now},
		{ID: "fixed-1", Supervisor: "fixed", Host: "host-b", Environment: "local", PID: 302, Status: WorkerIdle, LastHeartbeatAt: now.Add(time.Second)},
	} {
		if err := store.HeartbeatWorker(ctx, state); err != nil {
			t.Fatalf("heartbeat worker: %v", err)
		}
	}

	supervisors, err := store.Supervisors(ctx, now.Add(2*time.Second))
	if err != nil {
		t.Fatalf("supervisors: %v", err)
	}
	if len(supervisors) != 2 || supervisors[0].Host != "host-b" || supervisors[1].Host != "host-a" {
		t.Fatalf("same-name supervisors should remain distinct: %#v", supervisors)
	}
	latest, ok, err := store.Supervisor(ctx, "fixed", now.Add(2*time.Second))
	if err != nil || !ok {
		t.Fatalf("latest supervisor: ok=%v err=%v", ok, err)
	}
	if latest.Host != "host-b" || latest.PID != 202 {
		t.Fatalf("Supervisor(name) should return latest heartbeat instance, got %#v", latest)
	}

	if err := store.RequestTerminate(ctx, now.Add(2*time.Second), false); err != nil {
		t.Fatalf("request terminate: %v", err)
	}
	workers, err := store.Workers(ctx, now.Add(3*time.Second))
	if err != nil {
		t.Fatalf("workers: %v", err)
	}
	if len(workers) != 2 || workers[0].Host != "host-b" || workers[1].Host != "host-a" {
		t.Fatalf("same worker slot should remain distinct: %#v", workers)
	}
	for _, worker := range workers {
		if worker.Status != WorkerTerminating {
			t.Fatalf("worker should derive terminating status, got %#v", worker)
		}
	}
}

func TestRedisStoreMetricsSnapshotUsesFixedKeysAndClearBoundary(t *testing.T) {
	// 需求背景：historical scenario 03 固定 Redis metrics key schema，后续 queue length、purge 和 UI/API
	// 都应复用该边界；clear metrics 只能删除 metrics keys，不能碰 heartbeat/control keys。
	ctx := context.Background()
	now := time.Date(2026, 5, 11, 19, 30, 0, 0, time.UTC)
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	store := NewRedisStoreFromClient(client, StoreOptions{Prefix: ":demo_horizon:", HeartbeatTTL: time.Minute})
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
		Failed:      1,
		Quality:     EventMetricQualityExact,
	}
	if err := store.AppendEventMetricWindows(ctx, []EventMetricWindow{eventWindow}, 24*time.Hour); err != nil {
		t.Fatalf("append event windows: %v", err)
	}
	if !server.Exists("demo_horizon:event_metrics_windows") {
		t.Log("event_metrics_windows key not yet created (lazy)")
	}
	read, err := store.EventMetricWindows(ctx, EventMetricWindowQuery{Page: PageRequest{Page: 1, PageSize: 10}})
	if err != nil {
		t.Fatalf("read event windows: %v", err)
	}
	if read.Total != 1 || read.Items[0].Failed != 1 {
		t.Fatalf("unexpected event windows: %#v", read)
	}
	if err := store.ClearMetrics(ctx); err != nil {
		t.Fatalf("clear metrics: %v", err)
	}
	// event_metrics windows 键已随 ClearMetrics 清理
	if !server.Exists("demo_horizon:supervisor:s1") || !server.Exists("demo_horizon:control") {
		t.Fatal("clear metrics must not delete heartbeat or control keys")
	}
}

func TestRedisStoreHighValueDetailsAndDiagnosticsUseIndependentKeys(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	store := NewRedisStoreFromClient(client, StoreOptions{Prefix: "demo_horizon", HeartbeatTTL: time.Minute})

	if err := store.SaveHighValueDetails(ctx, []HighValueJobDetail{
		{ID: "failed-1", Kind: HighValueDetailFailed, Connection: "redis", Queue: "default", JobID: "job-1", OccurredAt: now},
	}, time.Hour); err != nil {
		t.Fatalf("save high-value details: %v", err)
	}
	if server.Exists("demo_horizon:metrics:recent") {
		t.Fatal("high-value details must not use legacy recent metrics key")
	}
	if !server.Exists("demo_horizon:high_value_details") || !server.Exists("demo_horizon:high_value_detail:failed-1") {
		t.Fatal("expected high-value detail keys to exist")
	}
	page, err := store.HighValueDetails(ctx, HighValueDetailQuery{Kind: HighValueDetailFailed, Page: PageRequest{Page: 1, PageSize: 10}})
	if err != nil {
		t.Fatalf("read high-value details: %v", err)
	}
	if page.Total != 1 || len(page.Items) != 1 || page.Items[0].ID != "failed-1" {
		t.Fatalf("unexpected high-value detail page: %#v", page)
	}

	if err := store.SaveObservabilityDiagnostics(ctx, []ObservabilityDiagnostic{
		{Reason: MemoryDropBufferFull, Count: 3, ObservedAt: now},
	}, time.Hour); err != nil {
		t.Fatalf("save diagnostics: %v", err)
	}
	diagPage, err := store.ObservabilityDiagnostics(ctx, PageRequest{Page: 1, PageSize: 10})
	if err != nil {
		t.Fatalf("read diagnostics: %v", err)
	}
	if diagPage.Total != 1 || len(diagPage.Items) != 1 || diagPage.Items[0].Count != 3 {
		t.Fatalf("unexpected diagnostics page: %#v", diagPage)
	}
}

func TestRedisStoreAppendsEventMetricWindows(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Minute)
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	store := NewRedisStoreFromClient(client, StoreOptions{Prefix: "demo_horizon", HeartbeatTTL: time.Minute})

	windows := []EventMetricWindow{
		{
			WindowStart:         now.Add(-time.Minute),
			WindowEnd:           now,
			FlushAt:             now,
			SourcePrefix:        "demo_horizon",
			Connection:          "redis",
			Queue:               "default",
			JobName:             "MailJob",
			Processed:           3,
			SampleCount:         3,
			EffectiveSampleRate: 1,
			EstimatedTotal:      3,
			Quality:             EventMetricQualityExact,
		},
		{
			WindowStart:         now,
			WindowEnd:           now.Add(time.Minute),
			FlushAt:             now.Add(time.Second),
			Connection:          "redis",
			Queue:               "default",
			JobName:             "MailJob",
			Failed:              1,
			SampleCount:         1,
			EffectiveSampleRate: 0.5,
			EstimatedTotal:      2,
			Estimated:           true,
			Quality:             EventMetricQualityEstimated,
		},
	}
	if err := store.AppendEventMetricWindows(ctx, windows, time.Hour); err != nil {
		t.Fatalf("append event metric windows: %v", err)
	}
	if !server.Exists("demo_horizon:event_metrics:windows") {
		t.Fatal("expected event_metrics window index key")
	}
	if server.Exists("demo_horizon:metrics:recent") {
		t.Fatal("event_metrics windows must not use legacy recent metrics key")
	}
	page, err := store.EventMetricWindows(ctx, EventMetricWindowQuery{Page: PageRequest{Page: 1, PageSize: 10}})
	if err != nil {
		t.Fatalf("read event metric windows: %v", err)
	}
	if page.Total != 2 || len(page.Items) != 2 {
		t.Fatalf("windows should append, got %#v", page)
	}
	if !page.Items[0].WindowStart.Equal(now) || page.Items[0].EstimatedTotal != 2 {
		t.Fatalf("latest window metadata not preserved: %#v", page.Items[0])
	}
	if page.Items[1].Processed != 3 || page.Items[1].Quality != EventMetricQualityExact {
		t.Fatalf("older window corrupted: %#v", page.Items[1])
	}
}

func TestRedisStoreEventMetricWindowsFiltersAcrossFullRetainedSet(t *testing.T) {
	// 需求背景：Dashboard 来源下钻依赖 Total 和聚合都基于完整保留窗口；如果 Redis 只读第一页候选，
	// 较旧但匹配的窗口会被新近不匹配窗口遮住。
	ctx := context.Background()
	now := time.Date(2026, 5, 19, 12, 0, 0, 0, time.UTC)
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	store := NewRedisStoreFromClient(client, StoreOptions{Prefix: "demo_horizon", HeartbeatTTL: time.Minute})

	windows := make([]EventMetricWindow, 0, maxPageSize+1)
	for i := 0; i < maxPageSize; i++ {
		start := now.Add(time.Duration(i) * time.Minute)
		windows = append(windows, EventMetricWindow{
			WindowStart: start,
			WindowEnd:   start.Add(time.Minute),
			FlushAt:     start,
			SourceHost:  "newer-host",
			Connection:  "redis",
			Queue:       "other",
			Processed:   1,
			SampleCount: 1,
			Quality:     EventMetricQualityExact,
		})
	}
	oldStart := now.Add(-time.Hour)
	windows = append(windows, EventMetricWindow{
		WindowStart: oldStart,
		WindowEnd:   oldStart.Add(time.Minute),
		FlushAt:     oldStart,
		SourceHost:  "target-host",
		Connection:  "redis",
		Queue:       "default",
		Processed:   7,
		SampleCount: 7,
		Quality:     EventMetricQualityExact,
	})
	if err := store.AppendEventMetricWindows(ctx, windows, 24*time.Hour); err != nil {
		t.Fatalf("append event metric windows: %v", err)
	}

	page, err := store.EventMetricWindows(ctx, EventMetricWindowQuery{
		SourceHost: "target-host",
		Connection: "redis",
		Queue:      "default",
		Page:       PageRequest{Page: 1, PageSize: 10},
	})
	if err != nil {
		t.Fatalf("query event metric windows: %v", err)
	}
	if page.Total != 1 || len(page.Items) != 1 || page.Items[0].Processed != 7 {
		t.Fatalf("filtered query should scan beyond first maxPageSize candidates, got %#v", page)
	}
}

func TestRedisStoreHighValueDetailsFiltersAcrossFullRetainedSet(t *testing.T) {
	// 需求背景：高价值任务明细的 kind/time 查询需要完整保留集合语义；否则用户按时间回查旧失败任务时
	// 会因为第一页全是不匹配慢任务而得到错误的空结果。
	ctx := context.Background()
	// 需求背景：SaveHighValueDetails 会按真实当前时间执行 retention 清理；这里使用相对时间，
	// 避免固定日期随着日历推进后被 24h 保留窗口误删，导致过滤语义测试变成时间敏感。
	now := time.Now().UTC()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	store := NewRedisStoreFromClient(client, StoreOptions{Prefix: "demo_horizon", HeartbeatTTL: time.Minute})

	details := make([]HighValueJobDetail, 0, maxPageSize+1)
	for i := 0; i < maxPageSize; i++ {
		details = append(details, HighValueJobDetail{
			ID:         "newer-" + strconv.Itoa(i),
			Kind:       HighValueDetailSlowJob,
			Connection: "redis",
			Queue:      "other",
			OccurredAt: now.Add(time.Duration(i) * time.Minute),
		})
	}
	targetAt := now.Add(-time.Hour)
	details = append(details, HighValueJobDetail{
		ID:         "target",
		Kind:       HighValueDetailFailed,
		Connection: "redis",
		Queue:      "default",
		OccurredAt: targetAt,
	})
	if err := store.SaveHighValueDetails(ctx, details, 24*time.Hour); err != nil {
		t.Fatalf("save high-value details: %v", err)
	}

	page, err := store.HighValueDetails(ctx, HighValueDetailQuery{
		Kind:         HighValueDetailFailed,
		OccurredFrom: targetAt.Add(-time.Minute),
		OccurredTo:   targetAt.Add(time.Minute),
		Page:         PageRequest{Page: 1, PageSize: 10},
	})
	if err != nil {
		t.Fatalf("query high-value details: %v", err)
	}
	if page.Total != 1 || len(page.Items) != 1 || page.Items[0].ID != "target" {
		t.Fatalf("filtered query should scan beyond first maxPageSize candidates, got %#v", page)
	}
}

func TestRedisStoreEventMetricWindowsEmptyStore(t *testing.T) {
	// 测试目的：Redis event_metrics windows 不存在时应返回空结果。
	ctx := context.Background()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	store := NewRedisStoreFromClient(client, StoreOptions{Prefix: "demo_horizon", HeartbeatTTL: time.Minute})

	empty, err := store.EventMetricWindows(ctx, EventMetricWindowQuery{Page: PageRequest{Page: 1, PageSize: 10}})
	if err != nil {
		t.Fatalf("read empty event windows: %v", err)
	}
	if empty.Total != 0 || len(empty.Items) != 0 {
		t.Fatalf("empty store should return empty event windows, got %#v", empty)
	}
}

func TestRedisStoreQueueLengthSnapshotUsesIndependentFixedKey(t *testing.T) {
	// 需求背景：historical scenario 04 固定 Redis 队列长度 key 为 {prefix}:metrics:queue_lengths，
	// 并要求不能写入事件派生 metrics keys。
	ctx := context.Background()
	now := time.Date(2026, 5, 11, 20, 10, 0, 0, time.UTC)
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	store := NewRedisStoreFromClient(client, StoreOptions{Prefix: ":demo_horizon:", HeartbeatTTL: time.Minute})

	if err := store.SaveQueueLengthSnapshot(ctx, QueueLengthSnapshot{
		CapturedAt: now,
		Queues: []QueueLengthBucket{
			{Connection: "redis", Queue: "default", Size: 5},
		},
	}); err != nil {
		t.Fatalf("save queue length snapshot: %v", err)
	}
	if !server.Exists("demo_horizon:metrics:queue_lengths") {
		t.Fatal("expected queue length snapshot key to exist")
	}
	for _, key := range []string{"demo_horizon:metrics:snapshot", "demo_horizon:metrics:counters"} {
		if server.Exists(key) {
			t.Fatalf("queue length snapshot must not write metrics key %s", key)
		}
	}
	if server.Exists("demo_horizon:metrics:recent") {
		t.Fatal("queue length snapshot must not write legacy recent metrics key")
	}
	read, err := store.QueueLengthSnapshot(ctx)
	if err != nil {
		t.Fatalf("read queue length snapshot: %v", err)
	}
	if len(read.Queues) != 1 || read.Queues[0].Size != 5 {
		t.Fatalf("unexpected queue length snapshot: %#v", read)
	}
}

func TestRedisStoreBatchSummariesSupportListSearchDetailAndCorruptSkip(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 5, 13, 13, 0, 0, 0, time.UTC)
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	store := NewRedisStoreFromClient(client, StoreOptions{Prefix: "demo_horizon", HeartbeatTTL: time.Minute})

	if err := store.SaveBatchSummary(ctx, BatchSummary{ID: "batch-1", Name: "Daily reports", Status: BatchStatusFinished, Total: 3, Processed: 3, Failed: 1, CreatedAt: now}); err != nil {
		t.Fatalf("save first batch: %v", err)
	}
	if err := store.SaveBatchSummary(ctx, BatchSummary{ID: "batch-2", Name: "Weekly exports", Status: BatchStatusRunning, Total: 2, Pending: 1, Processed: 1, CreatedAt: now.Add(time.Hour)}); err != nil {
		t.Fatalf("save second batch: %v", err)
	}
	if !server.Exists("demo_horizon:batches") || !server.Exists("demo_horizon:batch:batch-1") {
		t.Fatal("expected batch summary keys to exist")
	}
	if err := client.Set(ctx, "demo_horizon:batch:bad", "{bad-json", 0).Err(); err != nil {
		t.Fatalf("seed corrupt batch: %v", err)
	}
	if err := client.ZAdd(ctx, "demo_horizon:batches", redis.Z{Score: float64(now.Add(2 * time.Hour).UnixNano()), Member: "bad"}).Err(); err != nil {
		t.Fatalf("index corrupt batch: %v", err)
	}

	items, err := store.Batches(ctx, "weekly")
	if err != nil {
		t.Fatalf("search batches: %v", err)
	}
	if len(items) != 1 || items[0].ID != "batch-2" {
		t.Fatalf("searched batches = %#v", items)
	}
	detail, ok, err := store.Batch(ctx, "batch-1")
	if err != nil || !ok {
		t.Fatalf("batch detail ok=%v err=%v", ok, err)
	}
	if detail.Status != BatchStatusFinished || detail.Failed != 1 {
		t.Fatalf("batch detail = %#v", detail)
	}
	all, err := store.Batches(ctx, "")
	if err != nil {
		t.Fatalf("list all batches: %v", err)
	}
	if len(all) != 2 || all[0].ID != "batch-2" || all[1].ID != "batch-1" {
		t.Fatalf("all batches = %#v", all)
	}
}

func TestRedisStoreBatchPageUsesBoundedSearchWindow(t *testing.T) {
	// 测试目的：分页批次 API 的 query/search 只承诺在当前有界窗口内匹配，不能为了搜索
	// 一个旧批次而 ZRevRange 0 -1 后逐条读取全量历史。
	ctx := context.Background()
	now := time.Date(2026, 5, 14, 11, 0, 0, 0, time.UTC)
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	store := NewRedisStoreFromClient(client, StoreOptions{Prefix: "demo_horizon", HeartbeatTTL: time.Minute})

	for _, summary := range []BatchSummary{
		{ID: "batch-old", Name: "Daily reports", CreatedAt: now},
		{ID: "batch-mid", Name: "Weekly exports", CreatedAt: now.Add(time.Minute)},
		{ID: "batch-new", Name: "Monthly payroll", CreatedAt: now.Add(2 * time.Minute)},
	} {
		if err := store.SaveBatchSummary(ctx, summary); err != nil {
			t.Fatalf("save batch %s: %v", summary.ID, err)
		}
	}

	firstPage, err := store.BatchesPage(ctx, "", PageRequest{Page: 1, PageSize: 2})
	if err != nil {
		t.Fatalf("first page batches: %v", err)
	}
	if firstPage.Total != 3 || len(firstPage.Items) != 2 || firstPage.Items[0].ID != "batch-new" || firstPage.Items[1].ID != "batch-mid" {
		t.Fatalf("first page = %#v", firstPage)
	}

	boundedSearch, err := store.BatchesPage(ctx, "daily", PageRequest{Page: 1, PageSize: 1})
	if err != nil {
		t.Fatalf("bounded search batches: %v", err)
	}
	if boundedSearch.Total != 0 || len(boundedSearch.Items) != 0 {
		t.Fatalf("daily should not match outside current page window: %#v", boundedSearch)
	}

	secondWindow, err := store.BatchesPage(ctx, "weekly", PageRequest{Page: 2, PageSize: 1})
	if err != nil {
		t.Fatalf("second window search batches: %v", err)
	}
	if secondWindow.Total != 1 || len(secondWindow.Items) != 1 || secondWindow.Items[0].ID != "batch-mid" {
		t.Fatalf("weekly should match inside requested page window: %#v", secondWindow)
	}
}

func TestRedisStoreTracksOrphanProcessesWithSortedSet(t *testing.T) {
	// 逻辑说明：Redis Store 使用 master 维度 sorted set 保存 pid -> first_seen，
	// 既能按 Laravel ProcessRepository 风格列出 orphan，也能按 age 找到需要二次终止并遗忘的记录。
	ctx := context.Background()
	now := time.Date(2026, 5, 12, 11, 10, 0, 0, time.UTC)
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	store := NewRedisStoreFromClient(client, StoreOptions{Prefix: ":demo_horizon:", HeartbeatTTL: time.Minute})

	if err := store.RecordOrphanProcess(ctx, "master-1", 2001, now.Add(-10*time.Minute)); err != nil {
		t.Fatalf("record old orphan: %v", err)
	}
	if err := store.RecordOrphanProcess(ctx, "master-1", 2002, now.Add(-time.Minute)); err != nil {
		t.Fatalf("record fresh orphan: %v", err)
	}
	if !server.Exists("demo_horizon:processes:orphans:master-1") {
		t.Fatal("expected orphan sorted set key to exist")
	}
	old, err := store.OrphanProcessesOlderThan(ctx, "master-1", 5*time.Minute, now)
	if err != nil {
		t.Fatalf("old orphan processes: %v", err)
	}
	if len(old) != 1 || old[0].PID != 2001 {
		t.Fatalf("expected old orphan pid 2001, got %#v", old)
	}
	if err := store.ForgetOrphanProcess(ctx, "master-1", 2001); err != nil {
		t.Fatalf("forget orphan: %v", err)
	}
	all, err := store.OrphanProcesses(ctx, "master-1")
	if err != nil {
		t.Fatalf("list orphan processes: %v", err)
	}
	if len(all) != 1 || all[0].PID != 2002 {
		t.Fatalf("unexpected orphan list after forget: %#v", all)
	}
}

func TestRedisStoreRejectsInvalidOrphansAndFiltersBadMembers(t *testing.T) {
	// 测试目的：Redis ProcessRepository 风格 orphan tracking 应显式拒绝坏输入，并忽略 Redis 中残留的坏 member。
	ctx := context.Background()
	now := time.Date(2026, 5, 12, 11, 40, 0, 0, time.UTC)
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	store := NewRedisStoreFromClient(client, StoreOptions{Prefix: "demo_horizon", HeartbeatTTL: time.Minute})

	if err := store.RecordOrphanProcess(ctx, "", 2001, now); err == nil {
		t.Fatal("expected empty master id error")
	}
	if err := store.RecordOrphanProcess(ctx, "master-1", -1, now); err == nil {
		t.Fatal("expected non-positive pid error")
	}
	if err := store.RecordOrphanProcess(ctx, "master-1", 2001, time.Time{}); err != nil {
		t.Fatalf("record orphan with implicit first seen: %v", err)
	}
	if err := client.ZAdd(ctx, "demo_horizon:processes:orphans:master-1",
		redis.Z{Score: float64(now.UnixNano()), Member: "bad"},
		redis.Z{Score: float64(now.UnixNano()), Member: "-2"},
	).Err(); err != nil {
		t.Fatalf("seed bad orphan members: %v", err)
	}
	all, err := store.OrphanProcesses(ctx, "master-1")
	if err != nil {
		t.Fatalf("list orphan processes: %v", err)
	}
	if len(all) != 1 || all[0].PID != 2001 || all[0].FirstSeenAt.IsZero() {
		t.Fatalf("bad members should be filtered, got %#v", all)
	}
	empty, err := store.OrphanProcesses(ctx, " ")
	if err != nil {
		t.Fatalf("empty master orphan list: %v", err)
	}
	if len(empty) != 0 {
		t.Fatalf("empty master should return no orphan records, got %#v", empty)
	}
}
