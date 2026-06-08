package horizon

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	queuecontract "github.com/prismgo/framework/contracts/queue"
	"github.com/prismgo/framework/event"
	goexception "github.com/prismgo/framework/exception"
	horizoncmd "github.com/prismgo/framework/horizon/cmd"
	goprocess "github.com/prismgo/framework/process"
	"github.com/prismgo/framework/queue"
)

// runtimeCommandAdapter 把核心 Manager/Store 适配为 cmd 包可使用的窄 Runtime 接口。
//
// 设计思路：所有命令文件放在 horizon/cmd 包内，cmd 包不能反向依赖父包；因此父包负责把核心模型投影为命令 DTO，
// 并把 Store、QueueManager、collector/flusher 的组合操作封装成命令语义，避免 Go import cycle。
type runtimeCommandAdapter struct {
	manager *Manager
	store   Store
}

// newRuntimeLoader 为 cmd 包命令创建运行时依赖解析器。
//
// 参数说明：load 负责构造或读取 Manager；返回的 loader 会在命令执行时按上下文解析 Store，确保错误边界和旧实现一致。
func newRuntimeLoader(load func() (*Manager, error)) horizoncmd.RuntimeLoader {
	return func(ctx context.Context) (horizoncmd.Runtime, error) {
		if load == nil {
			return nil, ErrStoreNotConfigured
		}
		manager, err := load()
		if err != nil {
			return nil, err
		}
		store, err := manager.ResolveStore(ctx)
		if err != nil {
			return nil, err
		}
		return &runtimeCommandAdapter{manager: manager, store: store}, nil
	}
}

func (r *runtimeCommandAdapter) UsesMemoryStore() bool {
	return r != nil && strings.EqualFold(r.manager.Config().Store, "memory")
}

func (r *runtimeCommandAdapter) StatusSnapshot(ctx context.Context, now time.Time) (horizoncmd.StatusSnapshot, error) {
	snapshot, err := r.store.StatusSnapshot(ctx, now)
	if err != nil {
		return horizoncmd.StatusSnapshot{}, err
	}
	return horizoncmd.StatusSnapshot{
		Status:               snapshot.Status,
		GlobalPaused:         snapshot.GlobalPaused,
		TerminateRequested:   snapshot.TerminateRequested,
		SupervisorCount:      snapshot.SupervisorCount,
		WorkerCount:          snapshot.WorkerCount,
		StaleSupervisorCount: snapshot.StaleSupervisorCount,
		StaleWorkerCount:     snapshot.StaleWorkerCount,
	}, nil
}

func (r *runtimeCommandAdapter) Masters(ctx context.Context, now time.Time) ([]horizoncmd.MasterState, error) {
	masters, err := r.store.Masters(ctx, now)
	if err != nil {
		return nil, err
	}
	out := make([]horizoncmd.MasterState, 0, len(masters))
	for _, master := range masters {
		out = append(out, horizoncmd.MasterState{
			ID:              master.ID,
			Host:            master.Host,
			PID:             master.PID,
			Status:          master.Status,
			StartedAt:       master.StartedAt,
			LastHeartbeatAt: master.LastHeartbeatAt,
			SupervisorCount: master.SupervisorCount,
			Environment:     master.Environment,
		})
	}
	return out, nil
}

func (r *runtimeCommandAdapter) Supervisors(ctx context.Context, now time.Time) ([]horizoncmd.SupervisorState, error) {
	supervisors, err := r.store.Supervisors(ctx, now)
	if err != nil {
		return nil, err
	}
	out := make([]horizoncmd.SupervisorState, 0, len(supervisors))
	for _, supervisor := range supervisors {
		out = append(out, toCommandSupervisorState(supervisor))
	}
	return out, nil
}

func (r *runtimeCommandAdapter) Supervisor(ctx context.Context, name string, now time.Time) (horizoncmd.SupervisorState, bool, error) {
	supervisor, ok, err := r.store.Supervisor(ctx, name, now)
	if err != nil || !ok {
		return horizoncmd.SupervisorState{}, ok, err
	}
	return toCommandSupervisorState(supervisor), true, nil
}

func (r *runtimeCommandAdapter) Workers(ctx context.Context, now time.Time) ([]horizoncmd.WorkerState, error) {
	workers, err := r.store.Workers(ctx, now)
	if err != nil {
		return nil, err
	}
	out := make([]horizoncmd.WorkerState, 0, len(workers))
	for _, worker := range workers {
		out = append(out, horizoncmd.WorkerState{
			ID:              worker.ID,
			Supervisor:      worker.Supervisor,
			Host:            worker.Host,
			PID:             worker.PID,
			Status:          worker.Status,
			LastHeartbeatAt: worker.LastHeartbeatAt,
		})
	}
	return out, nil
}

func (r *runtimeCommandAdapter) SetGlobalPaused(ctx context.Context, paused bool) error {
	if err := r.store.SetGlobalPaused(ctx, paused); err != nil {
		return err
	}
	return r.notifyControlTargets(ctx, "")
}

func (r *runtimeCommandAdapter) SetSupervisorPaused(ctx context.Context, name string, paused bool) error {
	if err := r.store.SetSupervisorPaused(ctx, name, paused); err != nil {
		return err
	}
	return r.notifyControlTargets(ctx, strings.TrimSpace(name))
}

func (r *runtimeCommandAdapter) RequestTerminate(ctx context.Context, at time.Time, wait bool) error {
	if err := r.store.RequestTerminate(ctx, at, wait); err != nil {
		return err
	}
	queueManager := r.manager.QueueManager()
	if queueManager == nil {
		return fmt.Errorf("horizon: queue manager is not configured")
	}
	if err := queueManager.RequestRestart(ctx); err != nil {
		return err
	}
	if err := r.notifyControlTargets(ctx, ""); err != nil {
		return err
	}
	return nil
}

func (r *runtimeCommandAdapter) MaxWorkerTimeout(environment string) (int, error) {
	cfg := r.manager.Config()
	environment = strings.TrimSpace(environment)
	if environment == "" {
		environment = "production"
	}
	if environment != "" && cfg.Environment != "" && !strings.EqualFold(environment, cfg.Environment) && len(cfg.Supervisors) > 0 {
		return 0, fmt.Errorf("horizon: timeout can only query loaded environment %q, got %q", cfg.Environment, environment)
	}
	maxTimeout := 0
	for _, supervisor := range cfg.Supervisors {
		if supervisor.Timeout > maxTimeout {
			maxTimeout = supervisor.Timeout
		}
	}
	if maxTimeout <= 0 {
		return 60, nil
	}
	return maxTimeout, nil
}

// notifyControlTargets 唤醒 fresh master/supervisor 进程，让它们尽快重读 Store control flag。
//
// 需求背景：Store 是 pause/continue/terminate 的事实源；进程通知只是降低响应延迟。
// 参数说明：supervisorName 为空时通知所有 fresh master/supervisor；非空时通知 master 和该 supervisor。
// 逻辑说明：该函数从不读取或通知 worker PID，避免把 control signal 变成 job 取消机制。
func (r *runtimeCommandAdapter) notifyControlTargets(ctx context.Context, supervisorName string) error {
	notifier := r.manager.ControlNotifier()
	if notifier == nil {
		return nil
	}
	now := time.Now().UTC()
	targets := make([]ControlTarget, 0)
	masters, err := r.store.Masters(ctx, now)
	if err != nil {
		return err
	}
	for _, master := range masters {
		if master.Status != MasterStale && master.PID > 0 {
			targets = append(targets, ControlTarget{Type: "master", ID: master.ID, PID: master.PID})
		}
	}
	supervisors, err := r.store.Supervisors(ctx, now)
	if err != nil {
		return err
	}
	for _, supervisor := range supervisors {
		if supervisor.Status == SupervisorStale || supervisor.PID <= 0 {
			continue
		}
		if supervisorName != "" && supervisor.Name != supervisorName {
			continue
		}
		targets = append(targets, ControlTarget{Type: "supervisor", Name: supervisor.Name, PID: supervisor.PID})
	}
	if len(targets) == 0 {
		return nil
	}
	if err := notifier.Notify(ctx, targets); err != nil {
		return fmt.Errorf("horizon: notify control targets failed: %w", err)
	}
	return nil
}

func (r *runtimeCommandAdapter) Snapshot(ctx context.Context, now time.Time) (horizoncmd.SnapshotSummary, error) {
	if now.IsZero() {
		now = time.Now()
	}
	if r == nil || r.manager == nil {
		return horizoncmd.SnapshotSummary{}, fmt.Errorf("horizon: runtime manager is not configured")
	}
	if r.store == nil {
		return horizoncmd.SnapshotSummary{}, ErrStoreNotConfigured
	}
	obs := normalizeObservabilityConfig(r.manager.Config().Observability)
	summary := horizoncmd.SnapshotSummary{CapturedAt: now}
	if obs.Enabled(ObservabilityQueueLengths) {
		queueLengths, err := r.captureQueueLengthSnapshot(ctx, now)
		if err != nil {
			return horizoncmd.SnapshotSummary{}, err
		}
		if err := r.store.SaveQueueLengthSnapshot(ctx, queueLengths); err != nil {
			return horizoncmd.SnapshotSummary{}, err
		}
		summary.QueueLengthStatus = horizoncmd.SnapshotStatusEnabled
		summary.QueueLengthCount = len(queueLengths.Queues)
	} else {
		summary.QueueLengthStatus = horizoncmd.SnapshotStatusSkipped
	}

	summary.MetricsStatus = snapshotStatus(obs.Enabled(ObservabilityEventMetrics))
	summary.WaitsStatus = snapshotStatus(obs.Enabled(ObservabilityWaits))
	summary.BatchSummariesStatus = snapshotStatus(obs.Enabled(ObservabilityBatchSummaries))

	// horizon:snapshot 与后台周期 flush 使用同一条 append-only writer，不能直接 drain collector 后写旧 read model。
	//
	// 兼容边界：queue length 仍由命令同步采样，因为它来自 queue backend 而不是 observability
	// collector；event_metrics、high-value detail、diagnostics、batch summary 和 metrics/history
	// read model 则全部交给 flusher。运行中 flusher 不存在时创建临时 flusher，但仍使用同一套
	// FlushSnapshotOnDemand writer 逻辑，避免恢复旧的“命令直接 FlushSnapshot 后手写 Store”的路径。
	flush := r.manager.Flusher()
	if flush == nil {
		coll := r.manager.Collector()
		if coll == nil {
			return horizoncmd.SnapshotSummary{}, fmt.Errorf("horizon: collector is not configured")
		}
		flush = newFlusher(obs, r.store, coll, r.manager.Config().Waits)
		flush.now = func() time.Time { return now }
	}
	flushSummary, err := flush.FlushSnapshotOnDemand(ctx)
	if err != nil {
		return horizoncmd.SnapshotSummary{}, err
	}
	if flushSummary != nil {
		summary.CapturedAt = flushSummary.CapturedAt
		summary.BucketCount = flushSummary.IncrementCount
		summary.FlushStatus = flushSummary.SchedulingStatus
		summary.FlushWindowCount = flushSummary.WindowCount
		summary.FlushDetailCount = flushSummary.DetailCount
		summary.FlushDiagnosticCount = flushSummary.DiagnosticCount
		summary.FlushBatchSummaryCount = flushSummary.BatchSummaryCount
		summary.FlushDropCount = flushSummary.DropCount
		summary.FlushQuality = flushSummary.Quality
		summary.FlushDegraded = flushSummary.Degraded
		summary.FlushError = flushSummary.Error
	}
	// 从 EventMetricWindow 读取聚合数据，替代已移除的 MetricsSnapshot。
	windows, err := r.store.EventMetricWindows(ctx, EventMetricWindowQuery{Page: PageRequest{Page: 1, PageSize: maxPageSize}})
	if err == nil && windows.Total > 0 {
		summary.BucketCount = len(aggregateMetricsBuckets(windows.Items))
		totals := aggregateMetricsTotals(windows.Items)
		summary.Totals = horizoncmd.MetricsCounters{
			Processed:       totals.Processed,
			Failed:          totals.Failed,
			Released:        totals.Released,
			PoisonEnvelopes: totals.PoisonEnvelopes,
		}
		if obs.Enabled(ObservabilityWaits) {
			r.dispatchLongWaitEventsFromCollector(ctx)
		}
	} else {
		summary.Totals = horizoncmd.MetricsCounters{}
	}
	return summary, nil
}

func snapshotStatus(enabled bool) string {
	if enabled {
		return horizoncmd.SnapshotStatusEnabled
	}
	return horizoncmd.SnapshotStatusSkipped
}

// buildRuntimeMetricsFromCollector 从 collector 读取当前聚合数据，构建轻量 MetricsSnapshot 供 autoscale 使用。
//
// 设计思路：supervisorWorkloads 每 tick 调用一次，使用 SnapshotPeek（不重置计数器）避免干扰 flusher 的定期 flush。
// 只构建 RuntimeForQueue 所需的 Buckets 数据，不填充 QueueWaits/Batches 等低频快照字段。
func (r *runtimeCommandAdapter) buildRuntimeMetricsFromCollector() MetricsSnapshot {
	snapshot := MetricsSnapshot{}
	coll := r.manager.Collector()
	if coll == nil {
		return snapshot
	}

	flushData := coll.SnapshotPeek(time.Now())
	if flushData == nil || len(flushData.windows) == 0 {
		return snapshot
	}

	// 将 event_metrics 窗口聚合为 connection+queue 维度的 bucket
	bucketMap := make(map[string]*MetricsBucketSnapshot)
	for _, w := range flushData.windows {
		key := w.connection + ":" + w.queue
		bucket, ok := bucketMap[key]
		if !ok {
			bucket = &MetricsBucketSnapshot{
				Connection: w.connection,
				Queue:      w.queue,
			}
			bucketMap[key] = bucket
		}
		bucket.ProcessedRuntimeTotalMS += w.runtimeMS
		bucket.ProcessedCount += w.samples
	}
	for _, bucket := range bucketMap {
		snapshot.Buckets = append(snapshot.Buckets, *bucket)
	}
	return snapshot
}

type horizonEventDispatcher interface {
	Dispatch(context.Context, event.Event)
}

// dispatchLongWaitEventsFromCollector 从 collector 当前 queued 状态计算长等待事件并派发。
//
// 逻辑说明：替代已移除的 dispatchLongWaitEvents；直接从 collector ComputeWaits 获取等待数据。
func (r *runtimeCommandAdapter) dispatchLongWaitEventsFromCollector(ctx context.Context) {
	dispatcher, ok := r.manager.EventDispatcher().(horizonEventDispatcher)
	if !ok || dispatcher == nil {
		return
	}
	coll := r.manager.Collector()
	if coll == nil {
		return
	}
	waits := coll.ComputeWaits(r.manager.Config().Waits, time.Now())
	for _, wait := range waits {
		if wait.Status != QueueWaitKnown || !wait.LongWait {
			continue
		}
		dispatcher.Dispatch(ctx, LongWaitEvent{
			Connection: wait.Connection,
			Queue:      wait.Queue,
			Threshold:  wait.Threshold,
			WaitMS:     wait.WaitMS,
			SampledAt:  wait.SampledAt,
		})
	}
}

func queueWaitsByKey(items []QueueWaitSnapshot) map[string]QueueWaitSnapshot {
	out := make(map[string]QueueWaitSnapshot, len(items))
	for _, item := range items {
		out[item.Key] = item
	}
	return out
}

func averageRuntimeMS(total int64, count int64) int64 {
	if total <= 0 || count <= 0 {
		return 0
	}
	return total / count
}

func (r *runtimeCommandAdapter) ClearMetrics(ctx context.Context) error {
	if err := r.store.ClearMetrics(ctx); err != nil {
		return err
	}
	// 使用 collector 的 FlushSnapshot 重置内存聚合状态。
	if coll := r.manager.Collector(); coll != nil {
		coll.FlushSnapshot(time.Now())
	}
	return nil
}

func (r *runtimeCommandAdapter) QueueTargets() []horizoncmd.QueueTarget {
	return toCommandQueueTargets(r.manager.Config().Supervisors)
}

func (r *runtimeCommandAdapter) ClearQueue(ctx context.Context, target horizoncmd.QueueTarget) error {
	queueManager := r.manager.QueueManager()
	if queueManager == nil {
		return fmt.Errorf("horizon: queue manager is not configured")
	}
	connection, err := queueManager.Queue(target.Connection)
	if err != nil {
		return queueOperationError("clear", target, err)
	}
	if err := connection.Clear(ctx, target.Queue); err != nil {
		return queueOperationError("clear", target, err)
	}
	return nil
}

func (r *runtimeCommandAdapter) ForgetFailedJob(ctx context.Context, id string) error {
	queueManager := r.manager.QueueManager()
	if queueManager == nil {
		return fmt.Errorf("horizon: queue manager is not configured")
	}
	failed := queueManager.Failed()
	if failed == nil {
		return fmt.Errorf("horizon: failed job store is not configured")
	}
	if _, err := failed.Find(ctx, id); err != nil {
		if errors.Is(err, queue.ErrEmpty) {
			return fmt.Errorf("horizon: failed job not found: %s", id)
		}
		return fmt.Errorf("horizon: read failed job %s: %w", id, err)
	}
	if err := failed.Forget(ctx, id); err != nil {
		return fmt.Errorf("horizon: delete failed job %s: %w", id, err)
	}
	return nil
}

// ForgetAllFailedJobs 删除 failed store 中的所有失败记录，保持 horizon:forget --all 的 Laravel 对齐语义。
//
// 逻辑说明：批量删除只调用 FailedStore.Flush，不读取或输出 failed payload，避免 CLI 错误路径泄露任务正文。
func (r *runtimeCommandAdapter) ForgetAllFailedJobs(ctx context.Context) error {
	queueManager := r.manager.QueueManager()
	if queueManager == nil {
		return fmt.Errorf("horizon: queue manager is not configured")
	}
	failed := queueManager.Failed()
	if failed == nil {
		return fmt.Errorf("horizon: failed job store is not configured")
	}
	if err := failed.Flush(ctx); err != nil {
		return fmt.Errorf("horizon: flush failed jobs: %w", err)
	}
	return nil
}

// Purge 执行 Laravel Horizon 风格 orphan worker process cleanup。
//
// 逻辑说明：该流程只扫描 Horizon worker process，和 Store 的 orphan PID tracking record 协作；
// 不调用 queue Clear、FailedStore、metrics trim 或 heartbeat trim，避免把 purge 扩大成通用清理命令。
func (r *runtimeCommandAdapter) Purge(ctx context.Context, now time.Time, signal string) (horizoncmd.PurgeSummary, error) {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if err := validatePurgeSignal(signal); err != nil {
		return horizoncmd.PurgeSummary{}, err
	}
	inspector := r.manager.ProcessInspector()
	if inspector == nil {
		return horizoncmd.PurgeSummary{}, fmt.Errorf("horizon: process inspector is not configured")
	}
	masterID, err := r.activeMasterID(ctx, now)
	if err != nil {
		return horizoncmd.PurgeSummary{}, err
	}
	activeWorkers, err := r.activeWorkerPIDs(ctx, now)
	if err != nil {
		return horizoncmd.PurgeSummary{}, err
	}
	processes, err := inspector.HorizonProcesses(ctx)
	if err != nil {
		return horizoncmd.PurgeSummary{}, err
	}
	summary := horizoncmd.PurgeSummary{}
	for _, process := range processes {
		if process.Kind != HorizonProcessWorker || process.PID <= 0 || activeWorkers[process.PID] {
			continue
		}
		if !r.purgeProcessMatchesNamespace(process) {
			continue
		}
		if err := r.store.RecordOrphanProcess(ctx, masterID, process.PID, now); err != nil {
			return horizoncmd.PurgeSummary{}, err
		}
		if err := inspector.Terminate(ctx, process.PID, false); err != nil {
			return horizoncmd.PurgeSummary{}, err
		}
		summary.OrphansDiscovered++
		summary.TerminateRequests++
	}
	timeout, err := r.MaxWorkerTimeout(r.manager.Config().Environment)
	if err != nil {
		return horizoncmd.PurgeSummary{}, err
	}
	old, err := r.store.OrphanProcessesOlderThan(ctx, masterID, time.Duration(timeout)*time.Second, now)
	if err != nil {
		return horizoncmd.PurgeSummary{}, err
	}
	for _, orphan := range old {
		if err := inspector.Terminate(ctx, orphan.PID, true); err != nil {
			return horizoncmd.PurgeSummary{}, err
		}
		if err := r.store.ForgetOrphanProcess(ctx, masterID, orphan.PID); err != nil {
			return horizoncmd.PurgeSummary{}, err
		}
		summary.TerminateRequests++
		summary.OrphansForgotten++
	}
	return summary, nil
}

func (r *runtimeCommandAdapter) purgeProcessMatchesNamespace(process HorizonProcess) bool {
	cfg := r.manager.Config()
	prefix := strings.TrimSpace(cfg.Prefix)
	if prefix == "" {
		prefix = normalizeStoreOptions(StoreOptions{}).Prefix
	}
	environment := strings.TrimSpace(cfg.Environment)
	if environment == "" {
		environment = "production"
	}
	if process.Prefix == "" || process.Environment == "" {
		// fake inspector 可能只填 Command；production Unix inspector 会尽量预解析字段。
		// 这里保留一次兜底解析，确保 purge 的 namespace 判断集中在同一个入口。
		applyHorizonProcessArgs(&process, strings.Fields(process.Command))
	}
	return process.Prefix == prefix && process.Environment == environment
}

// validatePurgeSignal 限制 horizon:purge 当前可表达的 signal 集合。
//
// 需求背景：ProcessInspector 首期只暴露“优雅终止/强制终止”两阶段语义；CLI 必须解析 --signal，
// 但不能静默接受无法传到底层的任意 POSIX signal。
func validatePurgeSignal(signal string) error {
	signal = strings.TrimSpace(strings.ToUpper(signal))
	if signal == "" || signal == "SIGTERM" || signal == "TERM" {
		return nil
	}
	return fmt.Errorf("horizon: purge signal %q is not supported", signal)
}

func (r *runtimeCommandAdapter) activeMasterID(ctx context.Context, now time.Time) (string, error) {
	masters, err := r.store.Masters(ctx, now)
	if err != nil {
		return "", err
	}
	for _, master := range masters {
		if master.Status != MasterStale && strings.TrimSpace(master.ID) != "" {
			return master.ID, nil
		}
	}
	return "default", nil
}

func (r *runtimeCommandAdapter) activeWorkerPIDs(ctx context.Context, now time.Time) (map[int]bool, error) {
	workers, err := r.store.Workers(ctx, now)
	if err != nil {
		return nil, err
	}
	out := make(map[int]bool, len(workers))
	for _, worker := range workers {
		if worker.Status != WorkerStale && worker.PID > 0 {
			out[worker.PID] = true
		}
	}
	return out, nil
}

// RunMaster 执行 horizon master 入口：记录 master heartbeat 并派生当前环境的 supervisor 子进程。
func (r *runtimeCommandAdapter) RunMaster(ctx context.Context, options horizoncmd.MasterOptions) error {
	cfg := r.manager.Config()
	environment := strings.TrimSpace(options.Environment)
	if environment == "" {
		environment = cfg.Environment
	}
	// fast termination 是唯一允许绕过租约互斥的窗口：旧进程已收到 terminate 且不等待收尾时，
	// 新 master 可以先写入 heartbeat 接管容量控制。普通启动必须依赖 Store 的原子租约防双开。
	allowMasterRace, err := r.allowFastTerminationRace(ctx)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	masterID := newProcessID("master")
	state := MasterState{
		ID:              masterID,
		Host:            hostname(),
		PID:             os.Getpid(),
		Status:          MasterRunning,
		StartedAt:       now,
		LastHeartbeatAt: now,
		SupervisorCount: len(cfg.Supervisors),
		Environment:     environment,
	}
	applySelfObservationToMaster(&state)
	acquired, err := r.store.AcquireMasterLease(ctx, state)
	if err != nil {
		return err
	}
	if !acquired {
		// 租约失败时再读取现有 heartbeat，只用于返回包含 existing_id/pid 的诊断错误；
		// 是否允许启动仍由 AcquireMasterLease 的原子结果决定，避免回到 TOCTOU 检查。
		if allowMasterRace {
			if err := r.store.HeartbeatMaster(ctx, state); err != nil {
				return err
			}
		} else if err := r.ensureNoFreshMasterConflict(ctx, environment, now); err != nil {
			return err
		}
	}
	if !acquired && !allowMasterRace {
		return fmt.Errorf("horizon: master already running host=%s environment=%s existing_id=%s pid=%d", state.Host, state.Environment, "", 0)
	}
	if err := r.startRuntimeMonitor(ctx); err != nil {
		return err
	}
	var processes []ManagedProcess
	for _, supervisor := range sortedSupervisorConfigs(cfg.Supervisors) {
		process, err := r.manager.ProcessRunner().Start(ctx, ProcessSpec{Args: []string{
			"horizon:supervisor",
			supervisor.Name,
			supervisor.Connection,
			"--environment=" + environment,
			"--master-id=" + masterID,
		}, NewProcessGroup: shouldIsolateHorizonChildSignals(cfg)})
		if err != nil {
			if cleanupErr := r.terminateStartedProcesses(context.WithoutCancel(ctx), processes); cleanupErr != nil {
				return fmt.Errorf("horizon: start supervisor failed: %w; cleanup: %v", err, cleanupErr)
			}
			return err
		}
		processes = append(processes, process)
	}
	interval := runtimeLoopInterval(cfg)
	err = waitMasterSupervisorProcesses(ctx, processes, interval, func(now time.Time) error {
		state.LastHeartbeatAt = now
		applySelfObservationToMaster(&state)
		return r.store.HeartbeatMaster(ctx, state)
	}, func(shutdownCtx context.Context) error {
		now := time.Now().UTC()
		state.LastHeartbeatAt = now
		applySelfObservationToMaster(&state)
		if err := r.store.HeartbeatMaster(shutdownCtx, state); err != nil {
			return err
		}
		if err := r.store.RequestTerminate(shutdownCtx, now, true); err != nil {
			return err
		}
		return r.notifyControlTargets(shutdownCtx, "")
	})
	if err != nil {
		return err
	}
	return r.completeTerminateIfDrained(ctx, cfg)
}

// RunSupervisor 执行单个 supervisor 入口：记录 supervisor heartbeat 并按固定进程数规则派生 worker。
func (r *runtimeCommandAdapter) RunSupervisor(ctx context.Context, options horizoncmd.SupervisorProcessOptions) error {
	cfg := r.manager.Config()
	supervisor, ok := cfg.Supervisors[strings.TrimSpace(options.Name)]
	if !ok {
		return fmt.Errorf("horizon: supervisor %q not found", options.Name)
	}
	environment := strings.TrimSpace(options.Environment)
	if environment == "" {
		environment = cfg.Environment
	}
	control, err := r.store.Control(ctx)
	if err != nil {
		return err
	}
	if !control.TerminateRequestedAt.IsZero() || control.GlobalPaused || control.PausedSupervisors[supervisor.Name] {
		// 启动前命中控制态时直接返回，避免留下会阻塞后续正常启动的 fresh supervisor lease。
		return nil
	}
	workloads, err := r.supervisorWorkloads(ctx, supervisor, time.Now().UTC())
	if err != nil {
		return err
	}
	pools := CalculateProcessPools(supervisor, workloads, ScaleState{}, time.Now().UTC())
	workerCount := processPoolWorkerCount(pools)
	for i := range pools {
		pools[i].CurrentWorkers = pools[i].TargetWorkers
	}
	now := time.Now().UTC()
	state := SupervisorState{
		Name:            supervisor.Name,
		Host:            hostname(),
		PID:             os.Getpid(),
		MasterID:        strings.TrimSpace(options.MasterID),
		Environment:     environment,
		Status:          SupervisorRunning,
		StartedAt:       now,
		LastHeartbeatAt: now,
		WorkerCount:     workerCount,
		Connection:      supervisor.Connection,
		Queues:          append([]string(nil), supervisor.Queues...),
		Pools:           pools,
	}
	applySelfObservationToSupervisor(&state)
	acquired, err := r.store.AcquireSupervisorLease(ctx, state)
	if err != nil {
		return err
	}
	allowSupervisorRace := false
	if !acquired {
		// supervisor 与 master 使用同一套租约语义，但冲突范围额外包含 supervisor name，
		// 允许同一环境下不同 supervisor 或不同 host 各自独立运行。
		allowSupervisorRace, err = r.allowFastTerminationRace(ctx)
		if err != nil {
			return err
		}
		if allowSupervisorRace {
			if err := r.store.HeartbeatSupervisor(ctx, state); err != nil {
				return err
			}
		} else if err := r.ensureNoFreshSupervisorConflict(ctx, supervisor.Name, environment, now); err != nil {
			return err
		}
	}
	if !acquired && !allowSupervisorRace {
		return fmt.Errorf("horizon: supervisor already running name=%s host=%s environment=%s master_id=%s pid=%d", state.Name, state.Host, state.Environment, "", 0)
	}
	var processes []ManagedProcess
	var specs []ProcessSpec
	for _, pool := range pools {
		for i := 0; i < pool.TargetWorkers; i++ {
			workerID := workerIDForPool(supervisor.Name, pool, i+1)
			spec := ProcessSpec{Args: workerArgs(supervisor, workerID, environment, cfg.Prefix, pool.Queues), NewProcessGroup: shouldIsolateHorizonChildSignals(cfg)}
			process, err := r.manager.ProcessRunner().Start(ctx, spec)
			if err != nil {
				if cleanupErr := r.terminateStartedProcesses(context.WithoutCancel(ctx), processes); cleanupErr != nil {
					return fmt.Errorf("horizon: start worker failed: %w; cleanup: %v", err, cleanupErr)
				}
				return err
			}
			specs = append(specs, spec)
			processes = append(processes, process)
		}
	}
	return r.supervisorRuntimeLoop(ctx, supervisor, environment, state, specs, processes)
}

func (r *runtimeCommandAdapter) terminateStartedProcesses(ctx context.Context, processes []ManagedProcess) error {
	inspector := r.manager.ProcessInspector()
	if inspector == nil {
		return fmt.Errorf("horizon: process inspector is not configured")
	}
	var cleanupErr error
	for _, process := range processes {
		if process == nil || process.PID() <= 0 {
			continue
		}
		if err := inspector.Terminate(ctx, process.PID(), false); err != nil && cleanupErr == nil {
			cleanupErr = err
		}
	}
	return cleanupErr
}

// allowFastTerminationRace 判断当前 terminate 控制标记是否允许新进程抢跑。
//
// 设计原因：该例外属于 runtime policy，不属于 Store 原子租约本身；把判断留在 runtime 层能让
// Memory/Redis Store 都维持简单、保守、可测试的互斥语义。
func (r *runtimeCommandAdapter) allowFastTerminationRace(ctx context.Context) (bool, error) {
	control, err := r.store.Control(ctx)
	if err != nil {
		return false, err
	}
	return r.manager.Config().FastTermination && !control.TerminateShouldWait && !control.TerminateRequestedAt.IsZero(), nil
}

// ensureNoFreshMasterConflict 在 master 写入 heartbeat 前检查同 host/environment 下是否已有 fresh master。
//
// 设计原因：Horizon master 是容量控制入口，同一命名空间内双开会让两个进程同时派生 supervisor。
// terminating 状态只在 fast termination 抢跑窗口内允许，新 master 随后会清理旧 terminate flag。
func (r *runtimeCommandAdapter) ensureNoFreshMasterConflict(ctx context.Context, environment string, now time.Time) error {
	masters, err := r.store.Masters(ctx, now)
	if err != nil {
		return err
	}
	currentHost := hostname()
	control, err := r.store.Control(ctx)
	if err != nil {
		return err
	}
	allowTerminating := r.manager.Config().FastTermination && !control.TerminateShouldWait && !control.TerminateRequestedAt.IsZero()
	for _, master := range masters {
		if master.Status == MasterStale || master.Host != currentHost || master.Environment != environment {
			continue
		}
		if allowTerminating && master.Status == MasterRunning {
			continue
		}
		return fmt.Errorf("horizon: master already running host=%s environment=%s existing_id=%s pid=%d", master.Host, master.Environment, master.ID, master.PID)
	}
	return nil
}

// completeTerminateIfDrained 清理等待型 terminate 的一次性控制状态和旧 heartbeat 记录。
func (r *runtimeCommandAdapter) completeTerminateIfDrained(ctx context.Context, cfg Config) error {
	cleanupCtx := context.WithoutCancel(ctx)
	control, err := r.store.Control(cleanupCtx)
	if err != nil {
		return err
	}
	if control.TerminateRequestedAt.IsZero() || !shouldWaitForTerminate(cfg, control) {
		return nil
	}
	if err := r.store.ClearTerminateRequest(cleanupCtx); err != nil {
		return err
	}
	ttl := cfg.HeartbeatTTL
	if ttl <= 0 {
		ttl = time.Minute
	}
	return r.store.Trim(cleanupCtx, time.Now().UTC().Add(ttl+time.Second))
}

// ensureNoFreshSupervisorConflict 防止同 host/environment/supervisor name 的 fresh supervisor 双开。
//
// 需求背景：同名 fresh supervisor 同时运行会重复管理同一组 worker。不同 host 或不同 environment 的同名
// supervisor 属于独立运行域，不应互相阻塞。
func (r *runtimeCommandAdapter) ensureNoFreshSupervisorConflict(ctx context.Context, name string, environment string, now time.Time) error {
	supervisors, err := r.store.Supervisors(ctx, now)
	if err != nil {
		return err
	}
	currentHost := hostname()
	control, err := r.store.Control(ctx)
	if err != nil {
		return err
	}
	allowTerminating := r.manager.Config().FastTermination && !control.TerminateShouldWait && !control.TerminateRequestedAt.IsZero()
	for _, supervisor := range supervisors {
		if supervisor.Status == SupervisorStale || supervisor.Name != name || supervisor.Host != currentHost || supervisor.Environment != environment {
			continue
		}
		if allowTerminating && supervisor.Status == SupervisorTerminating {
			continue
		}
		return fmt.Errorf("horizon: supervisor already running name=%s host=%s environment=%s master_id=%s pid=%d", supervisor.Name, supervisor.Host, supervisor.Environment, supervisor.MasterID, supervisor.PID)
	}
	return nil
}

// supervisorRuntimeLoop 是 horizon:supervisor 的长期运行控制循环。
//
// 逻辑边界：
//  1. Store control flag 是 pause/continue/terminate 的事实来源，循环每个 tick 都重新读取。
//  2. ProcessPoolState.TargetWorkers 来自 scaler，实际 OS 子进程由本循环用 slot 表持续对账。
//  3. worker 异常退出会进入 exits channel；非终止状态下按原 spec 补拉，终止/暂停状态下停止补拉。
//  4. 缩容不会直接丢弃 slot，而是先发送 graceful terminate，再由 timeout 兜底 force terminate。
//  5. fast_termination 只影响收到 terminate 后是否等待旧 worker：false 或 --wait 都会等待；true 且无 --wait 会直接返回。
func (r *runtimeCommandAdapter) supervisorRuntimeLoop(ctx context.Context, supervisor SupervisorConfig, environment string, state SupervisorState, specs []ProcessSpec, processes []ManagedProcess) error {
	interval := runtimeLoopInterval(r.manager.Config())
	wake := newRuntimeControlWake(ctx)
	defer wake.Stop()
	// 结构化日志字段，为当前 supervisor 提供可检索的运行时上下文
	logFields := map[string]any{"component": "horizon", "subsystem": "supervisor", "supervisor": supervisor.Name, "environment": environment, "host": hostname()}
	// exits 可能同时收到初始 worker、补拉 worker 以及 nil process 的退出事件。
	// buffer 至少覆盖常规目标规模，避免 watcher goroutine 因 supervisor 主循环短暂处理 tick 而阻塞。
	exits := make(chan processExit, maxInt(len(processes)+supervisor.MaxProcesses+1, 16))
	// slots 是运行中 worker 的权威内存视图。Store 只暴露状态快照，不反向驱动进程生命周期。
	slots := initialSupervisorProcessSlots(state.Pools, specs, processes)
	nextSlotID := len(slots)
	for id, slot := range slots {
		watchProcess(id, slot.process, exits)
	}
	workloads := make(chan supervisorWorkloadResult, 1)
	workloadInFlight := false
	startWorkloadSample := func(sampledAt time.Time) {
		if workloadInFlight {
			return
		}
		workloadInFlight = true
		startRecoveringGoroutineWithPanicHandler(ctx, "supervisor", logFields, func(err error) {
			result := supervisorWorkloadResult{sampledAt: sampledAt, err: err, reported: true}
			select {
			case workloads <- result:
			case <-ctx.Done():
			}
		}, func() {
			updated, err := r.supervisorWorkloads(ctx, supervisor, sampledAt)
			result := supervisorWorkloadResult{workloads: updated, sampledAt: sampledAt, err: err}
			select {
			case workloads <- result:
			case <-ctx.Done():
			}
		})
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		var now time.Time
		var workloadResult supervisorWorkloadResult
		workloadReady := false
		select {
		case <-ctx.Done():
			return nil
		case exit := <-exits:
			if exit.err != nil {
				// Laravel Horizon 对齐：worker 异常退出时上报错误并移除死 worker，
				// 不崩溃 supervisor。下一轮 tick 的 reconcile 会按目标容量补拉。
				if !exit.reported {
					goexception.Report(ctx, exit.err, logFields)
				}
				delete(slots, exit.index)
				continue
			}
			// 未知 slot 说明该退出事件已经被缩容/终止路径清理过，忽略即可。
			slot, ok := slots[exit.index]
			if !ok {
				continue
			}
			// 已经发过 graceful terminate 的 worker 正常退出后，slot 才真正从实际 worker 集合移除。
			if !slot.terminatingAt.IsZero() {
				delete(slots, exit.index)
				continue
			}
			control, err := r.store.Control(ctx)
			if err != nil {
				goexception.Report(ctx, err, logFields)
				continue
			}
			if !control.TerminateRequestedAt.IsZero() {
				if !shouldWaitForTerminate(r.manager.Config(), control) {
					return nil
				}
				// terminate 等待模式下，已经自行退出的 worker 从 slots 移除；剩余 worker 继续等待或由 tick 强杀。
				delete(slots, exit.index)
				if len(slots) == 0 {
					return nil
				}
				continue
			}
			if control.GlobalPaused || control.PausedSupervisors[state.Name] {
				// pause 是运行期控制状态，不代表 supervisor 退出；退出的 worker 只从当前容量中扣除，
				// 目标容量保留到 continue 后由下一轮对账补齐。
				delete(slots, exit.index)
				state.Status = SupervisorPaused
				state.LastHeartbeatAt = time.Now().UTC()
				state.Pools = poolsWithCurrentWorkers(state.Pools, supervisorSlotCounts(slots, true))
				state.WorkerCount = supervisorSlotWorkerCount(slots, true)
				applySelfObservationToSupervisor(&state)
				if err := r.store.HeartbeatSupervisor(ctx, state); err != nil {
					goexception.Report(ctx, err, logFields)
					continue
				}
				continue
			}
			// 非控制态退出按原启动 spec 补拉，保留 worker 与 queue/pool 的绑定关系。
			process, err := r.manager.ProcessRunner().Start(ctx, slot.spec)
			if err != nil {
				goexception.Report(ctx, err, logFields)
				continue
			}
			slot.process = process
			slots[exit.index] = slot
			watchProcess(exit.index, process, exits)
		case now = <-ticker.C:
			now = now.UTC()
		case _, ok := <-wake.C:
			if !ok {
				continue
			}
			now = time.Now().UTC()
		case workloadResult = <-workloads:
			workloadInFlight = false
			workloadReady = true
			now = workloadResult.sampledAt
		}
		control, err := r.store.Control(ctx)
		if err != nil {
			goexception.Report(ctx, err, logFields)
			continue
		}
		if !control.TerminateRequestedAt.IsZero() {
			state.Status = SupervisorTerminating
			state.LastHeartbeatAt = now
			if shouldWaitForTerminate(r.manager.Config(), control) {
				// 等待型 terminate 会先对全部活跃 worker 发送 graceful terminate。
				// 后续 tick 继续刷新 terminating heartbeat，并在 supervisor timeout 后 force terminate 未退出 worker。
				if err := r.terminateSupervisorSlots(ctx, slots, now, false); err != nil {
					goexception.Report(ctx, err, logFields)
					continue
				}
				if err := r.forceExpiredSupervisorSlots(ctx, slots, now, supervisorShutdownTimeout(supervisor)); err != nil {
					goexception.Report(ctx, err, logFields)
					continue
				}
				state.Pools = poolsWithCurrentWorkers(state.Pools, supervisorSlotCounts(slots, true))
				state.WorkerCount = supervisorSlotWorkerCount(slots, true)
				if err := r.store.HeartbeatSupervisor(ctx, state); err != nil {
					goexception.Report(ctx, err, logFields)
					continue
				}
				if len(slots) == 0 {
					return nil
				}
				continue
			}
			// fast_termination=true 且未传 --wait 时，旧 supervisor 不等待 worker 收尾，允许新 master 先启动。
			applySelfObservationToSupervisor(&state)
			return r.store.HeartbeatSupervisor(ctx, state)
		}
		if control.GlobalPaused || control.PausedSupervisors[state.Name] {
			// pause tick 只刷新可读状态，不采样 workload、不 autoscale、不补拉或扩容 worker。
			state.Status = SupervisorPaused
			state.LastHeartbeatAt = now
			state.Pools = poolsWithCurrentWorkers(state.Pools, supervisorSlotCounts(slots, true))
			state.WorkerCount = supervisorSlotWorkerCount(slots, true)
			applySelfObservationToSupervisor(&state)
			if err := r.store.HeartbeatSupervisor(ctx, state); err != nil {
				goexception.Report(ctx, err, logFields)
				continue
			}
			continue
		}
		if workloadReady {
			if workloadResult.err != nil {
				if !workloadResult.reported {
					goexception.Report(ctx, workloadResult.err, logFields)
				}
				continue
			}
			// scaler 只计算目标，reconcileSupervisorSlots 负责把目标转成实际启动/缩容动作。
			state.Pools = CalculateProcessPools(supervisor, workloadResult.workloads, scaleStateFromPools(state.Pools), now)
			if err := r.reconcileSupervisorSlots(ctx, supervisor, environment, state.Pools, slots, &nextSlotID, exits, now); err != nil {
				goexception.Report(ctx, err, logFields)
				continue
			}
		} else {
			startWorkloadSample(now.UTC())
		}
		if err := r.forceExpiredSupervisorSlots(ctx, slots, now, supervisorShutdownTimeout(supervisor)); err != nil {
			goexception.Report(ctx, err, logFields)
			continue
		}
		state.Pools = poolsWithCurrentWorkers(state.Pools, supervisorSlotCounts(slots, true))
		state.WorkerCount = supervisorSlotWorkerCount(slots, true)
		state.Status = SupervisorRunning
		state.LastHeartbeatAt = now
		applySelfObservationToSupervisor(&state)
		if err := r.store.HeartbeatSupervisor(ctx, state); err != nil {
			goexception.Report(ctx, err, logFields)
			continue
		}
	}
}

func scaleStateFromPools(pools []ProcessPoolState) ScaleState {
	state := ScaleState{CurrentWorkers: make(map[string]int, len(pools))}
	for _, pool := range pools {
		// CurrentWorkers 是实际对账后的 worker 数；旧快照没有该值时回退到 TargetWorkers，保持兼容。
		current := pool.CurrentWorkers
		if current == 0 {
			current = pool.TargetWorkers
		}
		state.CurrentWorkers[pool.Queue] = current
	}
	return state
}

// supervisorProcessSlot 表示 supervisor 当前负责的一个 worker 子进程。
//
// poolName 绑定 scaler 的 ProcessPoolState.Name，用于缩容时按池裁剪；spec 保存补拉该 worker
// 所需的命令行参数；terminatingAt/forceSent 记录两阶段终止状态，避免重复发送 terminate/kill。
type supervisorProcessSlot struct {
	poolName      string
	spec          ProcessSpec
	process       ManagedProcess
	terminatingAt time.Time
	forceSent     bool
}

// initialSupervisorProcessSlots 把 RunSupervisor 启动阶段已经创建的 worker 进程映射到 slot 表。
//
// 启动阶段按 pools 的顺序生成 specs/processes，所以这里按同样顺序恢复 pool 归属。
// 这让 runtime loop 接管后可以准确知道每个 worker 属于哪个 queue pool，并在 worker 退出时用原 spec 补拉。
func initialSupervisorProcessSlots(pools []ProcessPoolState, specs []ProcessSpec, processes []ManagedProcess) map[int]supervisorProcessSlot {
	slots := make(map[int]supervisorProcessSlot, len(processes))
	index := 0
	for _, pool := range pools {
		for i := 0; i < pool.TargetWorkers && index < len(processes); i++ {
			slot := supervisorProcessSlot{poolName: pool.Name, process: processes[index]}
			if index < len(specs) {
				slot.spec = specs[index]
			}
			slots[index] = slot
			index++
		}
	}
	for index < len(processes) {
		slot := supervisorProcessSlot{process: processes[index]}
		if index < len(specs) {
			slot.spec = specs[index]
		}
		slots[index] = slot
		index++
	}
	return slots
}

// reconcileSupervisorSlots 把 scaler 给出的目标池状态同步到真实 worker 子进程。
//
// 对账顺序刻意分三步：
//  1. 先终止已不存在的池，防止配置/策略切换后遗留旧 queue worker。
//  2. 再为目标数不足的池启动缺失 worker，保证扩容尽快生效。
//  3. 最后对目标数过多的池发 graceful terminate，真正移除等待 watcher 收到退出事件。
//
// terminating 中的 slot 不计入 active counts，因此缩容后的池不会因为旧 worker 仍在收尾而阻塞后续目标判断。
func (r *runtimeCommandAdapter) reconcileSupervisorSlots(ctx context.Context, supervisor SupervisorConfig, environment string, pools []ProcessPoolState, slots map[int]supervisorProcessSlot, nextSlotID *int, exits chan<- processExit, now time.Time) error {
	desired := make(map[string]ProcessPoolState, len(pools))
	for _, pool := range pools {
		desired[pool.Name] = pool
	}
	for id, slot := range slots {
		if _, ok := desired[slot.poolName]; !ok && slot.terminatingAt.IsZero() {
			if err := r.terminateSupervisorSlot(ctx, id, slot, slots, now, false); err != nil {
				return err
			}
		}
	}
	counts := supervisorSlotCounts(slots, false)
	for _, pool := range pools {
		for counts[pool.Name] < pool.TargetWorkers {
			*nextSlotID = *nextSlotID + 1
			id := *nextSlotID
			workerID := workerIDForPool(supervisor.Name, pool, id+1)
			spec := ProcessSpec{Args: workerArgs(supervisor, workerID, environment, r.manager.Config().Prefix, pool.Queues), NewProcessGroup: shouldIsolateHorizonChildSignals(r.manager.Config())}
			process, err := r.manager.ProcessRunner().Start(ctx, spec)
			if err != nil {
				return err
			}
			slots[id] = supervisorProcessSlot{poolName: pool.Name, spec: spec, process: process}
			watchProcess(id, process, exits)
			counts[pool.Name]++
		}
		for counts[pool.Name] > pool.TargetWorkers {
			id, slot, ok := newestActiveSupervisorSlot(slots, pool.Name)
			if !ok {
				break
			}
			if err := r.terminateSupervisorSlot(ctx, id, slot, slots, now, false); err != nil {
				return err
			}
			counts[pool.Name]--
		}
	}
	return nil
}

// terminateSupervisorSlots 对当前全部 slot 发送同一种终止请求。
//
// graceful=false/force=false 用于正常 drain；force=true 用于超过 supervisor timeout 后的兜底强杀。
func (r *runtimeCommandAdapter) terminateSupervisorSlots(ctx context.Context, slots map[int]supervisorProcessSlot, now time.Time, force bool) error {
	for id, slot := range slots {
		if err := r.terminateSupervisorSlot(ctx, id, slot, slots, now, force); err != nil {
			return err
		}
	}
	return nil
}

// terminateSupervisorSlot 通过 ProcessInspector 发送两阶段终止请求，并把结果写回 slot 表。
//
// 该函数不会等待进程退出；等待由 watchProcess 和 exits channel 完成。这样 supervisor 主循环可以继续刷新
// heartbeat、响应 control flag，并对其他 worker 做补拉或超时处理。
func (r *runtimeCommandAdapter) terminateSupervisorSlot(ctx context.Context, id int, slot supervisorProcessSlot, slots map[int]supervisorProcessSlot, now time.Time, force bool) error {
	if slot.process == nil || slot.process.PID() <= 0 {
		delete(slots, id)
		return nil
	}
	if !force && !slot.terminatingAt.IsZero() {
		return nil
	}
	if force && slot.forceSent {
		return nil
	}
	if err := r.manager.ProcessInspector().Terminate(ctx, slot.process.PID(), force); err != nil {
		return err
	}
	if force {
		slot.forceSent = true
	} else {
		slot.terminatingAt = now
	}
	slots[id] = slot
	return nil
}

// forceExpiredSupervisorSlots 对已经 graceful terminating 且超过 timeout 的 worker 发送 force terminate。
//
// timeout 为 0 时表示测试或配置要求立即进入第二阶段；生产配置通常来自 supervisor.timeout。
func (r *runtimeCommandAdapter) forceExpiredSupervisorSlots(ctx context.Context, slots map[int]supervisorProcessSlot, now time.Time, timeout time.Duration) error {
	for id, slot := range slots {
		if slot.terminatingAt.IsZero() || slot.forceSent {
			continue
		}
		if timeout <= 0 || !now.Before(slot.terminatingAt.Add(timeout)) {
			if err := r.terminateSupervisorSlot(ctx, id, slot, slots, now, true); err != nil {
				return err
			}
		}
	}
	return nil
}

// newestActiveSupervisorSlot 选择某个 pool 中最新启动的活跃 worker 作为缩容对象。
//
// 新 worker 通常处理任务历史更短，缩容它能减少对长期运行 worker 的扰动；terminating worker 已经在收尾，
// 不再作为新的缩容目标。
func newestActiveSupervisorSlot(slots map[int]supervisorProcessSlot, poolName string) (int, supervisorProcessSlot, bool) {
	newest := -1
	var selected supervisorProcessSlot
	for id, slot := range slots {
		if slot.poolName != poolName || !slot.terminatingAt.IsZero() {
			continue
		}
		if id > newest {
			newest = id
			selected = slot
		}
	}
	return newest, selected, newest >= 0
}

// supervisorSlotCounts 统计每个 pool 当前由 supervisor 跟踪的 worker 数。
//
// includeTerminating=false 用于扩缩容决策，只看仍然应该承载目标容量的 worker；
// includeTerminating=true 用于 Store 状态展示，让运维能看到正在收尾的 worker 仍被 supervisor 管理。
func supervisorSlotCounts(slots map[int]supervisorProcessSlot, includeTerminating bool) map[string]int {
	counts := make(map[string]int)
	for _, slot := range slots {
		if slot.poolName == "" {
			continue
		}
		if !includeTerminating && !slot.terminatingAt.IsZero() {
			continue
		}
		counts[slot.poolName]++
	}
	return counts
}

// supervisorSlotWorkerCount 汇总 supervisorSlotCounts 的结果，供 SupervisorState.WorkerCount 使用。
func supervisorSlotWorkerCount(slots map[int]supervisorProcessSlot, includeTerminating bool) int {
	total := 0
	for _, count := range supervisorSlotCounts(slots, includeTerminating) {
		total += count
	}
	return total
}

// poolsWithCurrentWorkers 返回带有实际 worker 数的 pool 快照。
//
// TargetWorkers 保留 scaler 决策，CurrentWorkers 记录当前 supervisor 实际跟踪的 worker 数，
// 二者分离后 UI/CLI 可以看出正在扩容、缩容或 terminate drain 的中间状态。
func poolsWithCurrentWorkers(pools []ProcessPoolState, counts map[string]int) []ProcessPoolState {
	out := append([]ProcessPoolState(nil), pools...)
	for i := range out {
		out[i].CurrentWorkers = counts[out[i].Name]
	}
	return out
}

// shouldWaitForTerminate 实现 Laravel Horizon 风格的 fast_termination/--wait 矩阵。
//
// fast_termination=false：旧 master/supervisor 必须等待 worker 优雅退出。
// fast_termination=true 且未 --wait：旧进程可直接返回，让新 master 先启动。
// fast_termination=true 且 --wait：命令写入的内部等待策略覆盖 fast termination。
func shouldWaitForTerminate(cfg Config, control ControlState) bool {
	return !cfg.FastTermination || control.TerminateShouldWait
}

// supervisorShutdownTimeout 返回 graceful terminate 到 force terminate 的等待窗口。
//
// Horizon 的等待上限来自活跃 supervisor 的 worker timeout；单 supervisor runtime loop 使用自身 timeout。
func supervisorShutdownTimeout(supervisor SupervisorConfig) time.Duration {
	if supervisor.Timeout <= 0 {
		return 0
	}
	return time.Duration(supervisor.Timeout) * time.Second
}

// maxInt 用于给内部 channel 选择保守 buffer，不表达业务语义。
func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

type processExit struct {
	index    int
	err      error
	reported bool
}

type supervisorWorkloadResult struct {
	workloads []QueueWorkload
	sampledAt time.Time
	err       error
	reported  bool
}

// watchProcess 为每个子进程启动独立 goroutine 监听退出事件，并将结果发送到 exits channel。
//
// 需求背景：supervisor/monitor 需要可靠收到 worker 退出通知后才能补拉或对账。
// 设计思路：goroutine 只做 Wait 阻塞和 channel 发送，避免在 supervisor 主循环中串行等待。
// 容错设计：recover 捕获 process.Wait() 意外 panic 并将其转换为错误事件，防止 goroutine
// panic 导致整个 supervisor 进程崩溃。
func watchProcess(index int, process ManagedProcess, exits chan<- processExit) {
	startRecoveringGoroutineWithPanicHandler(context.Background(), "supervisor", nil, func(err error) {
		exits <- processExit{index: index, err: fmt.Errorf("horizon: watchProcess panic: %v", err), reported: true}
	}, func() {
		if process == nil {
			exits <- processExit{index: index}
			return
		}
		exits <- processExit{index: index, err: process.Wait()}
	})
}

func (r *runtimeCommandAdapter) supervisorWorkloads(ctx context.Context, supervisor SupervisorConfig, now time.Time) ([]QueueWorkload, error) {
	workloads := make([]QueueWorkload, 0, len(supervisor.Queues))
	queueManager := r.manager.QueueManager()
	var connection queuecontract.Queue
	if queueManager != nil {
		var err error
		connection, err = queueManager.Queue(supervisor.Connection)
		if err != nil {
			return nil, queueOperationError("size", horizoncmd.QueueTarget{Connection: supervisor.Connection, Queue: strings.Join(supervisor.Queues, ",")}, err)
		}
	}
	// 使用 collector 的聚合数据替代旧同步 snapshot 获取 runtime 信息
	metrics := r.buildRuntimeMetricsFromCollector()
	for _, queueName := range supervisor.Queues {
		workload := QueueWorkload{Queue: queueName, Runtime: RuntimeForQueue(metrics, supervisor.Connection, queueName)}
		if connection != nil {
			size, err := connection.Size(ctx, queueName)
			if err != nil {
				return nil, queueOperationError("size", horizoncmd.QueueTarget{Connection: supervisor.Connection, Queue: queueName}, err)
			}
			workload.Ready = size
		}
		workloads = append(workloads, workload)
	}
	return workloads, nil
}

// RunWorker 执行单个 horizon:work 入口，并在每轮 Once:true queue worker 调用周围维护 worker heartbeat。
func (r *runtimeCommandAdapter) RunWorker(ctx context.Context, options horizoncmd.WorkerOptions) error {
	runner := r.manager.WorkerRunner()
	if runner == nil {
		return fmt.Errorf("horizon: queue worker runner is not configured")
	}
	queueWorkerOptions := toQueueWorkerOptions(options)
	workerID := strings.TrimSpace(options.Name)
	if workerID == "" {
		workerID = newProcessID("worker")
	}
	state := WorkerState{
		ID:               workerID,
		Supervisor:       strings.TrimSpace(options.Supervisor),
		Environment:      strings.TrimSpace(options.Environment),
		Host:             hostname(),
		PID:              os.Getpid(),
		Status:           WorkerIdle,
		StartedAt:        time.Now().UTC(),
		LastHeartbeatAt:  time.Now().UTC(),
		ConfiguredQueues: splitQueueNames(options.Queue),
	}
	applySelfObservationToWorker(&state)
	recorder := &workerEventRecorder{store: r.store, state: state, collector: r.manager.coll}
	// 结构化日志字段，为当前 worker 提供可检索的运行时上下文
	workerLogFields := map[string]any{"component": "horizon", "subsystem": "worker", "worker_id": workerID, "supervisor": state.Supervisor, "host": hostname(), "worker_pid": os.Getpid()}
	queueWorkerOptions.EventObserver = func(eventCtx context.Context, ev queue.Event) context.Context {
		recorder.record(eventCtx, ev)
		eventCtx = contextWithWorkerSupervisor(eventCtx, state.Supervisor)
		r.collectWorkerEvent(eventCtx, ev, state.Supervisor)
		return eventCtx
	}
	session, err := runner.Begin(ctx, queueWorkerOptions)
	if err != nil {
		return err
	}
	if session == nil {
		return fmt.Errorf("horizon: queue worker session is not configured")
	}
	if err := r.startRuntimeMonitor(ctx); err != nil {
		_ = session.Close()
		return err
	}
	if err := session.Activate(ctx); err != nil {
		_ = session.Close()
		return err
	}
	defer func() {
		if closeErr := session.Close(); closeErr != nil {
			goexception.Report(ctx, closeErr, map[string]any{"component": "horizon", "subsystem": "worker", "worker_id": strings.TrimSpace(options.Name)})
		}
	}()
	recorder.heartbeat(ctx)
	// 启动独立 heartbeat ticker goroutine，按 runtimeLoopInterval 周期性刷新
	interval := runtimeLoopInterval(r.manager.Config())
	stopHeartbeat := recorder.startPeriodicHeartbeat(ctx, interval)
	defer stopHeartbeat()
	started := time.Now()
	var processed int64 // 本地计数器，用于 MaxJobs 退出条件
	for {
		if ctx.Err() != nil {
			recorder.terminating(context.WithoutCancel(ctx))
			return recorder.result(nil)
		}
		if shouldStopWorkerLoop(options, processed, started) {
			return recorder.result(nil)
		}
		control, err := r.store.Control(ctx)
		if err != nil {
			goexception.Report(ctx, err, workerLogFields)
			continue
		}
		if !control.TerminateRequestedAt.IsZero() {
			recorder.terminating(context.WithoutCancel(ctx))
			return recorder.result(nil)
		}
		if control.GlobalPaused || control.PausedSupervisors[state.Supervisor] {
			recorder.paused(ctx)
			sleepContext(ctx, workerPauseSleep(options.Sleep))
			continue
		}
		recorder.beginRound()
		err = session.Work(ctx)
		if err != nil {
			if ctx.Err() != nil {
				recorder.terminating(context.WithoutCancel(ctx))
				return recorder.result(nil)
			}
			goexception.Report(ctx, err, workerLogFields)
			recorder.terminating(context.WithoutCancel(ctx))
			return recorder.result(err)
		}
		if recorder.sawJob {
			processed++
		}
		if !recorder.sawJob && options.StopWhenEmpty {
			recorder.terminating(context.WithoutCancel(ctx))
			return recorder.result(nil)
		}
		// 逻辑说明：--sleep 只表示空队列轮询间隔，不能在已消费任务后强制等待。
		//
		// 需求背景：Horizon 每轮以 Once:true 调用底层 queue worker。如果成功处理任务后仍然
		// 执行 sleep，默认 sleep=3 会把轻量任务吞吐限制为“每个 worker 每 3 秒 1 条”，
		// RabbitMQ 这类 push 队列即使已有大量 ready delivery 也无法快速 drain。
		//
		// 设计思路：只有当前轮没有看到 queue.job_processing 事件时才 sleep；成功消费后立即
		// 进入下一轮，让 worker 在 backlog 存在时连续处理任务。
		if !recorder.sawJob && options.Sleep > 0 {
			sleepContext(ctx, time.Duration(options.Sleep)*time.Second)
		}
	}
}

// collectWorkerEvent 将 horizon:work runtime 收到的普通 queue event 采集到 Horizon collector。
//
// 用途：在 worker/supervisor runtime 边界补齐 SourceSupervisor，让 event_metrics window 能按
// namespace + host + environment + supervisor + connection + queue 保留完整来源分片。
// 使用方式：RunWorker 通过 queue.WorkerOptions.EventObserver 调用；该函数只读取当前 worker options 中已有的
// supervisor 身份和 Manager 内存配置，不访问 Store、Redis、supervisor 列表或网络。
// 设计原因：普通 queue event 本身不应该新增 supervisor 字段；只有 Horizon worker runtime 知道
// 当前事件来自哪个 --supervisor。缺失 supervisor 时必须保留空字符串语义，不得从 queue/config 反推。
// 设计思路：复用 collector 的 inputFromEventWithPressure 构造安全事件投影，使采样使用该 collector
// 持有的独立随机源；再通过 Manager.applyCollectorSource 补齐 prefix/environment/host，
// 最后显式写入 runtime supervisor 并非阻塞 Collect。
// 需求背景：issue 43 要求 worker runtime 事件桥在更新 heartbeat 的同时，collector 和全局事件出口都能收到同一事件。
func (r *runtimeCommandAdapter) collectWorkerEvent(ctx context.Context, ev queue.Event, supervisor string) {
	if r == nil || r.manager == nil || r.manager.coll == nil || ev == nil {
		return
	}
	if r.manager.CollBound() {
		return
	}
	input := r.manager.coll.inputFromEventWithPressure(ev, r.manager.samplingPressure())
	input = r.manager.applyCollectorSource(input)
	input.SourceSupervisor = strings.TrimSpace(supervisor)
	_ = r.manager.coll.Collect(ctx, input)
}

// Listen 执行 horizon:listen 本地开发流程：轮询 horizon.watch 路径并在文件变化后重启 horizon。
//
// 需求背景：Laravel Horizon 的 listen 命令用于开发期监听源码变更并重启 Horizon；Prismgo 不引入
// Node/chokidar 或第三方 watcher，使用标准库轮询实现稳定、可测试的重启语义。
func (r *runtimeCommandAdapter) Listen(ctx context.Context, options horizoncmd.ListenOptions) (horizoncmd.ListenSummary, error) {
	poll := options.Poll
	if poll <= 0 {
		poll = time.Second
	}
	environment := strings.TrimSpace(options.Environment)
	if environment == "" {
		environment = firstNonEmpty(r.manager.Config().Environment, "local")
	}
	watchPaths := normalizedWatchPaths(r.manager.Config().Watch)
	summary := horizoncmd.ListenSummary{WatchPathCount: len(watchPaths)}
	process, cancel, exits, err := r.startListenProcess(ctx, environment)
	if err != nil {
		return summary, err
	}
	summary.Starts++
	defer cancel()
	signature := scanWatchSignature(watchPaths)
	ticker := time.NewTicker(poll)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return summary, r.stopListenProcess(context.Background(), process, cancel, exits)
		case err := <-exits:
			if ctx.Err() != nil {
				return summary, nil
			}
			return summary, err
		case <-ticker.C:
			next := scanWatchSignature(watchPaths)
			if watchSignatureEqual(signature, next) {
				continue
			}
			summary.Restarts++
			if err := r.stopListenProcess(ctx, process, cancel, exits); err != nil {
				return summary, err
			}
			process, cancel, exits, err = r.startListenProcess(ctx, environment)
			if err != nil {
				return summary, err
			}
			summary.Starts++
			signature = next
		}
	}
}

func (r *runtimeCommandAdapter) startListenProcess(ctx context.Context, environment string) (ManagedProcess, context.CancelFunc, <-chan error, error) {
	childCtx, cancel := context.WithCancel(ctx)
	process, err := r.manager.ProcessRunner().Start(childCtx, ProcessSpec{Args: []string{"horizon", "--environment=" + environment}})
	if err != nil {
		cancel()
		return nil, nil, nil, err
	}
	exits := make(chan error, 1)
	// 启动独立 goroutine 等待 listen 子进程退出。
	// 容错设计：recover 捕获 process.Wait() 意外 panic 并转换为错误，防止 goroutine panic 导致 listen 进程崩溃。
	startRecoveringGoroutineWithPanicHandler(ctx, "listen", nil, func(err error) {
		exits <- fmt.Errorf("horizon: startListenProcess panic: %v", err)
	}, func() {
		exits <- process.Wait()
	})
	return process, cancel, exits, nil
}

// stopListenProcess 先通过 ProcessInspector 发出终止请求，再取消子进程上下文并等待退出。
//
// 逻辑说明：ProcessInspector 是 Prismgo 现有的进程控制边界；cancel 用于兜底唤醒由 ProcessRunner
// 启动的子进程，避免 listen 在开发期因为旧进程不退出而永久卡住。
func (r *runtimeCommandAdapter) stopListenProcess(ctx context.Context, process ManagedProcess, cancel context.CancelFunc, exits <-chan error) error {
	if process != nil && process.PID() > 0 {
		if err := r.manager.ProcessInspector().Terminate(ctx, process.PID(), false); err != nil {
			return err
		}
	}
	if cancel != nil {
		cancel()
	}
	select {
	case err := <-exits:
		return err
	case <-time.After(200 * time.Millisecond):
		if process != nil && process.PID() > 0 {
			if err := r.manager.ProcessInspector().Terminate(ctx, process.PID(), true); err != nil {
				return err
			}
		}
		return nil
	}
}

type watchFileState struct {
	size    int64
	modUnix int64
}

func normalizedWatchPaths(paths []string) []string {
	out := make([]string, 0, len(paths))
	seen := map[string]bool{}
	for _, path := range paths {
		path = strings.TrimSpace(path)
		if path == "" || seen[path] {
			continue
		}
		seen[path] = true
		out = append(out, path)
	}
	sort.Strings(out)
	return out
}

func scanWatchSignature(paths []string) map[string]watchFileState {
	out := map[string]watchFileState{}
	for _, root := range paths {
		info, err := os.Stat(root)
		if err != nil {
			continue
		}
		if !info.IsDir() {
			out[root] = watchFileState{size: info.Size(), modUnix: info.ModTime().UnixNano()}
			continue
		}
		_ = filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
			if err != nil || entry.IsDir() {
				return nil
			}
			info, err := entry.Info()
			if err != nil {
				return nil
			}
			out[path] = watchFileState{size: info.Size(), modUnix: info.ModTime().UnixNano()}
			return nil
		})
	}
	return out
}

func watchSignatureEqual(left map[string]watchFileState, right map[string]watchFileState) bool {
	if len(left) != len(right) {
		return false
	}
	for path, l := range left {
		if r, ok := right[path]; !ok || r != l {
			return false
		}
	}
	return true
}

// captureQueueLengthSnapshot 采集所有 supervisor 队列目标的当前长度。
//
// 逻辑说明：该函数采用 all-or-nothing 语义；任一 connection 解析或 Size 调用失败时直接返回错误，
// 不保存部分结果，也不覆盖上一次成功保存的 QueueLengthSnapshot。
func (r *runtimeCommandAdapter) captureQueueLengthSnapshot(ctx context.Context, now time.Time) (QueueLengthSnapshot, error) {
	targets := toCommandQueueTargets(r.manager.Config().Supervisors)
	snapshot := QueueLengthSnapshot{CapturedAt: now, Queues: make([]QueueLengthBucket, 0, len(targets))}
	if len(targets) == 0 {
		return snapshot, nil
	}
	queueManager := r.manager.QueueManager()
	if queueManager == nil {
		return QueueLengthSnapshot{}, fmt.Errorf("horizon: queue manager is not configured")
	}
	for _, target := range targets {
		connection, err := queueManager.Queue(target.Connection)
		if err != nil {
			return QueueLengthSnapshot{}, queueOperationError("size", target, err)
		}
		size, err := connection.Size(ctx, target.Queue)
		if err != nil {
			return QueueLengthSnapshot{}, queueOperationError("size", target, err)
		}
		snapshot.Queues = append(snapshot.Queues, QueueLengthBucket{
			Connection: target.Connection,
			Queue:      target.Queue,
			Size:       size,
		})
	}
	return snapshot, nil
}

func toCommandSupervisorState(supervisor SupervisorState) horizoncmd.SupervisorState {
	return horizoncmd.SupervisorState{
		Name:            supervisor.Name,
		Host:            supervisor.Host,
		PID:             supervisor.PID,
		Status:          supervisor.Status,
		StartedAt:       supervisor.StartedAt,
		LastHeartbeatAt: supervisor.LastHeartbeatAt,
		WorkerCount:     supervisor.WorkerCount,
		Connection:      supervisor.Connection,
		Queues:          append([]string(nil), supervisor.Queues...),
		Pools:           toCommandProcessPools(supervisor.Pools),
	}
}

func toCommandProcessPools(pools []ProcessPoolState) []horizoncmd.ProcessPoolState {
	out := make([]horizoncmd.ProcessPoolState, 0, len(pools))
	for _, pool := range pools {
		out = append(out, horizoncmd.ProcessPoolState{
			Name:           pool.Name,
			Queue:          pool.Queue,
			Queues:         append([]string(nil), pool.Queues...),
			CurrentWorkers: pool.CurrentWorkers,
			TargetWorkers:  pool.TargetWorkers,
		})
	}
	return out
}

// toCommandQueueTargets 从 supervisor 配置推导稳定去重的 connection+queue 目标列表。
//
// 需求背景：Horizon 维护命令只能作用于当前环境已配置的 supervisor 队列范围，不能回退到 queue manager 默认队列。
func toCommandQueueTargets(supervisors map[string]SupervisorConfig) []horizoncmd.QueueTarget {
	seen := make(map[horizoncmd.QueueTarget]struct{})
	for _, supervisor := range supervisors {
		connection := strings.TrimSpace(supervisor.Connection)
		if connection == "" {
			continue
		}
		for _, queueName := range supervisor.Queues {
			queueName = strings.TrimSpace(queueName)
			if queueName == "" {
				continue
			}
			seen[horizoncmd.QueueTarget{Connection: connection, Queue: queueName}] = struct{}{}
		}
	}
	targets := make([]horizoncmd.QueueTarget, 0, len(seen))
	for target := range seen {
		targets = append(targets, target)
	}
	sort.Slice(targets, func(i, j int) bool {
		if targets[i].Connection == targets[j].Connection {
			return targets[i].Queue < targets[j].Queue
		}
		return targets[i].Connection < targets[j].Connection
	})
	return targets
}

// queueOperationError 包装队列 adapter 错误，保留 operation、connection 和 queue 以便 CLI/UI 精确定位失败目标。
func queueOperationError(operation string, target horizoncmd.QueueTarget, err error) error {
	return fmt.Errorf("horizon: queue operation=%s connection=%s queue=%s failed: %w", operation, target.Connection, target.Queue, err)
}

// sortedSupervisorConfigs 稳定排序 supervisor 配置，保证 master 派生子进程顺序可测试、可预期。
func sortedSupervisorConfigs(supervisors map[string]SupervisorConfig) []SupervisorConfig {
	out := make([]SupervisorConfig, 0, len(supervisors))
	for _, supervisor := range supervisors {
		out = append(out, supervisor)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// configuredWorkerCount 实现 issue 05 固定进程数规则。
func processPoolWorkerCount(pools []ProcessPoolState) int {
	total := 0
	for _, pool := range pools {
		total += pool.TargetWorkers
	}
	return total
}

// workerArgs 把 supervisor 配置投影为 horizon:work 命令行参数。
//
// 需求背景：master/supervisor 通过子进程启动 horizon:work，所有 worker 运行边界
// 都必须显式体现在命令行参数中。如果 retry_after 只停留在 SupervisorConfig，
// worker 进程会使用命令默认值，造成配置页面、Horizon UI 和实际消费行为不一致。
//
// 设计思路：该函数只做纯参数投影，不读取全局配置，也不推断队列后端类型；调用方传入的
// queues 用于 auto/simple balance 后的单队列池，空值时回退到 supervisor.Queues。
//
// 参数说明：supervisor 是当前 supervisor 的静态配置；workerID 是子进程 worker 名称；
// environment 是 worker 继承的 Horizon 环境；queues 是当前进程实际绑定的队列切片。
func workerArgs(supervisor SupervisorConfig, workerID string, environment string, prefix string, queues []string) []string {
	if len(queues) == 0 {
		queues = supervisor.Queues
	}
	if strings.TrimSpace(prefix) == "" {
		prefix = normalizeStoreOptions(StoreOptions{}).Prefix
	}
	return []string{
		"horizon:work",
		supervisor.Connection,
		"--name=" + workerID,
		"--supervisor=" + supervisor.Name,
		"--environment=" + environment,
		"--prefix=" + strings.TrimSpace(prefix),
		"--queue=" + strings.Join(queues, ","),
		"--sleep=" + strconv.Itoa(supervisor.Sleep),
		"--timeout=" + strconv.Itoa(supervisor.Timeout),
		"--tries=" + strconv.Itoa(supervisor.Tries),
		"--backoff=" + joinInts(supervisor.Backoff),
		"--retry-after=" + strconv.Itoa(supervisor.RetryAfter),
		"--max-jobs=" + strconv.Itoa(supervisor.MaxJobs),
		"--max-time=" + strconv.Itoa(supervisor.MaxTime),
	}
}

func workerIDForPool(supervisorName string, pool ProcessPoolState, index int) string {
	if pool.Queue == "" {
		return supervisorName + "-" + strconv.Itoa(index)
	}
	return supervisorName + "-" + pool.Queue + "-" + strconv.Itoa(index)
}

func joinInts(values []int) string {
	if len(values) == 0 {
		return "0"
	}
	parts := make([]string, 0, len(values))
	for _, value := range values {
		parts = append(parts, strconv.Itoa(value))
	}
	return strings.Join(parts, ",")
}

func waitProcesses(processes []ManagedProcess) error {
	for _, process := range processes {
		if process == nil {
			continue
		}
		if err := process.Wait(); err != nil {
			return err
		}
	}
	return nil
}

// processWaitResult 保存单个子进程 Wait() 的退出结果。
//
// 需求背景：master/supervisor 的子进程 Wait() 可能正常退出、返回错误，或由 panic recovery
// 转换成错误。启动期判断需要保留 reported 标记，避免同一个 panic 先被 recovery 上报后又被
// 启动失败聚合路径重复上报。
// 字段说明：err 是 Wait() 的原始错误；reported 表示该错误已经由 recovery 路径上报过。
type processWaitResult struct {
	err      error
	reported bool
}

// processStartupMonitor 描述 master 启动窗口内判断 supervisor 是否真正启动成功的策略。
//
// 需求背景：长期运行期 supervisor 异常退出应保持 Horizon 自恢复语义；但启动期所有 supervisor
// 都在窗口内退出失败时，CLI 必须收到非零错误。该结构把“启动期”策略从通用等待循环中抽出，
// 避免影响 waitProcessesWithHeartbeat 的原有调用者。
// 参数说明：window 是启动观察窗口；failed 将聚合后的 Wait 错误包装成对 CLI 友好的启动失败错误。
type processStartupMonitor struct {
	window time.Duration
	failed func(error) error
}

// startupMonitorState 保存当前启动窗口内已经收集到的 supervisor 退出结果。
//
// 设计思路：等待循环在 tick、wake、子进程退出三个分支都会触发启动期判定，因此把重复的状态推进
// 和成功/失败判断集中到方法里，避免在 select 分支中复制复杂条件。
type startupMonitorState struct {
	monitor  *processStartupMonitor
	deadline time.Time
	results  []processWaitResult
	active   bool
}

// waitMasterSupervisorProcesses 等待 master 派生的 supervisor 子进程，同时执行启动期失败探测。
//
// 需求背景：`horizon` CLI 通过 RunMaster 启动 supervisor 子进程；旧逻辑会把启动阶段的
// supervisor Wait() 错误只上报到 exception handler，最终让 CLI 以成功状态退出。
// 设计思路：启动窗口使用 runtime loop interval，至少覆盖一次 runtime tick；窗口到期时只要还有
// 任意 supervisor 子进程未退出，就认为真实进程树已经跨过启动期。只有窗口内所有 supervisor
// 都退出且存在 Wait 错误时，才返回 `horizon: supervisor startup failed` 包装错误。
// 参数说明：ctx 是命令生命周期；processes 是已启动的 supervisor 进程；interval 是 runtime loop tick；
// heartbeat 刷新 master heartbeat；onCancel 处理 Ctrl+C graceful terminate。
func waitMasterSupervisorProcesses(ctx context.Context, processes []ManagedProcess, interval time.Duration, heartbeat func(time.Time) error, onCancel ...func(context.Context) error) error {
	monitor := &processStartupMonitor{
		window: interval,
		failed: func(err error) error {
			return fmt.Errorf("horizon: supervisor startup failed: %w", err)
		},
	}
	return waitProcessesWithHeartbeatStartup(ctx, processes, interval, heartbeat, monitor, onCancel...)
}

// waitProcessesWithHeartbeat 等待子进程退出并周期性执行 heartbeat 回调。
//
// 需求背景：该函数是 master 长期运行等待 supervisor 的通用入口，旧调用者依赖“子进程错误只上报、
// 不返回”的语义。启动期失败检测由 waitMasterSupervisorProcesses 单独传入 startup monitor，
// 这里显式传 nil，确保普通调用保持原有长期运行行为。
// 参数说明：ctx 控制等待生命周期；processes 是待等待子进程；interval 是 heartbeat tick；
// heartbeat 是可选心跳回调；onCancel 是 ctx 取消后执行的 graceful shutdown 回调。
func waitProcessesWithHeartbeat(ctx context.Context, processes []ManagedProcess, interval time.Duration, heartbeat func(time.Time) error, onCancel ...func(context.Context) error) error {
	return waitProcessesWithHeartbeatStartup(ctx, processes, interval, heartbeat, nil, onCancel...)
}

// waitProcessesWithHeartbeatStartup 是 waitProcessesWithHeartbeat 的内部实现。
//
// 需求背景：普通 master 等待逻辑需要继续支持 heartbeat、control wake 和 Ctrl+C graceful terminate；
// 新增的启动期失败检测只服务 RunMaster，不能改变 panic recovery、heartbeat 错误上报和长期运行期
// supervisor 异常退出的既有行为。
// 设计思路：startup 为 nil 时完全保持旧语义；startup 非 nil 时先缓存启动窗口内的 Wait 错误，
// 等待至少一次 tick/wake 来确认是否仍有真实 supervisor 进程存活。确认启动成功后再把缓存错误按
// 长期运行语义上报；确认所有子进程都已错误退出则聚合错误并返回给 CLI。
// 参数说明：ctx 是命令上下文；processes 是待等待的子进程；interval 是 heartbeat tick；
// heartbeat 是每次 tick/wake 的 master heartbeat 回调；startup 是可选启动期探针；
// onCancel 是 ctx 取消时要执行的 graceful shutdown 回调集合。
func waitProcessesWithHeartbeatStartup(ctx context.Context, processes []ManagedProcess, interval time.Duration, heartbeat func(time.Time) error, startup *processStartupMonitor, onCancel ...func(context.Context) error) error {
	// 结构化日志字段，master 级别上下文
	masterLogFields := map[string]any{"component": "horizon", "subsystem": "master", "host": hostname()}
	if len(processes) == 0 {
		return nil
	}
	done := make(chan processWaitResult, len(processes))
	active := 0
	for _, process := range processes {
		if process == nil {
			continue
		}
		active++
		// 为每个子进程启动独立 goroutine 等待退出。
		// 容错设计：recover 捕获 p.Wait() 意外 panic 并转换为错误，防止 goroutine panic 导致 master 进程崩溃。
		startRecoveringGoroutineWithPanicHandler(ctx, "master", masterLogFields, func(err error) {
			done <- processWaitResult{err: fmt.Errorf("horizon: waitProcessesWithHeartbeat panic: %v", err), reported: true}
		}, func() {
			done <- processWaitResult{err: process.Wait()}
		})
	}
	if active == 0 {
		return nil
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	wake := newRuntimeControlWake(ctx)
	defer wake.Stop()
	ctxDone := ctx.Done()
	wakeC := wake.C
	cancelHandled := false
	startupState := newStartupMonitorState(startup, time.Now().UTC())
	for active > 0 {
		select {
		case <-ctxDone:
			ctxDone = nil
			if !cancelHandled {
				cancelHandled = true
				heartbeat = nil
				shutdownCtx := context.WithoutCancel(ctx)
				for _, handler := range onCancel {
					if handler == nil {
						continue
					}
					if err := handler(shutdownCtx); err != nil {
						goexception.Report(shutdownCtx, err, masterLogFields)
					}
				}
			}
		case result := <-done:
			active--
			if result.err != nil && startupState.active {
				startupState.record(result)
				if active == 0 {
					if err := startupState.failure(ctx, masterLogFields); err != nil {
						return err
					}
					startupState.active = false
				}
				continue
			}
			if result.err != nil && !result.reported {
				// Laravel Horizon 对齐：上报 supervisor 退出错误但不崩溃 master，继续监控其他 supervisor。
				goexception.Report(ctx, result.err, masterLogFields)
			}
		case now := <-ticker.C:
			now = now.UTC()
			if err := startupState.check(ctx, masterLogFields, now, active); err != nil {
				return err
			}
			if heartbeat != nil {
				if err := heartbeat(now.UTC()); err != nil {
					goexception.Report(ctx, err, masterLogFields)
				}
			}
		case _, ok := <-wakeC:
			now := time.Now().UTC()
			if err := startupState.check(ctx, masterLogFields, now, active); err != nil {
				return err
			}
			if ok && heartbeat != nil {
				if err := heartbeat(now); err != nil {
					goexception.Report(ctx, err, masterLogFields)
				}
			}
			if !ok {
				wakeC = nil
			}
		}
	}
	return nil
}

// newStartupMonitorState 根据启动探针构造运行期状态。
//
// 参数说明：monitor 为空表示当前调用不需要启动期检测；now 是计算启动窗口 deadline 的基准时间。
func newStartupMonitorState(monitor *processStartupMonitor, now time.Time) startupMonitorState {
	state := startupMonitorState{monitor: monitor, active: monitor != nil}
	if !state.active {
		return state
	}
	window := monitor.window
	if window <= 0 {
		window = time.Second
	}
	state.deadline = now.Add(window)
	return state
}

// record 缓存启动窗口内收到的子进程退出结果。
//
// 逻辑说明：只有启动期 active 时调用；正常退出不会进入启动失败判断，错误退出才会被缓存。
func (s *startupMonitorState) record(result processWaitResult) {
	s.results = append(s.results, result)
}

// check 在 tick/wake 时推进启动期状态。
//
// 逻辑说明：若启动窗口到期时仍有 supervisor 子进程未退出，则启动期结束，并把此前缓存但尚未
// 上报的错误按长期运行语义上报；若窗口到期且没有任何存活子进程，则返回启动失败错误。
// 参数说明：ctx/fields 用于 exception.Report；now 是本次检查时间；activeProcesses 是当前仍在
// Wait() 中的真实 supervisor 子进程数量。
func (s *startupMonitorState) check(ctx context.Context, fields map[string]any, now time.Time, activeProcesses int) error {
	if s == nil || !s.active {
		return nil
	}
	if now.Before(s.deadline) {
		return nil
	}
	if activeProcesses > 0 {
		s.started(ctx, fields)
		return nil
	}
	if len(s.results) > 0 {
		return s.failure(ctx, fields)
	}
	return nil
}

// started 标记启动期已经通过。
//
// 设计思路：真实子进程跨过启动窗口后，即使另一个 supervisor 曾在窗口内报错，也应进入长期运行
// 语义，由 exception.Report 上报错误而不是让 CLI 返回失败。
func (s *startupMonitorState) started(ctx context.Context, fields map[string]any) {
	s.active = false
	s.reportUnreported(ctx, fields)
}

// failure 聚合启动窗口内的 supervisor Wait 错误并返回给 CLI。
//
// 需求背景：这是唯一会把 supervisor Wait 错误从 master 等待循环返回的路径，只用于“启动窗口内
// 没有任何 supervisor 子进程继续存活”的启动失败场景。
// 参数说明：ctx/fields 用于保留 exception.Report 上报。
func (s *startupMonitorState) failure(ctx context.Context, fields map[string]any) error {
	if s == nil || s.monitor == nil || len(s.results) == 0 {
		return nil
	}
	err := errors.Join(processWaitErrors(s.results)...)
	if s.monitor.failed != nil {
		err = s.monitor.failed(err)
	}
	goexception.Report(ctx, err, fields)
	return err
}

// reportUnreported 上报启动窗口内尚未被 panic recovery 上报过的 Wait 错误。
//
// 逻辑说明：启动已成功时，这些错误属于长期运行期异常退出，应沿用“上报但不返回”的语义。
func (s *startupMonitorState) reportUnreported(ctx context.Context, fields map[string]any) {
	for _, result := range s.results {
		if result.err != nil && !result.reported {
			goexception.Report(ctx, result.err, fields)
		}
	}
}

// processWaitErrors 从等待结果中提取非空错误，供 errors.Join 聚合。
//
// 参数说明：results 是启动窗口内缓存的子进程 Wait 结果；返回值只包含需要暴露给 CLI 的错误。
func processWaitErrors(results []processWaitResult) []error {
	errs := make([]error, 0, len(results))
	for _, result := range results {
		if result.err != nil {
			errs = append(errs, result.err)
		}
	}
	return errs
}

func runtimeLoopInterval(cfg Config) time.Duration {
	if cfg.LoopInterval > 0 {
		return cfg.LoopInterval
	}
	return time.Second
}

func (r *runtimeCommandAdapter) startRuntimeMonitor(ctx context.Context) error {
	if r == nil || r.manager == nil || r.manager.EventDispatcher() == nil {
		return nil
	}
	// Horizon 长驻 runtime 负责启动观测链路；Store 解析失败时直接返回错误，避免
	// collector 已接收事件但 flusher 无法落盘的半启动状态。
	return r.manager.RegisterMonitor(ctx)
}

func shouldIsolateHorizonChildSignals(cfg Config) bool {
	return !strings.EqualFold(strings.TrimSpace(cfg.Store), "memory")
}

func newProcessID(prefix string) string {
	return prefix + "-" + strconv.Itoa(os.Getpid()) + "-" + strconv.FormatInt(time.Now().UnixNano(), 36)
}

func hostname() string {
	host, err := os.Hostname()
	if err != nil || strings.TrimSpace(host) == "" {
		return "unknown"
	}
	return host
}

func toQueueWorkerOptions(options horizoncmd.WorkerOptions) queue.WorkerOptions {
	return queue.WorkerOptions{
		Connection:    options.Connection,
		Queues:        splitQueueNames(options.Queue),
		Once:          true,
		StopWhenEmpty: options.StopWhenEmpty,
		// Horizon 已在 horizon:work 外层持有 consumer lease；这里每轮只执行一次任务处理。
		SkipConsumerIntent: true,
		Sleep:              time.Duration(options.Sleep) * time.Second,
		Timeout:            time.Duration(options.Timeout) * time.Second,
		Tries:              options.Tries,
		Backoff:            parseBackoffDurations(options.Backoff),
		RetryAfter:         time.Duration(options.RetryAfter) * time.Second,
		MaxJobs:            1,
		MaxTime:            time.Duration(options.MaxTime) * time.Second,
	}
}

func splitQueueNames(value string) []string {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		if queueName := strings.TrimSpace(part); queueName != "" {
			out = append(out, queueName)
		}
	}
	return out
}

func parseBackoffDurations(value string) []time.Duration {
	value = strings.TrimSpace(value)
	if value == "" || value == "0" {
		return nil
	}
	parts := strings.Split(value, ",")
	out := make([]time.Duration, 0, len(parts))
	for _, part := range parts {
		n, err := strconv.Atoi(strings.TrimSpace(part))
		if err == nil && n > 0 {
			out = append(out, time.Duration(n)*time.Second)
		}
	}
	return out
}

func shouldStopWorkerLoop(options horizoncmd.WorkerOptions, processed int64, started time.Time) bool {
	if options.MaxJobs > 0 && processed >= int64(options.MaxJobs) {
		return true
	}
	return options.MaxTime > 0 && time.Since(started) >= time.Duration(options.MaxTime)*time.Second
}

// workerPauseSleep 返回暂停轮询间隔。
//
// 需求背景：issue 06 要求暂停期间复用 --sleep 节奏；当调用方传入 0 或负数时使用 1 秒兜底，
// 避免 paused worker 在没有任务可取时忙等消耗 CPU。
// 参数说明：seconds 来自 horizon:work 的 --sleep 选项，单位为秒。
func workerPauseSleep(seconds int) time.Duration {
	if seconds <= 0 {
		return time.Second
	}
	return time.Duration(seconds) * time.Second
}

func sleepContext(ctx context.Context, d time.Duration) {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}

type workerEventRecorder struct {
	store          Store
	state          WorkerState
	collector      *collector
	sawJob         bool
	heartbeatError HeartbeatError
	// mu 保护 state 字段在队列事件回调（主 goroutine 内执行 runner.Work 时）与
	// 独立 heartbeat ticker goroutine 之间的并发访问。
	mu sync.Mutex
}

func (r *workerEventRecorder) beginRound() {
	r.sawJob = false
}

// record 记录队列事件，只标记 sawJob 用于 StopWhenEmpty 判断和本地 processed 计数。
//
// 设计原因（issue 49/50/51）：队列事件热路径禁止直接写 Store，禁止维护 worker heartbeat state、
// CurrentJob、Processed、Failed、WorkerWorking 或调用 HeartbeatWorker。
// 任务量统计统一走 collector + flusher 的 event_metrics 聚合通道。
func (r *workerEventRecorder) record(ctx context.Context, ev queue.Event) {
	switch ev.(type) {
	case queue.JobProcessing:
		r.sawJob = true
	default:
		return
	}
}

// idle 将 worker 状态切换到 idle；仅由周期性 heartbeat 路径或测试调用，不写 Store。
//
// 设计原因：移除队列事件心跳后，idle 不再由队列热路径触发，只更新内存状态供下一次 heartbeat tick 落盘。
func (r *workerEventRecorder) idle() {
	r.mu.Lock()
	r.state.Status = WorkerIdle
	r.mu.Unlock()
}

// paused 将 worker 状态切换到 paused 并立即写入一次 Store 作为显式生命周期最终状态。
//
// 设计原因：pause 是显式控制命令触发的状态变更，需要在 worker 进入暂停循环前把 paused 状态持久化，
// 避免 Store 在暂停期间仍看到上一次周期性 heartbeat 写入的 idle 状态。
func (r *workerEventRecorder) paused(ctx context.Context) {
	r.mu.Lock()
	r.state.Status = WorkerPaused
	r.state.LastHeartbeatAt = time.Now().UTC()
	applySelfObservationToWorker(&r.state)
	r.mu.Unlock()
	r.heartbeat(ctx)
}

// terminating 将 worker 状态切换到 terminating 并立即写入一次 Store 作为显式生命周期最终状态。
//
// 设计原因：terminate 是显式控制命令或退出路径触发的最终状态变更，必须在 worker 退出前持久化，
// 避免 worker 进程结束后遗留 idle/running 状态的陈旧 heartbeat。
func (r *workerEventRecorder) terminating(ctx context.Context) {
	r.mu.Lock()
	r.state.Status = WorkerTerminating
	r.state.LastHeartbeatAt = time.Now().UTC()
	applySelfObservationToWorker(&r.state)
	r.mu.Unlock()
	r.heartbeat(ctx)
}

// heartbeat 将当前 worker 状态写入 Store，写入失败时记录诊断错误但不取消正在执行的 job。
//
// 安全边界：错误只包含稳定错误码 "heartbeat_write_failed"，不泄露 job payload 或 Store 凭据。
// 并发安全：在锁内完成状态快照后释放锁，再执行 Store 写入，避免 Store 慢时长时间持锁；
// 同时避免读写 r.state 的数据竞争（ticker goroutine 在锁内修改 LastHeartbeatAt 等字段）。
func (r *workerEventRecorder) heartbeat(ctx context.Context) {
	collectorMemory := collectorMemoryMetric(r.collector)
	r.mu.Lock()
	r.state.CollectorMemoryBytes = collectorMemory
	heartbeatError := r.heartbeatError
	if heartbeatError.Code != "" {
		r.state.LastHeartbeatErrorCode = heartbeatError.Code
		r.state.LastHeartbeatErrorMessage = heartbeatError.Message
		r.state.LastHeartbeatErrorAt = heartbeatError.FailedAt
	} else {
		r.state.LastHeartbeatErrorCode = ""
		r.state.LastHeartbeatErrorMessage = ""
		r.state.LastHeartbeatErrorAt = time.Time{}
	}
	// 在锁内拷贝状态快照，避免 Store 写入期间与 ticker/生命周期方法的数据竞争
	snapshot := r.state
	r.mu.Unlock()
	if err := r.store.HeartbeatWorker(ctx, snapshot); err != nil {
		r.recordHeartbeatError(time.Now().UTC())
	}
}

func collectorMemoryMetric(coll *collector) goprocess.Metric {
	if coll == nil {
		return goprocess.Metric{Value: nil, Unit: goprocess.UnitBytes, Status: goprocess.StatusUnavailable, Reason: "collector unavailable"}
	}
	return coll.MemoryEstimate()
}

// startPeriodicHeartbeat 启动独立 goroutine 按 interval 周期性刷新 worker heartbeat。
//
// 用途：替换原来每个队列事件都触发 heartbeat 的设计，改为按 runtimeLoopInterval 节奏
// 独立刷新，确保长任务执行期间 heartbeat 持续刷新不被阻塞。
// 返回值是停止函数，调用方应在 worker 退出前 defer 调用以停止 ticker 并等待 goroutine 结束。
// 设计原因：心跳写入与队列事件解耦，同时满足长任务不 stale 和事件量不撑爆 Store。
// 并发模型：每个 RunWorker 调用启动一个 ticker goroutine；ticker 在锁内更新 LastHeartbeatAt
// 与自省字段后释放锁，再通过 heartbeat() 的快照机制安全写入 Store。
// startPeriodicHeartbeat 启动独立 goroutine 按 interval 周期性刷新 worker heartbeat。
//
// 用途：替换原来每个队列事件都触发 heartbeat 的设计，改为按 runtimeLoopInterval 节奏
// 独立刷新，确保长任务执行期间 heartbeat 持续刷新不被阻塞。
// 返回值是停止函数，调用方应在 worker 退出前 defer 调用以停止 ticker 并等待 goroutine 结束。
// 设计原因：心跳写入与队列事件解耦，同时满足长任务不 stale 和事件量不撑爆 Store。
// 并发模型：每个 RunWorker 调用启动一个 ticker goroutine；ticker 在锁内更新 LastHeartbeatAt
// 与自省字段后释放锁，再通过 heartbeat() 的快照机制安全写入 Store。
// 容错设计：goroutine 内 recover 防止 panic 导致静默退出；Store 写入使用不受主 ctx
// 取消影响的独立 context，确保退出路径上最终 heartbeat 落盘不受 ctx 取消干扰。
func (r *workerEventRecorder) startPeriodicHeartbeat(ctx context.Context, interval time.Duration) func() {
	ticker := time.NewTicker(interval)
	done := make(chan struct{})
	exited := make(chan struct{})
	// 派生独立 context，避免主 ctx 取消阻断退出路径上的最终 heartbeat 写入
	hbCtx := context.WithoutCancel(ctx)
	startRecoveringGoroutineWithPanicHandler(context.Background(), "heartbeat", map[string]any{"worker_id": r.state.ID, "worker_pid": r.state.PID, "supervisor": r.state.Supervisor}, func(err error) {
		// panic 恢复：记录带 panic 值的诊断信息，goroutine 安全退出
		// heartbeat 停止后 worker 将被 supervisor 通过 stale 检测自动重新拉起
		r.setHeartbeatError(HeartbeatError{
			Code:     "heartbeat_write_failed",
			Message:  fmt.Sprintf("Worker heartbeat write failed (panic: %v).", err),
			FailedAt: time.Now().UTC(),
		})
	}, func() {
		defer close(exited)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				// 主 ctx 取消，ticker 退出；RunWorker 退出路径上的 terminating()
				// 已在主 goroutine 中显式写入最终状态
				return
			case <-done:
				return
			case <-ticker.C:
				r.mu.Lock()
				r.state.LastHeartbeatAt = time.Now().UTC()
				applySelfObservationToWorker(&r.state)
				r.mu.Unlock()
				r.heartbeat(hbCtx)
			}
		}
	})
	return func() {
		close(done)
		<-exited
	}
}

func (r *workerEventRecorder) recordHeartbeatError(at time.Time) {
	r.setHeartbeatError(HeartbeatError{
		Code:     "heartbeat_write_failed",
		Message:  "Worker heartbeat write failed.",
		FailedAt: at,
	})
}

func (r *workerEventRecorder) setHeartbeatError(err HeartbeatError) {
	r.mu.Lock()
	r.heartbeatError = err
	r.mu.Unlock()
}

func (r *workerEventRecorder) currentHeartbeatError() HeartbeatError {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.heartbeatError
}

func (r *workerEventRecorder) result(err error) error {
	heartbeatError := r.currentHeartbeatError()
	if err != nil {
		if heartbeatError.Code == "" {
			return err
		}
		return fmt.Errorf("%w; heartbeat=%s", err, heartbeatError.Code)
	}
	if heartbeatError.Code == "" {
		return nil
	}
	return fmt.Errorf("horizon: worker heartbeat diagnostic: %s", heartbeatError.Code)
}
