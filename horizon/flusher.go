package horizon

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"
)

// flusher 实现 ObservabilityFlusher 接口，提供异步批量 Store 写入。
//
// 设计思路：
//  1. 后台 goroutine 按 flush_interval 定期从 collector 拉取快照并写入 Store。
//  2. event_metrics 使用 batch/window 追加模型写入，不覆盖多窗口或多实例数据。
//  3. Store 写入失败不影响 worker 任务处理；失败时记录诊断并继续。
//  4. shutdown 时执行 best-effort flush，超时后放弃并记录诊断。
//
// 使用方式：Manager 创建 flusher 后调用 Start 启动后台循环，Stop 优雅关闭。
type flusher struct {
	mu    sync.Mutex
	cfg   ObservabilityConfig
	store Store
	coll  *collector
	waits map[string]int
	// writeSlot 串行化所有 Store 写入。
	//
	// 设计原因：collector.FlushSnapshot 会重置内存窗口。如果两个 flush 同时运行，后启动的一方
	// 可能先取走 collector 数据，但因为 Store writer 正忙或超时而没有写入 append-only facts。
	// 因此所有周期 flush、horizon:snapshot 和 shutdown flush 都必须先拿到 writer slot，再 drain
	// collector，确保“取走内存事实”和“写入 Store 事实”属于同一个串行临界区。
	writeSlot chan struct{}

	// 后台 flush 循环控制
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// 连续 flush 失败计数
	flushErrorStreak int
	// 连续 flush 成功计数，用于 Store 恢复后自动解除降级
	flushSuccessStreak int
	// 最近一次 flush 成功时间
	lastFlushSuccessAt time.Time
	// 最近一次 flush 错误信息
	lastFlushError string
	// 最近一次 flush 耗时
	lastFlushDuration time.Duration
	// flush lag（距离上次 flush 的时间）
	flushLag time.Duration
	// scheduleAccepted 记录被后台调度器接受并启动 writer goroutine 的周期 flush 次数。
	//
	// 用途：暴露 flusher 进程内调度状态，帮助判断 flush_interval tick 是否被正常接收。
	// 使用方式：Diagnostics 读取该计数；普通 tick 合并只进入进程内诊断，不写 ObservabilityDiagnostic。
	// 设计原因：周期 tick 是健康信号，不代表数据完整性缺口；需要可观测但不能污染 Store 诊断事实。
	// 设计思路：只在 scheduleMu 保护下从“空闲”切到“运行中”时递增，避免把合并 tick 误算成 writer。
	// 需求背景：issue 42 要求 accepted/running/merged/queued/skipped/timeout 或等价计数可诊断。
	scheduleAccepted int64
	// scheduleRunning 记录当前正在执行 Store 写入路径的后台周期 flush 数量。
	//
	// 用途：证明同一 flusher 实例最多只有一个后台周期 writer 正在运行。
	// 使用方式：runBackgroundFlush 进入 Store writer 前递增，退出时递减；Diagnostics 对外只读。
	// 设计原因：慢 Store 下无限 goroutine 会造成写入压力和窗口乱序，运行数是定位该问题的第一信号。
	// 设计思路：该值只描述后台周期 flush；on-demand 和 shutdown 仍通过 writeSlot 与它串行。
	// 需求背景：issue 42 要求周期 tick 不再为每次 flush 启动无界 goroutine，并能诊断并发触发。
	scheduleRunning int64
	// scheduleMerged 记录 writer 忙时被合并进下一轮后台 flush 的 tick 次数。
	//
	// 用途：显示 Store 慢或 flush_interval 过小时，周期触发是否被合并而不是并发写入。
	// 使用方式：scheduleBackgroundFlush 发现已有后台 writer 时递增，并设置 pendingFlush。
	// 设计原因：合并是有意降压策略，不是数据丢失；collector 数据仍留在内存，下一轮串行 drain。
	// 设计思路：多个忙时 tick 折叠为一个 pendingFlush，计数保留触发压力，写入只追加一轮。
	// 需求背景：issue 42 要求并发触发时最多一个 flush writer 运行，触发信号可合并或排队。
	scheduleMerged int64
	// scheduleQueued 记录忙时合并信号最终被串行执行为后续 flush 的次数。
	//
	// 用途：区分“触发被合并过”与“合并后的后续写入确实被执行过”。
	// 使用方式：后台 writer 完成本轮后看到 pendingFlush=true 时递增，然后继续下一轮。
	// 设计原因：只统计 merged 无法判断合并信号是否被 shutdown 或 timeout 前消化。
	// 设计思路：同一个后台 goroutine 串行消化 pendingFlush，不再额外创建 writer goroutine。
	// 需求背景：issue 42 要求触发信号在已有 flush 运行时被合并、排队或明确跳过，并可诊断。
	scheduleQueued int64
	// scheduleSkipped 记录调用方明确放弃写入的 flush 次数。
	//
	// 用途：暴露 on-demand snapshot 因等待串行 writer 超时而没有 drain collector 的情况。
	// 使用方式：FlushSnapshotOnDemand 在 flush_timeout 内拿不到 writeSlot 时递增。
	// 设计原因：超时摘要不能伪装成成功写入，且 collector 数据必须保留给后续串行 flush。
	// 设计思路：跳过只描述调度结果；真正影响数据完整性的 Store 超时仍走 recordFlushError。
	// 需求背景：issue 42 要求 on-demand snapshot 超时后摘要说明等待超时、合并、排队或跳过状态。
	scheduleSkipped int64
	// scheduleTimeout 记录 flush 调度或 Store 写入受 flush_timeout 截止的次数。
	//
	// 用途：帮助定位慢 Store、writer 长时间占用或 on-demand 等待超时。
	// 使用方式：后台 flush 等待 writeSlot 超时、Store 写入 context 到期、on-demand 等待超时时递增。
	// 设计原因：timeout 可能代表数据完整性风险，必须和普通 busy merge 区分。
	// 设计思路：计数保存在 flusher 内存诊断中；需要持久化风险时仍通过 recordFlushError/Store 诊断处理。
	// 需求背景：issue 42 要求 timeout 或等价诊断计数可见，shutdown deadline 放弃需记录降级。
	scheduleTimeout int64

	// scheduleMu 保护后台周期 flush 调度状态。
	//
	// 用途：把高频 tick 的“是否需要启动 writer”判断收敛到一个小临界区。
	// 使用方式：scheduleBackgroundFlush 和后台 writer 退出/续跑时持有该锁。
	// 设计原因：writeSlot 只能串行 Store 写入，不能阻止每个 tick 都创建等待 goroutine。
	// 设计思路：scheduleMu 控制 goroutine 数量，writeSlot 控制 Store 写入临界区，两者职责分离。
	// 需求背景：issue 42 要求 flush scheduler 与 flush writer 职责分离或通过小接口隔离。
	scheduleMu sync.Mutex
	// backgroundFlush 表示当前已有周期 flush 后台 goroutine 正在运行或等待 writer。
	//
	// 用途：作为单飞调度开关，防止 flush_interval tick 创建无界 goroutine。
	// 使用方式：scheduleBackgroundFlush 从 false 切 true 时才启动 goroutine；goroutine 退出前复位。
	// 设计原因：Store 慢时等待 writeSlot 的 goroutine 本身也会积压资源并扩大 shutdown 等待面。
	// 设计思路：一个后台 goroutine 可以串行消化当前 flush 和一个合并的 pending flush。
	// 需求背景：issue 42 要求同一 flusher 实例最多一个 Store 写入 flush 在运行。
	backgroundFlush bool
	// pendingFlush 表示后台 writer 运行期间又收到至少一次周期 flush 触发。
	//
	// 用途：保留 busy tick 的“需要再 flush 一次”信号，而不是直接丢弃或并发执行。
	// 使用方式：忙时 tick 设置为 true；后台 writer 完成本轮后清零并继续下一轮。
	// 设计原因：collector.FlushSnapshot 会重置内存窗口，只有串行 writer 才能保证 drain 后必定写 Store。
	// 设计思路：pendingFlush 是布尔合并，不按 tick 数排长队；计数通过 scheduleMerged 保留压力信息。
	// 需求背景：issue 42 允许合并或排队触发信号，但禁止无界并发写 Store。
	pendingFlush bool

	// 降级状态
	degraded       bool
	degradedReason string

	// 用于测试的时钟注入
	now                 func() time.Time
	beforePeriodicFlush func()
}

// newFlusher 创建未启动的异步 flusher。
//
// 参数说明：cfg 提供 flush_interval、flush_timeout、batch_size 等配置；
// store 是 Horizon Store 实现；coll 是与 flusher 配对的 collector。
func newFlusher(cfg ObservabilityConfig, store Store, coll *collector, waits map[string]int) *flusher {
	cfg = normalizeObservabilityConfig(cfg)
	return &flusher{
		cfg:       cfg,
		store:     store,
		coll:      coll,
		waits:     waits,
		writeSlot: make(chan struct{}, 1),
	}
}

// Start 启动后台 flush 循环。
//
// 参数说明：ctx 控制 flusher 生命周期，取消时触发 shutdown best-effort flush。
func (f *flusher) Start(ctx context.Context) {
	f.ctx, f.cancel = context.WithCancel(ctx)
	f.wg.Add(1)
	startRestartingTrackedGoroutineWithPanicHandler(f.ctx, &f.wg, "flusher", nil, func(err error) {
		f.recordFlushPanic(fmt.Errorf("flusher loop panic: %v", err))
	}, f.loop)
}

// Stop 优雅关闭 flusher：取消 context，执行 best-effort flush，等待后台退出。
func (f *flusher) Stop() {
	if f.cancel != nil {
		f.cancel()
	}
	f.wg.Wait()
}

// Flush 实现 ObservabilityFlusher 接口，执行单次 flush。
//
// 参数说明：ctx 控制单次 flush 超时；batch 是待写入的批次数据。
func (f *flusher) Flush(ctx context.Context, batch FlushBatch) error {
	if f == nil || f.store == nil {
		return nil
	}
	release, err := f.acquireWriteSlot(ctx)
	if err != nil {
		f.recordFlushError(err)
		return err
	}
	defer release()
	return f.flushOnce(ctx, &batch)
}

// loop 是后台 flush 主循环。
//
// 逻辑说明：按 flush_interval 定期执行 flush。context 取消时执行 shutdown best-effort flush。
// 容错设计：Start 通过 startRecoveringGoroutineWithPanicHandler 捕获 panic 并上报。
func (f *flusher) loop() {
	ticker := time.NewTicker(f.cfg.FlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-f.ctx.Done():
			// shutdown best-effort flush
			f.shutdownFlush()
			return
		case <-ticker.C:
			if f.beforePeriodicFlush != nil {
				f.beforePeriodicFlush()
			}
			f.periodicFlush()
		}
	}
}

// periodicFlush 定期执行 flush：从 collector 拉取快照，构建 FlushBatch，写入 Store。
//
// 并发语义：
//  1. tick 只调度一次后台 writer，不在 collector 热路径上同步等待 Store。
//  2. 后台 writer 先等待 writeSlot，再调用 FlushSnapshot；等待期间 collector 数据仍留在内存中。
//  3. 如果 flush_timeout 内拿不到 writeSlot，本轮只记录 Store 降级，不清空 collector。
//
// 这能让周期 flush 与手动 horizon:snapshot 共用同一条 append-only 写入面，同时避免 snapshot
// 或 shutdown 抢占时造成窗口、明细、诊断被提前 drain。
func (f *flusher) periodicFlush() {
	f.scheduleBackgroundFlush()
}

// scheduleBackgroundFlush 接收一次周期 tick，并按单飞语义安排后台 flush。
//
// 用途：这是 flush_interval tick 进入 Store 写入面的唯一调度入口。
// 使用方式：loop 每次 ticker.C 调用；如果没有后台 writer，就启动一个受 WaitGroup 追踪的 goroutine；
// 如果已有 writer，就只设置 pendingFlush 并增加 merged 诊断计数。
// 设计原因：原先每次 tick 都启动 goroutine，慢 Store 下会堆积大量等待 writeSlot 的后台任务。
// 设计思路：调度器只负责接收/合并触发和限制 goroutine 数量；真正 drain collector 和写 Store
// 仍由 runBackgroundFlush 在 writeSlot 串行临界区内完成。
// 需求背景：issue 42 要求周期 tick 不再启动无界 goroutine，并暴露 accepted/merged/queued 等计数。
func (f *flusher) scheduleBackgroundFlush() {
	f.scheduleMu.Lock()
	if f.backgroundFlush {
		f.pendingFlush = true
		f.recordScheduleMergedLocked()
		f.scheduleMu.Unlock()
		return
	}
	f.backgroundFlush = true
	f.recordScheduleAcceptedLocked()
	f.wg.Add(1)
	f.scheduleMu.Unlock()
	startRecoveringGoroutineWithPanicHandler(f.ctx, "flusher", nil, func(err error) {
		f.recordFlushError(fmt.Errorf("flusher background flush panic: %v", err))
		f.scheduleMu.Lock()
		f.backgroundFlush = false
		f.pendingFlush = false
		f.scheduleMu.Unlock()
	}, func() {
		defer f.wg.Done()
		for {
			f.runBackgroundFlushOnce()
			f.scheduleMu.Lock()
			if !f.pendingFlush {
				f.backgroundFlush = false
				f.scheduleMu.Unlock()
				return
			}
			f.pendingFlush = false
			f.recordScheduleQueuedLocked()
			f.scheduleMu.Unlock()
		}
	})
}

// runBackgroundFlush 执行一次已接受的后台周期 flush。
//
// 用途：把 collector 当前聚合快照转换成 FlushBatch，并写入 append-only Store 与兼容 read model。
// 使用方式：只能由 scheduleBackgroundFlush 启动的单飞 goroutine 调用；函数内部必须先拿 writeSlot，
// 再调用 collector.FlushSnapshot，避免等待超时后把尚未落盘的数据从内存中清走。
// 设计原因：collector.FlushSnapshot 是 destructive drain；如果多个 writer 并发 drain/write，就会出现
// 窗口乱序、Store 写入压力放大或 shutdown 无法追踪后台写入。
// 设计思路：每轮使用独立 background context 和 flush_timeout，超时只记录诊断，不遗留 goroutine。
// 容错设计：scheduleBackgroundFlush 通过 startRecoveringGoroutineWithPanicHandler 捕获 panic 并上报。
// 需求背景：issue 42 要求慢 Store 下连续 tick、写入顺序不倒退和 timeout 诊断可验证。
func (f *flusher) runBackgroundFlush() {
	defer func() {
		if rec := recover(); rec != nil {
			f.recordFlushError(fmt.Errorf("flusher background flush panic: %v", rec))
			_ = rec
		}
	}()
	f.runBackgroundFlushOnce()
}

func (f *flusher) runBackgroundFlushOnce() {
	bgCtx, cancel := context.WithTimeout(context.Background(), f.cfg.FlushTimeout)
	defer cancel()
	f.recordScheduleRunning(1)
	defer f.recordScheduleRunning(-1)

	release, err := f.acquireWriteSlot(bgCtx)
	if err != nil {
		f.recordScheduleTimeout()
		f.recordFlushError(err)
		return
	}
	defer release()

	now := f.currentTime()
	snapshot := f.coll.FlushSnapshot(now)
	if snapshot == nil {
		return
	}

	f.applySnapshotDiagnostics(snapshot, now)
	b := f.buildFlushBatch(snapshot, now)
	if err := f.flushOnce(bgCtx, &b); err != nil {
		if bgCtx.Err() != nil {
			f.recordScheduleTimeout()
		}
	}
}

// recordScheduleAcceptedLocked 记录一次从空闲进入后台 flush 的调度。
//
// 调用方必须已持有 scheduleMu；该函数只更新诊断计数，不改变调度状态。
func (f *flusher) recordScheduleAcceptedLocked() {
	f.mu.Lock()
	f.scheduleAccepted++
	f.mu.Unlock()
}

// recordScheduleMergedLocked 记录一次被已有后台 writer 合并的 tick。
//
// 调用方必须已持有 scheduleMu；该计数用于解释 tick 没有启动新 goroutine 的原因。
func (f *flusher) recordScheduleMergedLocked() {
	f.mu.Lock()
	f.scheduleMerged++
	f.mu.Unlock()
}

// recordScheduleQueuedLocked 记录一次 pending flush 被后台 writer 串行接续执行。
//
// 调用方必须已持有 scheduleMu；该计数证明合并信号没有被静默遗忘。
func (f *flusher) recordScheduleQueuedLocked() {
	f.mu.Lock()
	f.scheduleQueued++
	f.mu.Unlock()
}

// recordScheduleTimeout 记录一次调度等待或 Store 写入超时。
//
// 该计数属于进程内 flusher 诊断；是否需要持久化 ObservabilityDiagnostic 由调用路径按数据完整性风险决定。
func (f *flusher) recordScheduleTimeout() {
	f.mu.Lock()
	f.scheduleTimeout++
	f.mu.Unlock()
}

// recordScheduleRunning 更新当前后台周期 writer 运行数。
//
// 参数说明：delta 为 +1 表示进入 runBackgroundFlush，-1 表示退出；下限保护避免异常路径产生负数。
func (f *flusher) recordScheduleRunning(delta int64) {
	f.mu.Lock()
	f.scheduleRunning += delta
	if f.scheduleRunning < 0 {
		f.scheduleRunning = 0
	}
	f.mu.Unlock()
}

// recordScheduleSkipped 记录一次明确未执行 Store 写入的 flush 请求。
//
// 使用场景：on-demand snapshot 等待 writeSlot 超过 flush_timeout，此时不能 drain collector，
// 也不能返回“已写入”的摘要。
func (f *flusher) recordScheduleSkipped() {
	f.mu.Lock()
	f.scheduleSkipped++
	f.mu.Unlock()
}

// shutdownFlush 执行优雅退出时的 best-effort flush。
//
// 逻辑说明：使用 flush_timeout 控制等待上限，超时后放弃并记录诊断。
//
// 关键边界：
//  1. 不能复用 f.ctx。Stop 会先取消运行 context，如果把它传给 Store，best-effort 写入会在
//     进入 Store 前就被 context.Canceled 拒绝。
//  2. shutdown 必须等待当前已接受的 writer，但等待也受独立 deadline 限制；超时后不能启动
//     未追踪 goroutine 继续写 Store。
//  3. shutdown 写出的 event_metrics windows 和 batch summaries 必须标记 partial，读侧才能区分
//     这是退出时提前落盘的未完整窗口。
//
// shutdownFlush 执行优雅退出时的 best-effort flush。
//
// 逻辑说明：使用 flush_timeout 控制等待上限，超时后放弃并记录诊断。
// 容错设计：flusher 主 goroutine 通过 startRecoveringGoroutineWithPanicHandler 捕获 panic 并上报。
func (f *flusher) shutdownFlush() {
	now := f.currentTime()

	// 给 collector 一点时间排空 buffer
	select {
	case <-time.After(100 * time.Millisecond):
	}

	// shutdown 不能复用已取消的运行 context；独立 deadline 允许 best-effort 写入完成或明确超时。
	stopCtx, cancel := context.WithTimeout(context.Background(), f.cfg.FlushTimeout)
	defer cancel()

	release, err := f.acquireWriteSlot(stopCtx)
	if err != nil {
		f.recordShutdownFlushError(stopCtx, err)
		return
	}
	defer release()

	snapshot := f.coll.FlushSnapshot(now)
	if snapshot == nil {
		return
	}

	// 标记为 partial（shutdown 时窗口可能不完整）
	batch := f.buildFlushBatch(snapshot, now)
	markFlushBatchPartial(&batch)
	if err := f.flushOnce(stopCtx, &batch); err != nil {
		f.recordShutdownFlushError(stopCtx, err)
	}
}

// acquireWriteSlot 等待当前 Store writer 空闲；调用方拿到 slot 后才允许 drain collector。
//
// 参数说明：ctx 是调用方的 flush deadline。返回的 release 必须在 Store 写入结束后调用。
// 设计原因：把等待 writer 和 drain collector 的顺序固定在一个 helper 中，避免新增 flush 入口
// 绕过串行约束。该函数不创建 goroutine，也不在超时后持有任何后台状态。
func (f *flusher) acquireWriteSlot(ctx context.Context) (func(), error) {
	if f.writeSlot == nil {
		f.writeSlot = make(chan struct{}, 1)
	}
	select {
	case f.writeSlot <- struct{}{}:
		return func() { <-f.writeSlot }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (f *flusher) applySnapshotDiagnostics(snapshot *flushSnapshot, now time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if snapshot.degraded {
		f.degraded = true
		f.degradedReason = snapshot.degradedReason
	}
	if f.flushErrorStreak > 0 {
		snapshot.lastFlushError = f.lastFlushError
	}
	f.flushLag = now.Sub(snapshot.lastFlushAt)
	if f.flushLagExceeded(snapshot.lastFlushAt, f.flushLag) {
		f.degraded = true
		f.degradedReason = MemoryDropFlushLagExceeded
		snapshot.degraded = true
		snapshot.degradedReason = MemoryDropFlushLagExceeded
		snapshot.flushLag = f.flushLag
	}
}

// recordShutdownFlushError 记录 shutdown flush 的数据完整性风险。
//
// 语义说明：Store 写失败、等待 writer 超时或 deadline 到期都意味着进程退出前可能有观测事实
// 未落盘，因此必须进入 flusher 诊断状态，并尽力写入一条 ObservabilityDiagnostic。诊断写入
// 复用同一个 shutdown context，避免为了记录诊断反而延长进程退出或遗留后台写入。
func (f *flusher) recordShutdownFlushError(ctx context.Context, err error) {
	if err == nil {
		return
	}
	f.recordFlushError(err)
	if f.store == nil {
		return
	}
	diag := ObservabilityDiagnostic{
		Reason:     MemoryDropStoreUnavailable,
		Count:      1,
		ObservedAt: f.currentTime(),
		Gap:        observabilityGapForReason(MemoryDropStoreUnavailable),
	}
	// 诊断写入也受同一个 shutdown deadline 约束，避免退出路径遗留后台写入。
	_ = f.store.SaveObservabilityDiagnostics(ctx, []ObservabilityDiagnostic{diag}, f.cfg.DiagnosticsRetention)
}

// buildFlushBatch 将 collector 快照转换为 FlushBatch。
func (f *flusher) buildFlushBatch(snapshot *flushSnapshot, now time.Time) FlushBatch {
	batch := FlushBatch{
		WindowStart: snapshot.windowStart,
		WindowEnd:   snapshot.windowEnd,
		Memory:      snapshot.memState,
	}
	unknownGap := snapshotHasUnknownGap(snapshot.drops)

	// 转换 event_metrics 窗口为增量
	for _, w := range snapshot.windows {
		increment := f.eventMetricIncrementFromWindow(w, snapshot, now)
		if unknownGap {
			increment.Unknown = true
			increment.Degraded = true
			increment.Quality = eventMetricQualityForWindow(increment.Estimated, increment.Degraded, increment.Partial, increment.Unknown)
		}
		batch.Increments = append(batch.Increments, increment)
		batch.EventMetricWindows = append(batch.EventMetricWindows, eventMetricWindowFromIncrement(increment))
	}

	// 转换高价值明细
	batch.HighValueDetails = append(batch.HighValueDetails, snapshot.details...)
	batch.BatchSummaries = append(batch.BatchSummaries, snapshot.batchSummaries...)

	// 转换诊断
	for reason, count := range snapshot.drops {
		batch.Diagnostics = append(batch.Diagnostics, ObservabilityDiagnostic{
			Reason:     reason,
			Count:      count,
			ObservedAt: now,
			Gap:        observabilityGapForReason(reason),
		})
	}
	if snapshot.degraded && snapshot.degradedReason != "" && len(snapshot.drops) == 0 {
		batch.Diagnostics = append(batch.Diagnostics, ObservabilityDiagnostic{
			Reason:     snapshot.degradedReason,
			Count:      1,
			ObservedAt: now,
			Gap:        observabilityGapForReason(snapshot.degradedReason),
		})
	}
	batch.Diagnostics = append(batch.Diagnostics, snapshot.diags...)

	return batch
}

// flushOnce 执行单次 Store 写入。
//
// 逻辑说明：先写 event_metrics 增量，再写 batch summaries、高价值明细和诊断。
// 聚合指标与明细数据分开 flush，避免低价值明细拖慢核心指标。
func (f *flusher) flushOnce(ctx context.Context, batch *FlushBatch) error {
	start := time.Now()
	f.enforceBatchSummaryLimit(batch)

	// 1. 写入 event_metrics 增量（核心指标优先）
	if err := f.writeEventMetricsWindow(ctx, batch); err != nil {
		f.recordFlushError(err)
		return err
	}

	// 2. 写入 batch summaries（低频独立通道，失败不影响指标）
	if err := f.writeBatchSummaries(ctx, batch); err != nil {
		f.recordFlushError(err)
	}

	// 3. 写入高价值明细（独立通道，失败不影响指标）
	if err := f.writeHighValueDetails(ctx, batch); err != nil {
		// 记录失败但不返回错误，核心指标已写入
		f.recordFlushError(err)
	}

	// 4. 写入诊断
	if err := f.writeDiagnostics(ctx, batch); err != nil {
		f.recordFlushError(err)
	}

	f.mu.Lock()
	f.lastFlushDuration = time.Since(start)
	f.lastFlushSuccessAt = start
	f.flushErrorStreak = 0
	f.flushSuccessStreak++
	if f.flushDurationNearTimeout(f.lastFlushDuration) {
		f.degraded = true
		f.degradedReason = MemoryDropFlushTimeoutNear
	} else if f.degradedReason == MemoryDropStoreUnavailable && f.flushSuccessStreak >= 3 {
		f.degraded = false
		f.degradedReason = ""
	} else if f.degradedReason != MemoryDropStoreUnavailable && f.degradedReason != MemoryDropFlushLagExceeded {
		f.degraded = false
		f.degradedReason = ""
	}
	f.lastFlushError = ""
	f.mu.Unlock()

	return nil
}

// flushLagExceeded 判断周期 flush 是否已经明显落后于配置间隔。
//
// 设计原因：偶发调度抖动不应立刻降级；超过两个 flush_interval 说明 Store 或后台循环存在持续压力。
func (f *flusher) flushLagExceeded(lastFlushAt time.Time, lag time.Duration) bool {
	if lastFlushAt.IsZero() || f.cfg.FlushInterval <= 0 {
		return false
	}
	return lag > 2*f.cfg.FlushInterval
}

// flushDurationNearTimeout 判断本次 Store 写入耗时是否接近 flush_timeout。
//
// 语义说明：达到 80% timeout 时提前进入降级，给后续采样/明细暂停策略留出缓冲。
func (f *flusher) flushDurationNearTimeout(duration time.Duration) bool {
	if f.cfg.FlushTimeout <= 0 {
		return false
	}
	return duration >= (f.cfg.FlushTimeout * 8 / 10)
}

// writeEventMetricsWindow 将 event_metrics 窗口增量写入 Store。
//
// 逻辑说明：使用追加 batch/window 模型写入，记录窗口边界。
// Store 实现负责按窗口追加而非覆盖。
func (f *flusher) writeEventMetricsWindow(ctx context.Context, batch *FlushBatch) error {
	if len(batch.Increments) == 0 && len(batch.EventMetricWindows) == 0 &&
		!f.cfg.Enabled(ObservabilityEventMetrics) && !f.cfg.Enabled(ObservabilityWaits) {
		return nil
	}
	// 写入 append-only window 事实模型。
	windows := batch.EventMetricWindows
	if len(windows) == 0 {
		windows = make([]EventMetricWindow, 0, len(batch.Increments))
		for _, inc := range batch.Increments {
			windows = append(windows, eventMetricWindowFromIncrement(inc))
		}
	}
	if len(windows) > 0 {
		if err := f.store.AppendEventMetricWindows(ctx, windows, f.cfg.EventMetricsRetention); err != nil {
			return err
		}
	}

	return nil
}

// eventMetricIncrementFromWindow 把 collector 内存窗口补齐为可持久化增量。
//
// 逻辑说明：window 边界只来自事件发生时间；flushAt 只用于诊断。采样率小于 1 时保留
// SampleCount 和 EstimatedTotal，避免 read model 把估算值误展示为 exact。
// 来源说明：collector 已按来源分片聚合；flusher 只补齐 prefix/host fallback，不合并不同来源。
func (f *flusher) eventMetricIncrementFromWindow(w *eventMetricsWindow, snapshot *flushSnapshot, flushAt time.Time) EventMetricIncrement {
	windowStart := w.windowStart
	if windowStart.IsZero() {
		windowStart = snapshot.windowStart
	}
	windowEnd := windowStart.Add(f.cfg.MetricsWindow)
	if f.cfg.MetricsWindow <= 0 {
		windowEnd = snapshot.windowEnd
	}
	sampleRate := w.sampleRate
	if sampleRate <= 0 {
		sampleRate = f.cfg.EventMetricsSampleRate
	}
	samples := w.eventSamples
	if samples == 0 {
		samples = w.processed + w.failed + w.released + w.poison + w.queued
	}
	estimatedTotal := samples
	if sampleRate > 0 && sampleRate < 1 {
		estimatedTotal = int64(float64(samples)/sampleRate + 0.5)
	}
	degraded := w.degraded || snapshot.degraded
	partial := false
	quality := eventMetricQualityForWindow(w.estimated, degraded, partial, false)
	host, _ := os.Hostname()
	return EventMetricIncrement{
		WindowStart:         windowStart,
		WindowEnd:           windowEnd,
		FlushAt:             flushAt,
		MetricsWindowMS:     int64(windowEnd.Sub(windowStart) / time.Millisecond),
		SourcePrefix:        firstNonEmpty(w.sourcePrefix, f.storePrefix()),
		SourceHost:          firstNonEmpty(w.sourceHost, host),
		SourceEnvironment:   w.environment,
		SourceSupervisor:    w.supervisor,
		Connection:          w.connection,
		Queue:               w.queue,
		JobName:             w.jobName,
		Processed:           w.processed,
		Failed:              w.failed,
		Released:            w.released,
		Poison:              w.poison,
		Queued:              w.queued,
		RuntimeMS:           w.runtimeMS,
		Samples:             samples,
		RuntimeSampleCount:  w.samples,
		EffectiveSampleRate: sampleRate,
		EstimatedTotal:      estimatedTotal,
		Estimated:           w.estimated,
		Degraded:            degraded,
		Unknown:             false,
		Partial:             partial,
		Quality:             quality,
	}
}

// eventMetricWindowFromIncrement 将 flush 增量投影为 Store 追加窗口。
//
// 设计原因：测试和后续低频 summary 通道可以直接构造 Increment；这里统一补齐采样、估算和 quality 默认值，
// 避免 Store 实现重复理解 event_metrics 质量语义。
// 关键边界：metrics_window_ms 必须随窗口写入 Store，读侧以此检测同一 namespace/environment 的
// 配置漂移；flush_at 不参与窗口归属，也不能被用来推断 metrics_window。
func eventMetricWindowFromIncrement(inc EventMetricIncrement) EventMetricWindow {
	quality := inc.Quality
	samples := inc.Samples
	if samples == 0 {
		samples = inc.Processed + inc.Failed + inc.Released + inc.Poison + inc.Queued
	}
	unknown := inc.Unknown || (inc.EffectiveSampleRate == 0 && samples == 0)
	if quality == "" {
		quality = eventMetricQualityForWindow(inc.Estimated, inc.Degraded, inc.Partial, unknown)
	}
	sampleRate := inc.EffectiveSampleRate
	if sampleRate <= 0 && samples > 0 && !unknown {
		sampleRate = 1
	}
	estimatedTotal := inc.EstimatedTotal
	if estimatedTotal == 0 {
		estimatedTotal = samples
		if sampleRate > 0 && sampleRate < 1 {
			estimatedTotal = int64(float64(samples)/sampleRate + 0.5)
		}
	}
	return EventMetricWindow{
		WindowStart:         inc.WindowStart,
		WindowEnd:           inc.WindowEnd,
		FlushAt:             inc.FlushAt,
		MetricsWindowMS:     eventMetricMetricsWindowMS(inc.MetricsWindowMS, inc.WindowStart, inc.WindowEnd),
		SourcePrefix:        inc.SourcePrefix,
		SourceHost:          inc.SourceHost,
		SourceEnvironment:   inc.SourceEnvironment,
		SourceSupervisor:    inc.SourceSupervisor,
		Connection:          inc.Connection,
		Queue:               inc.Queue,
		JobName:             inc.JobName,
		Processed:           inc.Processed,
		Failed:              inc.Failed,
		Released:            inc.Released,
		Poison:              inc.Poison,
		Queued:              inc.Queued,
		RuntimeMS:           inc.RuntimeMS,
		SampleCount:         samples,
		RuntimeSampleCount:  inc.RuntimeSampleCount,
		EffectiveSampleRate: sampleRate,
		EstimatedTotal:      estimatedTotal,
		Estimated:           inc.Estimated,
		Degraded:            inc.Degraded,
		Unknown:             unknown,
		Partial:             inc.Partial,
		Quality:             quality,
	}
}

// eventMetricMetricsWindowMS 返回持久化和 read model 使用的 metrics_window 毫秒值。
//
// 参数说明：value 是写入端显式携带的配置值；start/end 是兼容旧窗口数据的回退来源。
// 设计原因：旧数据可能没有 metrics_window_ms，但新数据必须优先使用显式配置，避免 flush 延迟
// 或测试构造的时间跨度被误当作配置漂移。
func eventMetricMetricsWindowMS(value int64, start time.Time, end time.Time) int64 {
	if value > 0 {
		return value
	}
	if !start.IsZero() && end.After(start) {
		return int64(end.Sub(start) / time.Millisecond)
	}
	return 0
}

// eventMetricQuality 统一兼容旧调用点的 window 质量优先级：partial > degraded > estimated > exact。
func eventMetricQuality(estimated bool, degraded bool, partial bool) string {
	return eventMetricQualityForWindow(estimated, degraded, partial, false)
}

// eventMetricQualityForWindow 统一 window 质量优先级：partial > unknown > degraded > estimated > exact。
//
// 设计原因：partial 是 shutdown 未完成窗口，优先级最高；unknown 表示不可量化丢失，
// 必须高于 degraded/estimated，避免读侧把不完整计数展示为可估算值。
func eventMetricQualityForWindow(estimated bool, degraded bool, partial bool, unknown bool) string {
	switch {
	case partial:
		return EventMetricQualityPartial
	case unknown:
		return EventMetricQualityUnknown
	case degraded:
		return EventMetricQualityDegraded
	case estimated:
		return EventMetricQualityEstimated
	default:
		return EventMetricQualityExact
	}
}

// snapshotHasUnknownGap 判断当前 flush 快照是否包含不可量化缺口。
//
// 设计原因：buffer 满、入口限流和高价值明细容量上限会直接丢失原始事件或明细，
// 后续读模型无法从 overflow bucket 或采样率推回完整分布，因此相关 window 必须标记 unknown。
func snapshotHasUnknownGap(drops map[string]int64) bool {
	for reason, count := range drops {
		if count > 0 && observabilityGapForReason(reason) == ObservabilityGapUnknown {
			return true
		}
	}
	return false
}

// observabilityGapForReason 把稳定 drop/degradation reason 分类为可量化或不可量化缺口。
//
// 语义说明：aggregate_key_overflow 仍保留 overflow bucket 计数，属于可量化降级；
// Store/flush 问题发生在已生成 batch 之后，不改变该 batch 的内存计数来源。
// 入口 buffer、rate limit 和明细容量上限会丢失事件或明细本身，必须让相关窗口进入 unknown。
func observabilityGapForReason(reason string) string {
	switch reason {
	case MemoryDropAggregateOverflow,
		MemoryDropBatchSummaryLimit,
		MemoryDropStoreUnavailable,
		MemoryDropFlushLagExceeded,
		MemoryDropFlushTimeoutNear:
		return ObservabilityGapQuantifiable
	default:
		return ObservabilityGapUnknown
	}
}

// markFlushBatchPartial 标记 shutdown best-effort flush 的未完成窗口。
//
// 逻辑说明：进程退出时不能等待窗口自然结束；写入 Store 的数据仍有排障价值，但必须显式标记 partial。
func markFlushBatchPartial(batch *FlushBatch) {
	if batch == nil {
		return
	}
	for i := range batch.Increments {
		batch.Increments[i].Partial = true
		batch.Increments[i].Quality = eventMetricQualityForWindow(batch.Increments[i].Estimated, batch.Increments[i].Degraded, true, batch.Increments[i].Unknown)
	}
	for i := range batch.EventMetricWindows {
		batch.EventMetricWindows[i].Partial = true
		batch.EventMetricWindows[i].Quality = eventMetricQualityForWindow(batch.EventMetricWindows[i].Estimated, batch.EventMetricWindows[i].Degraded, true, batch.EventMetricWindows[i].Unknown)
	}
	for i := range batch.BatchSummaries {
		batch.BatchSummaries[i].Partial = true
		batch.BatchSummaries[i].Quality = EventMetricQualityPartial
	}
}

// storePrefix 从已解析 Store 取 Horizon prefix，作为 event_metrics window 的来源维度。
func (f *flusher) storePrefix() string {
	if f == nil || f.store == nil {
		return ""
	}
	switch store := f.store.(type) {
	case *MemoryStore:
		return store.options.Prefix
	case *RedisStore:
		return store.options.Prefix
	default:
		return ""
	}
}

// writeHighValueDetails 将高价值诊断明细写入 Store。
//
// 设计边界：这些明细不是可靠事实源，Store 不可用时静默丢弃。
func (f *flusher) writeHighValueDetails(ctx context.Context, batch *FlushBatch) error {
	if len(batch.HighValueDetails) == 0 {
		return nil
	}
	return f.store.SaveHighValueDetails(ctx, batch.HighValueDetails, f.cfg.HighValueDetailRetention)
}

type batchSummaryBatchStore interface {
	SaveBatchSummaries(context.Context, []BatchSummary, time.Duration) error
}

// writeBatchSummaries 将 BatchEvent 派生的低频 summary 写入独立 Store 通道。
//
// 设计边界：该通道只保存批次级聚合进度和窗口质量，不保存批次内 job payload 或 per-job 明细。
func (f *flusher) writeBatchSummaries(ctx context.Context, batch *FlushBatch) error {
	if len(batch.BatchSummaries) == 0 {
		return nil
	}
	items := make([]BatchSummary, 0, len(batch.BatchSummaries))
	for _, item := range batch.BatchSummaries {
		items = append(items, f.prepareBatchSummary(item, batch))
	}
	if writer, ok := f.store.(batchSummaryBatchStore); ok {
		return writer.SaveBatchSummaries(ctx, items, f.cfg.BatchSummaryRetention)
	}
	for _, item := range items {
		if err := f.store.SaveBatchSummary(ctx, item); err != nil {
			return err
		}
	}
	return nil
}

// enforceBatchSummaryLimit 是 Store 写入前的防御层。
//
// 设计原因：collector 正常会先限流，但 Flush(ctx, batch) 允许调用方直接构造 FlushBatch；
// 这里再次按 batch_summary_size 截断，防止一次超大批次绕过 collector 内存上限。
func (f *flusher) enforceBatchSummaryLimit(batch *FlushBatch) {
	if batch == nil || len(batch.BatchSummaries) == 0 {
		return
	}
	limit := f.cfg.BatchSummarySize
	if limit <= 0 {
		limit = f.cfg.BatchSize
	}
	if limit <= 0 {
		return
	}
	items, dropped := compactBatchSummaries(batch.BatchSummaries, limit)
	if dropped == 0 {
		return
	}
	batch.BatchSummaries = items
	batch.Diagnostics = append(batch.Diagnostics, ObservabilityDiagnostic{
		Reason:     MemoryDropBatchSummaryLimit,
		Count:      int64(dropped),
		ObservedAt: f.currentTime(),
		Gap:        observabilityGapForReason(MemoryDropBatchSummaryLimit),
	})
	f.mu.Lock()
	f.degraded = true
	f.degradedReason = MemoryDropBatchSummaryLimit
	f.mu.Unlock()
}

// compactBatchSummaries 按 batch ID 去重，并在达到 limit 后丢弃新 batch。
//
// 语义说明：重复 ID 不计为丢弃，因为它只是同一 batch 的较新状态覆盖旧状态。
func compactBatchSummaries(input []BatchSummary, limit int) ([]BatchSummary, int) {
	if limit <= 0 || len(input) == 0 {
		return input, 0
	}
	out := make([]BatchSummary, 0, minInt(len(input), limit))
	indexByID := make(map[string]int, limit)
	dropped := 0
	for _, item := range input {
		id := strings.TrimSpace(item.ID)
		if id == "" {
			dropped++
			continue
		}
		if idx, ok := indexByID[id]; ok {
			out[idx] = item
			continue
		}
		if len(out) >= limit {
			dropped++
			continue
		}
		indexByID[id] = len(out)
		out = append(out, item)
	}
	return out, dropped
}

// minInt 返回较小整数，避免为一个局部容量计算引入额外依赖。
func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (f *flusher) prepareBatchSummary(item BatchSummary, batch *FlushBatch) BatchSummary {
	if item.WindowStart.IsZero() {
		item.WindowStart = batch.WindowStart
	}
	if item.WindowEnd.IsZero() {
		item.WindowEnd = batch.WindowEnd
	}
	if item.FlushAt.IsZero() {
		item.FlushAt = batch.WindowEnd
	}
	if item.UpdatedAt.IsZero() {
		item.UpdatedAt = item.FlushAt
	}
	if item.Quality == "" {
		item.Quality = eventMetricQuality(false, false, item.Partial)
	}
	return item
}

// writeDiagnostics 将诊断信息写入 Store。
func (f *flusher) writeDiagnostics(ctx context.Context, batch *FlushBatch) error {
	if len(batch.Diagnostics) == 0 {
		return nil
	}
	return f.store.SaveObservabilityDiagnostics(ctx, batch.Diagnostics, f.cfg.DiagnosticsRetention)
}

// recordFlushError 记录 flush 失败诊断。
func (f *flusher) recordFlushError(err error) {
	f.mu.Lock()
	f.flushErrorStreak++
	f.flushSuccessStreak = 0
	f.lastFlushError = err.Error()
	if f.flushErrorStreak >= 3 {
		f.degraded = true
		f.degradedReason = MemoryDropStoreUnavailable
	}
	f.mu.Unlock()
}

// recordFlushPanic 记录 flusher 主 loop panic 后的降级状态。
//
// 语义说明：panic 与普通 Store 写失败不同，第一次出现就表示后台调度 loop 已发生中断，
// 因此立即进入 flusher_panic 降级；restart wrapper 会负责下一轮恢复。
func (f *flusher) recordFlushPanic(err error) {
	f.mu.Lock()
	f.flushErrorStreak++
	f.flushSuccessStreak = 0
	f.lastFlushError = err.Error()
	f.degraded = true
	f.degradedReason = MemoryDropFlusherPanic
	f.mu.Unlock()
}

// currentTime 返回当前时间，测试可注入固定时钟。
func (f *flusher) currentTime() time.Time {
	if f.now != nil {
		return f.now()
	}
	return time.Now()
}

// Diagnostics 返回当前 flusher 诊断状态。
func (f *flusher) Diagnostics() FlusherDiagnostics {
	if f == nil {
		return FlusherDiagnostics{}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return FlusherDiagnostics{
		LastFlushAt:       f.lastFlushSuccessAt,
		LastFlushError:    f.lastFlushError,
		LastFlushDuration: f.lastFlushDuration,
		FlushErrorStreak:  f.flushErrorStreak,
		FlushLag:          f.flushLag,
		SchedulerAccepted: f.scheduleAccepted,
		SchedulerRunning:  f.scheduleRunning,
		SchedulerMerged:   f.scheduleMerged,
		SchedulerQueued:   f.scheduleQueued,
		SchedulerSkipped:  f.scheduleSkipped,
		SchedulerTimeout:  f.scheduleTimeout,
		Degraded:          f.degraded,
		DegradedReason:    f.degradedReason,
	}
}

// FlusherDiagnostics 暴露 flusher 诊断状态给读模型和命令输出。
//
// 用途：为 Dashboard/API/测试暴露 flusher 最近写入状态、降级状态和 issue 42 引入的调度单飞计数。
// 使用方式：Manager.samplingPressure 和读模型按需读取；调用方不得根据这些计数重放或补写 Store 数据。
// 设计原因：周期 tick 合并、busy skip 和 timeout 是运行时健康信号，必须可诊断，但普通合并不应写入
// ObservabilityDiagnostic，以免把非数据丢失事件当成持久化缺口。
// 设计思路：结构体只保存数字、时间和稳定 reason，不包含 Store 凭据、flush payload 或错误堆栈。
// 需求背景：issue 42 要求 accepted/running/merged/queued/skipped/timeout 或等价计数可见。
type FlusherDiagnostics struct {
	// LastFlushAt 是最近一次成功 Store 写入开始时间，用于判断 flush 是否长期停滞。
	LastFlushAt time.Time `json:"last_flush_at"`
	// LastFlushError 是最近一次 flush 失败的短错误文本，不包含 payload 或 Store 凭据。
	LastFlushError string `json:"last_flush_error,omitempty"`
	// LastFlushDuration 是最近一次 Store 写入耗时，用于动态采样和 timeout-near 降级判断。
	LastFlushDuration time.Duration `json:"last_flush_duration_ms"`
	// FlushErrorStreak 是连续 flush 失败次数，达到阈值后进入 store_unavailable 降级。
	FlushErrorStreak int `json:"flush_error_streak"`
	// FlushLag 是当前 flush 距离 collector 上次 drain 的时间差，用于识别后台写入落后。
	FlushLag time.Duration `json:"flush_lag_ms"`
	// SchedulerAccepted 是周期 tick 被接受并启动后台 writer 的次数。
	SchedulerAccepted int64 `json:"scheduler_accepted"`
	// SchedulerRunning 是当前正在运行的后台周期 writer 数；正常情况下只能是 0 或 1。
	SchedulerRunning int64 `json:"scheduler_running"`
	// SchedulerMerged 是已有后台 writer 运行时被合并的 tick 次数。
	SchedulerMerged int64 `json:"scheduler_merged"`
	// SchedulerQueued 是合并 tick 被同一个后台 writer 串行续跑的次数。
	SchedulerQueued int64 `json:"scheduler_queued"`
	// SchedulerSkipped 是 flush 请求明确未执行 Store 写入的次数。
	SchedulerSkipped int64 `json:"scheduler_skipped"`
	// SchedulerTimeout 是等待 writer 或 Store 写入达到 flush_timeout 的次数。
	SchedulerTimeout int64 `json:"scheduler_timeout"`
	// Degraded 表示 flusher 当前处于降级状态。
	Degraded bool `json:"degraded"`
	// DegradedReason 是稳定机器可读降级原因。
	DegradedReason string `json:"degraded_reason,omitempty"`
}

// FlushSnapshotOnDemand 供 horizon:snapshot 命令触发按需 flush。
//
// 逻辑说明：立即从 collector 拉取快照并写入 Store，返回 flush 摘要供命令输出。
// 该方法阻塞等待 flush 完成，使用 flush_timeout 控制。
//
// 重要顺序：
//  1. 先用 flush_timeout 派生 flushCtx。
//  2. 用 flushCtx 等待 writeSlot。
//  3. 拿到 writeSlot 后才调用 collector.FlushSnapshot。
//
// 这个顺序保证手动 snapshot 不会在已有 writer 正忙时把 windows/details/diagnostics 从 collector
// 中取走，却因为等待超时而没有写入 append-only Store。返回摘要中的 SchedulingStatus 用于 CLI
// 明确展示本次是实际 flushed，还是因为 writer 忙而 skipped_busy。
func (f *flusher) FlushSnapshotOnDemand(ctx context.Context) (*FlushSummary, error) {
	flushCtx, cancel := context.WithTimeout(ctx, f.cfg.FlushTimeout)
	defer cancel()

	release, err := f.acquireWriteSlot(flushCtx)
	if err != nil {
		f.recordScheduleSkipped()
		f.recordScheduleTimeout()
		f.recordFlushError(err)
		return &FlushSummary{
			CapturedAt:       f.currentTime(),
			FlushedAt:        f.currentTime(),
			SchedulingStatus: FlushSchedulingSkippedBusy,
			Error:            err.Error(),
		}, err
	}
	defer release()

	now := f.currentTime()
	if f.coll == nil {
		return nil, fmt.Errorf("horizon: collector is not configured")
	}
	snapshot := f.coll.FlushSnapshot(now)
	if snapshot == nil {
		return nil, fmt.Errorf("horizon: collector snapshot is nil")
	}

	f.applySnapshotDiagnostics(snapshot, now)
	batch := f.buildFlushBatch(snapshot, now)

	startTime := now
	err = f.flushOnce(flushCtx, &batch)

	summary := &FlushSummary{
		CapturedAt:        startTime,
		FlushedAt:         f.currentTime(),
		SchedulingStatus:  FlushSchedulingFlushed,
		BatchCount:        1,
		WindowStart:       snapshot.windowStart,
		WindowEnd:         snapshot.windowEnd,
		IncrementCount:    len(batch.Increments),
		WindowCount:       len(batch.EventMetricWindows),
		DetailCount:       len(batch.HighValueDetails),
		DiagnosticCount:   len(batch.Diagnostics),
		BatchSummaryCount: len(batch.BatchSummaries),
		DropCount:         totalDropCount(snapshot.drops),
		Degraded:          snapshot.degraded,
		Quality:           flushSummaryQuality(batch, snapshot),
	}
	if err != nil {
		summary.Error = err.Error()
	}

	return summary, nil
}

const (
	FlushSchedulingFlushed     = "flushed"
	FlushSchedulingSkippedBusy = "skipped_busy"
)

// FlushSummary 是 horizon:snapshot 命令输出的 flush 摘要。
type FlushSummary struct {
	CapturedAt        time.Time `json:"captured_at"`
	FlushedAt         time.Time `json:"flushed_at"`
	SchedulingStatus  string    `json:"scheduling_status"`
	BatchCount        int       `json:"batch_count"`
	WindowStart       time.Time `json:"window_start"`
	WindowEnd         time.Time `json:"window_end"`
	IncrementCount    int       `json:"increment_count"`
	WindowCount       int       `json:"window_count"`
	DetailCount       int       `json:"detail_count"`
	DiagnosticCount   int       `json:"diagnostic_count"`
	BatchSummaryCount int       `json:"batch_summary_count"`
	DropCount         int64     `json:"drop_count"`
	Degraded          bool      `json:"degraded"`
	Quality           string    `json:"quality,omitempty"`
	Error             string    `json:"error,omitempty"`
}

func flushSummaryQuality(batch FlushBatch, snapshot *flushSnapshot) string {
	for _, window := range batch.EventMetricWindows {
		if window.Quality != "" {
			return window.Quality
		}
	}
	if snapshot != nil && snapshot.degraded {
		return EventMetricQualityDegraded
	}
	return EventMetricQualityExact
}
