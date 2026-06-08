package horizon

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	queuecontract "github.com/prismgo/framework/contracts/queue"
	encodingpkg "github.com/prismgo/framework/encoding"
	"github.com/prismgo/framework/event"
	goprocess "github.com/prismgo/framework/process"
	"github.com/prismgo/framework/queue"
)

type workerSupervisorContextKey struct{}

// ErrStoreNotConfigured 表示运行时命令缺少 Horizon Store resolver。
var ErrStoreNotConfigured = errors.New("horizon: store resolver is not configured")

// ErrEventDispatcherNotConfigured 表示 Horizon 事件采集缺少显式注入的事件总线。
var ErrEventDispatcherNotConfigured = errors.New("horizon: event dispatcher is not configured")

// QueueManager 表示 Horizon 队列维护和后续 worker lifecycle 需要的队列管理器边界。
//
// 使用方式：生产路径可直接注入 *queue.Manager；测试路径注入替身验证命令不访问 queue manager 内部配置列表。
type QueueManager interface {
	Queue(string) (queuecontract.Queue, error)
	Failed() queue.FailedStore
	RequestRestart(context.Context) error
}

// QueueAdapter 把 prismgo/queue.Manager 适配为 Horizon 的窄 QueueManager 接口。
//
// 设计原因：Horizon 命令层只需要 Queue 的 Size/Clear/consumer intent 能力，
// 因此通过 adapter 收窄使用边界，避免维护命令依赖 queue manager 的内部配置列表。
type QueueAdapter struct {
	manager *queue.Manager
}

// NewQueueAdapter 创建 Horizon 队列 adapter；manager 为空时后续方法会返回明确错误。
func NewQueueAdapter(manager *queue.Manager) *QueueAdapter {
	return &QueueAdapter{manager: manager}
}

// Queue 返回指定名称的队列 transport，并只通过 contracts/queue.Queue 暴露维护命令能力。
func (a *QueueAdapter) Queue(name string) (queuecontract.Queue, error) {
	if a == nil || a.manager == nil {
		return nil, fmt.Errorf("horizon: queue manager is not configured")
	}
	return a.manager.Queue(name)
}

// Failed 返回失败任务存储，供 horizon:forget 读取和删除 failed record。
func (a *QueueAdapter) Failed() queue.FailedStore {
	if a == nil || a.manager == nil {
		return nil
	}
	return a.manager.Failed()
}

// RequestRestart 保留 worker lifecycle 边界；issue 04 的 snapshot/clear/forget 不会调用该方法。
func (a *QueueAdapter) RequestRestart(ctx context.Context) error {
	if a == nil || a.manager == nil {
		return fmt.Errorf("horizon: queue manager is not configured")
	}
	return a.manager.RequestRestart(ctx)
}

// QueueWorkerAdapter 把 prismgo/queue.Worker 适配为 Horizon 单轮 worker runner。
type QueueWorkerAdapter struct {
	worker queue.WorkerSessionFactory
}

// NewQueueWorkerAdapter 创建 production 路径使用的 worker runner。
func NewQueueWorkerAdapter(manager *queue.Manager) *QueueWorkerAdapter {
	if manager == nil {
		return &QueueWorkerAdapter{}
	}
	return &QueueWorkerAdapter{worker: queue.NewWorker(manager)}
}

// Begin 创建 horizon:work 生命周期内复用的 queue worker 会话。
func (a *QueueWorkerAdapter) Begin(ctx context.Context, options queue.WorkerOptions) (queuecontract.WorkerSession, error) {
	if a == nil || a.worker == nil {
		return nil, fmt.Errorf("horizon: queue manager is not configured")
	}
	return a.worker.Begin(ctx, options)
}

// EventDispatcher 表示 Horizon collector 订阅 queue 事件需要的最小事件总线接口。
//
// 设计原因：生产路径可直接传入 *event.Dispatcher，测试也可以传入兼容实现；Manager 不使用全局
// event bus fallback，避免重复应用初始化时产生隐式监听器。
type EventDispatcher interface {
	ListenFunc(string, func(context.Context, event.Event) error)
}

// StoreResolver 表示 Horizon Store 的运行时解析边界。
type StoreResolver interface {
	ResolveStore(context.Context, Config) (Store, error)
}

// Manager 持有 Horizon 静态配置和显式注入的运行时依赖。
type Manager struct {
	// mu 保护事件注册状态，确保重复注册不会导致重复订阅。
	mu sync.Mutex
	// config 是当前 Manager 使用的 Horizon 静态配置快照。
	config Config
	// queueManager 是后续 queue snapshot/worker loop 使用的显式依赖边界。
	queueManager QueueManager
	// workerRunner 创建 queue worker session，供 horizon:work 在自身循环中复用现有 queue worker。
	workerRunner queue.WorkerSessionFactory
	// processRunner 启动真实 OS 子进程，测试路径可注入 fake runner 验证命令行参数。
	processRunner ProcessRunner
	// controlNotifier 唤醒 fresh master/supervisor 进程，使其尽快重读 Store control flag。
	controlNotifier ControlNotifier
	// processInspector 扫描和终止 Horizon orphan worker 进程，horizon:purge 通过该接口避免直接依赖 OS 进程表。
	processInspector ProcessInspector
	// processObserver 读取 PID 的 CPU、内存和 goroutine 字段级观测结果，HTTP 列表按分页结果调用。
	processObserver goprocess.Observer
	// eventDispatcher 是 collector 订阅 queue 事件的显式依赖，不允许回退到全局 event bus。
	eventDispatcher EventDispatcher
	// storeResolver 负责延迟解析 Horizon Store，命令层不直接创建 Redis 或 memory store。
	storeResolver StoreResolver
	// coll 是非阻塞事件采集器。
	coll *collector
	// flush 是异步批量 Store 写入器，按 flush_interval 定期将 collector 聚合数据写入 Store。
	flush *flusher
	// collBound 标记事件订阅状态，确保重复注册幂等。
	collBound bool
}

// Option 自定义 Manager 构造依赖。
type Option func(*Manager)

// NewManager 通过显式依赖注入构造 Horizon manager。
//
// 逻辑说明：构造器保存配置和依赖，并创建未启动的 collector；事件订阅必须由调用方显式调用
// RegisterMonitor，避免应用启动时产生难以追踪的副作用。
// collector 和 flusher 在 RegisterMonitor 时启动，确保先注册事件监听再开始处理。
//
// 需求背景：Horizon Store records 接入 Payload Encoding 后，Manager 属于严格装配路径。即使测试或
// 命令绕过 LoadConfigFrom 直接传入 Config，也必须在这里拒绝非法 horizon.encoding，不能等到
// RedisStore 构造时静默回退。
//
// 参数说明：cfg 是已经解析或调用方显式构造的 Horizon 配置；options 用于注入 Store、queue、
// process 和 event 等运行边界，避免构造器直接访问全局资源。
func NewManager(cfg Config, options ...Option) (*Manager, error) {
	// 逻辑说明：只保存规范化后的 codec 名称，具体 codec 实例由 RedisStore 根据 StoreOptions
	// 创建；这样 Manager 不需要暴露 Encoding() 或持有编码实现，保持 public interface 简单。
	codec, err := encodingpkg.ResolveWithDefault(encodingpkg.NameMsgpack, cfg.Encoding)
	if err != nil {
		return nil, fmt.Errorf("horizon.encoding: %w", err)
	}
	cfg.Encoding = codec.Name()
	manager := &Manager{
		config:           cfg,
		processRunner:    OSProcessRunner{},
		controlNotifier:  NoopControlNotifier{},
		processInspector: OSProcessInspector{},
		processObserver:  goprocess.NewObserver(goprocess.ObserverOptions{}),
	}
	for _, option := range options {
		if option != nil {
			option(manager)
		}
	}
	manager.coll = newCollector(cfg.Observability)
	return manager, nil
}

// WithQueueManager 注入队列管理器依赖。
func WithQueueManager(manager QueueManager) Option {
	return func(h *Manager) {
		h.queueManager = manager
	}
}

// WithWorkerRunner 注入 horizon:work 使用的单轮 queue worker 执行器。
func WithWorkerRunner(runner queue.WorkerSessionFactory) Option {
	return func(h *Manager) {
		h.workerRunner = runner
	}
}

// WithProcessRunner 注入 master/supervisor 启动子进程的执行器。
func WithProcessRunner(runner ProcessRunner) Option {
	return func(h *Manager) {
		h.processRunner = runner
	}
}

// WithControlNotifier 注入 Horizon control signal 唤醒器。
func WithControlNotifier(notifier ControlNotifier) Option {
	return func(h *Manager) {
		h.controlNotifier = notifier
	}
}

// WithProcessInspector 注入 horizon:purge 使用的 OS 进程检查器。
func WithProcessInspector(inspector ProcessInspector) Option {
	return func(h *Manager) {
		h.processInspector = inspector
	}
}

// WithProcessObserver 注入只读进程资源观测器。
//
// 需求背景：issue 27 要求 Horizon 列表按当前分页 PID 补充 CPU、内存等 OS 观测字段，但这些
// 跨平台采样逻辑必须由 prismgo/process 统一封装。该选项只用于替换采样边界，方便测试固定
// “只采样当前页”行为，生产路径默认使用 prismgo/process.NewObserver。
func WithProcessObserver(observer goprocess.Observer) Option {
	return func(h *Manager) {
		h.processObserver = observer
	}
}

// WithEventDispatcher 注入事件调度器依赖。
func WithEventDispatcher(dispatcher EventDispatcher) Option {
	return func(h *Manager) {
		h.eventDispatcher = dispatcher
	}
}

// WithStoreFactory 注入 Horizon Store resolver。
func WithStoreFactory(factory StoreResolver) Option {
	return func(h *Manager) {
		h.storeResolver = factory
	}
}

// Config 返回 manager 使用的静态配置视图。
func (m *Manager) Config() Config {
	if m == nil {
		return Config{}
	}
	return m.config
}

// QueueManager 返回显式注入的队列管理器依赖。
func (m *Manager) QueueManager() QueueManager {
	if m == nil {
		return nil
	}
	return m.queueManager
}

// EventDispatcher 返回显式注入的事件调度器依赖。
// WorkerRunner 返回 horizon:work 使用的 queue worker 执行器。
func (m *Manager) WorkerRunner() queue.WorkerSessionFactory {
	if m == nil {
		return nil
	}
	return m.workerRunner
}

// ProcessRunner 返回 master/supervisor 使用的子进程启动器。
func (m *Manager) ProcessRunner() ProcessRunner {
	if m == nil || m.processRunner == nil {
		return OSProcessRunner{}
	}
	return m.processRunner
}

func (m *Manager) ControlNotifier() ControlNotifier {
	if m == nil || m.controlNotifier == nil {
		return NoopControlNotifier{}
	}
	return m.controlNotifier
}

func (m *Manager) ProcessInspector() ProcessInspector {
	if m == nil || m.processInspector == nil {
		return OSProcessInspector{}
	}
	return m.processInspector
}

// ProcessObserver 返回 Horizon HTTP 明细列表使用的进程资源观测器。
//
// 设计思路：Manager 只暴露 prismgo/process 的公共 Observer 接口，HTTP handler 不知道 /proc、
// Windows fallback 或采样窗口细节，避免把跨平台实现散落到 Dashboard/API 层。
func (m *Manager) ProcessObserver() goprocess.Observer {
	if m == nil || m.processObserver == nil {
		return goprocess.NewObserver(goprocess.ObserverOptions{})
	}
	return m.processObserver
}

func (m *Manager) EventDispatcher() EventDispatcher {
	if m == nil {
		return nil
	}
	return m.eventDispatcher
}

// Collector 返回当前 Manager 持有的非阻塞事件采集器。
//
// 使用方式：测试路径通过该实例直接注入事件验证采集行为；
// 生产路径不直接调用 Collector，而是由 RegisterMonitor 注册的事件监听器调用。
func (m *Manager) Collector() *collector {
	if m == nil {
		return nil
	}
	return m.coll
}

// Flusher 返回当前 Manager 持有的异步批量写入器。
//
// 使用方式：命令层通过该实例执行按需 flush 或获取诊断状态。
func (m *Manager) Flusher() *flusher {
	if m == nil {
		return nil
	}
	return m.flush
}

// CollBound 返回事件订阅是否已完成。
func (m *Manager) CollBound() bool {
	if m == nil {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.collBound
}

// StoreFactory 返回显式注入的 Horizon Store resolver。
func (m *Manager) StoreFactory() StoreResolver {
	if m == nil {
		return nil
	}
	return m.storeResolver
}

// RegisterMonitor 将 Horizon 事件采集器注册到注入的 event dispatcher。
//
// 逻辑说明：该方法把 queue 事件转发到新 collector 采集入口；
// 事件到达后通过 collector.Collect 非阻塞入队，由后台 goroutine 异步处理。
// 同时启动 collector 后台处理循环和 flusher 后台写入循环。
// 同一个 Manager 重复调用时保持幂等，避免重复监听导致 metrics 翻倍。
func (m *Manager) RegisterMonitor(ctx context.Context) error {
	if m == nil || m.eventDispatcher == nil {
		return ErrEventDispatcherNotConfigured
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.collBound {
		return nil
	}
	if m.coll == nil {
		m.coll = newCollector(m.config.Observability)
	}

	// 启动 collector 后台处理循环
	m.coll.Start(ctx)

	// 创建并启动 flusher（依赖 Store）
	store, err := m.ResolveStore(ctx)
	if err != nil {
		// Store 不可用时 monitor 启动必须整体失败；否则 collector 已启动但无法落盘，
		// 还会注册事件监听并造成应用以“半监控”状态运行。
		m.coll.Stop()
		return err
	}
	if store != nil && m.flush == nil {
		m.flush = newFlusher(m.config.Observability, store, m.coll, m.config.Waits)
		m.flush.Start(ctx)
	}

	// 注册 queue 事件监听器，将事件转发到 collector
	for _, name := range monitorEventNames() {
		eventName := name
		m.eventDispatcher.ListenFunc(eventName, func(ctx context.Context, ev event.Event) error {
			input := m.coll.inputFromEventWithPressure(ev, m.samplingPressure())
			input = m.applyCollectorSource(input)
			if input.SourceSupervisor == "" {
				input.SourceSupervisor = workerSupervisorFromContext(ctx)
			}
			_ = m.coll.Collect(ctx, input)
			return nil
		})
	}
	m.collBound = true
	return nil
}

func (m *Manager) samplingPressure() SamplingPressure {
	if m == nil || m.coll == nil {
		return SamplingPressure{}
	}
	pressure := m.coll.SamplingPressure()
	if m.flush == nil {
		return pressure
	}
	diag := m.flush.Diagnostics()
	pressure.FlushLag = diag.FlushLag
	pressure.FlushDuration = diag.LastFlushDuration
	pressure.FlushErrorStreak = diag.FlushErrorStreak
	if pressure.FlushInterval <= 0 {
		pressure.FlushInterval = m.config.Observability.FlushInterval
	}
	if pressure.FlushTimeout <= 0 {
		pressure.FlushTimeout = m.config.Observability.FlushTimeout
	}
	return pressure
}

// applyCollectorSource 为 queue event 补齐当前 Manager 可确定的实例来源维度。
//
// 设计原因：queue 事件本身只知道 connection/queue/job；Horizon 的 namespace、environment
// 和 host 来自当前进程配置。这里在事件进入 collector 前补齐这些稳定来源，保证 Store 写入
// 能按实例分片，同时允许测试或未来 worker 路径显式传入 SourceSupervisor。
func (m *Manager) applyCollectorSource(input CollectorInput) CollectorInput {
	if m == nil {
		return input
	}
	if input.SourcePrefix == "" {
		input.SourcePrefix = m.config.Prefix
	}
	if input.SourceEnvironment == "" {
		input.SourceEnvironment = m.config.Environment
	}
	if input.SourceHost == "" {
		input.SourceHost = hostname()
	}
	return input
}

// contextWithWorkerSupervisor 返回携带 Horizon worker runtime supervisor 身份的 context。
//
// 用途：让 horizon:work 通过 queue.WorkerOptions.EventObserver 把 --supervisor 身份沿原 queue -> event bus
// 桥传递给 Manager.RegisterMonitor 的普通事件监听路径。
// 使用方式：RunWorker 的 EventObserver 在事件继续转发给全局 sink 前写入；RegisterMonitor 从监听器 context 中读取。
// 设计原因：普通 queue event 合同不包含 supervisor 字段，直接修改 queue event 会把 Horizon 运行时维度
// 泄漏到通用 queue 包；但只直接 Collect 又可能在已有 event bus monitor 下造成重复采集。
// 设计思路：context 只携带当前 worker runtime 已知的 supervisor 字符串；空值保持空字符串语义，
// collector 会记录 event_metrics_source_supervisor_unknown 诊断。
// 需求背景：issue 43 要求 SourceSupervisor 来自当前 worker runtime 身份，同时保留原事件转发语义。
func contextWithWorkerSupervisor(ctx context.Context, supervisor string) context.Context {
	return context.WithValue(ctx, workerSupervisorContextKey{}, strings.TrimSpace(supervisor))
}

// workerSupervisorFromContext 读取 Horizon worker runtime supervisor 身份。
//
// 用途：在普通 event dispatcher 监听路径中补齐 CollectorInput.SourceSupervisor。
// 使用方式：只由 Manager.RegisterMonitor 调用；缺失或非字符串值时返回空字符串。
// 设计原因：缺失 supervisor 是可诊断状态，不应从 queue、host、environment 或 supervisor 配置反推。
// 设计思路：保持小而私有的 context key，避免与业务 context 值冲突。
// 需求背景：issue 43 要求缺少 supervisor runtime 上下文时继续采集，并保留空 SourceSupervisor 分片。
func workerSupervisorFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	value, _ := ctx.Value(workerSupervisorContextKey{}).(string)
	return strings.TrimSpace(value)
}

// Shutdown 停止 collector 和 flusher 后台循环。
//
// 逻辑说明：先停止 collector 接收新事件，再触发 flusher best-effort flush。
// 调用方应在进程退出前调用此方法。
func (m *Manager) Shutdown() {
	if m == nil {
		return
	}
	if m.coll != nil {
		m.coll.Stop()
	}
	if m.flush != nil {
		m.flush.Stop()
	}
}

// ResolveStore 通过注入的 resolver 获取 Horizon Store，命令层不直接创建 Redis client。
func (m *Manager) ResolveStore(ctx context.Context) (Store, error) {
	if m == nil || m.storeResolver == nil {
		return nil, ErrStoreNotConfigured
	}
	return m.storeResolver.ResolveStore(ctx, m.config)
}

// monitorEventNames 返回 issue 03 明确纳入 metrics 的 queue 事件集合。
//
// 设计原因：保持精确订阅列表可以防止后续 queue 新增事件被 collector 静默吞入，导致 CLI/UI 统计口径漂移。
func monitorEventNames() []string {
	return []string{
		queue.EventJobQueued,
		queue.EventJobProcessing,
		queue.EventJobProcessed,
		queue.EventJobReleased,
		queue.EventJobFailed,
		queue.EventBatchCreated,
		queue.EventBatchUpdated,
		queue.EventBatchCancelled,
		queue.EventBatchFinished,
		queue.EventConsumerStarted,
		queue.EventConsumerStopped,
		queue.EventConsumerStopFailed,
		queue.EventPoisonEnvelope,
	}
}
