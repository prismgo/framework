package queue

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	queuecontract "github.com/prismgo/framework/contracts/queue"
	"github.com/prismgo/framework/queue/internal/helper"
	"github.com/prismgo/framework/queue/payload"
	"github.com/prismgo/framework/routine"
)

// workerLeakGuard 限制因超时不响应 context 而泄漏的 goroutine 数量上限。
//
// 需求背景：processWithTimeout 在超时后会放弃等待 goroutine 返回，如果 job 不响应
// context 取消，goroutine 会一直运行直到 job 自行结束。done channel 有 1 缓冲不会阻塞，
// 但长时间运行的 goroutine 仍会占用资源。该信号量把同时存在的泄漏 goroutine 数量限制在
// 可接受范围内；达到上限后 worker 会阻塞等待一个泄漏 goroutine 结束再继续。
var workerLeakGuard = make(chan struct{}, 32)

// WorkerOptions 控制 queue:work 的运行边界。
type WorkerOptions struct {
	Connection    string
	Queues        []string
	Once          bool
	StopWhenEmpty bool
	// EventObserver 观察当前 worker/session 热路径产生的队列事件。
	//
	// 需求背景：Horizon 需要采集 worker 维度的 queue 事件，但不应该临时替换进程级
	// UseEventSink，也不应该为了恢复 event sink 捕获 queue 全量运行时状态。
	// 设计思路：该回调随 WorkerOptions 进入 WorkQueue 的 context；queue.fire 会先通知
	// 当前 observer，并使用其返回的 context 继续调用全局 sink，保持 queue -> event bridge
	// 和业务监听器行为不变。
	EventObserver func(context.Context, Event) context.Context
	// SkipConsumerIntent 表示本次 Work 调用不自行获取/释放底层连接的消费意图。
	//
	// 需求背景：普通 queue:work 会在整个 Work 生命周期内持有 RabbitMQ consumer lease；
	// Horizon 的 horizon:work 外层也有自己的长生命周期循环。如果 Horizon 每轮 Once:true
	// 都让底层 Work 创建并取消 consumer，RabbitMQ Go 客户端可能已经把下一条 delivery
	// 放进本地缓冲，随后 basic.cancel 会让这条 delivery 长期停留在 broker 的 unacked 状态。
	//
	// 设计思路：默认值 false 保持 queue:work 原有行为；只有 Horizon 这类已经在外层持有
	// ConsumerIntentLeaser 的调用方才设置 true，避免重复获取和释放同一个消费意图。
	SkipConsumerIntent bool
	Sleep              time.Duration
	Timeout            time.Duration
	// TimeoutGrace 是任务达到 Timeout 后额外等待 Handle 感知 context 取消的宽限期。
	//
	// 需求背景：Go 不能安全强杀不响应 context 的 goroutine；如果 worker 在超时后无限等待
	// Handle 返回，RabbitMQ 等连接会一直保留 reserved/unacked 消息，prefetch=1 时该 worker
	// 也会停止消费后续任务。这里把“等待任务自行退出”和“释放队列投递”拆开：超时先取消
	// runCtx，再等待一个很短的 grace；若任务仍不返回，则按超时失败进入现有 retry/failed 流程。
	TimeoutGrace time.Duration
	Tries        int
	Backoff      []time.Duration
	RetryAfter   time.Duration
	MaxJobs      int
	MaxTime      time.Duration
}

// Worker 长轮询队列并执行任务。
type Worker struct {
	manager *Manager
	runtime *Runtime
}

// WorkerSessionFactory 创建 worker 生命周期 session。
//
// 设计原因：WorkerOptions 属于 prismgo/queue worker 实现配置，不迁入 contracts；
// factory 因此留在 queue 包内，避免 contracts/queue 反向依赖 queue 包。
type WorkerSessionFactory interface {
	Begin(context.Context, WorkerOptions) (queuecontract.WorkerSession, error)
}

// WorkerSession 持有一次 worker 生命周期内复用的 queue view 与 consumer intent。
type WorkerSession struct {
	worker          *Worker
	queueConn       queuecontract.Queue
	options         WorkerOptions
	consumerIntent  ConsumerIntentLeaser
	consumerQueues  []string
	releaseConsumer func() error
	activateOnce    sync.Once
	activateErr     error
	closeOnce       sync.Once
	closeErr        error
}

// NewWorker 创建 worker。
func NewWorker(manager *Manager) *Worker {
	return &Worker{manager: manager, runtime: manager.runtimeOrDefault()}
}

// Work 按选项持续消费任务。
func (w *Worker) Work(ctx context.Context, options WorkerOptions) (err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	options = w.normalizeOptions(options)
	queueConn, err := w.manager.Queue(options.Connection)
	if err != nil {
		return err
	}
	options.Queues = w.workerQueues(options)
	if !options.SkipConsumerIntent {
		if leaser, ok := queueConn.(ConsumerIntentLeaser); ok {
			release, leaseErr := leaser.AcquireConsumerIntent(options.Queues)
			if leaseErr != nil {
				return leaseErr
			}
			if release != nil {
				defer func() {
					if releaseErr := release(); releaseErr != nil && err == nil {
						err = releaseErr
					}
				}()
			}
		}
	}
	if provider, ok := queueConn.(queuecontract.PopSessionProvider); ok {
		queueConn = provider.NewPopSession()
		if queueConn != nil {
			defer func() {
				if closeErr := queueConn.Close(); closeErr != nil && err == nil {
					err = closeErr
				}
			}()
		}
	}
	return w.WorkQueue(ctx, queueConn, options)
}

// Begin 创建一个 worker 生命周期 session，只解析连接并准备 worker-local 资源。
//
// 参数说明：ctx 控制连接解析阶段；options 是 worker 运行配置，Begin 会复制并归一化 Queues。
func (w *Worker) Begin(ctx context.Context, options WorkerOptions) (queuecontract.WorkerSession, error) {
	if w == nil || w.manager == nil {
		return nil, fmt.Errorf("queue: worker is not configured")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	options = w.normalizeOptions(options)
	queues := w.workerQueues(options)
	options.Queues = queues
	queueConn, err := w.manager.Queue(options.Connection)
	if err != nil {
		return nil, err
	}
	var consumerIntent ConsumerIntentLeaser
	if leaser, ok := queueConn.(ConsumerIntentLeaser); ok {
		consumerIntent = leaser
	}
	if provider, ok := queueConn.(queuecontract.PopSessionProvider); ok {
		queueConn = provider.NewPopSession()
	}
	return &WorkerSession{
		worker:         w,
		queueConn:      queueConn,
		options:        options,
		consumerIntent: consumerIntent,
		consumerQueues: append([]string(nil), queues...),
	}, nil
}

// Activate 幂等获取 consumer intent，允许 Horizon 在 monitor 启动后再触发 started 事件。
func (s *WorkerSession) Activate(ctx context.Context) error {
	if s == nil {
		return fmt.Errorf("queue: worker session is nil")
	}
	s.activateOnce.Do(func() {
		if s.consumerIntent == nil {
			return
		}
		s.releaseConsumer, s.activateErr = s.consumerIntent.AcquireConsumerIntent(s.consumerQueues)
	})
	return s.activateErr
}

// Work 执行一轮队列消费，并强制复用当前 session 已持有的生命周期资源。
func (s *WorkerSession) Work(ctx context.Context) error {
	if s == nil || s.worker == nil || s.queueConn == nil {
		return fmt.Errorf("queue: worker session is not configured")
	}
	options := s.options
	options.Once = true
	options.SkipConsumerIntent = true
	return s.worker.WorkQueue(ctx, s.queueConn, options)
}

// Close 幂等释放 worker-local queue view 和 consumer intent。
func (s *WorkerSession) Close() error {
	if s == nil {
		return nil
	}
	s.closeOnce.Do(func() {
		if s.queueConn != nil {
			s.closeErr = s.queueConn.Close()
		}
		if s.releaseConsumer != nil {
			if err := s.releaseConsumer(); err != nil && s.closeErr == nil {
				s.closeErr = err
			}
		}
	})
	return s.closeErr
}

// WorkQueue consumes jobs from an already resolved queue connection.
//
// Callers that use this method own any connection/session lifecycle setup.
// Worker.Work remains the public queue:work path that resolves the connection,
// acquires consumer intent, and creates a worker-local pop session per call.
func (w *Worker) WorkQueue(ctx context.Context, queueConn queuecontract.Queue, options WorkerOptions) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if queueConn == nil {
		return errors.New("queue: connection is nil")
	}
	options = w.normalizeOptions(options)
	ctx = contextWithEventObserver(ctx, options.EventObserver)
	start := time.Now()
	processed := 0
	for {
		if w.shouldStop(ctx, options, processed, start) {
			return nil
		}
		ok, err := w.runNextJob(ctx, queueConn, options)
		if err != nil {
			return err
		}
		if ok {
			processed++
		}
		if options.Once {
			return nil
		}
		if !ok {
			if options.StopWhenEmpty {
				return nil
			}
			sleepContext(ctx, options.Sleep)
		}
	}
}

func (w *Worker) runNextJob(ctx context.Context, queueConn queuecontract.Queue, options WorkerOptions) (bool, error) {
	options.Queues = w.workerQueues(options)
	reserved, err := w.popReserved(ctx, queueConn, options)
	if errors.Is(err, ErrEmpty) {
		return false, nil
	}
	if errors.Is(err, ErrPoisonEnvelope) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, w.processReserved(ctx, queueConn, reserved, options)
}

func (w *Worker) popReserved(ctx context.Context, queueConn queuecontract.Queue, options WorkerOptions) (queuecontract.ReservedJob, error) {
	queues := w.workerQueues(options)
	if len(queues) > 1 {
		reserved, err := queueConn.Pop(ctx, queues, queuecontract.PopNoWait)
		if !errors.Is(err, ErrEmpty) {
			return reserved, err
		}
	}
	return queueConn.Pop(ctx, queues, queuecontract.PopWaitAvailable)
}

// workerQueues 归一化 worker 队列：去空白、去重并保留首次出现顺序，空列表补默认队列。
func (w *Worker) workerQueues(options WorkerOptions) []string {
	defaultQueue := "default"
	if w != nil && w.runtime != nil {
		defaultQueue = w.runtime.defaultQueue
	}
	return helper.NormalizeQueues(options.Queues, defaultQueue)
}

func (w *Worker) envelopeFromReserved(reserved queuecontract.ReservedJob) (*payload.Envelope, error) {
	if typed, ok := reserved.(interface{ envelope() *payload.Envelope }); ok {
		if env := typed.envelope(); env != nil {
			if attempts := reserved.Attempts(); attempts > 0 {
				env.Attempts = attempts
			}
			return env, nil
		}
	}
	env, err := envelopeFromQueuePayload(w.runtime, reserved.Payload())
	if err != nil {
		return nil, err
	}
	if attempts := reserved.Attempts(); attempts > 0 {
		env.Attempts = attempts
	}
	return env, nil
}

func (w *Worker) processReserved(ctx context.Context, queueConn queuecontract.Queue, reserved queuecontract.ReservedJob, options WorkerOptions) error {
	ctx = withQueueCacheDriver(ctx, w.runtime.cacheDriver)
	env, err := w.envelopeFromReserved(reserved)
	if err != nil {
		return err
	}
	if stale, err := staleDebounce(ctx, env); stale {
		if deleteErr := reserved.Delete(ctx); deleteErr != nil {
			return deleteErr
		}
		return nil
	} else if err != nil {
		return err
	}
	if env.UniqueUntil && env.UniqueKey != "" {
		_ = releaseUnique(ctx, env)
	}
	err = w.processWithTimeout(ctx, queueConn, env, options)
	if err == nil || errors.Is(err, ErrSkipped) || errors.Is(err, ErrBatchCancelled) {
		if deleteErr := reserved.Delete(ctx); deleteErr != nil {
			return deleteErr
		}
		if releaseErr := releaseUnique(ctx, env); releaseErr != nil {
			return releaseErr
		}
		return nil
	}
	return w.handleFailure(ctx, reserved, env, options, err)
}

func (w *Worker) processWithTimeout(ctx context.Context, queueConn queuecontract.Queue, env *payload.Envelope, options WorkerOptions) error {
	timeout := envTimeout(env, options.Timeout)
	if timeout <= 0 {
		return newQueueJobRunner(w.manager, w.runtime, queueConn, options.Connection).Process(ctx, env)
	}
	runCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	done := make(chan error, 1)
	// 使用独立 goroutine context 启动任务，但把带 deadline 的 runCtx 传给 job。
	// 设计思路：done 必须带缓冲；worker 在 timeout+grace 后会先返回，后台 Handle
	// 迟到结束时仍能写入结果，不会因为无人接收而阻塞 routine.Task 的收尾逻辑。
	//
	// 泄漏保护：workerLeakGuard 信号量限制同时泄漏的 goroutine 数量。
	// goroutine 结束时释放信号量；如果信号量已满，新的 goroutine 会等待旧 goroutine 退出。
	workerLeakGuard <- struct{}{}
	routine.Task(context.Background(), func(context.Context) error {
		defer func() { <-workerLeakGuard }()
		done <- newQueueJobRunner(w.manager, w.runtime, queueConn, options.Connection).Process(runCtx, env)
		return nil
	}).
		Component("queue").
		Name("worker.process").
		Fields(map[string]any{
			"connection": options.Connection,
			"queue":      env.Queue,
			"job_name":   env.Name,
			"job_id":     env.ID,
		}).
		Go()
	select {
	case err := <-done:
		return err
	case <-runCtx.Done():
		// 逻辑说明：超时后仍给任务一次短暂机会返回真实错误，避免把已经响应 context 的任务
		// 全部折叠成 DeadlineExceeded；但 grace 到期必须释放 worker，不能继续占住队列消费槽。
		grace := options.TimeoutGrace
		if grace <= 0 {
			grace = 100 * time.Millisecond
		}
		timer := time.NewTimer(grace)
		defer timer.Stop()
		select {
		case err := <-done:
			if err != nil {
				return err
			}
			return runCtx.Err()
		case <-timer.C:
			return runCtx.Err()
		}
	}
}

func (w *Worker) normalizeOptions(options WorkerOptions) WorkerOptions {
	if options.Connection == "" {
		options.Connection = w.runtime.defaultConnection
	}
	if len(options.Queues) == 0 {
		options.Queues = []string{w.runtime.defaultQueue}
	}
	if options.Sleep <= 0 {
		options.Sleep = time.Second
	}
	if options.Tries <= 0 {
		options.Tries = 1
	}
	if options.TimeoutGrace <= 0 {
		options.TimeoutGrace = 100 * time.Millisecond
	}
	return options
}

func (w *Worker) shouldStop(ctx context.Context, options WorkerOptions, processed int, start time.Time) bool {
	if ctx.Err() != nil {
		return true
	}
	if options.MaxJobs > 0 && processed >= options.MaxJobs {
		return true
	}
	if options.MaxTime > 0 && time.Since(start) >= options.MaxTime {
		return true
	}
	return w.manager.restartRequested(ctx, start)
}

func envTimeout(env *payload.Envelope, fallback time.Duration) time.Duration {
	if env.TimeoutSec > 0 {
		return time.Duration(env.TimeoutSec) * time.Second
	}
	return fallback
}

func sleepContext(ctx context.Context, d time.Duration) {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}
