package queue

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	encodingcontract "github.com/prismgo/framework/contracts/encoding"
	queuecontract "github.com/prismgo/framework/contracts/queue"
	"github.com/prismgo/framework/queue/payload"
	redisqueue "github.com/prismgo/framework/queue/redis"
)

// MemoryBatchStore 是独立于 transport driver 的进程内批次 repository。
//
// 需求背景：Laravel 的 batch metadata 不属于 Redis/RabbitMQ transport；未配置外部
// repository 时，Manager 使用该内存实现覆盖 sync 和测试场景，避免再从默认队列连接探测
// BatchStore 能力。
type MemoryBatchStore struct {
	mu      sync.RWMutex
	batches map[string]payload.BatchStatus
}

// NewMemoryBatchStore 创建进程内批次 repository。
func NewMemoryBatchStore() *MemoryBatchStore {
	return &MemoryBatchStore{
		batches: make(map[string]payload.BatchStatus),
	}
}

func (s *MemoryBatchStore) CreateBatch(_ context.Context, status payload.BatchStatus) error {
	s.mu.Lock()
	s.batches[status.ID] = status
	s.mu.Unlock()
	return nil
}

func (s *MemoryBatchStore) GetBatch(_ context.Context, id string) (*payload.BatchStatus, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	status, ok := s.batches[id]
	if !ok {
		return nil, ErrEmpty
	}
	return &status, nil
}

func (s *MemoryBatchStore) UpdateBatch(_ context.Context, status payload.BatchStatus) error {
	s.mu.Lock()
	s.batches[status.ID] = status
	s.mu.Unlock()
	return nil
}

// DeleteBatch 删除批次 metadata。
//
// 需求背景：Laravel PendingBatch::dispatch 在 store batch 后如果 Batch::add 失败，会删除
// 已创建的 batch metadata。Prismgo batch bulk 投递也采用同一边界：只回滚 metadata，不承诺
// 撤回 transport 已经接收的消息。
func (s *MemoryBatchStore) DeleteBatch(_ context.Context, id string) error {
	s.mu.Lock()
	delete(s.batches, id)
	s.mu.Unlock()
	return nil
}

// MarkBatchJob 在 repository 内部原子更新批次进度。
//
// 参数 id 是批次 ID；success 表示当前任务是否成功完成。该方法集中维护 pending、
// processed、failed 和 finished_at，避免调用方先读再写造成并发覆盖。
//
// 防重复标记：当 Processed >= Total 时跳过计数变更，防止 worker 重试导致
// 同一 job 被重复标记时 Processed 超过 Total。
func (s *MemoryBatchStore) MarkBatchJob(_ context.Context, id string, success bool) (payload.BatchStatus, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	status, ok := s.batches[id]
	if !ok {
		return payload.BatchStatus{}, ErrEmpty
	}
	// 防护：如果所有任务已被标记完成，忽略重复标记请求
	if status.Processed >= status.Total && status.Total > 0 {
		return status, nil
	}
	if status.Pending > 0 {
		status.Pending--
	}
	status.Processed++
	if !success {
		status.Failed++
	}
	if status.Pending == 0 && status.FinishedAt.IsZero() {
		status.FinishedAt = time.Now()
	}
	s.batches[id] = status
	return status, nil
}

func (s *MemoryBatchStore) CancelBatch(_ context.Context, id string) (payload.BatchStatus, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	status, ok := s.batches[id]
	if !ok {
		return payload.BatchStatus{}, ErrEmpty
	}
	if status.Cancelled {
		return status, nil
	}
	status.Cancelled = true
	if status.CancelledAt.IsZero() {
		status.CancelledAt = time.Now()
	}
	s.batches[id] = status
	return status, nil
}

// BatchStore 保存批次元数据。
type BatchStore interface {
	// CreateBatch 创建新批次记录。
	CreateBatch(context.Context, payload.BatchStatus) error
	// GetBatch 按 ID 获取批次状态。
	GetBatch(context.Context, string) (*payload.BatchStatus, error)
	// UpdateBatch 更新批次状态。
	UpdateBatch(context.Context, payload.BatchStatus) error
	// DeleteBatch 删除批次 metadata，用于投递阶段失败后的 Laravel 风格 metadata 回滚。
	DeleteBatch(context.Context, string) error
	// MarkBatchJob 原子标记批次内一个任务的执行结果，并返回更新后的批次状态。
	//
	// 需求背景：worker 可能并发完成同一批次中的多个任务，不能由 Manager 先
	// GetBatch 再 UpdateBatch，否则不同 worker 的 Pending/Processed/Failed 计数会互相覆盖。
	MarkBatchJob(context.Context, string, bool) (payload.BatchStatus, error)
	// CancelBatch 原子取消批次，并返回更新后的批次状态。
	//
	// 需求背景：取消和任务完成可能并发发生，必须在 store 内部一次性落库，避免覆盖进度字段。
	CancelBatch(context.Context, string) (payload.BatchStatus, error)
}

// BatchBuilder 是批量投递入口。
type BatchBuilder struct {
	manager *Manager
	jobs    []Job
	name    string
	options []DispatchOption
}

// Batch 创建绑定当前 Manager 的批次构建器。
func (m *Manager) Batch(jobs ...Job) *BatchBuilder {
	return &BatchBuilder{manager: m, jobs: append([]Job(nil), jobs...)}
}

// Name 设置批次名称。
func (b *BatchBuilder) Name(name string) *BatchBuilder {
	b.name = name
	return b
}

// Options 设置批次内任务共享投递选项。
func (b *BatchBuilder) Options(options ...DispatchOption) *BatchBuilder {
	b.options = append(b.options, options...)
	return b
}

// Dispatch 创建批次并投递其中全部任务。
func (b *BatchBuilder) Dispatch(ctx context.Context) (payload.BatchStatus, error) {
	if b == nil || b.manager == nil {
		return payload.BatchStatus{}, fmt.Errorf("queue: batch builder is nil")
	}
	if len(b.jobs) == 0 {
		return payload.BatchStatus{}, fmt.Errorf("queue: batch is empty")
	}
	for _, job := range b.jobs {
		if job == nil {
			return payload.BatchStatus{}, fmt.Errorf("queue: batch contains nil job")
		}
	}
	id := uuid.NewString()
	status := payload.BatchStatus{ID: id, Name: b.name, Total: len(b.jobs), Pending: len(b.jobs), CreatedAt: time.Now()}
	if err := b.manager.createBatch(ctx, status); err != nil {
		return payload.BatchStatus{}, err
	}
	fire(ctx, BatchEvent{EventName: EventBatchCreated, Batch: status})
	accepted, err := b.dispatchJobs(ctx, id)
	if err != nil {
		if accepted > 0 {
			updated, partialErr := b.manager.markBatchDispatchPartialFailure(ctx, id, status, accepted)
			if partialErr != nil {
				return status, fmt.Errorf("%w; update partial batch metadata: %v", err, partialErr)
			}
			return updated, err
		}
		if rollbackErr := b.manager.deleteBatch(ctx, id); rollbackErr != nil {
			return status, fmt.Errorf("%w; rollback batch metadata: %v", err, rollbackErr)
		}
		return status, err
	}
	return status, nil
}

type batchReadyGroupKey struct {
	connection string
	queue      string
}

type batchReadyItem struct {
	env  *payload.Envelope
	body queuecontract.Payload
}

// dispatchJobs 按 Laravel Batch::add 的职责边界投递批次任务。
//
// 设计思路：ready 且不带 unique/debounce 的 payload 按 connection+queue 聚合后调用
// Queue.Bulk；delay、unique、debounce 任务保留单条路径，避免绕过延迟、唯一锁或防抖状态语义。
// 参数 batchID 是已创建的 batch metadata ID，会写入每个 envelope。
// 返回值 accepted 表示 transport 已接收的任务数；发生部分失败时，Dispatch 会据此修正 batch 进度。
func (b *BatchBuilder) dispatchJobs(ctx context.Context, batchID string) (int, error) {
	dispatcher := NewDispatcher(b.manager)
	ready := make(map[batchReadyGroupKey][]batchReadyItem)
	order := []batchReadyGroupKey{}
	accepted := 0
	for _, job := range b.jobs {
		prepared, err := b.prepareJob(ctx, dispatcher, batchID, job)
		if err != nil {
			return accepted, err
		}
		if prepared.single {
			if err := b.dispatchSingle(ctx, prepared); err != nil {
				return accepted, err
			}
			accepted++
			continue
		}
		key := batchReadyGroupKey{connection: prepared.connection, queue: prepared.queue}
		if _, ok := ready[key]; !ok {
			order = append(order, key)
		}
		ready[key] = append(ready[key], batchReadyItem{env: prepared.env, body: prepared.body})
	}
	for _, key := range order {
		items := ready[key]
		queueConn, err := b.manager.Queue(key.connection)
		if err != nil {
			return accepted, err
		}
		bodies := make([]queuecontract.Payload, 0, len(items))
		for _, item := range items {
			bodies = append(bodies, item.body)
		}
		result, err := queueConn.Bulk(ctx, key.queue, bodies)
		if err != nil {
			bulkAccepted := clampBulkAccepted(result.Accepted, len(items))
			for _, item := range items[:bulkAccepted] {
				fireJobQueued(ctx, key.connection, key.queue, 0, item.env)
			}
			return accepted + bulkAccepted, err
		}
		bulkAccepted := clampBulkAccepted(result.Accepted, len(items))
		for _, item := range items[:bulkAccepted] {
			fireJobQueued(ctx, key.connection, key.queue, 0, item.env)
		}
		if bulkAccepted < len(items) {
			// BulkResult.Accepted 是 transport 已接收数量；即使 driver 未返回 error，也必须把剩余任务标记失败，
			// 否则 batch metadata 会长期保留未实际入队的 pending job。
			return accepted + bulkAccepted, fmt.Errorf("queue: bulk accepted %d of %d jobs", bulkAccepted, len(items))
		}
		accepted += bulkAccepted
	}
	return accepted, nil
}

func clampBulkAccepted(accepted int, total int) int {
	if accepted < 0 {
		return 0
	}
	if accepted > total {
		return total
	}
	return accepted
}

type batchPreparedJob struct {
	connection string
	queue      string
	delay      time.Duration
	env        *payload.Envelope
	body       queuecontract.Payload
	queueConn  queuecontract.Queue
	single     bool
}

func (b *BatchBuilder) prepareJob(ctx context.Context, dispatcher *Dispatcher, batchID string, job Job) (batchPreparedJob, error) {
	runtime := dispatcher.runtime
	ctx = withQueueCacheDriver(ctx, runtime.cacheDriver)
	opts := normalizeDispatchOptions(runtime, job, applyOptions(append(append([]DispatchOption(nil), b.options...), withBatch(batchID))...))
	env, err := newDispatcherPayloadFactory(runtime).MakeEnvelope(job, payload.EnvelopeOptions{
		Queue: opts.Queue, Delay: opts.Delay, Tries: opts.Tries, MaxExceptions: opts.MaxExceptions,
		Timeout: opts.Timeout, FailOnTimeout: opts.FailOnTimeout, Encrypted: opts.Encrypted,
		Backoff: opts.Backoff, RetryUntil: opts.RetryUntil, Chain: opts.Chain, BatchID: opts.BatchID,
		UniqueKey: opts.UniqueKey, UniqueFor: opts.UniqueFor, UniqueVia: queueCacheStoreNameOrDefault(opts.uniqueVia, runtime.cacheDriver),
		UniqueUntil: opts.UniqueUntil, DebounceKey: opts.DebounceKey, DebounceFor: opts.DebounceFor,
		DebounceVia: queueCacheStoreNameOrDefault(opts.debounceVia, runtime.cacheDriver), Tags: opts.Tags, Silenced: opts.Silenced,
	})
	if err != nil {
		return batchPreparedJob{}, err
	}
	queueConn, err := b.manager.Queue(opts.Connection)
	if err != nil {
		return batchPreparedJob{}, err
	}
	dispatcher.installSyncImmediateProcessor(queueConn, opts.Connection)
	if err := prepareDispatchRuntime(ctx, env, &opts); err != nil {
		return batchPreparedJob{}, err
	}
	body, err := encodeQueueEnvelope(runtime, env)
	if err != nil {
		return batchPreparedJob{}, err
	}
	return batchPreparedJob{
		connection: opts.Connection, queue: opts.Queue, delay: opts.Delay, env: env, body: body,
		queueConn: queueConn, single: opts.Delay > 0 || opts.UniqueKey != "" || opts.DebounceKey != "",
	}, nil
}

func (b *BatchBuilder) dispatchSingle(ctx context.Context, job batchPreparedJob) error {
	if job.delay > 0 {
		if err := job.queueConn.Later(ctx, job.queue, job.body, job.delay); err != nil {
			return err
		}
		fireJobQueued(ctx, job.connection, job.queue, job.delay, job.env)
		return nil
	}
	if err := job.queueConn.Push(ctx, job.queue, job.body); err != nil {
		return err
	}
	fireJobQueued(ctx, job.connection, job.queue, 0, job.env)
	return nil
}

// markBatchDispatchPartialFailure 保留已有投递成功的 batch metadata，并把未被 transport 接收的任务记为失败。
//
// 需求背景：批量或混合批次可能在部分任务已入队后才失败。此时删除 metadata 会导致已入队任务执行时
// 找不到 batch；保持全部 Pending 又会让未入队任务永久 pending。因此这里把 accepted 留在 Pending，
// 把剩余任务计入 Processed+Failed，明确表达“投递阶段未进入 transport 的任务已经失败”。
func (m *Manager) markBatchDispatchPartialFailure(ctx context.Context, id string, status payload.BatchStatus, accepted int) (payload.BatchStatus, error) {
	accepted = clampBulkAccepted(accepted, status.Total)
	failed := status.Total - accepted
	status.ID = id
	status.Pending = accepted
	status.Processed = failed
	status.Failed = failed
	if accepted == 0 {
		status.FinishedAt = time.Now()
	} else {
		status.FinishedAt = time.Time{}
	}
	store, ok := m.batchStore()
	if !ok {
		return payload.BatchStatus{}, ErrEmpty
	}
	if err := store.UpdateBatch(ctx, status); err != nil {
		return payload.BatchStatus{}, err
	}
	fire(ctx, BatchEvent{EventName: EventBatchUpdated, Batch: status})
	return status, nil
}

// BatchStatus 读取批次状态。
func (m *Manager) BatchStatus(ctx context.Context, id string) (payload.BatchStatus, error) {
	store, ok := m.batchStore()
	if !ok {
		return payload.BatchStatus{}, ErrEmpty
	}
	status, err := store.GetBatch(ctx, id)
	if err != nil {
		return payload.BatchStatus{}, err
	}
	return *status, nil
}

// CancelBatch 取消批次，后续未执行任务会被 worker 跳过。
func (m *Manager) CancelBatch(ctx context.Context, id string) error {
	store, ok := m.batchStore()
	if !ok {
		return ErrEmpty
	}
	status, err := store.CancelBatch(ctx, id)
	if err != nil {
		return err
	}
	fire(ctx, BatchEvent{EventName: EventBatchCancelled, Batch: status})
	return nil
}

// MarkBatchJob 标记批次内一个任务完成，并派发批次进度事件。
//
// 设计思路：计数变更必须交给 BatchStore 的原子实现完成；Manager 只负责把缺失批次视为
// 无操作，以及根据返回状态发出 updated/finished 事件。
func (m *Manager) MarkBatchJob(ctx context.Context, id string, success bool) error {
	store, ok := m.batchStore()
	if !ok {
		return nil
	}
	status, err := store.MarkBatchJob(ctx, id, success)
	if errors.Is(err, ErrEmpty) {
		return nil
	}
	if err != nil {
		return err
	}
	name := EventBatchUpdated
	if status.Pending == 0 {
		name = EventBatchFinished
	}
	fire(ctx, BatchEvent{EventName: name, Batch: status})
	return nil
}

func (m *Manager) createBatch(ctx context.Context, status payload.BatchStatus) error {
	store, ok := m.batchStore()
	if !ok {
		return errors.New("queue: batch store is not configured")
	}
	return store.CreateBatch(ctx, status)
}

func (m *Manager) deleteBatch(ctx context.Context, id string) error {
	store, ok := m.batchStore()
	if !ok {
		return errors.New("queue: batch store is not configured")
	}
	return store.DeleteBatch(ctx, id)
}

func (m *Manager) batchStore() (BatchStore, bool) {
	runtime := m.runtimeOrDefault()
	if runtime == nil || runtime.batch == nil {
		return nil, false
	}
	return runtime.batch, true
}

func buildBatchStore(cfg Config, codec encodingcontract.Codec) (BatchStore, error) {
	switch normalizeDriverName(cfg.Batching.Driver) {
	case "", "memory":
		return NewMemoryBatchStore(), nil
	case "redis":
		options := redisqueue.RedisOptions{
			Connection: firstNonEmpty(cfg.Batching.Store, "default"),
			Prefix:     firstNonEmpty(cfg.Batching.Prefix, "prismgo_queue"),
			FailedTTL:  cfg.Batching.TTL,
			Codec:      codec,
		}
		client, err := redisqueue.ResolveQueueClient(options)
		if err != nil {
			return nil, err
		}
		return redisqueue.NewRedisBatchStoreFromClient(client, options), nil
	default:
		return nil, fmt.Errorf("queue: unknown batch store driver %q", cfg.Batching.Driver)
	}
}
