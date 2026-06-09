package queue

import (
	"context"
	"errors"
	"fmt"
	"time"

	queuecontract "github.com/prismgo/framework/contracts/queue"
	"github.com/prismgo/framework/queue/payload"
)

// Dispatcher 负责 Job -> options -> payload -> transport 的投递编排。
type Dispatcher struct {
	manager *Manager
	runtime *Runtime
}

// NewDispatcher 创建投递编排器。
func NewDispatcher(manager *Manager) *Dispatcher {
	return &Dispatcher{manager: manager, runtime: manager.runtimeOrDefault()}
}

// Dispatch 投递任务，sync 连接会由 SyncQueue immediate runner 在当前调用栈立即执行。
func (d *Dispatcher) Dispatch(ctx context.Context, job Job, options ...DispatchOption) (string, error) {
	if d == nil || d.manager == nil {
		return "", fmt.Errorf("queue dispatcher is nil")
	}
	m := d.manager
	runtime := d.runtime
	ctx = withQueueCacheDriver(ctx, runtime.cacheDriver)
	opts := normalizeDispatchOptions(runtime, job, applyOptions(options...))
	env, err := newDispatcherPayloadFactory(runtime).MakeEnvelope(job, payload.EnvelopeOptions{
		Queue:         opts.Queue,
		Delay:         opts.Delay,
		Tries:         opts.Tries,
		MaxExceptions: opts.MaxExceptions,
		Timeout:       opts.Timeout,
		FailOnTimeout: opts.FailOnTimeout,
		Encrypted:     opts.Encrypted,
		Backoff:       opts.Backoff,
		RetryUntil:    opts.RetryUntil,
		Chain:         opts.Chain,
		BatchID:       opts.BatchID,
		UniqueKey:     opts.UniqueKey,
		UniqueFor:     opts.UniqueFor,
		UniqueVia:     queueCacheStoreNameOrDefault(opts.uniqueVia, runtime.cacheDriver),
		UniqueUntil:   opts.UniqueUntil,
		DebounceKey:   opts.DebounceKey,
		DebounceFor:   opts.DebounceFor,
		DebounceVia:   queueCacheStoreNameOrDefault(opts.debounceVia, runtime.cacheDriver),
		Tags:          opts.Tags,
		Silenced:      opts.Silenced,
	})
	if err != nil {
		return "", err
	}
	queueConn, err := m.Queue(opts.Connection)
	if err != nil {
		return "", err
	}
	d.installSyncImmediateProcessor(queueConn, opts.Connection)
	if err := prepareDispatchRuntime(ctx, env, &opts); err != nil {
		return "", err
	}
	body, err := encodeQueueEnvelope(runtime, env)
	if err != nil {
		return "", err
	}
	if opts.Delay > 0 {
		if err := queueConn.Later(ctx, opts.Queue, body, opts.Delay); err != nil {
			return "", err
		}
		fireJobQueued(ctx, opts.Connection, opts.Queue, opts.Delay, env)
		return env.ID, nil
	}
	if err := queueConn.Push(ctx, opts.Queue, body); err != nil {
		return "", err
	}
	fireJobQueued(ctx, opts.Connection, opts.Queue, 0, env)
	return env.ID, nil
}

// fireJobQueued 在 transport 已确认接受 payload 后派发 queue.job_queued。
//
// 需求背景：issue 08 要求单条和批量投递都以“成功进入 transport”为事件边界，避免
// RabbitMQ confirm/nack/timeout 失败时 Horizon 或业务监听器提前看到假入队事件。
func fireJobQueued(ctx context.Context, connection string, queueName string, delay time.Duration, env *payload.Envelope) {
	if env == nil {
		return
	}
	fire(ctx, JobQueued{
		Connection: connection,
		Queue:      queueName,
		JobID:      env.ID,
		JobName:    env.Name,
		Delay:      delay,
		Tags:       append([]string(nil), env.Tags...),
		Silenced:   env.Silenced,
		QueuedAt:   time.Unix(env.CreatedAt, 0),
	})
}

func (d *Dispatcher) installSyncImmediateProcessor(queueConn queuecontract.Queue, connection string) {
	immediate, ok := queueConn.(interface {
		SetImmediateProcessor(func(context.Context, string, queuecontract.Payload) error)
	})
	if !ok {
		return
	}
	immediate.SetImmediateProcessor(func(ctx context.Context, queueName string, body queuecontract.Payload) error {
		env, err := envelopeFromQueuePayload(d.runtime, body)
		if err != nil {
			return err
		}
		env.Queue = firstString(env.Queue, queueName)
		err = newQueueJobRunner(d.manager, d.runtime, queueConn, connection).Process(ctx, env)
		if env.UniqueKey != "" {
			_ = releaseUnique(ctx, env)
		}
		if errors.Is(err, ErrSkipped) {
			return nil
		}
		return err
	})
}

type dispatcherPayloadRegistry struct {
	runtime *Runtime
}

func newDispatcherPayloadFactory(runtime *Runtime) *payload.Factory {
	return payload.NewFactory(dispatcherPayloadRegistry{runtime: runtime}, runtime.encryptPayload)
}

func (r dispatcherPayloadRegistry) TypeName(job queuecontract.Job) (string, error) {
	return JobTypeName(job)
}

func (r dispatcherPayloadRegistry) Register(job queuecontract.Job) {
	if r.runtime != nil && r.runtime.registry != nil {
		r.runtime.registry.registerJobType(job)
	}
}

func (r dispatcherPayloadRegistry) Marshal(job queuecontract.Job) (payload.Payload, error) {
	return r.runtime.registry.marshalWithCodec(job, r.runtime.codec)
}

// DispatchJob 实现 contracts/queue.Dispatcher。
//
// 需求背景：event 的 queued listener 只能依赖 queue contract，不能 import prismgo/queue
// 实现包。该方法把 contract 层只读选项转换为 queue 包内部的函数式选项，并复用现有
// Dispatch 行为。
//
// 参数说明：
// ctx 是投递调用链上下文；job 是实现 contracts/queue.Job 的任务；options 是可选的连接、
// 队列、延迟、重试、退避和超时设置。
func (d *Dispatcher) DispatchJob(ctx context.Context, job queuecontract.Job, options queuecontract.DispatchOptions) (string, error) {
	return d.Dispatch(ctx, job, dispatchOptionsFromQueueOptions(options)...)
}

// RequestRestart 代理 worker 重启信号，满足 contracts/queue.Dispatcher。
func (d *Dispatcher) RequestRestart(ctx context.Context) error {
	if d == nil || d.manager == nil {
		return fmt.Errorf("queue dispatcher is nil")
	}
	return d.manager.RequestRestart(ctx)
}

// Close 关闭底层 manager 的连接缓存，满足 contracts/queue.Dispatcher。
func (d *Dispatcher) Close() error {
	if d == nil || d.manager == nil {
		return nil
	}
	return d.manager.Close()
}

func dispatchOptionsFromQueueOptions(options queuecontract.DispatchOptions) []DispatchOption {
	if options == nil {
		return nil
	}
	out := []DispatchOption{}
	if connection := options.QueueConnection(); connection != "" {
		out = append(out, OnConnection(connection))
	}
	if queueName := options.QueueName(); queueName != "" {
		out = append(out, OnQueue(queueName))
	}
	if delay := options.QueueDelay(); delay > 0 {
		out = append(out, Delay(delay))
	}
	if tries := options.QueueTries(); tries > 0 {
		out = append(out, Tries(tries))
	}
	if maxExceptions := options.QueueMaxExceptions(); maxExceptions > 0 {
		out = append(out, MaxExceptions(maxExceptions))
	}
	if timeout := options.QueueTimeout(); timeout > 0 {
		out = append(out, Timeout(timeout))
	}
	if options.QueueFailOnTimeout() {
		out = append(out, FailOnTimeout())
	}
	if options.QueueEncrypted() {
		out = append(out, Encrypt())
	}
	if backoff := options.QueueBackoff(); len(backoff) > 0 {
		out = append(out, Backoff(backoff...))
	}
	if retryUntil := options.QueueRetryUntil(); !retryUntil.IsZero() {
		out = append(out, RetryUntil(retryUntil))
	}
	if batchID := options.QueueBatchID(); batchID != "" {
		out = append(out, withBatch(batchID))
	}
	if uniqueKey := options.QueueUniqueKey(); uniqueKey != "" {
		out = append(out, Unique(uniqueKey, options.QueueUniqueFor()))
		if options.QueueUniqueUntil() {
			out = append(out, UniqueUntilProcessing())
		}
	}
	if debounceKey := options.QueueDebounceKey(); debounceKey != "" {
		out = append(out, Debounce(debounceKey, options.QueueDebounceFor()))
	}
	if tags := options.QueueTags(); len(tags) > 0 {
		out = append(out, Tags(tags...))
	}
	if options.QueueSilenced() {
		out = append(out, Silenced())
	}
	return out
}
