package queue

import (
	"context"

	"github.com/prismgo/framework/queue/payload"
)

// ChainBuilder 描述一组必须顺序执行的任务。
type ChainBuilder struct {
	manager *Manager
	jobs    []Job
	options []DispatchOption
}

// Chain 使用默认 Manager 创建链式任务。
func Chain(jobs ...Job) *ChainBuilder {
	return Resolve().Chain(jobs...)
}

// Chain 使用当前 Manager 创建链式任务。
func (m *Manager) Chain(jobs ...Job) *ChainBuilder {
	return &ChainBuilder{manager: m, jobs: append([]Job(nil), jobs...)}
}

// Options 设置首个任务以及后续任务共用的投递选项。
func (b *ChainBuilder) Options(options ...DispatchOption) *ChainBuilder {
	b.options = append(b.options, options...)
	return b
}

// Dispatch 投递链式任务的第一个任务。
func (b *ChainBuilder) Dispatch(ctx context.Context) (string, error) {
	if len(b.jobs) == 0 {
		return "", nil
	}
	pending := make([]payload.PendingJob, 0, len(b.jobs)-1)
	for _, job := range b.jobs[1:] {
		item, err := b.manager.pendingJob(job, applyOptions(b.options...))
		if err != nil {
			return "", err
		}
		pending = append(pending, item)
	}
	options := append([]DispatchOption(nil), b.options...)
	options = append(options, withChain(pending))
	return NewDispatcher(b.manager).Dispatch(ctx, b.jobs[0], options...)
}

func (m *Manager) pendingJob(job Job, opts DispatchOptions) (payload.PendingJob, error) {
	runtime := m.runtimeOrDefault()
	opts = normalizeDispatchOptions(runtime, job, opts)
	name, err := JobTypeName(job)
	if err != nil {
		return payload.PendingJob{}, err
	}
	body, err := runtime.registry.marshalWithCodec(job, runtime.codec)
	if err != nil {
		return payload.PendingJob{}, err
	}
	if opts.Encrypted {
		body, err = runtime.encryptPayload(body)
		if err != nil {
			return payload.PendingJob{}, err
		}
	}
	return payload.PendingJob{
		Name:           name,
		Payload:        body,
		MaxTries:       opts.Tries,
		MaxExceptions:  opts.MaxExceptions,
		TimeoutSec:     seconds(opts.Timeout),
		FailOnTimeout:  opts.FailOnTimeout,
		Encrypted:      opts.Encrypted,
		BackoffSec:     secondsList(opts.Backoff),
		RetryUntil:     unixSeconds(opts.RetryUntil),
		Queue:          opts.Queue,
		UniqueKey:      opts.UniqueKey,
		UniqueForSec:   seconds(opts.UniqueFor),
		UniqueVia:      queueCacheStoreNameOrDefault(opts.uniqueVia, runtime.cacheDriver),
		UniqueUntil:    opts.UniqueUntil,
		DebounceKey:    opts.DebounceKey,
		DebounceForSec: seconds(opts.DebounceFor),
		DebounceVia:    queueCacheStoreNameOrDefault(opts.debounceVia, runtime.cacheDriver),
	}, nil
}
