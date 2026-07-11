package queue

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	queuecontract "github.com/prismgo/framework/contracts/queue"
	goexception "github.com/prismgo/framework/exception"
	"github.com/prismgo/framework/internal/stackx"
	"github.com/prismgo/framework/queue/payload"
)

// JobRunner 负责把 payload.Envelope 恢复为 Job 并运行 middleware + Handle。
type JobRunner struct {
	manager    *Manager
	runtime    *Runtime
	registry   *Registry
	connection string
	queueConn  queuecontract.Queue
	middleware []Middleware
}

// Process 执行一个任务信封。
func (p *JobRunner) Process(ctx context.Context, env *payload.Envelope) (err error) {
	if p != nil && p.runtime != nil {
		ctx = withQueueCacheDriver(ctx, p.runtime.cacheDriver)
	}
	if env == nil {
		return fmt.Errorf("queue: envelope is nil")
	}
	start := time.Now()
	fire(ctx, JobProcessing{
		Connection: p.connection,
		Queue:      env.Queue,
		JobID:      env.ID,
		JobName:    env.Name,
		Attempts:   env.Attempts,
		Tags:       append([]string(nil), env.Tags...),
		Silenced:   env.Silenced,
		QueuedAt:   time.Unix(env.CreatedAt, 0),
	})
	defer func() {
		if r := recover(); r != nil {
			// 捕获 panic 发生位置的结构化堆栈
			stack := stackx.Capture(0)
			panicErr := fmt.Errorf("queue: job panic: %v", r)
			err = goexception.WithStackTrace(panicErr, stack)
		}
	}()
	if err := p.ensureBatchActive(ctx, env); err != nil {
		return err
	}
	if stale, err := staleDebounce(ctx, env); stale || err != nil {
		return err
	}
	payload, err := payloadForQueueEnvelope(p.runtime, env)
	if err != nil {
		return err
	}
	job, err := p.registry.unmarshalWithCodec(env.Name, payload, p.runtime.codec)
	if err != nil {
		return err
	}
	err = p.runJob(ctx, job)
	// 需求背景：worker 超时超过 grace 后会先按 DeadlineExceeded 释放/失败当前 envelope，
	// 原 Handle goroutine 之后仍可能迟到返回。这里在所有成功后置动作前检查 ctx，
	// 避免迟到成功继续更新 batch、派发 chain 或发出 JobProcessed 事件。
	if ctxErr := ctx.Err(); ctxErr != nil {
		if err != nil {
			return err
		}
		return ctxErr
	}
	if err != nil {
		return err
	}
	if err := p.updateBatch(ctx, env, true); err != nil {
		return err
	}
	if err := p.dispatchNextChain(ctx, env); err != nil {
		return err
	}
	fire(ctx, JobProcessed{
		Connection: p.connection,
		Queue:      env.Queue,
		JobID:      env.ID,
		JobName:    env.Name,
		Duration:   time.Since(start),
		Tags:       append([]string(nil), env.Tags...),
		Silenced:   env.Silenced,
	})
	return nil
}

func (p *JobRunner) runJob(ctx context.Context, job Job) error {
	stack := append([]Middleware(nil), p.middleware...)
	if provider, ok := job.(MiddlewareProvider); ok {
		stack = append(stack, provider.Middleware()...)
	}
	final := func(ctx context.Context) error { return job.Handle(ctx) }
	for i := len(stack) - 1; i >= 0; i-- {
		current := stack[i]
		next := final
		final = func(ctx context.Context) error {
			return current.Handle(ctx, job, next)
		}
	}
	err := final(ctx)
	if errors.Is(err, ErrSkipped) {
		return nil
	}
	return err
}

func (p *JobRunner) ensureBatchActive(ctx context.Context, env *payload.Envelope) error {
	if env.BatchID == "" {
		return nil
	}
	status, err := p.manager.BatchStatus(ctx, env.BatchID)
	if err != nil {
		return nil
	}
	if status.Cancelled {
		return ErrBatchCancelled
	}
	return nil
}

func (p *JobRunner) updateBatch(ctx context.Context, env *payload.Envelope, success bool) error {
	if env.BatchID == "" {
		return nil
	}
	return p.manager.MarkBatchJob(ctx, env.BatchID, success)
}

func (p *JobRunner) dispatchNextChain(ctx context.Context, env *payload.Envelope) error {
	if len(env.Chain) == 0 {
		return nil
	}
	next := env.Chain[0]
	nextEnv := &payload.Envelope{
		ID:             newID(),
		Name:           next.Name,
		Queue:          firstString(next.Queue, env.Queue),
		Payload:        next.Payload,
		MaxTries:       next.MaxTries,
		MaxExceptions:  next.MaxExceptions,
		TimeoutSec:     next.TimeoutSec,
		FailOnTimeout:  next.FailOnTimeout,
		Encrypted:      next.Encrypted,
		BackoffSec:     next.BackoffSec,
		RetryUntil:     next.RetryUntil,
		Chain:          append([]payload.PendingJob(nil), env.Chain[1:]...),
		BatchID:        env.BatchID,
		UniqueKey:      next.UniqueKey,
		UniqueForSec:   next.UniqueForSec,
		UniqueVia:      next.UniqueVia,
		UniqueUntil:    next.UniqueUntil,
		DebounceKey:    next.DebounceKey,
		DebounceForSec: next.DebounceForSec,
		DebounceVia:    next.DebounceVia,
		Tags:           append([]string(nil), env.Tags...),
		Silenced:       env.Silenced,
		CreatedAt:      time.Now().Unix(),
		AvailableAt:    time.Now().Unix(),
	}
	if p.queueConn != nil {
		body, err := encodeQueueEnvelope(p.runtime, nextEnv)
		if err != nil {
			return err
		}
		if err := p.queueConn.Push(ctx, nextEnv.Queue, body); err != nil {
			return err
		}
		fireJobQueued(ctx, p.connection, nextEnv.Queue, 0, nextEnv)
		return nil
	}
	return fmt.Errorf("queue: job runner queue connection is not configured")
}

func newID() string {
	return uuid.NewString()
}

func firstString(value string, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}
