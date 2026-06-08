package queue

import (
	"context"
	"errors"
	"fmt"
	"time"

	queuecontract "github.com/prismgo/framework/contracts/queue"
	"github.com/prismgo/framework/exception"
	"github.com/prismgo/framework/queue/payload"
)

// FailureHandler owns retry, release, failed-store and job_failed policy.
type FailureHandler struct {
	worker *Worker
}

func newFailureHandler(worker *Worker) *FailureHandler {
	return &FailureHandler{worker: worker}
}

func (h *FailureHandler) Handle(ctx context.Context, reserved queuecontract.ReservedJob, env *payload.Envelope, options WorkerOptions, err error) error {
	if delay, ok := ReleaseDelay(err); ok {
		return h.release(ctx, reserved, env, options, delay, err)
	}
	env.Exceptions++
	if !shouldFailNow(env, err) && shouldRetry(env, options) {
		return h.release(ctx, reserved, env, options, nextBackoff(env, options), err)
	}
	failed := payload.FailedJob{
		ID:         newID(),
		Connection: options.Connection,
		Queue:      env.Queue,
		JobID:      env.ID,
		JobName:    env.Name,
		Envelope:   *cloneEnvelope(env),
		Error:      err.Error(),
		FailedAt:   time.Now(),
		Tags:       append([]string(nil), env.Tags...),
		Silenced:   env.Silenced,
	}
	h.callFailedHook(ctx, env, err)
	// 失败归档必须先于 ack/delete。若 Record 失败，保留原消息等待 retry_after 后重试，
	// 避免既没有失败记录、又删除了队列消息。
	if recordErr := h.worker.runtime.failed.Record(ctx, failed); recordErr != nil {
		return recordErr
	}
	if env.BatchID != "" {
		if batchErr := h.worker.manager.MarkBatchJob(ctx, env.BatchID, false); batchErr != nil {
			return batchErr
		}
	}
	if deleteErr := reserved.Delete(ctx); deleteErr != nil {
		return deleteErr
	}
	_ = releaseUnique(ctx, env)
	exception.Report(ctx, err, map[string]any{
		"component":  "queue",
		"subsystem":  "worker",
		"connection": options.Connection,
		"queue":      env.Queue,
		"job_name":   env.Name,
		"job_id":     env.ID,
		"attempts":   env.Attempts,
		"status":     500,
	})
	fire(ctx, JobFailed{FailedJob: failed})
	return nil
}

func (h *FailureHandler) release(ctx context.Context, reserved queuecontract.ReservedJob, env *payload.Envelope, options WorkerOptions, delay time.Duration, err error) error {
	if err := reserved.Release(ctx, delay); err != nil {
		return err
	}
	fire(ctx, JobReleased{
		Connection: options.Connection,
		Queue:      env.Queue,
		JobID:      env.ID,
		JobName:    env.Name,
		Delay:      delay,
		Err:        fmt.Sprint(err),
		Tags:       append([]string(nil), env.Tags...),
		Silenced:   env.Silenced,
	})
	return nil
}

func (h *FailureHandler) callFailedHook(ctx context.Context, env *payload.Envelope, err error) {
	if h == nil || h.worker == nil || h.worker.runtime == nil || h.worker.runtime.registry == nil || env == nil {
		return
	}
	body, payloadErr := payloadForQueueEnvelope(h.worker.runtime, env)
	if payloadErr != nil {
		return
	}
	job, restoreErr := h.worker.runtime.registry.unmarshalWithCodec(env.Name, body, h.worker.runtime.codec)
	if restoreErr != nil {
		return
	}
	provider, ok := job.(FailedProvider)
	if !ok {
		return
	}
	defer func() {
		_ = recover()
	}()
	provider.Failed(ctx, err)
}

func (w *Worker) handleFailure(ctx context.Context, reserved queuecontract.ReservedJob, env *payload.Envelope, options WorkerOptions, err error) error {
	return newFailureHandler(w).Handle(ctx, reserved, env, options, err)
}

func shouldRetry(env *payload.Envelope, options WorkerOptions) bool {
	if env.RetryUntil > 0 && time.Now().Unix() >= env.RetryUntil {
		return false
	}
	tries := env.MaxTries
	if tries <= 0 {
		tries = options.Tries
	}
	return tries <= 0 || env.Attempts < tries
}

func shouldFailNow(env *payload.Envelope, err error) bool {
	if env == nil {
		return true
	}
	if shouldFailError(err) {
		return true
	}
	if env.FailOnTimeout && errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	return env.MaxExceptions > 0 && env.Exceptions >= env.MaxExceptions
}

func nextBackoff(env *payload.Envelope, options WorkerOptions) time.Duration {
	values := durations(env.BackoffSec)
	if len(values) == 0 {
		values = options.Backoff
	}
	if len(values) == 0 {
		return 0
	}
	index := env.Attempts - 1
	if index < 0 {
		index = 0
	}
	if index >= len(values) {
		index = len(values) - 1
	}
	return values[index]
}
