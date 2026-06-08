package queue

import (
	"context"
	"fmt"
)

// RetryFailed 按 failed id 重新入队失败任务。
//
// 逻辑说明：该流程对齐 Laravel queue:retry 的 raw payload 重投语义。它从 FailedStore
// 读取原始失败记录，保留原 job id，重置 attempts 与 reservation 字段，重新编码后通过
// Queue.Push 写回原 connection/queue；只有入队成功后才删除 failed 记录。
func (d *Dispatcher) RetryFailed(ctx context.Context, failedID string) error {
	if d == nil || d.manager == nil || d.runtime == nil {
		return fmt.Errorf("queue dispatcher is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	failed, err := d.runtime.failed.Find(ctx, failedID)
	if err != nil {
		return err
	}
	env := *cloneEnvelope(&failed.Envelope)
	env.Attempts = 0
	env.Exceptions = 0
	env.ReservedAt = 0
	queueName := firstNonEmpty(failed.Queue, env.Queue, d.runtime.defaultQueue)
	env.Queue = queueName
	body, err := encodeQueueEnvelope(d.runtime, &env)
	if err != nil {
		return err
	}
	queueConn, err := d.manager.Queue(firstNonEmpty(failed.Connection, d.manager.defaultConnection))
	if err != nil {
		return err
	}
	d.installSyncImmediateProcessor(queueConn, firstNonEmpty(failed.Connection, d.manager.defaultConnection))
	if err := queueConn.Push(ctx, queueName, body); err != nil {
		return err
	}
	return d.runtime.failed.Forget(ctx, failedID)
}
