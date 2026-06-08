package payload

import (
	"fmt"
	"time"

	encodingcontract "github.com/prismgo/framework/contracts/encoding"
	queuecontract "github.com/prismgo/framework/contracts/queue"
	queueerrors "github.com/prismgo/framework/queue/internal/errors"
)

// ReservationCodec 统一 raw body 的 reservation policy。
//
// 需求背景：Redis、Sync、RabbitMQ 都需要在 Pop 时执行 attempts++、reserved_at 写入和
// queue 名称补齐。该模块只处理纯 payload 变换，不做 Redis 原子替换、RabbitMQ ack、
// poison event 或 failed store 等 driver I/O。
type ReservationCodec struct {
	codec encodingcontract.Codec
}

// NewReservationCodec 创建 reservation payload codec。
func NewReservationCodec(codec encodingcontract.Codec) ReservationCodec {
	return ReservationCodec{codec: QueueCodec(codec)}
}

// ReservedPayload 是 reservation 后的 envelope 和重新编码 raw body。
type ReservedPayload struct {
	Envelope *Envelope
	Body     queuecontract.Payload
}

// maxAttempts 防止 Attempts 在极端重试场景下溢出 int 范围。
// 超过此值后不再递增，避免整数回绕导致 worker 逻辑异常。
const maxAttempts = 1<<31 - 1

// Reserve 解码 raw body，补齐队列名，递增 attempts，写入 reserved_at 并重新编码。
func (c ReservationCodec) Reserve(body queuecontract.Payload, queue string, now time.Time) (ReservedPayload, error) {
	var env Envelope
	if err := QueueCodec(c.codec).Unmarshal(body, &env); err != nil {
		return ReservedPayload{}, fmt.Errorf("%w: %w", queueerrors.ErrPoisonEnvelope, err)
	}
	env.Queue = firstNonEmpty(queue, env.Queue, "default")
	// 溢出保护：attempts 达到上限后不再递增，防止极端重试场景下整数回绕
	if env.Attempts < maxAttempts {
		env.Attempts++
	}
	env.ReservedAt = now.Unix()
	updated, err := QueueCodec(c.codec).Marshal(&env)
	if err != nil {
		return ReservedPayload{}, err
	}
	return ReservedPayload{Envelope: &env, Body: queuecontract.Payload(updated)}, nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
