package payload

import (
	"fmt"

	encodingcontract "github.com/prismgo/framework/contracts/encoding"
	queuecontract "github.com/prismgo/framework/contracts/queue"
)

// Metadata 是 driver header、事件和 reserved adapter 可安全读取的 envelope 摘要。
//
// 设计思路：transport 只需要 id/name/queue/attempts 等元数据，不应该理解完整
// runtime schema 或复制 durable DTO。Inspect 统一从 raw body 中解码这些字段。
type Metadata struct {
	ID       string
	Name     string
	Queue    string
	Attempts int
}

// Inspect 解码 raw queue body 并返回安全元数据。
//
// 参数 body 是 Queue contract 上传输的完整 encoded envelope；codec 是当前连接使用的
// queue payload codec。调用方只消费返回摘要，不持有 Envelope 指针。
func Inspect(body queuecontract.Payload, codec encodingcontract.Codec) (Metadata, error) {
	var env Envelope
	if err := QueueCodec(codec).Unmarshal(body, &env); err != nil {
		return Metadata{}, fmt.Errorf("queue payload inspect: %w", err)
	}
	return Metadata{ID: env.ID, Name: env.Name, Queue: env.Queue, Attempts: env.Attempts}, nil
}
