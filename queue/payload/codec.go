package payload

import (
	encodingcontract "github.com/prismgo/framework/contracts/encoding"
	"github.com/prismgo/framework/encoding"
)

// QueueCodec 包装 JSON Payload Encoding，让 envelope.payload 保持 raw JSON。
//
// 需求背景：queue 父包、Redis、RabbitMQ 和 failed repository 都要写同一份 durable
// payload。JSON 模式下 []byte 默认会被 encoding/json 编成 base64；队列 payload
// 已经是业务 Job 的 JSON 字节，必须作为 raw JSON 嵌入 Envelope，避免 driver 各自
// 复制一套字段搬运逻辑。
func QueueCodec(codec encodingcontract.Codec) encodingcontract.Codec {
	if codec == nil {
		return encoding.Msgpack()
	}
	if codec.Name() != encoding.NameJSON {
		return codec
	}
	return jsonQueueCodec{inner: codec}
}

type jsonQueueCodec struct {
	inner encodingcontract.Codec
}

func (c jsonQueueCodec) Marshal(value any) ([]byte, error) {
	switch v := value.(type) {
	case Envelope:
		return MarshalEnvelope(v)
	case *Envelope:
		if v == nil {
			return []byte("null"), nil
		}
		return MarshalEnvelope(*v)
	case FailedJob:
		return MarshalFailedJob(v)
	case *FailedJob:
		if v == nil {
			return []byte("null"), nil
		}
		return MarshalFailedJob(*v)
	default:
		return c.inner.Marshal(value)
	}
}

func (c jsonQueueCodec) Unmarshal(data []byte, value any) error {
	switch v := value.(type) {
	case *Envelope:
		env, err := UnmarshalEnvelope(data)
		if err != nil {
			return err
		}
		*v = env
		return nil
	case *FailedJob:
		failed, err := UnmarshalFailedJob(data)
		if err != nil {
			return err
		}
		*v = failed
		return nil
	default:
		return c.inner.Unmarshal(data, value)
	}
}

func (c jsonQueueCodec) Name() string {
	return c.inner.Name()
}
