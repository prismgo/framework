package encoding

// Codec 定义 Prismgo 内部 Payload Encoding 的公共能力边界。
//
// 需求背景：cache、session、queue、Horizon 都需要把内部持久化或传输 payload
// 转成 []byte，但这些能力不应该互相依赖具体实现包。该接口只描述“如何编码/解码”，
// 不包含默认值、注册表、facade 或 provider，保持 contracts 包只表达公共 contract。
//
// 设计思路：实现包负责提供 json/msgpack codec；调用方通过配置解析出具体 codec 后，
// 在各自能力边界内使用。接口方法保持最小集合，避免第一版暴露自定义 codec 注册能力。
type Codec interface {
	// Marshal 将 v 编码成当前 codec 的持久化字节。
	//
	// 参数说明：v 是调用方准备写入内部存储或内部传输通道的 Go 值。
	// 返回值说明：返回的 []byte 是唯一应该交给 store、driver 或连接层的 payload bytes；
	// 如果值无法按当前 codec 表达，必须返回 error，调用方不得静默降级到其他 codec。
	Marshal(v any) ([]byte, error)

	// Unmarshal 将当前 codec 的持久化字节解码到 v。
	//
	// 参数说明：data 是 Marshal 生成或历史存储中读取到的 payload bytes；v 必须是可写目标。
	// 设计原因：解码错误代表当前配置的 Payload Encoding 与存储内容不匹配，调用方应按自身语义
	// 暴露错误或视为缓存不可用，不能在公共 contract 内做 JSON/msgpack fallback。
	Unmarshal(data []byte, v any) error

	// Name 返回当前 codec 的规范名称。
	//
	// 使用方式：名称用于配置归一化、错误信息和诊断展示；第一版只允许 msgpack 与 json。
	Name() string
}
