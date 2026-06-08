package encoding

import (
	stdjson "encoding/json"
	"fmt"
	"reflect"
	"strings"

	contract "github.com/prismgo/framework/contracts/encoding"
	"github.com/ugorji/go/codec"
)

const (
	// NameMsgpack 是内置 msgpack Payload Encoding 的配置名称。
	//
	// 需求背景：第一版默认使用 msgpack，以减少内部 payload 的存储体积。
	NameMsgpack = "msgpack"
	// NameJSON 是内置 JSON Payload Encoding 的配置名称。
	//
	// 使用方式：显式配置 json 可用于回滚、渐进迁移和人工排障。
	NameJSON = "json"
)

// msgpackHandle 保存 msgpack codec 的共享句柄配置。
//
// 设计思路：ugorji/codec 默认支持 codec/json tag；这里额外指定 map[string]any，
// 保证无 schema 解码时和标准 JSON 更接近，避免解出 map[interface{}]interface{}。
// RawToString/WriteExt 让字符串按 msgpack 新规范读取为 string，同时普通 []byte 仍按
// binary bytes 处理，满足 cache 普通 []byte 不嵌入 raw JSON 的要求。
var msgpackHandle = func() *codec.MsgpackHandle {
	handle := &codec.MsgpackHandle{}
	handle.MapType = reflect.TypeOf(map[string]any{})
	handle.RawToString = true
	handle.WriteExt = true
	return handle
}()

// jsonCodec 是标准库 JSON 的 Payload Encoding 实现。
//
// 设计原因：JSON 模式承担回滚、渐进迁移和人工排障场景，因此保持 encoding/json 的默认语义，
// 不额外加入 raw bytes、fallback 或 Prismgo 私有扩展。
type jsonCodec struct{}

// Marshal 使用 Go 标准库 JSON 编码 payload。
//
// 参数说明：v 是 cache/session/queue/Horizon 等内部能力要持久化的值。
// 设计原因：JSON codec 必须尽量保持旧格式兼容，因此直接复用 encoding/json 的语义。
func (jsonCodec) Marshal(v any) ([]byte, error) {
	return stdjson.Marshal(v)
}

// Unmarshal 使用 Go 标准库 JSON 解码 payload。
//
// 参数说明：data 是 JSON 模式下从内部 store 读取出的 payload bytes；v 是解码目标。
func (jsonCodec) Unmarshal(data []byte, v any) error {
	return stdjson.Unmarshal(data, v)
}

// Name 返回 JSON codec 的规范配置名。
func (jsonCodec) Name() string {
	return NameJSON
}

// msgpackCodec 是基于 ugorji/codec 的 msgpack Payload Encoding 实现。
//
// 设计思路：实现类型不导出，调用方只能通过 JSON、Msgpack、Resolve 等公开构造入口获取
// Codec，避免第一版出现外部依赖具体实现类型或自定义注册能力。
type msgpackCodec struct{}

// Marshal 使用 ugorji/codec 将 payload 编码为 msgpack bytes。
//
// 参数说明：v 是内部 payload 值。结构体字段名沿用 json tag，且不启用 struct-to-array，
// 避免字段顺序成为持久化协议。
func (msgpackCodec) Marshal(v any) ([]byte, error) {
	var out []byte
	encoder := codec.NewEncoderBytes(&out, msgpackHandle)
	if err := encoder.Encode(v); err != nil {
		return nil, err
	}
	return out, nil
}

// Unmarshal 使用 ugorji/codec 将 msgpack bytes 解码到目标值。
//
// 参数说明：data 必须是当前 msgpack codec 生成的 bytes；v 是调用方提供的可写目标。
// 解码失败时直接返回错误，调用方不得在严格装配路径静默回退到 JSON。
func (msgpackCodec) Unmarshal(data []byte, v any) error {
	decoder := codec.NewDecoderBytes(data, msgpackHandle)
	return decoder.Decode(v)
}

// Name 返回 msgpack codec 的规范配置名。
func (msgpackCodec) Name() string {
	return NameMsgpack
}

// JSON 返回内置 JSON Payload Encoding codec。
//
// 使用方式：配置显式选择 json 时调用，主要服务于回滚、人工排障和旧格式兼容。
func JSON() contract.Codec {
	return jsonCodec{}
}

// Msgpack 返回内置 msgpack Payload Encoding codec。
//
// 使用方式：全局默认或能力级配置选择 msgpack 时调用，是第一版默认 codec。
func Msgpack() contract.Codec {
	return msgpackCodec{}
}

// NormalizeName 将配置中的 codec 名称归一化。
//
// 参数说明：name 来自 encoding.default 或能力级 encoding 配置。
// 设计思路：空字符串不是非法值，而是“继承上层默认”；非法名称必须返回错误，
// 严格装配路径据此阻止对应能力启动，避免静默回退。
func NormalizeName(name string) (string, error) {
	normalized := strings.ToLower(strings.TrimSpace(name))
	switch normalized {
	case "", NameMsgpack, NameJSON:
		return normalized, nil
	default:
		return "", fmt.Errorf("encoding: unsupported payload encoding %q", name)
	}
}

// Resolve 按规范名称解析内置 Payload Encoding codec。
//
// 参数说明：name 必须是非空的 msgpack 或 json。需要处理继承语义时应使用
// ResolveWithDefault，避免把空值误当成默认 codec。
func Resolve(name string) (contract.Codec, error) {
	normalized, err := NormalizeName(name)
	if err != nil {
		return nil, err
	}
	switch normalized {
	case NameMsgpack:
		return Msgpack(), nil
	case NameJSON:
		return JSON(), nil
	default:
		return nil, fmt.Errorf("encoding: payload encoding name is required")
	}
}

// ResolveWithDefault 处理全局默认与能力级覆盖后的 codec 解析。
//
// 参数说明：defaultName 是 encoding.default；overrideName 是 cache/session/queue/horizon
// 等能力自己的 encoding 配置。overrideName 为空时继承 defaultName；defaultName 为空时
// 最终归一为 msgpack。
func ResolveWithDefault(defaultName, overrideName string) (contract.Codec, error) {
	def, err := NormalizeName(defaultName)
	if err != nil {
		return nil, err
	}
	override, err := NormalizeName(overrideName)
	if err != nil {
		return nil, err
	}
	if def == "" {
		def = NameMsgpack
	}
	if override != "" {
		return Resolve(override)
	}
	return Resolve(def)
}
