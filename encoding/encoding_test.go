package encoding

import (
	"bytes"
	"encoding/json"
	"testing"
)

// taggedPayload 是 Payload Encoding 测试使用的结构体样本。
//
// 需求背景：runtime retry contract 要求 msgpack 与 JSON 一样沿用 json tag 作为持久化字段名，并尊重
// json:"-" 与 omitempty；集中定义样本可以让多个测试复用同一组 schema 约束。
type taggedPayload struct {
	// PublicName 用于验证 json tag 字段名在 JSON 和 msgpack 下都作为持久化 schema。
	PublicName string `json:"public_name"`
	// Secret 用于验证 json:"-" 字段不会进入内部 payload，避免泄露不应持久化的数据。
	Secret string `json:"-"`
	// Empty 用于验证 omitempty 语义，避免零值字段污染 msgpack map schema。
	Empty string `json:"empty,omitempty"`
}

// nestedAddress 是嵌套结构体测试中使用的内层地址结构。
//
// 需求背景：cache/session/queue 存储的 payload 通常包含嵌套 struct，需要验证
// msgpack 在嵌套层级仍然沿用 json tag 作为字段名。
type nestedAddress struct {
	// City 验证嵌套 struct 的 json tag 字段名。
	City string `json:"city"`
	// Zip 验证嵌套 struct 的 json tag 字段名。
	Zip string `json:"zip"`
}

// nestedPayload 是嵌套结构体测试样本。
//
// 需求背景：cache/session/queue 存储的 payload 通常包含嵌套 struct、map、slice，
// 需要验证 msgpack schema-less 解码在所有层级都产出 map[string]any，并且 json tag、
// json:"-" 和 omitempty 在嵌套结构中同样生效。
type nestedPayload struct {
	// Name 验证顶层 json tag。
	Name string `json:"name"`
	// Address 验证嵌套 struct 的 msgpack 编解码行为。
	Address nestedAddress `json:"address"`
	// Tags 验证 string slice 的往返。
	Tags []string `json:"tags"`
	// Meta 验证 map 字段在 schema-less 解码时的类型行为。
	Meta map[string]any `json:"meta"`
	// Internal 验证 json:"-" 在嵌套结构中同样排除字段。
	Internal string `json:"-"`
	// Optional 验证 omitempty 在嵌套结构中同样省略零值字段。
	Optional string `json:"optional,omitempty"`
}

// TestBuiltInCodecsRoundTripPayloads 验证内置 codec 能按公共 Codec contract 完成往返。
//
// 需求背景：cache/session/queue/Horizon 后续都依赖同一套 contract；这里用公开构造函数
// 覆盖 JSON 和 msgpack 两条路径，确保调用方不用知道具体实现类型。
//
// 设计思路：测试只观察 Marshal/Unmarshal 的行为，不验证私有结构体或内部 handle 配置。
func TestBuiltInCodecsRoundTripPayloads(t *testing.T) {
	for _, codec := range []struct {
		name  string
		codec interface {
			Marshal(any) ([]byte, error)
			Unmarshal([]byte, any) error
			Name() string
		}
	}{
		{name: NameJSON, codec: JSON()},
		{name: NameMsgpack, codec: Msgpack()},
	} {
		t.Run(codec.name, func(t *testing.T) {
			in := taggedPayload{PublicName: "alice", Secret: "hidden"}
			data, err := codec.codec.Marshal(in)
			if err != nil {
				t.Fatalf("marshal failed: %v", err)
			}
			var out taggedPayload
			if err := codec.codec.Unmarshal(data, &out); err != nil {
				t.Fatalf("unmarshal failed: %v", err)
			}
			if out.PublicName != "alice" || out.Secret != "" {
				t.Fatalf("round trip = %#v, want public name only", out)
			}
		})
	}
}

// TestMsgpackUsesJSONTagsAsMapFieldNames 验证 msgpack 结构体编码沿用 json tag。
//
// 需求背景：Payload Encoding 的持久化字段名必须继续使用现有 json tag，避免要求业务
// 同时维护 msgpack tag。测试还确认没有启用 struct-to-array，字段顺序不会成为协议。
func TestMsgpackUsesJSONTagsAsMapFieldNames(t *testing.T) {
	data, err := Msgpack().Marshal(taggedPayload{PublicName: "alice", Secret: "hidden"})
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	if json.Valid(data) {
		t.Fatal("msgpack data should not be JSON")
	}
	var out map[string]any
	if err := Msgpack().Unmarshal(data, &out); err != nil {
		t.Fatalf("unmarshal map failed: %v", err)
	}
	if out["public_name"] != "alice" {
		t.Fatalf("public_name = %#v, want alice", out["public_name"])
	}
	if _, ok := out["Secret"]; ok {
		t.Fatalf("json:\"-\" field should not be encoded: %#v", out)
	}
	if _, ok := out["empty"]; ok {
		t.Fatalf("omitempty field should not be encoded: %#v", out)
	}
}

// TestResolveStrictlySupportsBuiltInCodecs 验证 codec 名称解析只接受内置集合。
//
// 参数说明：测试输入覆盖大小写/空白归一化、非法名称、以及缺少继承上下文的空值。
// 设计原因：严格装配路径必须把非法 encoding 暴露为错误，不能静默回退。
func TestResolveStrictlySupportsBuiltInCodecs(t *testing.T) {
	codec, err := Resolve(" JSON ")
	if err != nil {
		t.Fatalf("resolve json failed: %v", err)
	}
	if codec.Name() != NameJSON {
		t.Fatalf("codec name = %q, want json", codec.Name())
	}
	if _, err := Resolve("gob"); err == nil {
		t.Fatal("expected unsupported encoding error")
	}
	if _, err := Resolve(""); err == nil {
		t.Fatal("empty Resolve should fail without inheritance context")
	}
}

// TestResolveWithDefaultInheritsAndNormalizesEmptyGlobalDefault 验证全局默认与能力级覆盖规则。
//
// 需求背景：encoding.default 可以为空但最终必须归一为 msgpack；能力级 encoding 为空时继承，
// 非空时覆盖全局默认。该测试固定第一版的配置粒度。
func TestResolveWithDefaultInheritsAndNormalizesEmptyGlobalDefault(t *testing.T) {
	codec, err := ResolveWithDefault("", "")
	if err != nil {
		t.Fatalf("resolve inherited default failed: %v", err)
	}
	if codec.Name() != NameMsgpack {
		t.Fatalf("codec = %q, want msgpack", codec.Name())
	}
	codec, err = ResolveWithDefault("msgpack", "json")
	if err != nil {
		t.Fatalf("resolve override failed: %v", err)
	}
	if codec.Name() != NameJSON {
		t.Fatalf("codec = %q, want json", codec.Name())
	}
}

// TestJSONCodecUsesStandardByteSliceBase64Semantics 验证通用 JSON codec 的 []byte 行为。
//
// 设计思路：普通 []byte 应使用 encoding/json 的 base64 string 语义；queue raw JSON 兼容
// 由 queue 专用类型处理，不在通用 codec 中把所有 []byte 当 raw JSON 嵌入。
func TestJSONCodecUsesStandardByteSliceBase64Semantics(t *testing.T) {
	data, err := JSON().Marshal([]byte("hello"))
	if err != nil {
		t.Fatalf("marshal bytes failed: %v", err)
	}
	if !bytes.Equal(data, []byte(`"aGVsbG8="`)) {
		t.Fatalf("json bytes = %s, want base64 string", data)
	}
}

// TestMsgpackCodecRoundTripsByteSlice 验证 msgpack codec 的 []byte 往返完整性。
//
// 需求背景：cache/session/queue/horizon 大量涉及二进制 payload 存储，msgpack handle 配置了
// WriteExt=true（[]byte 编码为 msgpack bin 类型）和 RawToString=true（旧规范 raw 解码为 string）。
// 必须保证 []byte 经过 Marshal→Unmarshal 后内容完整保留，不被意外转为 string 或丢失。
//
// 设计思路：覆盖普通字节、含零字节的二进制数据、空字节切片三种场景，确保 msgpack bin 类型
// 往返不受 RawToString 影响。
func TestMsgpackCodecRoundTripsByteSlice(t *testing.T) {
	cases := []struct {
		name  string
		input []byte
	}{
		{name: "普通文本字节", input: []byte("hello msgpack")},
		{name: "含零字节的二进制数据", input: []byte{0x00, 0xFF, 0x01, 0xFE, 0x80}},
		{name: "空字节切片", input: []byte{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			data, err := Msgpack().Marshal(tc.input)
			if err != nil {
				t.Fatalf("marshal failed: %v", err)
			}
			var out []byte
			if err := Msgpack().Unmarshal(data, &out); err != nil {
				t.Fatalf("unmarshal failed: %v", err)
			}
			if !bytes.Equal(out, tc.input) {
				t.Fatalf("round trip = %v, want %v", out, tc.input)
			}
		})
	}
}

// TestMsgpackDecodesNestedStructAsMapStringAny 验证 msgpack schema-less 解码在嵌套
// 层级产出 map[string]any，而非 map[interface{}]interface{}。
//
// 需求背景：msgpackHandle 设置了 MapType=map[string]any{}，保证无 schema 解码时和标准
// JSON 更接近。cache/session/queue 实际 payload 包含嵌套 struct 和 map，如果 MapType
// 配置在嵌套层级不生效，解码出的 map[interface{}]interface{} 无法直接断言为
// map[string]any，会导致调用方类型 panic 或 JSON 序列化失败。
//
// 设计思路：用一个包含嵌套 struct、map[string]any、[]string 的复合 payload 编码后，
// 解码到 map[string]any 并逐层断言类型和值。同时验证 json:"-" 和 omitempty 在嵌套
// 结构中同样生效。
func TestMsgpackDecodesNestedStructAsMapStringAny(t *testing.T) {
	in := nestedPayload{
		Name:     "test-shop",
		Address:  nestedAddress{City: "Shanghai", Zip: "200000"},
		Tags:     []string{"priority", "vip"},
		Meta:     map[string]any{"region": "cn-east", "count": int64(42)},
		Internal: "should-be-excluded",
	}
	data, err := Msgpack().Marshal(in)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	// schema-less 解码：目标类型是 map[string]any，验证 MapType 配置在所有层级生效。
	var out map[string]any
	if err := Msgpack().Unmarshal(data, &out); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	// 顶层字段验证 json tag 作为持久化字段名。
	if out["name"] != "test-shop" {
		t.Fatalf("name = %v, want test-shop", out["name"])
	}

	// 嵌套 struct 必须解码为 map[string]any，不能是 map[interface{}]interface{}。
	addr, ok := out["address"].(map[string]any)
	if !ok {
		t.Fatalf("address type = %T, want map[string]any", out["address"])
	}
	if addr["city"] != "Shanghai" {
		t.Fatalf("address.city = %v, want Shanghai", addr["city"])
	}
	if addr["zip"] != "200000" {
		t.Fatalf("address.zip = %v, want 200000", addr["zip"])
	}

	// string slice 往返。
	tags, ok := out["tags"].([]any)
	if !ok {
		t.Fatalf("tags type = %T, want []any", out["tags"])
	}
	if len(tags) != 2 || tags[0] != "priority" || tags[1] != "vip" {
		t.Fatalf("tags = %v, want [priority vip]", tags)
	}

	// map[string]any 字段必须解码为 map[string]any。
	meta, ok := out["meta"].(map[string]any)
	if !ok {
		t.Fatalf("meta type = %T, want map[string]any", out["meta"])
	}
	if meta["region"] != "cn-east" {
		t.Fatalf("meta.region = %v, want cn-east", meta["region"])
	}

	// json:"-" 字段在嵌套结构中同样排除。
	if _, ok := out["Internal"]; ok {
		t.Fatal("json:\"-\" field Internal should not be encoded in nested struct")
	}

	// omitempty 零值字段在嵌套结构中同样省略。
	if _, ok := out["optional"]; ok {
		t.Fatal("omitempty field optional should not be encoded when empty")
	}
}
