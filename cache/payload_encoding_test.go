package cache

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	encodingpkg "github.com/prismgo/framework/encoding"
)

// cacheEncodingPayload 是 cache Payload Encoding 测试使用的结构体样本。
//
// 设计思路：字段同时覆盖 json tag、json:"-" 和普通字符串值，便于验证 cache value 通过
// repository codec 编码后仍保持 Prismgo 约定的持久化字段 schema。
type cacheEncodingPayload struct {
	// PublicName 用于验证 cache value 编码沿用 json tag 字段名。
	PublicName string `json:"public_name"`
	// Secret 用于验证 cache value 编码不会持久化 json:"-" 字段。
	Secret string `json:"-"`
}

// TestCacheDefaultPayloadEncodingStoresMsgpackValues 验证 cache 默认 value 编码为 msgpack。
//
// 需求背景：Laravel config contract 要求 cache.encoding 为空时继承 encoding.default，最终默认行为是 msgpack。
// 设计思路：通过公开 Put 写入，再读取 memory store 中的实际 bytes，确认落盘格式不是 JSON，
// 同时用 repository codec 解码验证 json tag 仍然作为持久化字段名。
func TestCacheDefaultPayloadEncodingStoresMsgpackValues(t *testing.T) {
	ctx := context.Background()
	m, err := NewManager(Config{
		Default: "memory",
		Stores:  map[string]StoreConfig{"memory": {Driver: "memory"}},
	})
	if err != nil {
		t.Fatalf("new manager failed: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })
	repo := m.defaultRepository()

	if err := repo.Put(ctx, "profile", cacheEncodingPayload{PublicName: "alice", Secret: "hidden"}, time.Minute); err != nil {
		t.Fatalf("put failed: %v", err)
	}
	raw := rawMemoryValue(t, repo, "profile")
	if bytes.HasPrefix(bytes.TrimSpace(raw), []byte("{")) {
		t.Fatalf("default cache payload should not be JSON: %q", raw)
	}
	var decoded cacheEncodingPayload
	if err := repo.decode(raw, &decoded); err != nil {
		t.Fatalf("decode raw msgpack failed: %v", err)
	}
	if decoded.PublicName != "alice" || decoded.Secret != "" {
		t.Fatalf("decoded = %#v, want public field only", decoded)
	}
}

// TestCacheExplicitJSONPayloadEncodingKeepsJSONBytes 验证显式 JSON 模式保持旧格式可读性。
//
// 需求背景：显式 cache.encoding=json 用于回滚、渐进迁移和人工排障，因此写入 bytes 应尽量
// 保持历史 JSON cache value 行为。
func TestCacheExplicitJSONPayloadEncodingKeepsJSONBytes(t *testing.T) {
	ctx := context.Background()
	m, err := NewManager(Config{
		Default:  "memory",
		Encoding: encodingpkg.NameJSON,
		Stores:   map[string]StoreConfig{"memory": {Driver: "memory"}},
	})
	if err != nil {
		t.Fatalf("new manager failed: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })
	repo := m.defaultRepository()

	if err := repo.Put(ctx, "profile", cacheEncodingPayload{PublicName: "alice", Secret: "hidden"}, time.Minute); err != nil {
		t.Fatalf("put failed: %v", err)
	}
	raw := rawMemoryValue(t, repo, "profile")
	if !bytes.Contains(raw, []byte(`"public_name":"alice"`)) {
		t.Fatalf("json payload = %s, want public_name field", raw)
	}
	if bytes.Contains(raw, []byte("hidden")) {
		t.Fatalf("json payload should honor json:\"-\": %s", raw)
	}
}

// TestCacheRejectsInvalidPayloadEncoding 验证 cache 严格装配路径拒绝非法 encoding。
//
// 设计原因：非法配置不能静默回退到 msgpack 或 JSON，否则运维会误以为配置已经生效。
func TestCacheRejectsInvalidPayloadEncoding(t *testing.T) {
	_, err := NewManager(Config{
		Default:  "memory",
		Encoding: "gob",
		Stores:   map[string]StoreConfig{"memory": {Driver: "memory"}},
	})
	if err == nil {
		t.Fatal("expected invalid cache encoding error")
	}
}

// TestCacheByteSlicesUseCodecDefaultSemantics 验证普通 []byte 使用各 codec 默认规则。
//
// 需求背景：cache 普通 []byte 不是 queue raw JSON payload；JSON 模式应是标准 base64 string，
// msgpack 模式应是 binary bytes，并且两者都能通过当前 repository codec 正确读回。
func TestCacheByteSlicesUseCodecDefaultSemantics(t *testing.T) {
	ctx := context.Background()
	jsonManager, err := NewManager(Config{
		Default:  "memory",
		Encoding: encodingpkg.NameJSON,
		Stores:   map[string]StoreConfig{"memory": {Driver: "memory"}},
	})
	if err != nil {
		t.Fatalf("new json manager failed: %v", err)
	}
	t.Cleanup(func() { _ = jsonManager.Close() })
	if err := jsonManager.defaultRepository().Put(ctx, "bytes", []byte("hello"), time.Minute); err != nil {
		t.Fatalf("json put failed: %v", err)
	}
	if got := rawMemoryValue(t, jsonManager.defaultRepository(), "bytes"); !bytes.Equal(got, []byte(`"aGVsbG8="`)) {
		t.Fatalf("json []byte payload = %s, want base64 JSON string", got)
	}

	msgpackManager, err := NewManager(Config{
		Default: "memory",
		Stores:  map[string]StoreConfig{"memory": {Driver: "memory"}},
	})
	if err != nil {
		t.Fatalf("new msgpack manager failed: %v", err)
	}
	t.Cleanup(func() { _ = msgpackManager.Close() })
	if err := msgpackManager.defaultRepository().Put(ctx, "bytes", []byte("hello"), time.Minute); err != nil {
		t.Fatalf("msgpack put failed: %v", err)
	}
	var out []byte
	raw := rawMemoryValue(t, msgpackManager.defaultRepository(), "bytes")
	if bytes.Equal(raw, []byte(`"aGVsbG8="`)) {
		t.Fatalf("msgpack []byte payload should not use JSON base64 bytes: %s", raw)
	}
	if err := msgpackManager.defaultRepository().decode(raw, &out); err != nil {
		t.Fatalf("decode msgpack []byte failed: %v", err)
	}
	if !bytes.Equal(out, []byte("hello")) {
		t.Fatalf("decoded bytes = %q, want hello", out)
	}
}

// TestCacheCountersDoNotUsePayloadEncoding 验证计数器路径不走 Payload Encoding。
//
// 需求背景：Increment/Decrement 是底层 store 的原子计数器语义，不属于 cache value 编码边界。
// 设计思路：直接检查 memory store 中的原始 bytes，确认计数器仍是十进制表示。
func TestCacheCountersDoNotUsePayloadEncoding(t *testing.T) {
	ctx := context.Background()
	m, err := NewManager(Config{
		Default: "memory",
		Stores:  map[string]StoreConfig{"memory": {Driver: "memory"}},
	})
	if err != nil {
		t.Fatalf("new manager failed: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })
	repo := m.defaultRepository()

	if got, err := repo.Increment(ctx, "counter", 2); err != nil || got != 2 {
		t.Fatalf("increment = %d, %v; want 2 nil", got, err)
	}
	if raw := rawMemoryValue(t, repo, "counter"); !bytes.Equal(raw, []byte("2")) {
		t.Fatalf("counter raw bytes = %q, want decimal bytes", raw)
	}
	// 计数器 key 使用专用十进制 bytes 表示；调用方不应在同一个 key 上混用 Put/Get value
	// 语义和 Increment/Decrement 计数器语义。
}

// TestCacheNumericValuesUseRawDecimalPayloads 验证数值语义 value 直写为十进制 bytes。
//
// 需求背景：数值 value 应能直接作为底层原子计数器的初始值，同时普通复杂 value 仍走
// Payload Encoding。
func TestCacheNumericValuesUseRawDecimalPayloads(t *testing.T) {
	ctx := context.Background()
	m, err := NewManager(Config{
		Default: "memory",
		Stores:  map[string]StoreConfig{"memory": {Driver: "memory"}},
	})
	if err != nil {
		t.Fatalf("new manager failed: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })
	bindCacheManagerForTest(t, m)
	repo := m.defaultRepository()

	added, err := repo.Add(ctx, "count", 1, time.Minute)
	if err != nil || !added {
		t.Fatalf("add count = %v, %v; want true nil", added, err)
	}
	if raw := rawMemoryValue(t, repo, "count"); !bytes.Equal(raw, []byte("1")) {
		t.Fatalf("count raw bytes = %q, want 1", raw)
	}
	count, err := repo.Increment(ctx, "count")
	if err != nil || count != 2 {
		t.Fatalf("increment count = %d, %v; want 2 nil", count, err)
	}
	if raw := rawMemoryValue(t, repo, "count"); !bytes.Equal(raw, []byte("2")) {
		t.Fatalf("count raw bytes after increment = %q, want 2", raw)
	}
	got, err := GetFrom[int](ctx, "memory", "count")
	if err != nil || got != 2 {
		t.Fatalf("typed get count = %d, %v; want 2 nil", got, err)
	}
}

func TestCacheNumericStringsUseRawDecimalPayloads(t *testing.T) {
	ctx := context.Background()
	m, err := NewManager(Config{
		Default: "memory",
		Stores:  map[string]StoreConfig{"memory": {Driver: "memory"}},
	})
	if err != nil {
		t.Fatalf("new manager failed: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })
	bindCacheManagerForTest(t, m)
	repo := m.defaultRepository()

	if err := repo.Put(ctx, "string-count", " 1 ", time.Minute); err != nil {
		t.Fatalf("put numeric string failed: %v", err)
	}
	if raw := rawMemoryValue(t, repo, "string-count"); !bytes.Equal(raw, []byte("1")) {
		t.Fatalf("numeric string raw bytes = %q, want 1", raw)
	}
	count, err := repo.Increment(ctx, "string-count")
	if err != nil || count != 2 {
		t.Fatalf("increment numeric string = %d, %v; want 2 nil", count, err)
	}
	if err := repo.Put(ctx, "name", "alice", time.Minute); err != nil {
		t.Fatalf("put normal string failed: %v", err)
	}
	name, err := GetFrom[string](ctx, "memory", "name")
	if err != nil || name != "alice" {
		t.Fatalf("normal string = %q, %v; want alice nil", name, err)
	}
}

func TestCacheFloatValuesUseRawDecimalPayloadsButNotCounters(t *testing.T) {
	ctx := context.Background()
	m, err := NewManager(Config{
		Default: "memory",
		Stores:  map[string]StoreConfig{"memory": {Driver: "memory"}},
	})
	if err != nil {
		t.Fatalf("new manager failed: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })
	bindCacheManagerForTest(t, m)
	repo := m.defaultRepository()

	if err := repo.Put(ctx, "float", 1.5, time.Minute); err != nil {
		t.Fatalf("put float failed: %v", err)
	}
	if raw := rawMemoryValue(t, repo, "float"); !bytes.Equal(raw, []byte("1.5")) {
		t.Fatalf("float raw bytes = %q, want 1.5", raw)
	}
	got, err := GetFrom[float64](ctx, "memory", "float")
	if err != nil || got != 1.5 {
		t.Fatalf("typed get float = %f, %v; want 1.5 nil", got, err)
	}
	if _, err := GetFrom[int](ctx, "memory", "float"); err == nil {
		t.Fatal("expected int decode of float payload to fail")
	}
	if _, err := repo.Increment(ctx, "float"); !errors.Is(err, ErrInvalidCounter) {
		t.Fatalf("float increment err = %v, want ErrInvalidCounter", err)
	}
}

func TestCachePutManyNumericPayloadsDecodeThroughBatchPaths(t *testing.T) {
	ctx := context.Background()
	m, err := NewManager(Config{
		Default: "memory",
		Stores:  map[string]StoreConfig{"memory": {Driver: "memory"}},
	})
	if err != nil {
		t.Fatalf("new manager failed: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })
	bindCacheManagerForTest(t, m)
	repo := m.defaultRepository()

	if err := repo.PutMany(ctx, map[string]int{"one": 1, "two": 2}, time.Minute); err != nil {
		t.Fatalf("put many numeric failed: %v", err)
	}
	values, err := Many[int](ctx, []string{"one", "two"})
	if err != nil {
		t.Fatalf("many int failed: %v", err)
	}
	if values["one"] != 1 || values["two"] != 2 {
		t.Fatalf("many int values = %#v", values)
	}
	raw, err := repo.Many(ctx, []string{"one", "two"})
	if err != nil {
		t.Fatalf("repo many failed: %v", err)
	}
	if raw["one"].(float64) != 1 || raw["two"].(float64) != 2 {
		t.Fatalf("repo many values = %#v", raw)
	}
}

// TestCacheMsgpackDoesNotMigrateLegacyJSONValues 验证 msgpack 模式不自动迁移旧 JSON value。
//
// 需求背景：Laravel config contract 明确默认 msgpack 模式不做 JSON fallback 读取，也不做自动迁移写回。
// 设计思路：手动种入旧 JSON bytes，调用普通 Get 后确认底层 bytes 没被重写。
func TestCacheMsgpackDoesNotMigrateLegacyJSONValues(t *testing.T) {
	ctx := context.Background()
	m, err := NewManager(Config{
		Default: "memory",
		Stores:  map[string]StoreConfig{"memory": {Driver: "memory"}},
	})
	if err != nil {
		t.Fatalf("new manager failed: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })
	repo := m.defaultRepository()
	store := repo.store.(*memoryStore)
	if err := store.Set(ctx, repo.key("legacy"), []byte(`{"public_name":"alice"}`)); err != nil {
		t.Fatalf("seed legacy JSON failed: %v", err)
	}
	_, _ = repo.Get(ctx, "legacy")
	raw := rawMemoryValue(t, repo, "legacy")
	if !bytes.Equal(raw, []byte(`{"public_name":"alice"}`)) {
		t.Fatalf("legacy JSON bytes were rewritten: %q", raw)
	}
}

// rawMemoryValue 读取 memory store 中指定 key 的原始 bytes。
//
// 参数说明：repo 必须是 memory repository；key 是业务侧 cache key，会在 helper 内转换成
// repository 实际存储 key。该 helper 只用于测试落盘格式，不进入生产路径。
func rawMemoryValue(t *testing.T, repo *Repository, key string) []byte {
	t.Helper()
	store, ok := repo.store.(*memoryStore)
	if !ok {
		t.Fatalf("repo store = %T, want memoryStore", repo.store)
	}
	item, ok := store.items[repo.key(key)]
	if !ok {
		t.Fatalf("missing raw memory key %q", repo.key(key))
	}
	raw, err := rawBytes(item.value)
	if err != nil {
		t.Fatalf("raw bytes failed: %v", err)
	}
	return raw
}
