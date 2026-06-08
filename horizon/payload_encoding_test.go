package horizon

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"
	encodingpkg "github.com/prismgo/framework/encoding"
	"github.com/redis/go-redis/v9"
)

func TestRedisStoreDefaultPayloadEncodingStoresMsgpackRecords(t *testing.T) {
	// 需求背景：runtime command contract 要求 Horizon Redis Store records 默认使用 msgpack，而不是继续写旧 JSON。
	// 该测试通过公开 Heartbeat/Master 接口验证外部行为仍可读，同时直接检查 Redis bytes 不是 JSON，
	// 避免 Dashboard/API/CLI JSON 输出和 Store 内部持久化格式被混为一谈。
	ctx := context.Background()
	now := time.Date(2026, 5, 17, 9, 0, 0, 0, time.UTC)
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	store := NewRedisStoreFromClient(client, StoreOptions{
		Prefix:       "horizon_encoding",
		HeartbeatTTL: time.Minute,
		Encoding:     encodingpkg.NameMsgpack,
	})

	state := MasterState{
		ID:              "master-1",
		Host:            "host-1",
		PID:             10,
		Status:          MasterRunning,
		StartedAt:       now.Add(-time.Minute),
		LastHeartbeatAt: now,
		SupervisorCount: 2,
		Environment:     "local",
	}
	if err := store.HeartbeatMaster(ctx, state); err != nil {
		t.Fatalf("heartbeat master: %v", err)
	}

	raw, err := client.Get(ctx, "horizon_encoding:master:master-1").Bytes()
	if err != nil {
		t.Fatalf("raw master record: %v", err)
	}
	var asJSON map[string]any
	if err := json.Unmarshal(raw, &asJSON); err == nil {
		t.Fatalf("default horizon records must not be JSON bytes: %#v", asJSON)
	}

	read, ok, err := store.Master(ctx, "master-1", now)
	if err != nil || !ok {
		t.Fatalf("read msgpack master: ok=%v err=%v", ok, err)
	}
	if read.ID != state.ID || read.SupervisorCount != state.SupervisorCount || read.Environment != state.Environment {
		t.Fatalf("read master = %#v, want %#v", read, state)
	}
}

func TestRedisStoreExplicitJSONPayloadEncodingKeepsReadableRecords(t *testing.T) {
	// 需求背景：显式 HORIZON_ENCODING=json 用于回滚、渐进迁移和人工排障，因此 RedisStore 写出的
	// Horizon record 应保持旧 JSON bytes 形态；读取仍必须只通过公开 Store 方法完成。
	ctx := context.Background()
	now := time.Date(2026, 5, 17, 9, 10, 0, 0, time.UTC)
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	store := NewRedisStoreFromClient(client, StoreOptions{
		Prefix:       "horizon_json",
		HeartbeatTTL: time.Minute,
		Encoding:     encodingpkg.NameJSON,
	})

	if err := store.SaveQueueLengthSnapshot(ctx, QueueLengthSnapshot{
		CapturedAt: now,
		Queues: []QueueLengthBucket{{
			Connection: "redis",
			Queue:      "default",
			Size:       42,
		}},
	}); err != nil {
		t.Fatalf("save queue length snapshot: %v", err)
	}

	raw, err := client.Get(ctx, "horizon_json:metrics:queue_lengths").Bytes()
	if err != nil {
		t.Fatalf("raw queue length record: %v", err)
	}
	if !json.Valid(raw) || !strings.Contains(string(raw), `"queues"`) {
		t.Fatalf("json horizon record = %q, want readable JSON object", raw)
	}
	read, err := store.QueueLengthSnapshot(ctx)
	if err != nil {
		t.Fatalf("read json queue length snapshot: %v", err)
	}
	if len(read.Queues) != 1 || read.Queues[0].Size != 42 {
		t.Fatalf("read queue length snapshot = %#v", read)
	}
}

func TestRedisStoreDefaultPayloadEncodingDoesNotFallbackToJSONRecords(t *testing.T) {
	// 需求背景：默认 msgpack 模式不做 JSON fallback，旧 Horizon RedisStore JSON record 不自动迁移。
	// 该测试手动写入旧 JSON bytes，验证读取单实体时返回解码错误，而不是隐式当成 JSON 继续消费。
	ctx := context.Background()
	now := time.Date(2026, 5, 17, 9, 20, 0, 0, time.UTC)
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	store := NewRedisStoreFromClient(client, StoreOptions{
		Prefix:       "horizon_no_fallback",
		HeartbeatTTL: time.Minute,
	})
	oldJSON, err := json.Marshal(MasterState{
		ID:              "master-json",
		Status:          MasterRunning,
		LastHeartbeatAt: now,
	})
	if err != nil {
		t.Fatalf("marshal old json record: %v", err)
	}
	if err := client.Set(ctx, "horizon_no_fallback:master:master-json", oldJSON, time.Minute).Err(); err != nil {
		t.Fatalf("seed old json record: %v", err)
	}

	if _, ok, err := store.Master(ctx, "master-json", now); err == nil || ok {
		t.Fatalf("default msgpack should reject old JSON record with ok=false and err, got ok=%v err=%v", ok, err)
	}
}

func TestHorizonRejectsInvalidPayloadEncodingOnStrictManagerPath(t *testing.T) {
	// 需求背景：Manager 是命令、HTTP 和 provider 复用的严格装配边界。非法 horizon.encoding
	// 必须在构造期显式失败，避免 RedisStore 低层 helper 的兼容兜底掩盖配置错误。
	if _, err := NewManager(Config{Store: "memory", Encoding: "gob"}); err == nil || !strings.Contains(err.Error(), "horizon.encoding") {
		t.Fatalf("NewManager invalid encoding error = %v, want horizon.encoding config error", err)
	}
}

func TestMemoryStoreBehaviorIsIndependentOfPayloadEncoding(t *testing.T) {
	// 需求背景：MemoryStore 是进程内测试/降级 store，可以继续保存 Go struct；即使调用方传入
	// Encoding，也只应影响 RedisStore 的持久化 bytes，不应改变 MemoryStore 的公开读模型行为。
	ctx := context.Background()
	now := time.Date(2026, 5, 17, 9, 30, 0, 0, time.UTC)
	store := NewMemoryStore(StoreOptions{
		Prefix:       "memory_encoding",
		HeartbeatTTL: time.Minute,
		Encoding:     encodingpkg.NameJSON,
	})

	if err := store.HeartbeatWorker(ctx, WorkerState{
		ID:              "worker-1",
		Supervisor:      "supervisor-default",
		Status:          WorkerIdle,
		LastHeartbeatAt: now,
		ConfiguredQueues: []string{
			"default",
		},
	}); err != nil {
		t.Fatalf("heartbeat worker: %v", err)
	}
	read, ok, err := store.Worker(ctx, "worker-1", now)
	if err != nil || !ok {
		t.Fatalf("read memory worker: ok=%v err=%v", ok, err)
	}
	if read.ID != "worker-1" || read.Status != WorkerIdle || len(read.ConfiguredQueues) != 1 {
		t.Fatalf("memory worker = %#v", read)
	}
}

func TestHorizonRecordPlainBytesUseCodecDefaultSemantics(t *testing.T) {
	// 需求背景：Horizon record 中如果后续出现普通 []byte 字段，不能像 queue.Payload 那样被当成
	// raw JSON。该测试把 []byte 放在 Store record 编码边界上，验证 JSON 使用标准 base64 字符串，
	// msgpack 仍能按二进制 bytes round-trip。
	type byteRecord struct {
		Name string `json:"name"`
		Body []byte `json:"body"`
	}
	record := byteRecord{Name: "payload", Body: []byte{0x01, 0x02, 0x03}}

	jsonStore := NewRedisStoreFromClient(nil, StoreOptions{Encoding: encodingpkg.NameJSON})
	jsonBody, err := jsonStore.encodeRecord(record)
	if err != nil {
		t.Fatalf("encode json byte record: %v", err)
	}
	var raw map[string]string
	if err := json.Unmarshal(jsonBody, &raw); err != nil {
		t.Fatalf("decode json byte record as map: %v", err)
	}
	if raw["body"] != base64.StdEncoding.EncodeToString(record.Body) {
		t.Fatalf("json []byte field = %q, want standard base64", raw["body"])
	}

	msgpackStore := NewRedisStoreFromClient(nil, StoreOptions{Encoding: encodingpkg.NameMsgpack})
	msgpackBody, err := msgpackStore.encodeRecord(record)
	if err != nil {
		t.Fatalf("encode msgpack byte record: %v", err)
	}
	var decoded byteRecord
	if err := msgpackStore.decodeRecord(msgpackBody, &decoded); err != nil {
		t.Fatalf("decode msgpack byte record: %v", err)
	}
	if decoded.Name != record.Name || !bytes.Equal(decoded.Body, record.Body) {
		t.Fatalf("decoded msgpack byte record = %#v", decoded)
	}
}
