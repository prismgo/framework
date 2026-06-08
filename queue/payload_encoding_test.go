package queue

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	miniredis "github.com/alicebob/miniredis/v2"
	configpkg "github.com/prismgo/framework/config"
	queuecontract "github.com/prismgo/framework/contracts/queue"
	encodingpkg "github.com/prismgo/framework/encoding"
	"github.com/prismgo/framework/queue/payload"
	redisqueue "github.com/prismgo/framework/queue/redis"
	"github.com/redis/go-redis/v9"
)

func TestRegistryMarshalUnmarshalUsesQueuePayloadEncoding(t *testing.T) {
	registry := NewRegistry()
	job := &testJob{Key: "encoded"}
	name, err := JobTypeName(job)
	if err != nil {
		t.Fatalf("JobTypeName error = %v", err)
	}
	registry.registerJobType(job)

	msgpackPayload, err := registry.marshalWithCodec(job, encodingpkg.Msgpack())
	if err != nil {
		t.Fatalf("msgpack marshal error = %v", err)
	}
	if json.Valid(msgpackPayload) {
		t.Fatalf("msgpack registry payload should not be JSON: %s", msgpackPayload)
	}
	restored, err := registry.unmarshalWithCodec(name, msgpackPayload, encodingpkg.Msgpack())
	if err != nil {
		t.Fatalf("msgpack unmarshal error = %v", err)
	}
	if restored.(*testJob).Key != "encoded" {
		t.Fatalf("restored msgpack job = %#v", restored)
	}

	jsonPayload, err := registry.marshalWithCodec(job, encodingpkg.JSON())
	if err != nil {
		t.Fatalf("json marshal error = %v", err)
	}
	body, err := payload.QueueCodec(encodingpkg.JSON()).Marshal(payload.Envelope{Payload: jsonPayload})
	if err != nil {
		t.Fatalf("json envelope marshal error = %v", err)
	}
	if !json.Valid(jsonPayload) || !json.Valid(body) {
		t.Fatalf("json registry payload should stay raw JSON: %s", jsonPayload)
	}
}

func TestRedisQueueUsesQueuePayloadEncodingForEnvelope(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	msgpackConn := redisqueue.NewRedisQueueFromClient(client, redisqueue.RedisOptions{Name: "redis_msgpack", Prefix: "redis_msgpack"})
	env := &payload.Envelope{ID: "job-msgpack", Name: testJobName(), Queue: "default", Payload: payload.Payload(`{"key":"redis"}`), CreatedAt: 1, AvailableAt: 1}
	msgpackBody, err := msgpackConn.Codec().Marshal(env)
	if err != nil {
		t.Fatalf("msgpack envelope marshal error = %v", err)
	}
	if err := msgpackConn.Push(context.Background(), "default", queuecontract.Payload(msgpackBody)); err != nil {
		t.Fatalf("msgpack redis Push error = %v", err)
	}
	rawItems, err := server.List(msgpackConn.ReadyKey("default"))
	if err != nil {
		t.Fatalf("read raw msgpack redis envelope: %v", err)
	}
	raw := rawItems[0]
	if json.Valid([]byte(raw)) {
		t.Fatalf("default redis envelope should be msgpack, got JSON: %s", raw)
	}
	reserved, err := msgpackConn.Pop(context.Background(), []string{"default"})
	popped := reservedEnvelope(reserved)
	if err != nil {
		t.Fatalf("msgpack redis Pop error = %v", err)
	}
	if popped == nil || popped.ID != "job-msgpack" || popped.Attempts != 1 {
		t.Fatalf("popped msgpack envelope = %#v", popped)
	}

	jsonConn := redisqueue.NewRedisQueueFromClient(client, redisqueue.RedisOptions{Name: "redis_json", Prefix: "redis_json", Codec: encodingpkg.JSON()})
	jsonEnv := &payload.Envelope{ID: "job-json", Name: testJobName(), Queue: "default", Payload: payload.Payload(`{"key":"json"}`), CreatedAt: 1, AvailableAt: 1}
	jsonBody, err := jsonConn.Codec().Marshal(jsonEnv)
	if err != nil {
		t.Fatalf("json envelope marshal error = %v", err)
	}
	if err := jsonConn.Push(context.Background(), "default", queuecontract.Payload(jsonBody)); err != nil {
		t.Fatalf("json redis Push error = %v", err)
	}
	jsonItems, err := server.List(jsonConn.ReadyKey("default"))
	if err != nil {
		t.Fatalf("read raw json redis envelope: %v", err)
	}
	jsonRaw := jsonItems[0]
	if !json.Valid([]byte(jsonRaw)) || !strings.Contains(jsonRaw, `"payload":{"key":"json"}`) {
		t.Fatalf("json redis envelope = %s, want raw nested payload JSON", jsonRaw)
	}
}

func TestQueuePoisonEnvelopeEventNamesPayloadEncodingAndBodyEncodingSeparately(t *testing.T) {
	for _, tc := range []struct {
		name  string
		codec interface {
			Marshal(any) ([]byte, error)
			Unmarshal([]byte, any) error
			Name() string
		}
		badBody string
	}{
		{name: "msgpack", codec: encodingpkg.Msgpack(), badBody: "{bad msgpack envelope"},
		{name: "json", codec: encodingpkg.JSON(), badBody: "{bad json envelope"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			server := miniredis.RunT(t)
			client := redis.NewClient(&redis.Options{Addr: server.Addr()})
			t.Cleanup(func() { _ = client.Close() })

			conn := redisqueue.NewRedisQueueFromClient(client, redisqueue.RedisOptions{Name: "redis_" + tc.name, Prefix: "redis_" + tc.name, Codec: tc.codec})
			var captured []PoisonEnvelope
			UseEventSink(func(_ context.Context, ev Event) {
				if poison, ok := ev.(PoisonEnvelope); ok {
					captured = append(captured, poison)
				}
			})
			t.Cleanup(func() { UseEventSink(nil) })

			if err := client.RPush(ctx, conn.ReadyKey("default"), tc.badBody).Err(); err != nil {
				t.Fatalf("push bad envelope: %v", err)
			}
			_, err := conn.Pop(ctx, []string{"default"})
			if !errors.Is(err, ErrPoisonEnvelope) {
				t.Fatalf("pop err = %v, want ErrPoisonEnvelope", err)
			}
			if strings.Contains(strings.ToLower(err.Error()), "bad json") {
				t.Fatalf("poison error should describe encoding decode, not hard-code JSON: %v", err)
			}
			if !strings.Contains(err.Error(), tc.codec.Name()) {
				t.Fatalf("poison error = %v, want %s payload encoding", err, tc.codec.Name())
			}
			if len(captured) != 1 {
				t.Fatalf("poison events = %d, want 1", len(captured))
			}
			event := captured[0]
			if event.Encoding != tc.codec.Name() || event.BodyEncoding != "base64" {
				t.Fatalf("event encoding fields = payload %q body %q, want %q/base64", event.Encoding, event.BodyEncoding, tc.codec.Name())
			}
			if event.BodyBase64 != base64.StdEncoding.EncodeToString([]byte(tc.badBody)) {
				t.Fatalf("event body_base64 = %q", event.BodyBase64)
			}
		})
	}
}

func TestQueueJSONCodecKeepsPayloadRawAtEnvelopeBoundaries(t *testing.T) {
	payloadType := reflect.TypeOf(payload.Payload{})
	if payloadType.PkgPath() == "encoding/json" || payloadType.Name() == "RawMessage" {
		t.Fatalf("queue.Payload must remain a queue public API type, got %s.%s", payloadType.PkgPath(), payloadType.Name())
	}
	codec := payload.QueueCodec(encodingpkg.JSON())
	env := payload.Envelope{
		Payload:  payload.Payload(`{"nested":"中文🙂"}`),
		Tags:     []string{"tenant:1", "mail"},
		Silenced: true,
		Chain:    []payload.PendingJob{{Name: "next", Payload: payload.Payload(`{"next":true}`)}},
	}
	body, err := codec.Marshal(env)
	if err != nil {
		t.Fatalf("json envelope marshal error = %v", err)
	}
	if strings.Contains(string(body), "eyJuZXN0ZWQi") || !strings.Contains(string(body), `"payload":{"nested":"中文🙂"}`) || !strings.Contains(string(body), `"payload":{"next":true}`) {
		t.Fatalf("json envelope payload = %s, want raw nested JSON rather than base64", body)
	}
	var restored payload.Envelope
	if err := codec.Unmarshal(body, &restored); err != nil {
		t.Fatalf("json envelope unmarshal error = %v", err)
	}
	if string(restored.Payload) != `{"nested":"中文🙂"}` || len(restored.Chain) != 1 || string(restored.Chain[0].Payload) != `{"next":true}` {
		t.Fatalf("restored envelope payloads = %s %#v", restored.Payload, restored.Chain)
	}
	if !restored.Silenced || len(restored.Tags) != 2 || restored.Tags[0] != "tenant:1" || restored.Tags[1] != "mail" {
		t.Fatalf("restored metadata = tags:%v silenced:%v", restored.Tags, restored.Silenced)
	}

	failedBody, err := codec.Marshal(payload.FailedJob{Envelope: env})
	if err != nil {
		t.Fatalf("json failed marshal error = %v", err)
	}
	if strings.Contains(string(failedBody), "eyJuZXN0ZWQi") || !strings.Contains(string(failedBody), `"payload":{"nested":"中文🙂"}`) {
		t.Fatalf("json failed payload = %s, want raw nested JSON", failedBody)
	}
	var restoredFailed payload.FailedJob
	if err := codec.Unmarshal(failedBody, &restoredFailed); err != nil {
		t.Fatalf("json failed unmarshal error = %v", err)
	}
	if string(restoredFailed.Envelope.Payload) != `{"nested":"中文🙂"}` || !restoredFailed.Envelope.Silenced {
		t.Fatalf("restored failed payload = %s", restoredFailed.Envelope.Payload)
	}
	if _, err := codec.Marshal(payload.Envelope{Payload: payload.Payload(`{"bad"`)}); err == nil || !strings.Contains(err.Error(), "payload") {
		t.Fatalf("invalid root payload err = %v, want payload path", err)
	}
	if _, err := codec.Marshal(payload.Envelope{Payload: payload.Payload(`{}`), Chain: []payload.PendingJob{{Payload: payload.Payload(`{"bad"`)}}}); err == nil || !strings.Contains(err.Error(), "chain[0].payload") {
		t.Fatalf("invalid chain payload err = %v, want chain payload path", err)
	}
	nilBody, err := codec.Marshal((*payload.Envelope)(nil))
	if err != nil || string(nilBody) != "null" {
		t.Fatalf("nil envelope marshal = %s err=%v", nilBody, err)
	}
	if payload.QueueCodec(nil).Name() != encodingpkg.NameMsgpack {
		t.Fatal("nil queue codec should default to msgpack")
	}
}

func TestBuildConfigQueueEncodingInheritsGlobalDefault(t *testing.T) {
	registry := useQueueTestContainer(t)
	t.Cleanup(func() {
		resetQueueEncodingConfigRegistryForTest()
	})
	configpkg.Add("encoding", func() map[string]any {
		return map[string]any{"default": encodingpkg.NameJSON}
	})
	configpkg.Add("queue", func() map[string]any {
		return map[string]any{"encoding": ""}
	})
	cfg := configpkg.New()
	if err := cfg.ReloadFromFile(filepath.Join(t.TempDir(), ".env")); err != nil {
		t.Fatalf("reload config: %v", err)
	}
	bindQueueConfigInRegistry(t, registry, cfg)

	built := BuildConfig()
	if built.Encoding != encodingpkg.NameJSON {
		t.Fatalf("BuildConfig encoding = %q, want inherited json", built.Encoding)
	}
	manager, err := NewManagerFromConfig()
	if err != nil {
		t.Fatalf("NewManagerFromConfig error = %v", err)
	}
	if manager.runtime.codec.Name() != encodingpkg.NameJSON {
		t.Fatalf("manager codec = %q, want json", manager.runtime.codec.Name())
	}
	direct, err := NewManager(Config{}, NewRegistry())
	if err != nil {
		t.Fatalf("direct NewManager error = %v", err)
	}
	if direct.runtime.codec.Name() != encodingpkg.NameMsgpack {
		t.Fatalf("direct NewManager codec = %q, want msgpack", direct.runtime.codec.Name())
	}
}

func TestBuildConfigQueueEncodingOverrideBeatsGlobalDefault(t *testing.T) {
	registry := useQueueTestContainer(t)
	t.Cleanup(func() {
		resetQueueEncodingConfigRegistryForTest()
	})
	configpkg.Add("encoding", func() map[string]any {
		return map[string]any{"default": encodingpkg.NameMsgpack}
	})
	configpkg.Add("queue", func() map[string]any {
		return map[string]any{"encoding": encodingpkg.NameJSON}
	})
	cfg := configpkg.New()
	if err := cfg.ReloadFromFile(filepath.Join(t.TempDir(), ".env")); err != nil {
		t.Fatalf("reload config: %v", err)
	}
	bindQueueConfigInRegistry(t, registry, cfg)

	if built := BuildConfig(); built.Encoding != encodingpkg.NameJSON {
		t.Fatalf("BuildConfig encoding = %q, want queue override json", built.Encoding)
	}
	manager, err := NewManagerFromConfig()
	if err != nil {
		t.Fatalf("NewManagerFromConfig with queue override error = %v", err)
	}
	if manager.runtime.codec.Name() != encodingpkg.NameJSON {
		t.Fatalf("manager codec = %q, want queue override json", manager.runtime.codec.Name())
	}
}

func TestBuildConfigQueueEncodingOverrideIgnoresInvalidGlobalDefault(t *testing.T) {
	registry := useQueueTestContainer(t)
	t.Cleanup(func() {
		resetQueueEncodingConfigRegistryForTest()
	})
	configpkg.Add("encoding", func() map[string]any {
		return map[string]any{"default": "bad-global"}
	})
	configpkg.Add("queue", func() map[string]any {
		return map[string]any{"encoding": encodingpkg.NameJSON}
	})
	cfg := configpkg.New()
	if err := cfg.ReloadFromFile(filepath.Join(t.TempDir(), ".env")); err != nil {
		t.Fatalf("reload config: %v", err)
	}
	bindQueueConfigInRegistry(t, registry, cfg)

	if built := BuildConfig(); built.Encoding != encodingpkg.NameJSON {
		t.Fatalf("BuildConfig encoding = %q, want explicit queue json", built.Encoding)
	}
	manager, err := NewManagerFromConfig()
	if err != nil {
		t.Fatalf("NewManagerFromConfig with invalid global and queue override error = %v", err)
	}
	if manager.runtime.codec.Name() != encodingpkg.NameJSON {
		t.Fatalf("manager codec = %q, want explicit queue json", manager.runtime.codec.Name())
	}
}

func resetQueueEncodingConfigRegistryForTest() {
	configpkg.Add("encoding", func() map[string]any {
		return map[string]any{"default": encodingpkg.NameMsgpack}
	})
	configpkg.Add("queue", func() map[string]any {
		return map[string]any{}
	})
}
