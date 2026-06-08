package session

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	encodingpkg "github.com/prismgo/framework/encoding"
	"github.com/redis/go-redis/v9"
)

func TestFileDriverUsesConfiguredPayloadEncoding(t *testing.T) {
	cfg := testConfig(t)
	driver := newTestFileDriver(t, cfg)
	id := newSessionID()

	if err := driver.Write(context.Background(), id, Payload{ID: id, Values: map[string]any{"name": "alice"}}, nil); err != nil {
		t.Fatalf("Write msgpack error = %v", err)
	}
	raw, err := os.ReadFile(driver.pathForID(id))
	if err != nil {
		t.Fatalf("read raw msgpack payload: %v", err)
	}
	if json.Valid(raw) {
		t.Fatalf("default payload should be msgpack, got JSON: %s", raw)
	}
	if _, err := driver.Read(context.Background(), id); err != nil {
		t.Fatalf("Read msgpack error = %v", err)
	}

	cfg.Encoding = encodingpkg.NameJSON
	jsonDriver := newTestFileDriver(t, cfg)
	jsonID := newSessionID()
	if err := jsonDriver.Write(context.Background(), jsonID, Payload{ID: jsonID, Values: map[string]any{"name": "bob"}}, nil); err != nil {
		t.Fatalf("Write json error = %v", err)
	}
	jsonRaw, err := os.ReadFile(jsonDriver.pathForID(jsonID))
	if err != nil {
		t.Fatalf("read raw json payload: %v", err)
	}
	if !json.Valid(jsonRaw) || !bytes.Contains(jsonRaw, []byte(`"bob"`)) {
		t.Fatalf("explicit json payload = %s, want JSON containing value", jsonRaw)
	}
}

func TestFileDriverDoesNotFallbackFromMsgpackToLegacyJSON(t *testing.T) {
	cfg := testConfig(t)
	driver := newTestFileDriver(t, cfg)
	id := newSessionID()
	raw, err := json.Marshal(Payload{ID: id, Values: map[string]any{"legacy": true}})
	if err != nil {
		t.Fatalf("marshal legacy fixture: %v", err)
	}
	if err := os.WriteFile(driver.pathForID(id), raw, 0o600); err != nil {
		t.Fatalf("write legacy fixture: %v", err)
	}
	if _, err := driver.Read(context.Background(), id); !errors.Is(err, ErrPayloadDeserialize) {
		t.Fatalf("Read legacy JSON under msgpack error = %v, want ErrPayloadDeserialize", err)
	}
}

func TestFileDriverEncryptsEncodedPayloadBytes(t *testing.T) {
	cfg := testConfig(t)
	cfg.Encrypt = true
	cfg.Encryptor = prefixEncryptor{}
	driver := newTestFileDriver(t, cfg)
	id := newSessionID()

	if err := driver.Write(context.Background(), id, Payload{ID: id, Values: map[string]any{"secret": "value"}}, nil); err != nil {
		t.Fatalf("Write encrypted msgpack error = %v", err)
	}
	raw, err := os.ReadFile(driver.pathForID(id))
	if err != nil {
		t.Fatalf("read encrypted payload: %v", err)
	}
	if !bytes.HasPrefix(raw, []byte("enc:")) || bytes.Contains(raw, []byte("secret")) {
		t.Fatalf("encrypted payload bytes = %q, want encrypted encoded bytes without plaintext", raw)
	}
	if _, err := driver.Read(context.Background(), id); err != nil {
		t.Fatalf("Read encrypted msgpack error = %v", err)
	}
}

func TestRedisDriverUsesConfiguredPayloadEncoding(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	cfg := testConfig(t)
	driver, err := NewRedisDriverFromClient(client, cfg)
	if err != nil {
		t.Fatalf("NewRedisDriverFromClient msgpack error = %v", err)
	}
	id := newSessionID()
	expiresAt := time.Now().Add(time.Hour)
	if err := driver.Write(context.Background(), id, Payload{ID: id, Values: map[string]any{"name": "redis"}}, &expiresAt); err != nil {
		t.Fatalf("Write msgpack redis error = %v", err)
	}
	raw, err := server.Get(driver.payloadKey(id))
	if err != nil {
		t.Fatalf("read raw redis payload: %v", err)
	}
	if json.Valid([]byte(raw)) {
		t.Fatalf("default redis payload should be msgpack, got JSON: %s", raw)
	}
	if _, err := driver.Read(context.Background(), id); err != nil {
		t.Fatalf("Read msgpack redis error = %v", err)
	}
}

func TestSessionRejectsInvalidPayloadEncoding(t *testing.T) {
	cfg := testConfig(t)
	cfg.Encoding = "gob"
	if _, err := NewManager(cfg, nil); err == nil {
		t.Fatal("expected invalid session encoding error")
	}
	if _, err := NewFileDriver(cfg); err == nil {
		t.Fatal("expected invalid file driver encoding error")
	}

	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	if _, err := NewRedisDriverFromClient(client, cfg); err == nil {
		t.Fatal("expected invalid redis driver encoding error")
	}
}
