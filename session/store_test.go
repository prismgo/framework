package session

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type memoryDriver struct {
	mu       sync.Mutex
	payloads map[string]Payload
}

func newMemoryDriver() *memoryDriver {
	return &memoryDriver{payloads: make(map[string]Payload)}
}

func (d *memoryDriver) Read(_ context.Context, id string) (Payload, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	payload, ok := d.payloads[id]
	if !ok {
		return Payload{}, ErrSessionNotFound
	}
	payload.Values = cloneMap(payload.Values)
	payload.OldFlash = append([]string(nil), payload.OldFlash...)
	payload.NewFlash = append([]string(nil), payload.NewFlash...)
	return payload, nil
}

func (d *memoryDriver) Write(_ context.Context, id string, payload Payload, _ *time.Time) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	payload.Values = cloneMap(payload.Values)
	payload.OldFlash = append([]string(nil), payload.OldFlash...)
	payload.NewFlash = append([]string(nil), payload.NewFlash...)
	d.payloads[id] = payload
	return nil
}

func (d *memoryDriver) Destroy(_ context.Context, id string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.payloads, id)
	return nil
}

func (d *memoryDriver) GC(_ context.Context, _ time.Time) error {
	return nil
}

func newTestManager(t *testing.T) (*Manager, *memoryDriver) {
	t.Helper()
	driver := newMemoryDriver()
	manager, err := NewManager(testConfig(t), driver)
	if err != nil {
		t.Fatalf("NewManager error = %v", err)
	}
	manager.clock = testNow
	return manager, driver
}

func TestStoreReadsAndSelection(t *testing.T) {
	manager, _ := newTestManager(t)
	store := newStore(manager, Payload{}, nil)

	if got := store.Get("missing", "fallback"); got != "fallback" {
		t.Fatalf("Get default = %v", got)
	}

	store.Put("name", "alice")
	store.Put("nil", nil)
	store.Put("role", "admin")

	if !store.Has("name") || store.Has("nil") {
		t.Fatalf("Has semantics mismatch")
	}
	if !store.Exists("nil") || !store.Missing("missing") {
		t.Fatalf("Exists/Missing semantics mismatch")
	}
	if got := store.All()["name"]; got != "alice" {
		t.Fatalf("All name = %v", got)
	}
	if got := store.Only("name", "missing"); len(got) != 1 || got["name"] != "alice" {
		t.Fatalf("Only = %#v", got)
	}
	if got := store.Except("role"); len(got) != 2 || got["role"] != nil {
		t.Fatalf("Except = %#v", got)
	}
}

func TestManagerStartSaveRoundTrip(t *testing.T) {
	manager, _ := newTestManager(t)
	rec := testResponse(t)
	store, err := manager.Start(context.Background(), testRequest(t), rec)
	if err != nil {
		t.Fatalf("Start error = %v", err)
	}
	store.Put("user_id", int64(1001))
	if err := store.Save(context.Background()); err != nil {
		t.Fatalf("Save error = %v", err)
	}

	cookies := rec.Result().Cookies()
	if len(cookies) != 1 || cookies[0].Name != manager.cfg.Cookie.Name {
		t.Fatalf("cookies = %#v", cookies)
	}

	next, err := manager.Start(context.Background(), testRequest(t, cookies[0]), testResponse(t))
	if err != nil {
		t.Fatalf("Start existing error = %v", err)
	}
	if got := next.Get("user_id"); got != int64(1001) {
		t.Fatalf("user_id = %v", got)
	}
}

func TestStoreAllReturnsCopy(t *testing.T) {
	manager, _ := newTestManager(t)
	store := newStore(manager, Payload{Values: map[string]any{"a": "b"}}, nil)
	values := store.All()
	values["a"] = "changed"
	if got := store.Get("a"); got != "b" {
		t.Fatalf("stored value changed through copy: %v", got)
	}
}

func TestStoreInvalidNumericIncrement(t *testing.T) {
	manager, _ := newTestManager(t)
	store := newStore(manager, Payload{}, nil)
	store.Put("count", "bad")
	if _, err := store.Increment("count"); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("Increment error = %v", err)
	}
}
