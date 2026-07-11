package session

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"
)

func TestFileDriverSessionLockTimeoutAndRelease(t *testing.T) {
	driver := newTestFileDriver(t, testConfig(t))
	id := newSessionID()
	ctx := context.Background()

	lock, err := driver.Lock(ctx, id, time.Second, 20*time.Millisecond)
	if err != nil {
		t.Fatalf("first Lock error = %v", err)
	}
	if _, err := driver.Lock(ctx, id, time.Second, 20*time.Millisecond); !errors.Is(err, ErrLockTimeout) {
		t.Fatalf("second Lock error = %v", err)
	}
	if err := lock.Release(ctx); err != nil {
		t.Fatalf("Release error = %v", err)
	}
	if err := lock.Release(ctx); !errors.Is(err, ErrLockNotHeld) {
		t.Fatalf("second Release error = %v", err)
	}
}

func TestFileDriverDifferentSessionLocksDoNotBlock(t *testing.T) {
	driver := newTestFileDriver(t, testConfig(t))
	ctx := context.Background()
	first, err := driver.Lock(ctx, newSessionID(), time.Second, 20*time.Millisecond)
	if err != nil {
		t.Fatalf("first Lock error = %v", err)
	}
	defer func() {
		if err := first.Release(ctx); err != nil {
			t.Errorf("release first lock: %v", err)
		}
	}()

	started := time.Now()
	second, err := driver.Lock(ctx, newSessionID(), time.Second, 200*time.Millisecond)
	if err != nil {
		t.Fatalf("different ID Lock error = %v", err)
	}
	defer func() {
		if err := second.Release(ctx); err != nil {
			t.Errorf("release second lock: %v", err)
		}
	}()
	// Windows CI 环境文件系统延迟较高，放宽阈值到 500ms
	if time.Since(started) > 500*time.Millisecond {
		t.Fatalf("different session ID lock waited too long: %v", time.Since(started))
	}
}

func TestFileDriverLockStaleAndContextCancel(t *testing.T) {
	driver := newTestFileDriver(t, testConfig(t))
	id := newSessionID()
	ctx := context.Background()
	first, err := driver.Lock(ctx, id, 10*time.Millisecond, 20*time.Millisecond)
	if err != nil {
		t.Fatalf("first Lock error = %v", err)
	}
	time.Sleep(30 * time.Millisecond)
	second, err := driver.Lock(ctx, id, 10*time.Millisecond, 200*time.Millisecond)
	if err != nil {
		t.Fatalf("stale Lock error = %v", err)
	}
	if err := first.Release(ctx); !errors.Is(err, ErrLockNotHeld) {
		t.Fatalf("stale first Release error = %v", err)
	}
	if err := second.Release(ctx); err != nil {
		t.Fatalf("second Release error = %v", err)
	}

	held, err := driver.Lock(ctx, id, time.Second, time.Second)
	if err != nil {
		t.Fatalf("held Lock error = %v", err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := driver.Lock(cancelled, id, time.Second, time.Second); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled Lock error = %v", err)
	}
	_ = held.Release(ctx)
}

func TestManagerSaveSerializesSameSessionID(t *testing.T) {
	cfg := testConfig(t)
	driver := newTestFileDriver(t, cfg)
	manager, err := NewManager(cfg, driver)
	if err != nil {
		t.Fatalf("NewManager error = %v", err)
	}
	manager.clock = testNow
	store := newStore(manager, Payload{}, nil)
	store.Put("seed", "value")

	var wg sync.WaitGroup
	errs := make(chan error, 3)
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- store.Save(context.Background())
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("Save error = %v", err)
		}
	}
	if _, err := driver.Read(context.Background(), store.ID()); err != nil {
		t.Fatalf("Read after concurrent Save error = %v", err)
	}
}

func TestManagerSaveReleasesRequestLockWhenWriteFails(t *testing.T) {
	cfg := testConfig(t)
	driver := newReleaseTrackingDriver()
	manager, err := NewManager(cfg, driver)
	if err != nil {
		t.Fatalf("NewManager error = %v", err)
	}
	manager.clock = testNow

	seed := newStore(manager, Payload{}, nil)
	seed.Put("seed", "value")
	if err := seed.Save(context.Background()); err != nil {
		t.Fatalf("initial Save error = %v", err)
	}

	store, err := manager.Start(context.Background(), testRequest(t, &http.Cookie{
		Name:  manager.cfg.Cookie.Name,
		Value: seed.ID(),
	}), nil)
	if err != nil {
		t.Fatalf("Start error = %v", err)
	}
	writeErr := errors.New("write failed")
	driver.writeErr = writeErr
	if err := store.Save(context.Background()); !errors.Is(err, writeErr) {
		t.Fatalf("Save error = %v, want %v", err, writeErr)
	}

	lock, err := driver.Lock(context.Background(), seed.ID(), time.Second, 20*time.Millisecond)
	if err != nil {
		t.Fatalf("Lock after failed Save error = %v", err)
	}
	if err := lock.Release(context.Background()); err != nil {
		t.Fatalf("Release after failed Save error = %v", err)
	}
}

func TestManagerStartReleasesRequestLockWhenRecoveringMissingSession(t *testing.T) {
	cfg := testConfig(t)
	driver := newReleaseTrackingDriver()
	manager, err := NewManager(cfg, driver)
	if err != nil {
		t.Fatalf("NewManager error = %v", err)
	}

	missingID := newSessionID()
	store, err := manager.Start(context.Background(), testRequest(t, &http.Cookie{
		Name:  manager.cfg.Cookie.Name,
		Value: missingID,
	}), nil)
	if err != nil {
		t.Fatalf("Start error = %v", err)
	}
	if store.ID() == missingID {
		t.Fatalf("missing session reused old ID")
	}

	lock, err := driver.Lock(context.Background(), missingID, time.Second, 20*time.Millisecond)
	if err != nil {
		t.Fatalf("Lock after missing recovery error = %v", err)
	}
	if err := lock.Release(context.Background()); err != nil {
		t.Fatalf("Release after missing recovery error = %v", err)
	}
}

func TestManagerStartReturnsLockAcquireError(t *testing.T) {
	cfg := testConfig(t)
	driver := newReleaseTrackingDriver()
	manager, err := NewManager(cfg, driver)
	if err != nil {
		t.Fatalf("NewManager error = %v", err)
	}
	id := newSessionID()
	driver.payloads[id] = Payload{ID: id, Values: map[string]any{"seed": "value"}, LastActivity: testNow()}

	lock, err := driver.Lock(context.Background(), id, time.Second, 20*time.Millisecond)
	if err != nil {
		t.Fatalf("seed Lock error = %v", err)
	}
	defer func() {
		if err := lock.Release(context.Background()); err != nil {
			t.Errorf("release lock: %v", err)
		}
	}()

	if _, err := manager.Start(context.Background(), testRequest(t, &http.Cookie{
		Name:  manager.cfg.Cookie.Name,
		Value: id,
	}), nil); !errors.Is(err, ErrLockTimeout) {
		t.Fatalf("Start error = %v, want %v", err, ErrLockTimeout)
	}
}

func TestManagerStartReleasesRequestLockWhenPayloadExpired(t *testing.T) {
	cfg := testConfig(t)
	driver := newReleaseTrackingDriver()
	manager, err := NewManager(cfg, driver)
	if err != nil {
		t.Fatalf("NewManager error = %v", err)
	}
	manager.clock = testNow
	id := newSessionID()
	expiredAt := testNow().Add(-time.Minute)
	driver.payloads[id] = Payload{
		ID:           id,
		Values:       map[string]any{"seed": "value"},
		LastActivity: testNow(),
		ExpiresAt:    &expiredAt,
	}

	store, err := manager.Start(context.Background(), testRequest(t, &http.Cookie{
		Name:  manager.cfg.Cookie.Name,
		Value: id,
	}), nil)
	if err != nil {
		t.Fatalf("Start expired error = %v", err)
	}
	if store.ID() == id {
		t.Fatalf("expired session reused old ID")
	}
	if _, err := driver.Read(context.Background(), id); !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("expired old ID read error = %v", err)
	}

	lock, err := driver.Lock(context.Background(), id, time.Second, 20*time.Millisecond)
	if err != nil {
		t.Fatalf("Lock after expired recovery error = %v", err)
	}
	if err := lock.Release(context.Background()); err != nil {
		t.Fatalf("Release after expired recovery error = %v", err)
	}
}

func TestRegenerateReleasesOldRequestLock(t *testing.T) {
	cfg := testConfig(t)
	driver := newReleaseTrackingDriver()
	manager, err := NewManager(cfg, driver)
	if err != nil {
		t.Fatalf("NewManager error = %v", err)
	}
	manager.clock = testNow

	seed := newStore(manager, Payload{}, nil)
	seed.Put("user_id", int64(42))
	if err := seed.Save(context.Background()); err != nil {
		t.Fatalf("initial Save error = %v", err)
	}
	oldID := seed.ID()

	store, err := manager.Start(context.Background(), testRequest(t, &http.Cookie{
		Name:  manager.cfg.Cookie.Name,
		Value: oldID,
	}), nil)
	if err != nil {
		t.Fatalf("Start error = %v", err)
	}
	if err := store.Regenerate(context.Background()); err != nil {
		t.Fatalf("Regenerate error = %v", err)
	}
	if store.ID() == oldID || store.Get("user_id") != int64(42) {
		t.Fatalf("regenerated ID=%s old=%s user=%#v", store.ID(), oldID, store.Get("user_id"))
	}
	if _, err := driver.Read(context.Background(), oldID); !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("old ID read error = %v", err)
	}

	lock, err := driver.Lock(context.Background(), oldID, time.Second, 20*time.Millisecond)
	if err != nil {
		t.Fatalf("Lock after regenerate error = %v", err)
	}
	if err := lock.Release(context.Background()); err != nil {
		t.Fatalf("Release after regenerate error = %v", err)
	}
	if err := store.Save(context.Background()); err != nil {
		t.Fatalf("Save regenerated error = %v", err)
	}
}

type releaseTrackingDriver struct {
	*memoryDriver
	lockMu   sync.Mutex
	held     bool
	writeErr error
}

func newReleaseTrackingDriver() *releaseTrackingDriver {
	return &releaseTrackingDriver{memoryDriver: newMemoryDriver()}
}

func (d *releaseTrackingDriver) Write(ctx context.Context, id string, payload Payload, expiresAt *time.Time) error {
	if d.writeErr != nil {
		return d.writeErr
	}
	return d.memoryDriver.Write(ctx, id, payload, expiresAt)
}

func (d *releaseTrackingDriver) Lock(_ context.Context, _ string, _ time.Duration, _ time.Duration) (Lock, error) {
	d.lockMu.Lock()
	defer d.lockMu.Unlock()
	if d.held {
		return nil, ErrLockTimeout
	}
	d.held = true
	return &releaseTrackingLock{driver: d}, nil
}

type releaseTrackingLock struct {
	driver *releaseTrackingDriver
}

func (l *releaseTrackingLock) Release(context.Context) error {
	l.driver.lockMu.Lock()
	defer l.driver.lockMu.Unlock()
	if !l.driver.held {
		return ErrLockNotHeld
	}
	l.driver.held = false
	return nil
}
