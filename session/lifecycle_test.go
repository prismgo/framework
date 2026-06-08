package session

import (
	"context"
	"net/http"
	"testing"
)

func TestRegeneratePreservesDataAndDestroysOldID(t *testing.T) {
	manager, driver := newTestManager(t)
	store := newStore(manager, Payload{}, nil)
	store.Put("user_id", int64(42))
	oldID := store.ID()
	if err := store.Save(context.Background()); err != nil {
		t.Fatalf("initial Save error = %v", err)
	}

	if err := store.Regenerate(context.Background()); err != nil {
		t.Fatalf("Regenerate error = %v", err)
	}
	if store.ID() == oldID {
		t.Fatalf("session ID was not regenerated")
	}
	if store.Get("user_id") != int64(42) {
		t.Fatalf("regenerate lost data")
	}
	if _, err := driver.Read(context.Background(), oldID); err != ErrSessionNotFound {
		t.Fatalf("old ID read error = %v", err)
	}
	if err := store.Save(context.Background()); err != nil {
		t.Fatalf("Save regenerated error = %v", err)
	}
}

func TestInvalidateClearsDataAndDestroysOldID(t *testing.T) {
	manager, driver := newTestManager(t)
	store := newStore(manager, Payload{}, nil)
	store.Put("user_id", int64(42))
	oldID := store.ID()
	if err := store.Save(context.Background()); err != nil {
		t.Fatalf("initial Save error = %v", err)
	}

	if err := store.Invalidate(context.Background()); err != nil {
		t.Fatalf("Invalidate error = %v", err)
	}
	if store.ID() == oldID || len(store.All()) != 0 {
		t.Fatalf("invalidated ID=%s old=%s values=%#v", store.ID(), oldID, store.All())
	}
	if _, err := driver.Read(context.Background(), oldID); err != ErrSessionNotFound {
		t.Fatalf("old ID read error = %v", err)
	}
}

func TestSaveWritesSessionCookie(t *testing.T) {
	manager, _ := newTestManager(t)
	rec := testResponse(t)
	store := newStore(manager, Payload{}, rec)
	if err := store.Save(context.Background()); err != nil {
		t.Fatalf("Save error = %v", err)
	}
	cookies := rec.Result().Cookies()
	if len(cookies) != 1 {
		t.Fatalf("cookies = %#v", cookies)
	}
	cookie := cookies[0]
	if cookie.Name != manager.cfg.Cookie.Name || cookie.Value != store.ID() {
		t.Fatalf("cookie = %#v", cookie)
	}
	if cookie.Path != manager.cfg.Cookie.Path || !cookie.HttpOnly {
		t.Fatalf("cookie attributes = %#v", cookie)
	}
}

func TestStartRecoversFromInvalidAndMissingSession(t *testing.T) {
	manager, _ := newTestManager(t)
	store, err := manager.Start(context.Background(), testRequest(t, &http.Cookie{
		Name:  manager.cfg.Cookie.Name,
		Value: "invalid id",
	}), nil)
	if err != nil {
		t.Fatalf("Start invalid error = %v", err)
	}
	if !validSessionID(store.ID()) {
		t.Fatalf("new ID invalid: %s", store.ID())
	}

	store, err = manager.Start(context.Background(), testRequest(t, &http.Cookie{
		Name:  manager.cfg.Cookie.Name,
		Value: newSessionID(),
	}), nil)
	if err != nil {
		t.Fatalf("Start missing error = %v", err)
	}
	if !validSessionID(store.ID()) {
		t.Fatalf("missing recovery ID invalid: %s", store.ID())
	}
}
