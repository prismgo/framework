package session

import (
	"context"
	"net/http"
	"testing"
)

func TestFlashLifecycle(t *testing.T) {
	manager, _ := newTestManager(t)
	first := newStore(manager, Payload{}, nil)
	first.Flash("status", "saved")
	first.Now("now", "only-current")
	if first.Get("status") != "saved" || first.Get("now") != "only-current" {
		t.Fatalf("current flash values missing")
	}
	if err := first.Save(context.Background()); err != nil {
		t.Fatalf("first Save error = %v", err)
	}

	second, err := manager.Start(context.Background(), testRequest(t, &http.Cookie{
		Name:  manager.cfg.Cookie.Name,
		Value: first.ID(),
	}), nil)
	if err != nil {
		t.Fatalf("second Start error = %v", err)
	}
	if second.Get("status") != "saved" {
		t.Fatalf("second status = %v", second.Get("status"))
	}
	if second.Exists("now") {
		t.Fatalf("Now value persisted")
	}
	if err := second.Save(context.Background()); err != nil {
		t.Fatalf("second Save error = %v", err)
	}

	third, err := manager.Start(context.Background(), testRequest(t, &http.Cookie{
		Name:  manager.cfg.Cookie.Name,
		Value: first.ID(),
	}), nil)
	if err != nil {
		t.Fatalf("third Start error = %v", err)
	}
	if third.Exists("status") {
		t.Fatalf("flash survived too long")
	}
}

func TestReflashAndKeep(t *testing.T) {
	manager, _ := newTestManager(t)
	store := newStore(manager, Payload{Values: map[string]any{
		"a": "one",
		"b": "two",
	}, OldFlash: []string{"a", "b"}}, nil)

	store.Keep("a")
	if err := store.Save(context.Background()); err != nil {
		t.Fatalf("Save keep error = %v", err)
	}
	if store.Exists("b") || !store.Exists("a") {
		t.Fatalf("Keep values = %#v", store.All())
	}

	store = newStore(manager, Payload{Values: map[string]any{
		"a": "one",
		"b": "two",
	}, OldFlash: []string{"a", "b"}}, nil)
	store.Reflash()
	if err := store.Save(context.Background()); err != nil {
		t.Fatalf("Save reflash error = %v", err)
	}
	if !store.Exists("a") || !store.Exists("b") {
		t.Fatalf("Reflash values = %#v", store.All())
	}
}
