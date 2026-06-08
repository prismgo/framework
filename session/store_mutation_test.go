package session

import (
	"context"
	"testing"
)

func TestStorePullForgetFlushIncrementDecrement(t *testing.T) {
	manager, _ := newTestManager(t)
	store := newStore(manager, Payload{}, nil)
	store.Put("name", "alice")
	store.Put("role", "admin")

	if got := store.Pull("name"); got != "alice" || store.Exists("name") {
		t.Fatalf("Pull got %v exists=%v", got, store.Exists("name"))
	}
	if got := store.Pull("missing", "fallback"); got != "fallback" {
		t.Fatalf("Pull default = %v", got)
	}

	store.Forget("role")
	if store.Exists("role") {
		t.Fatalf("Forget did not remove role")
	}

	next, err := store.Increment("count")
	if err != nil || next != 1 {
		t.Fatalf("Increment = %d, %v", next, err)
	}
	next, err = store.Increment("count", 4)
	if err != nil || next != 5 {
		t.Fatalf("Increment by 4 = %d, %v", next, err)
	}
	next, err = store.Decrement("count", 2)
	if err != nil || next != 3 {
		t.Fatalf("Decrement = %d, %v", next, err)
	}

	store.Flush()
	if len(store.All()) != 0 {
		t.Fatalf("Flush values = %#v", store.All())
	}

	if err := store.Save(context.Background()); err != nil {
		t.Fatalf("Save after mutation error = %v", err)
	}
}
