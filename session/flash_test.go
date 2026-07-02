package session

import (
	"context"
	"net/http"
	"testing"
)

func TestRemoveStringDoesNotShareBackingArray(t *testing.T) {
	original := []string{"a", "b", "c"}
	result := removeString(original, "b")

	// 验证结果正确
	if len(result) != 2 || result[0] != "a" || result[1] != "c" {
		t.Fatalf("removeString result = %v, want [a c]", result)
	}

	// 修改返回的切片不应影响原始切片
	if len(result) > 0 {
		result[0] = "modified"
	}
	if original[0] != "a" {
		t.Errorf("original was modified: got %q, want %q", original[0], "a")
	}
	if len(original) != 3 {
		t.Errorf("original length changed: got %d, want 3", len(original))
	}
}

func TestRemoveStringMultipleCallsDoNotPollute(t *testing.T) {
	original := []string{"a", "b", "c", "d"}

	// 第一次调用
	result1 := removeString(original, "b")
	if len(result1) != 3 || result1[0] != "a" || result1[1] != "c" || result1[2] != "d" {
		t.Fatalf("first removeString result = %v, want [a c d]", result1)
	}

	// 第二次调用
	result2 := removeString(original, "c")
	if len(result2) != 3 || result2[0] != "a" || result2[1] != "b" || result2[2] != "d" {
		t.Fatalf("second removeString result = %v, want [a b d]", result2)
	}

	// 第三次调用
	result3 := removeString(original, "a")
	if len(result3) != 3 || result3[0] != "b" || result3[1] != "c" || result3[2] != "d" {
		t.Fatalf("third removeString result = %v, want [b c d]", result3)
	}

	// 验证原始切片未被污染
	if len(original) != 4 || original[0] != "a" || original[1] != "b" || original[2] != "c" || original[3] != "d" {
		t.Errorf("original was polluted: got %v, want [a b c d]", original)
	}
}

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
