package cookie

import (
	"strings"
	"testing"
)

func TestQueueReplacesByScopeAndKeepsDistinctScopes(t *testing.T) {
	q := NewQueue()
	q.Queue(New("notice", "old", 5, Path("/")))
	q.Queue(New("notice", "new", 5, Path("/")))
	q.Queue(New("notice", "admin", 5, Path("/admin")))

	c, ok := q.Queued("notice")
	if !ok || c.Value != "new" {
		t.Fatalf("expected replacement in default scope, got %#v ok=%v", c, ok)
	}
	c, ok = q.Queued("notice", Scope{Path: "/admin"})
	if !ok || c.Value != "admin" {
		t.Fatalf("expected distinct path scope, got %#v ok=%v", c, ok)
	}
}

func TestQueueUnqueueAndFlushClearsQueue(t *testing.T) {
	q := NewQueue()
	q.Queue(New("a", "1", 5))
	q.Queue(New("b", "2", 5))
	q.Unqueue("a")

	if q.HasQueued("a") {
		t.Fatal("expected a to be unqueued")
	}
	rr := testResponse(t)
	if err := q.Flush(rr); err != nil {
		t.Fatalf("Flush returned error: %v", err)
	}
	headers := setCookieHeaders(t, rr)
	if len(headers) != 1 || !strings.Contains(headers[0], "b=2") {
		t.Fatalf("unexpected Set-Cookie headers: %#v", headers)
	}
	if q.HasQueued("b") {
		t.Fatal("expected queue to clear after flush")
	}
}

func TestQueueExpireAndForeverHelpers(t *testing.T) {
	q := NewQueue()
	q.Forever("remember", "yes")
	q.Expire("legacy", Path("/admin"))

	remember, ok := q.Queued("remember")
	if !ok || remember.Minutes != ForeverMinutes {
		t.Fatalf("unexpected forever cookie: %#v ok=%v", remember, ok)
	}
	legacy, ok := q.Queued("legacy", Scope{Path: "/admin"})
	if !ok || legacy.MaxAge != -1 || legacy.Value != "" {
		t.Fatalf("unexpected expire cookie: %#v ok=%v", legacy, ok)
	}
}

func TestQueueMakeForgetZeroValueAndFlushError(t *testing.T) {
	var q Queue
	q.Make("made", "yes", 1)
	q.Forget("old")

	if !q.HasQueued("made") || !q.HasQueued("old") {
		t.Fatalf("zero-value queue did not initialize")
	}
	if _, ok := q.Queued("missing"); ok {
		t.Fatalf("unexpected missing queue item")
	}
	q.Queue(New("bad name", "x", 1))
	if err := q.Flush(testResponse(t)); err == nil {
		t.Fatal("expected invalid cookie name error")
	}
	if !q.HasQueued("bad name") {
		t.Fatal("queue should remain intact when flush fails")
	}
}

func TestQueueCookieScopeForNormalizesEmptyPath(t *testing.T) {
	q := NewQueue()

	q.Queue(New("emptyPath", "val", 0, Path("")))

	c, ok := q.Queued("emptyPath")
	if !ok || c.Value != "val" {
		t.Fatalf("scopeFor should normalize empty path to DefaultPath; got %#v ok=%v", c, ok)
	}

	if !q.HasQueued("emptyPath") {
		t.Fatal("HasQueued should find cookie with empty Path after normalization")
	}

	q.Unqueue("emptyPath")
	if q.HasQueued("emptyPath") {
		t.Fatal("Unqueue should remove cookie with empty Path after normalization")
	}

	q.Queue(Cookie{Name: "literalPath", Value: "lit", Path: ""})
	if _, ok := q.Queued("literalPath"); !ok {
		t.Fatal("scopeFor should normalize empty path from Cookie literal to DefaultPath")
	}
}
