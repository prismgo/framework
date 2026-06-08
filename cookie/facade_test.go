package cookie

import (
	"errors"
	"testing"

	"github.com/gin-gonic/gin"
	containercontract "github.com/prismgo/framework/contracts/container"
)

func TestFacadeUseResolveCurrentAndDefault(t *testing.T) {
	registry := useIsolatedFacadeRegistry(t)

	q := NewQueue()
	if err := registry.Instance(serviceKey, q); err != nil {
		t.Fatalf("bind queue: %v", err)
	}
	if Resolve() != q {
		t.Fatal("facade did not return registered queue")
	}
	resolved := Resolve()
	if resolved != q {
		t.Fatal("Resolve returned unexpected queue")
	}
}

func TestFacadeRegisterFactory(t *testing.T) {
	registry := useIsolatedFacadeRegistry(t)

	q := NewQueue()
	_ = registry.Singleton(serviceKey, func(containercontract.Resolver) (any, error) {
		return q, nil
	})
	resolved := Resolve()
	if resolved != q {
		t.Fatal("Resolve should register the factory queue")
	}
}

func TestFacadeServiceProvider(t *testing.T) {
	registry := useIsolatedFacadeRegistry(t)

	if err := (ServiceProvider{}).Register(providerTestApp{registry: registry}); err != nil {
		t.Fatalf("register service provider: %v", err)
	}
	resolved := Resolve()
	if resolved == nil || Resolve() != resolved {
		t.Fatal("ServiceProvider did not lazy resolve the default queue")
	}
}

func TestFacadeQueueOperations(t *testing.T) {
	registry := useIsolatedFacadeRegistry(t)

	q := NewQueue()
	if err := registry.Instance(serviceKey, q); err != nil {
		t.Fatalf("bind queue: %v", err)
	}
	QueueCookie(New("notice", "ok", 5))
	QueueMake("made", "yes", 5)
	QueueForever("remember", "yes")
	QueueExpire("old")

	if !HasQueued("notice") || !HasQueued("made") || !HasQueued("remember") || !HasQueued("old") {
		t.Fatal("expected facade queue helpers to enqueue cookies")
	}
	c, ok := Queued("remember")
	if !ok || c.Minutes != ForeverMinutes {
		t.Fatalf("unexpected queued forever cookie: %#v ok=%v", c, ok)
	}
	Unqueue("notice")
	if HasQueued("notice") {
		t.Fatal("expected notice to be unqueued")
	}
	if err := Flush(testResponse(t)); err != nil {
		t.Fatalf("Flush returned error: %v", err)
	}
}

func TestRequestQueueHelpersRequireRequestQueue(t *testing.T) {
	registry := useIsolatedFacadeRegistry(t)
	gin.SetMode(gin.TestMode)
	processQueue := NewQueue()
	if err := registry.Instance(serviceKey, processQueue); err != nil {
		t.Fatalf("bind queue: %v", err)
	}

	rec := testResponse(t)
	c, _ := gin.CreateTestContext(rec)
	checkErr := func(name string, err error) {
		t.Helper()
		if !errors.Is(err, ErrQueueNotFound) {
			t.Fatalf("%s error = %v, want ErrQueueNotFound", name, err)
		}
	}

	checkErr("QueueCookieFrom", QueueCookieFrom(c, New("raw", "value", 5)))
	queued, err := QueueMakeFrom(c, "notice", "ok", 5)
	checkErr("QueueMakeFrom", err)
	if _, err := QueueForeverFrom(c, "remember", "yes"); err != nil {
		checkErr("QueueForeverFrom", err)
	}
	if _, err := QueueExpireFrom(c, "old"); err != nil {
		checkErr("QueueExpireFrom", err)
	}
	if _, err := QueueForgetFrom(c, "legacy"); err != nil {
		checkErr("QueueForgetFrom", err)
	}
	if _, _, err := QueuedFrom(c, "notice"); err != nil {
		checkErr("QueuedFrom", err)
	}
	if _, err := HasQueuedFrom(c, "notice"); err != nil {
		checkErr("HasQueuedFrom", err)
	}
	checkErr("UnqueueFrom", UnqueueFrom(c, "notice"))

	if queued.Name != "" {
		t.Fatalf("QueueMakeFrom returned cookie = %#v", queued)
	}
	if HasQueued("notice") {
		t.Fatal("request helper must not fall back to the process queue")
	}
}

func TestRequestQueueHelpersUseCurrentRequestQueue(t *testing.T) {
	gin.SetMode(gin.TestMode)
	rec := testResponse(t)
	c, _ := gin.CreateTestContext(rec)
	c.Set(QueueKey, NewQueue())

	if err := QueueCookieFrom(c, New("raw", "value", 5)); err != nil {
		t.Fatalf("QueueCookieFrom error = %v", err)
	}
	made, err := QueueMakeFrom(c, "made", "yes", 5, Path("/admin"))
	if err != nil {
		t.Fatalf("QueueMakeFrom error = %v", err)
	}
	forever, err := QueueForeverFrom(c, "remember", "yes")
	if err != nil {
		t.Fatalf("QueueForeverFrom error = %v", err)
	}
	expired, err := QueueExpireFrom(c, "old")
	if err != nil {
		t.Fatalf("QueueExpireFrom error = %v", err)
	}
	forgotten, err := QueueForgetFrom(c, "legacy")
	if err != nil {
		t.Fatalf("QueueForgetFrom error = %v", err)
	}

	if made.Name != "made" || made.Path != "/admin" {
		t.Fatalf("made cookie = %#v", made)
	}
	if forever.Minutes != ForeverMinutes {
		t.Fatalf("forever cookie = %#v", forever)
	}
	if expired.MaxAge != -1 || forgotten.MaxAge != -1 {
		t.Fatalf("expired=%#v forgotten=%#v", expired, forgotten)
	}

	queued, ok, err := QueuedFrom(c, "made", Scope{Path: "/admin"})
	if err != nil || !ok || queued.Value != "yes" {
		t.Fatalf("QueuedFrom = %#v ok=%v err=%v", queued, ok, err)
	}
	has, err := HasQueuedFrom(c, "raw")
	if err != nil || !has {
		t.Fatalf("HasQueuedFrom raw = %v err=%v", has, err)
	}
	if err := UnqueueFrom(c, "raw"); err != nil {
		t.Fatalf("UnqueueFrom error = %v", err)
	}
	has, err = HasQueuedFrom(c, "raw")
	if err != nil {
		t.Fatalf("HasQueuedFrom after unqueue error = %v", err)
	}
	if has {
		t.Fatal("raw should be unqueued")
	}
}
