package session

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestContractMethodConcepts(t *testing.T) {
	manager, _ := newTestManager(t)
	store := newStore(manager, Payload{}, nil)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodGet, "/", nil)
	c.Set(string(storeContextKey), store)

	if got := Get(c, "missing", "fallback"); got != "fallback" {
		t.Fatalf("facade Get default = %v", got)
	}
	if err := Put(c, "name", "alice"); err != nil {
		t.Fatalf("facade Put error = %v", err)
	}
	if !Has(c, "name") || !Exists(c, "name") || Missing(c, "name") {
		t.Fatalf("facade read predicates mismatch")
	}
	if got := Pull(c, "name"); got != "alice" || Exists(c, "name") {
		t.Fatalf("facade Pull got %v exists=%v", got, Exists(c, "name"))
	}
	if err := Flash(c, "status", "saved"); err != nil {
		t.Fatalf("facade Flash error = %v", err)
	}
	if err := Now(c, "notice", "now"); err != nil {
		t.Fatalf("facade Now error = %v", err)
	}
	if err := Reflash(c); err != nil {
		t.Fatalf("facade Reflash error = %v", err)
	}
	if err := Keep(c, "status"); err != nil {
		t.Fatalf("facade Keep error = %v", err)
	}
	if err := Forget(c, "notice"); err != nil {
		t.Fatalf("facade Forget error = %v", err)
	}
	if err := Flush(c); err != nil {
		t.Fatalf("facade Flush error = %v", err)
	}
	if err := Regenerate(c); err != nil {
		t.Fatalf("facade Regenerate error = %v", err)
	}
	if err := Invalidate(c); err != nil {
		t.Fatalf("facade Invalidate error = %v", err)
	}
	if err := Save(c); err != nil {
		t.Fatalf("facade Save error = %v", err)
	}
}

func TestDefaultManagerFallbackStartsSession(t *testing.T) {
	manager, err := NewManager(testConfig(t), nil)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	bindSessionManagerForTest(t, manager)
	store, err := Start(context.Background(), testRequest(t), testResponse(t))
	if err != nil {
		t.Fatalf("Start error = %v", err)
	}
	if store == nil || !validSessionID(store.ID()) {
		t.Fatalf("store = %#v", store)
	}
}
