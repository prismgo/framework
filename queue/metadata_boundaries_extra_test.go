package queue

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/prismgo/framework/container"
	"github.com/prismgo/framework/queue/payload"
)

func TestPayloadForEnvelopeHandlesEncryptedAndInvalidMetadataBoundaries(t *testing.T) {
	runtime := &Runtime{}
	if _, err := payloadForQueueEnvelope(runtime, nil); err == nil {
		t.Fatal("expected nil envelope error")
	}
	plain, err := payloadForQueueEnvelope(runtime, &payload.Envelope{Payload: []byte(`{"key":"plain"}`)})
	if err != nil {
		t.Fatalf("plain payload: %v", err)
	}
	if string(plain) != `{"key":"plain"}` {
		t.Fatalf("plain payload = %s", plain)
	}
	if _, err := payloadForQueueEnvelope(runtime, &payload.Envelope{Encrypted: true, Payload: []byte(`"token"`)}); err == nil {
		t.Fatal("expected missing cipher error")
	}

	encrypter := testQueueEncrypter(t)
	runtime.payloadEncrypter = encrypter
	if _, err := payloadForQueueEnvelope(runtime, &payload.Envelope{Encrypted: true, Payload: []byte(`{bad-json`)}); err == nil {
		t.Fatal("expected invalid encrypted token error")
	}
	token, err := encrypter.Encrypt(context.Background(), []byte(`{"key":"secret"}`))
	if err != nil {
		t.Fatalf("encrypt payload: %v", err)
	}
	body, err := encryptedRawMessage(string(token))
	if err != nil {
		t.Fatalf("encrypted raw message: %v", err)
	}
	decrypted, err := payloadForQueueEnvelope(runtime, &payload.Envelope{Encrypted: true, Payload: body})
	if err != nil {
		t.Fatalf("decrypt payload: %v", err)
	}
	if string(decrypted) != `{"key":"secret"}` {
		t.Fatalf("decrypted payload = %s", decrypted)
	}
}

func TestFacadeAndMiddlewareBoundariesRemainNoopOrExplicit(t *testing.T) {
	// 需求背景：Horizon 会通过应用容器注册队列 Manager，nil registry 必须显式失败，避免隐藏注册错误。
	container.SetProvider(nil)
	t.Cleanup(func() { container.SetProvider(nil) })
	if _, err := container.Make[*Manager]("queue.manager"); !errors.Is(err, container.ErrNoCurrentContainer) {
		t.Fatalf("Make without current container error = %v, want ErrNoCurrentContainer", err)
	}

	// 设计思路：nil Manager 的 middleware 注册保持 no-op，便于调用方在可选依赖未装配时安全跳过。
	var manager *Manager
	manager.UseMiddleware(MiddlewareFunc(func(ctx context.Context, _ Job, next Next) error {
		return next(ctx)
	}))
}

func TestMiddlewareBoundariesPreserveCallerSemantics(t *testing.T) {
	useTestCache(t, "memory")
	ctx := context.Background()
	called := false
	next := func(context.Context) error {
		called = true
		return nil
	}

	// 需求背景：middleware builder 允许按配置关闭，关闭后必须透明执行后续任务。
	var disabled *ThrottlesExceptionsMiddleware
	if err := disabled.Handle(ctx, &testJob{}, next); err != nil || !called {
		t.Fatalf("nil throttle should call next, called=%v err=%v", called, err)
	}

	called = false
	if err := ThrottlesExceptions(0, time.Second).Handle(ctx, &testJob{}, next); err != nil || !called {
		t.Fatalf("disabled throttle should call next, called=%v err=%v", called, err)
	}

	original := errors.New("ignored by predicate")
	err := ThrottlesExceptions(2, time.Second).When(func(error) bool { return false }).Handle(ctx, &testJob{}, func(context.Context) error {
		return original
	})
	if !errors.Is(err, original) {
		t.Fatalf("predicate mismatch error = %v, want original", err)
	}

	manager := newSyncManager()
	manager.UseMiddleware(MiddlewareFunc(func(ctx context.Context, _ Job, next Next) error { return next(ctx) }))
	snapshot := manager.runtime.middlewareSnapshot()
	if len(snapshot) != 1 {
		t.Fatalf("middleware snapshot length = %d", len(snapshot))
	}
	snapshot[0] = nil
	if manager.runtime.middleware[0] == nil {
		t.Fatal("middleware snapshot should not alias manager storage")
	}
}
