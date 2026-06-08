package event

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prismgo/framework/exception"
)

// fakeEvent 是测试专用事件类型，名称由实例字段决定。
type fakeEvent struct {
	name string
}

func (f fakeEvent) Name() string { return f.name }

func TestDispatcherSyncOrdering(t *testing.T) {
	d := New()
	var got []string
	var mu sync.Mutex
	record := func(tag string) ListenerFunc {
		return func(_ context.Context, _ Event) error {
			mu.Lock()
			defer mu.Unlock()
			got = append(got, tag)
			return nil
		}
	}
	d.ListenFunc("user.created", record("a"))
	d.ListenFunc("user.created", record("b"))
	d.ListenFunc("user.created", record("c"))

	d.Dispatch(context.Background(), fakeEvent{name: "user.created"})

	if len(got) != 3 || got[0] != "a" || got[1] != "b" || got[2] != "c" {
		t.Fatalf("expected listeners to fire in registration order, got %v", got)
	}
}

func TestDispatcherNoMatchIsNoop(t *testing.T) {
	d := New()
	d.ListenFunc("a", func(_ context.Context, _ Event) error {
		t.Fatal("listener should not fire for different event name")
		return nil
	})
	d.Dispatch(context.Background(), fakeEvent{name: "b"})
}

func TestDispatcherListenerErrorDoesNotAbort(t *testing.T) {
	d := New()
	var fired int32
	d.ListenFunc("e", func(_ context.Context, _ Event) error {
		atomic.AddInt32(&fired, 1)
		return errors.New("boom")
	})
	d.ListenFunc("e", func(_ context.Context, _ Event) error {
		atomic.AddInt32(&fired, 1)
		return nil
	})
	d.Dispatch(context.Background(), fakeEvent{name: "e"})

	if got := atomic.LoadInt32(&fired); got != 2 {
		t.Fatalf("expected both listeners to run despite error, got %d", got)
	}
}

func TestDispatcherListenerErrorIsReported(t *testing.T) {
	registry := useIsolatedFacadeRegistry(t)

	boom := errors.New("boom")
	var reported error
	var fields map[string]any
	if err := registry.Instance("exception.handler", exception.New(
		exception.WithPanicStack(false),
		exception.WithReporter(func(_ any, err error, got map[string]any) {
			reported = err
			fields = got
		}),
	)); err != nil {
		t.Fatalf("bind exception handler: %v", err)
	}

	d := New()
	var fired int32
	d.ListenFunc("e", func(_ context.Context, _ Event) error {
		return boom
	})
	d.ListenFunc("e", func(_ context.Context, _ Event) error {
		atomic.AddInt32(&fired, 1)
		return nil
	})
	d.Dispatch(context.Background(), fakeEvent{name: "e"})

	if atomic.LoadInt32(&fired) != 1 {
		t.Fatal("subsequent listener should still execute after error")
	}
	if !errors.Is(reported, boom) {
		t.Fatalf("reported error = %v, want boom", reported)
	}
	assertEventReportFields(t, fields, "dispatcher", "e", "sync", false)
}

func TestDispatcherListenerPanicIsReported(t *testing.T) {
	registry := useIsolatedFacadeRegistry(t)

	var reported error
	var fields map[string]any
	if err := registry.Instance("exception.handler", exception.New(
		exception.WithPanicStack(false),
		exception.WithReporter(func(_ any, err error, got map[string]any) {
			reported = err
			fields = got
		}),
	)); err != nil {
		t.Fatalf("bind exception handler: %v", err)
	}

	d := New()
	var second int32
	d.ListenFunc("e", func(_ context.Context, _ Event) error {
		panic("kaboom")
	})
	d.ListenFunc("e", func(_ context.Context, _ Event) error {
		atomic.AddInt32(&second, 1)
		return nil
	})

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("dispatch should not propagate panic, got %v", r)
		}
	}()
	d.Dispatch(context.Background(), fakeEvent{name: "e"})

	if atomic.LoadInt32(&second) != 1 {
		t.Fatal("subsequent listener should still execute after panic")
	}
	if reported == nil || reported.Error() != "event listener panic: kaboom" {
		t.Fatalf("reported panic error = %v", reported)
	}
	assertEventReportFields(t, fields, "dispatcher", "e", "sync", false)
	if fields["stack"] == "" {
		t.Fatalf("expected panic report stack field, got %#v", fields)
	}
}

func TestDispatcherWildcard(t *testing.T) {
	d := New()
	var hits []string
	var mu sync.Mutex
	add := func(tag string) ListenerFunc {
		return func(_ context.Context, ev Event) error {
			mu.Lock()
			defer mu.Unlock()
			hits = append(hits, tag+":"+ev.Name())
			return nil
		}
	}

	d.ListenFunc("prismgo.created", add("exact"))
	d.ListenFunc("prismgo.*", add("prefix"))
	d.ListenFunc("*", add("all"))

	d.Dispatch(context.Background(), fakeEvent{name: "prismgo.created"})
	d.Dispatch(context.Background(), fakeEvent{name: "user.login"})

	mu.Lock()
	defer mu.Unlock()
	wantContains := map[string]int{
		"exact:prismgo.created":  1,
		"prefix:prismgo.created": 1,
		"all:prismgo.created":    1,
		"all:user.login":         1,
	}
	count := map[string]int{}
	for _, h := range hits {
		count[h]++
	}
	for k, want := range wantContains {
		if count[k] != want {
			t.Errorf("event %q expected %d times, got %d (all hits: %v)", k, want, count[k], hits)
		}
	}
	if count["prefix:user.login"] != 0 {
		t.Errorf("prismgo.* should not match user.login, hits: %v", hits)
	}
}

func TestDispatcherAsyncRunsInGoroutine(t *testing.T) {
	d := New()
	done := make(chan struct{})
	var ranOn string
	mainGoroutine := goroutineID()

	d.Listen("e", Async(func(_ context.Context, _ Event) error {
		ranOn = goroutineID()
		close(done)
		return nil
	}))

	d.Dispatch(context.Background(), fakeEvent{name: "e"})

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("async listener did not execute within 1s")
	}
	if ranOn == mainGoroutine {
		t.Fatalf("async listener ran on caller goroutine (id=%s)", ranOn)
	}
}

func TestDispatcherAsyncPanicIsRecovered(t *testing.T) {
	d := New()
	done := make(chan struct{}, 1)
	d.Listen("e", Async(func(_ context.Context, _ Event) error {
		defer func() { done <- struct{}{} }()
		panic("async-boom")
	}))

	d.Dispatch(context.Background(), fakeEvent{name: "e"})

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("async listener panic should still allow defer to run")
	}
	// 主流程能继续执行即视为通过：dispatcher 不会因 goroutine panic 退出进程，
	// 真实环境由 logrus 记录栈。
}

func TestDispatcherForget(t *testing.T) {
	d := New()
	d.ListenFunc("e", func(_ context.Context, _ Event) error {
		t.Fatal("forgotten listener should not fire")
		return nil
	})
	d.Forget("e")
	d.Dispatch(context.Background(), fakeEvent{name: "e"})
}

func TestDispatcherHas(t *testing.T) {
	d := New()
	if d.Has("e") {
		t.Fatal("Has should return false before Listen")
	}
	d.ListenFunc("e", func(_ context.Context, _ Event) error { return nil })
	if !d.Has("e") {
		t.Fatal("Has should return true after Listen")
	}
	if d.Has("other") {
		t.Fatal("Has should not match unrelated event")
	}
}

func TestDispatcherNilSafe(t *testing.T) {
	var d *Dispatcher
	// 不应 panic。
	d.Dispatch(context.Background(), fakeEvent{name: "x"})

	d = New()
	d.Dispatch(context.Background(), nil) // nil 事件
	d.Listen("e", nil)                    // nil 监听器
	d.ListenFunc("e", nil)                // nil 函数
	var nilCtx context.Context            //nolint:staticcheck // 故意测试 nil ctx 兜底
	d.Dispatch(nilCtx, fakeEvent{name: "e"})
	d.Dispatch(context.Background(), fakeEvent{name: ""}) // 空名事件
}

// goroutineID 通过栈第一行解析当前 goroutine ID，仅用于测试断言。
func goroutineID() string {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	s := string(buf[:n])
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' {
			return s[:i]
		}
	}
	return s
}

func assertEventReportFields(t *testing.T, fields map[string]any, subsystem, eventName, listener string, async bool) {
	t.Helper()
	if fields["component"] != "event" ||
		fields["subsystem"] != subsystem ||
		fields["event"] != eventName ||
		fields["listener"] != listener ||
		fields["async"] != async ||
		fields["status"] != 500 {
		t.Fatalf("reported fields = %#v", fields)
	}
}
