//go:build !windows

package http

import (
	"context"
	"net/http"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/prismgo/framework/event"
)

func TestListenAndServeGracefulLifecycleEvents(t *testing.T) {
	bus := event.New()
	var (
		mu     sync.Mutex
		events []string
	)
	bus.Listen("server.*", event.ListenerFunc(func(_ context.Context, ev event.Event) error {
		mu.Lock()
		defer mu.Unlock()
		events = append(events, ev.Name())
		return nil
	}))

	server := &http.Server{
		Addr:    "127.0.0.1:0", // 让内核挑选可用端口
		Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }),
	}

	done := make(chan struct{})
	go func() {
		_ = ListenAndServeGraceful(server, time.Second, WithDispatcher(bus), WithShutdownSignals())
		close(done)
	}()

	// 给监听协程一点时间真正进入 ListenAndServe。
	time.Sleep(100 * time.Millisecond)
	// 直接给当前进程发信号触发优雅关闭。
	if err := syscall.Kill(syscall.Getpid(), syscall.SIGTERM); err != nil {
		// Windows 不支持 SIGTERM；测试环境跳过即可。
		t.Skipf("syscall.Kill not supported on this platform: %v", err)
	}

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("server did not shut down within 3s")
	}

	mu.Lock()
	defer mu.Unlock()
	want := []string{
		event.EventServerStarting,
		event.EventServerStarted,
		event.EventServerStopping,
		event.EventServerStopped,
	}
	if len(events) < len(want) {
		t.Fatalf("expected at least %d lifecycle events, got %d: %v", len(want), len(events), events)
	}
	// 顺序断言：starting → started → stopping → stopped。
	for i, name := range want {
		if events[i] != name {
			t.Fatalf("event[%d] = %q, want %q (full: %v)", i, events[i], name, events)
		}
	}
}
