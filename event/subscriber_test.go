package event

import (
	"context"
	"sync/atomic"
	"testing"

	eventcontract "github.com/prismgo/framework/contracts/event"
)

type counterSubscriber struct {
	created int32
	updated int32
}

func (s *counterSubscriber) Subscribe(d eventcontract.Dispatcher) {
	d.ListenFunc("user.created", func(_ context.Context, _ Event) error {
		atomic.AddInt32(&s.created, 1)
		return nil
	})
	d.ListenFunc("user.updated", func(_ context.Context, _ Event) error {
		atomic.AddInt32(&s.updated, 1)
		return nil
	})
}

func TestSubscribe(t *testing.T) {
	d := New()
	sub := &counterSubscriber{}
	d.Subscribe(sub)

	d.Dispatch(context.Background(), fakeEvent{name: "user.created"})
	d.Dispatch(context.Background(), fakeEvent{name: "user.updated"})
	d.Dispatch(context.Background(), fakeEvent{name: "user.updated"})

	if atomic.LoadInt32(&sub.created) != 1 {
		t.Errorf("created listener fired %d times, want 1", sub.created)
	}
	if atomic.LoadInt32(&sub.updated) != 2 {
		t.Errorf("updated listener fired %d times, want 2", sub.updated)
	}
}

func TestSubscribeNilSafe(t *testing.T) {
	d := New()
	d.Subscribe(nil) // 不应 panic
}
