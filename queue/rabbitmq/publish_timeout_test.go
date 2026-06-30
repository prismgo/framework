package rabbitmq

import (
	"context"
	"testing"
	"time"
)

// TestPublishTimeout_DefaultWhenZero 验证 PublishTimeout=0 时使用默认超时。
func TestPublishTimeout_DefaultWhenZero(t *testing.T) {
	conn := &Connection{
		options: resolvedOptions{
			PublishTimeout: 0, // 显式设置为 0
		},
	}

	ctx := context.Background()
	pushCtx, cancel := conn.publishWaitContext(ctx)
	defer cancel()

	// 应该创建了带超时的 context
	deadline, ok := pushCtx.Deadline()
	if !ok {
		t.Fatal("expected context to have deadline when PublishTimeout=0")
	}

	// 超时时间应该是默认值 5 秒
	expected := time.Now().Add(5 * time.Second)
	if deadline.Sub(expected) > 100*time.Millisecond {
		t.Errorf("expected deadline ~%v, got %v", expected, deadline)
	}
}

// TestPublishTimeout_CustomValue 验证 PublishTimeout>0 时使用自定义超时。
func TestPublishTimeout_CustomValue(t *testing.T) {
	conn := &Connection{
		options: resolvedOptions{
			PublishTimeout: 10 * time.Second,
		},
	}

	ctx := context.Background()
	pushCtx, cancel := conn.publishWaitContext(ctx)
	defer cancel()

	deadline, ok := pushCtx.Deadline()
	if !ok {
		t.Fatal("expected context to have deadline")
	}

	expected := time.Now().Add(10 * time.Second)
	if deadline.Sub(expected) > 100*time.Millisecond {
		t.Errorf("expected deadline ~%v, got %v", expected, deadline)
	}
}

// TestPublishTimeout_ExistingDeadline 验证已有 deadline 的 context 不被覆盖。
func TestPublishTimeout_ExistingDeadline(t *testing.T) {
	conn := &Connection{
		options: resolvedOptions{
			PublishTimeout: 10 * time.Second,
		},
	}

	ctx, originalCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer originalCancel()

	pushCtx, cancel := conn.publishWaitContext(ctx)
	defer cancel()

	deadline, ok := pushCtx.Deadline()
	if !ok {
		t.Fatal("expected context to have deadline")
	}

	// 应该保留原有的 3 秒超时，而不是使用配置的 10 秒
	expected := time.Now().Add(3 * time.Second)
	if deadline.Sub(expected) > 100*time.Millisecond {
		t.Errorf("expected deadline ~%v, got %v", expected, deadline)
	}
}
