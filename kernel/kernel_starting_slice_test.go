package kernel

import (
	"context"
	"testing"
)

// TestKernelStartingCallbacksSliceCopy 验证 starting callbacks 切片拷贝独立
// 这是 Medium #6 的测试：prepareStartingCallbacks 切片拷贝可能共享底层数组
func TestKernelStartingCallbacksSliceCopy(t *testing.T) {
	useKernelTestContainer(t)

	k := New("test")

	// 注册多个 starting callbacks
	callCount := 0
	for i := 0; i < 5; i++ {
		err := k.Starting(func(k *Kernel) error {
			callCount++
			return nil
		})
		if err != nil {
			t.Fatalf("failed to register starting callback: %v", err)
		}
	}

	// 执行 starting callbacks
	err := k.RunContext(context.Background())
	if err != nil {
		t.Fatalf("RunContext failed: %v", err)
	}

	// 验证所有 callbacks 都被执行
	if callCount != 5 {
		t.Errorf("expected 5 callbacks to be called, got %d", callCount)
	}

	// 再次执行，应该只执行一次（startingStateSucceeded）
	callCount = 0
	err = k.RunContext(context.Background())
	if err != nil {
		t.Fatalf("second RunContext failed: %v", err)
	}
	if callCount != 0 {
		t.Errorf("expected 0 callbacks on second run, got %d", callCount)
	}
}
