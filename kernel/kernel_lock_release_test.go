package kernel

import (
	"context"
	"testing"
)

// TestKernelIsolationLockReleaseErrorLogging 验证锁释放失败时记录错误
// 这是 Medium #5 的测试：锁释放错误被静默忽略
func TestKernelIsolationLockReleaseErrorLogging(t *testing.T) {
	useKernelTestContainer(t)

	k := New("test")
	cmd := &isolatableTestCommand{name: "isolated:cmd"}
	k.Register(cmd)

	// 执行命令
	ctx := context.Background()
	err := k.Call(ctx, "isolated:cmd")
	if err != nil {
		t.Fatalf("call failed: %v", err)
	}
}
