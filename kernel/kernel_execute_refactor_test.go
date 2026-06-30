package kernel

import (
	"context"
	"testing"
)

// TestKernelExecuteRegisteredCommandRefactor 验证重构后的 executeRegisteredCommand 功能正常
// 这是 Medium #7 的测试：executeRegisteredCommand 函数过长，需要拆分
func TestKernelExecuteRegisteredCommandRefactor(t *testing.T) {
	useKernelTestContainer(t)

	k := New("test")
	cmd := &simpleTestCommand{name: "test:cmd"}
	k.Register(cmd)

	// 执行命令
	err := k.Call(context.Background(), "test:cmd")
	if err != nil {
		t.Fatalf("call failed: %v", err)
	}
}
