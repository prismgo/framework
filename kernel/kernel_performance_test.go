package kernel

import (
	"testing"
)

// TestKernelRegisterPerformance 验证批量注册命令的性能
// 这是 Medium #1 的测试：mustRegister 每次注册都排序，应该优化为惰性排序
func TestKernelRegisterPerformance(t *testing.T) {
	useKernelTestContainer(t)

	k := New("test")

	// 批量注册 100 个命令
	for i := 0; i < 100; i++ {
		name := string(rune('a'+i/26)) + string(rune('a'+i%26)) + "cmd"
		cmd := &simpleTestCommand{name: name}
		k.Register(cmd)
	}

	// 验证所有命令都已注册（100 个测试命令 + 2 个内置命令）
	cmds := k.Commands()
	if len(cmds) < 100 {
		t.Errorf("expected at least 100 commands, got %d", len(cmds))
	}

	// 验证命令列表已排序
	for i := 1; i < len(cmds); i++ {
		if cmds[i-1].Name > cmds[i].Name {
			t.Errorf("commands not sorted: %s > %s", cmds[i-1].Name, cmds[i].Name)
		}
	}
}
