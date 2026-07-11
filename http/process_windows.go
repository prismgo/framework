package http

import (
	"context"
	"fmt"
	"net"
	"time"
)

// Reload 在 Windows 平台上不支持平滑重载。
//
// 设计说明：Windows 不支持 Unix 信号机制，因此无法通过 SIGUSR2 触发平滑重载。
// 调用此方法将返回错误，提示使用 Restart 方法替代。
func (pm *ProcessManager) Reload(pid int) (int, error) {
	return 0, fmt.Errorf("reload is not supported on Windows; use Restart instead")
}

func (pm *ProcessManager) waitForNewPID(oldPID int, timeout time.Duration) (int, error) {
	return 0, fmt.Errorf("waitForNewPID is not supported on Windows")
}

// WatchReloadSignal 在 Windows 平台上返回空操作清理函数。
//
// 设计说明：Windows 不支持 Unix 信号机制，因此无法监听 SIGUSR2 信号。
// 此函数在 Windows 上为 no-op，返回一个空的清理函数。
func WatchReloadSignal(ctx context.Context, listener net.Listener, executable string, args []string, onReady func()) func() {
	return func() {}
}

func spawnReloadChild(listener net.Listener, executable string, args []string, onReady func()) error {
	return fmt.Errorf("spawnReloadChild is not supported on Windows")
}
