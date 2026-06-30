package rabbitmq

import (
	"sync"
	"testing"
	"time"
)

// TestDeliveryRegistryCleanup_CanBeStopped 验证清理 goroutine 可以从外部停止。
func TestDeliveryRegistryCleanup_CanBeStopped(t *testing.T) {
	// 重置全局状态
	deliveryRegistryCleanupOnce = sync.Once{}
	deliveryRegistryCleanupCancel = nil

	// 启动清理 goroutine
	startDeliveryRegistryCleanup()

	// 验证已启动
	if deliveryRegistryCleanupCancel == nil {
		t.Fatal("expected cleanup to be started")
	}

	// 停止清理 goroutine
	stopDeliveryRegistryCleanup()

	// 验证已停止（cancel 函数应该被调用）
	// 等待一小段时间确保 goroutine 退出
	time.Sleep(10 * time.Millisecond)

	// 再次启动应该可以成功（once 已重置）
	deliveryRegistryCleanupOnce = sync.Once{}
	startDeliveryRegistryCleanup()

	if deliveryRegistryCleanupCancel == nil {
		t.Fatal("expected cleanup to be restarted")
	}

	// 清理
	stopDeliveryRegistryCleanup()
}
