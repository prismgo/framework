package queue

import (
	"strings"
	"testing"
)

// TestTruncateStack_ShortStack 验证短堆栈不被截断。
func TestTruncateStack_ShortStack(t *testing.T) {
	stack := []byte("short stack trace")
	result := truncateStack(stack)
	if string(result) != string(stack) {
		t.Errorf("expected %q, got %q", stack, result)
	}
}

// TestTruncateStack_LongStack 验证长堆栈被截断到 4KB。
func TestTruncateStack_LongStack(t *testing.T) {
	// 创建 10KB 的堆栈
	largeStack := make([]byte, 10*1024)
	for i := range largeStack {
		largeStack[i] = 'A'
	}

	result := truncateStack(largeStack)

	// 应该被截断到 4KB + 截断提示
	maxSize := 4*1024 + 100 // 4KB + 一些余量
	if len(result) > maxSize {
		t.Errorf("stack too large: %d bytes (max %d)", len(result), maxSize)
	}

	// 应该包含截断提示
	if !strings.Contains(string(result), "truncated") {
		t.Errorf("expected truncation message, got: %q", result)
	}
}

// TestTruncateStack_ExactlyAtLimit 验证恰好等于限制的堆栈不被截断。
func TestTruncateStack_ExactlyAtLimit(t *testing.T) {
	stack := make([]byte, 4*1024)
	for i := range stack {
		stack[i] = 'B'
	}

	result := truncateStack(stack)
	if len(result) != len(stack) {
		t.Errorf("expected %d bytes, got %d", len(stack), len(result))
	}
}
