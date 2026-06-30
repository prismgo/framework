package stackx

import (
	"bytes"
	"strings"
	"testing"
)

// TestCapture_ReturnsNonEmpty 验证 Capture 返回非空堆栈。
func TestCapture_ReturnsNonEmpty(t *testing.T) {
	stack := Capture()
	if len(stack) == 0 {
		t.Error("expected non-empty stack")
	}
}

// TestCapture_ContainsTestFunction 验证堆栈包含当前测试函数。
func TestCapture_ContainsTestFunction(t *testing.T) {
	stack := Capture()
	if !strings.Contains(string(stack), "TestCapture_ContainsTestFunction") {
		t.Errorf("expected stack to contain test function name, got: %q", stack)
	}
}

// TestTruncate_ShortStack 验证短堆栈不被截断。
func TestTruncate_ShortStack(t *testing.T) {
	stack := []byte("short stack trace")
	result := truncate(stack)
	if string(result) != string(stack) {
		t.Errorf("expected %q, got %q", stack, result)
	}
}

// TestTruncate_LongStack 验证长堆栈被截断到 4KB。
func TestTruncate_LongStack(t *testing.T) {
	largeStack := make([]byte, 10*1024)
	for i := range largeStack {
		largeStack[i] = 'A'
	}

	result := truncate(largeStack)

	maxSize := 4*1024 + 100
	if len(result) > maxSize {
		t.Errorf("stack too large: %d bytes (max %d)", len(result), maxSize)
	}

	if !strings.Contains(string(result), "truncated") {
		t.Errorf("expected truncation message, got: %q", result)
	}
}

// TestTruncate_ExactlyAtLimit 验证恰好等于限制的堆栈不被截断。
func TestTruncate_ExactlyAtLimit(t *testing.T) {
	stack := make([]byte, 4*1024)
	for i := range stack {
		stack[i] = 'B'
	}

	result := truncate(stack)
	if len(result) != len(stack) {
		t.Errorf("expected %d bytes, got %d", len(stack), len(result))
	}
}

// TestTruncate_ExactlyOverLimit 验证超过限制即被截断，并对齐到行边界。
func TestTruncate_ExactlyOverLimit(t *testing.T) {
	// 构造超过 4KB 的堆栈，包含换行符
	var stack []byte
	for i := 0; i < 500; i++ {
		stack = append(stack, []byte("line content here\n")...)
	}

	result := truncate(stack)

	if !strings.HasSuffix(string(result), "... stack trace truncated ...") {
		t.Errorf("expected truncation suffix, got: %q", result[len(result)-50:])
	}

	// 验证截断点对齐到换行符
	if len(result) > len(truncationSuffix) {
		lastChar := result[len(result)-len(truncationSuffix)-1]
		if lastChar != '\n' {
			t.Errorf("expected truncation point at newline, got byte %d", lastChar)
		}
	}
}

// TestTruncate_PreservesExactPrefix 验证截断后前缀与原始堆栈完全一致。
func TestTruncate_PreservesExactPrefix(t *testing.T) {
	// 构造超过 4KB 的堆栈，包含换行符
	var largeStack []byte
	for i := 0; i < 1000; i++ {
		largeStack = append(largeStack, []byte("goroutine frame\n")...)
	}

	result := truncate(largeStack)

	if len(result) < 4*1024 {
		t.Fatalf("result too short: %d bytes", len(result))
	}

	// 验证截断后的内容（去除 suffix）与原始堆栈前缀一致
	contentLen := len(result) - len(truncationSuffix)
	if !bytes.Equal(result[:contentLen], largeStack[:contentLen]) {
		t.Error("truncated prefix does not match original stack prefix")
	}
}

// TestTruncate_AlignsToLineBoundary 验证截断点对齐到行边界。
func TestTruncate_AlignsToLineBoundary(t *testing.T) {
	// 构造超过 4KB 的堆栈
	var stack []byte
	for i := 0; i < 500; i++ {
		stack = append(stack, []byte("goroutine 1 [running]:\n")...)
		stack = append(stack, []byte("main.main()\n")...)
		stack = append(stack, []byte("\t/path/to/file.go:10 +0x45\n")...)
	}

	result := truncate(stack)

	if !strings.HasSuffix(string(result), "\n... stack trace truncated ...") {
		t.Errorf("truncation suffix should start with newline, got: %q", result[len(result)-50:])
	}

	// 验证截断点前是换行符
	contentLen := len(result) - len(truncationSuffix)
	if contentLen > 0 && result[contentLen-1] != '\n' {
		t.Errorf("expected newline before suffix, got byte %d", result[contentLen-1])
	}
}
