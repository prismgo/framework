package exception

import "testing"

// BenchmarkIsSensitiveLogKey 验证 isSensitiveLogKey 的性能基线。
// 优化目标：避免每次调用都构造 strings.NewReplacer。
func BenchmarkIsSensitiveLogKey(b *testing.B) {
	keys := []string{
		"password",
		"api_key",
		"authorization",
		"service_key",
		"safe_field",
		"X-Custom-Header",
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, key := range keys {
			_ = isSensitiveLogKey(key)
		}
	}
}
