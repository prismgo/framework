package queue

import (
	"reflect"
	"testing"

	redisqueue "github.com/prismgo/framework/queue/redis"
)

// TestArchitectureKeepsManagerAndConnectionConfigLaravelShaped 固定 Laravel config contract 的 Laravel 对齐边界。
//
// 需求背景：Laravel QueueManager 只解析连接和 connector；运行期依赖、driver 强类型配置和
// Redis state 浅 adapter 都会让父包重新变厚，因此用反射守卫阻止这些职责回流。
func TestArchitectureKeepsManagerAndConnectionConfigLaravelShaped(t *testing.T) {
	managerType := reflect.TypeOf(Manager{})
	for _, field := range []string{
		"defaultQueue",
		"cacheDriver",
		"failed",
		"batch",
		"restart",
		"registry",
		"codec",
		"middleware",
		"payloadCipher",
		"connectors",
		"connectorsMu",
	} {
		if _, ok := managerType.FieldByName(field); ok {
			t.Fatalf("Manager still has runtime field %q", field)
		}
	}

	configType := reflect.TypeOf(ConnectionConfig{})
	for _, field := range []string{"Redis", "RabbitMQ"} {
		if _, ok := configType.FieldByName(field); ok {
			t.Fatalf("ConnectionConfig still has driver-specific field %q", field)
		}
	}

	var _ FailedStore = (*redisqueue.RedisFailedStore)(nil)
	var _ BatchStore = (*redisqueue.RedisBatchStore)(nil)
}
