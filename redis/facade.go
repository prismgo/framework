package redis

import (
	"context"

	"github.com/prismgo/framework/container"
	containercontract "github.com/prismgo/framework/contracts/container"
	rediscontract "github.com/prismgo/framework/contracts/redis"
	"github.com/prismgo/framework/facade"
	goredis "github.com/redis/go-redis/v9"
)

const (
	serviceKey           = "redis"
	defaultConnectionKey = "redis.connection"
)

// Resolve 返回当前 Application 绑定的 Redis Factory。
func Resolve() rediscontract.Factory {
	return facade.Resolve[rediscontract.Factory](serviceKey)
}

// ManagerInstance 返回当前 Application 绑定的具体 Redis Manager。
func ManagerInstance() *Manager {
	return facade.Resolve[*Manager](serviceKey)
}

// Connection 解析指定名称的 Redis 连接；未传名称时使用默认连接。
func Connection(name ...string) (rediscontract.Connection, error) {
	factory, err := container.Make[rediscontract.Factory](serviceKey)
	if err != nil {
		return nil, err
	}
	return factory.Connection(name...)
}

// Client 返回指定 Redis 连接的 go-redis client。
func Client(name ...string) (goredis.UniversalClient, error) {
	conn, err := Connection(name...)
	if err != nil {
		return nil, err
	}
	return conn.Client(), nil
}

// ManagerCloseOption 返回 Redis Manager 的关闭选项，供 bootstrap 注册时使用。
func ManagerCloseOption() containercontract.BindingOption {
	return container.WithContextCloser(func(ctx context.Context, manager *Manager) error {
		return manager.Close(ctx)
	})
}
