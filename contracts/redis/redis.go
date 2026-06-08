// Package redis 定义 PrismGo 共享 Redis 能力的公共契约。
//
// 需求背景：
// cache、queue、session、horizon 都需要 Redis，但它们不应该各自复制 host、port、
// password、database 等连接配置，也不应该各自持有无法统一关闭的 Redis client。
//
// 设计思路：
// 契约层只定义“按名称解析连接”和“暴露 go-redis 原生 client”的边界，不重新抽象
// Redis 命令全集。业务或框架包需要 Redis 专有能力时，直接使用 Connection.Client()。
// PrismGo 管理的 client 会通过 go-redis hook 自动派发 Redis 命令事件。
//
// 使用方式：
//
//	factory, err := redis.Resolve()
//	if err != nil {
//	    return err
//	}
//	conn, err := factory.Connection("cache")
//	if err != nil {
//	    return err
//	}
//	return conn.Client().Set(ctx, "key", "value", 0).Err()
package redis

import (
	"context"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// Factory 是 Redis 能力的统一入口，负责按名称懒加载连接并管理连接生命周期。
//
// 逻辑说明：
// Connection("cache") 这类调用只声明需要哪个连接；真实 Redis client 应在第一次解析时
// 创建并缓存，Application 关闭时再通过 Close 统一释放。Connections 只暴露已创建连接，
// 不触发新连接创建。
//
// 调用示例：
//
//	conn, err := factory.Connection("cache")
//	if err != nil {
//	    return err
//	}
//	n, err := conn.Client().Incr(ctx, "counter").Result()
//	_ = n
type Factory interface {
	// Connection 返回命名连接。
	//
	// 参数说明：
	// name 可选；未传或传入空字符串时使用默认连接。实现方应缓存同名连接，直到 Purge
	// 或 Close 被调用。
	//
	// 调用示例：
	//
	//	conn, err := factory.Connection("cache")
	//	if err != nil {
	//	    return err
	//	}
	//	return conn.Client().Ping(ctx).Err()
	Connection(name ...string) (Connection, error)

	// DefaultConnection 返回默认连接。
	//
	// 逻辑说明：
	// 该方法等价于 Connection()，但让 provider、facade 或测试代码可以显式表达
	// “我要默认连接”而不是依赖可变参数空值。
	//
	// 调用示例：
	//
	//	conn, err := factory.DefaultConnection()
	//	if err != nil {
	//	    return err
	//	}
	//	return conn.Client().Set(ctx, "key", "value", 0).Err()
	DefaultConnection() (Connection, error)

	// Connections 返回已解析连接快照。
	//
	// 设计思路：
	// 该方法服务诊断、测试和生命周期检查，不能为了生成快照而创建尚未使用的连接。
	// 返回 map 的修改不应影响 Factory 内部状态。
	//
	// 调用示例：
	//
	//	for name, conn := range factory.Connections() {
	//	    log.Println(name, conn.Name())
	//	}
	Connections() map[string]Connection

	// Purge 关闭并移除指定连接。
	//
	// 参数说明：
	// name 可选；未传时清理默认连接。下一次 Connection(name) 应重新读取配置并创建连接。
	// 适用于测试隔离、配置热替换或连接需要强制重建的场景。
	//
	// 调用示例：
	//
	//	if err := factory.Purge("cache"); err != nil {
	//	    return err
	//	}
	//	_, err := factory.Connection("cache")
	Purge(name ...string) error

	// EnableEvents 开启 PrismGo 管理 client 的 Redis 命令事件。
	//
	// 逻辑说明：
	// 事件覆盖通过 Connection.Client() 返回的 go-redis client 执行的命令和 pipeline。
	//
	// 调用示例：
	//
	//	factory.EnableEvents()
	//	_ = conn.Client().Get(ctx, "key").Err()
	EnableEvents()

	// DisableEvents 关闭 PrismGo 管理 client 的 Redis 命令事件。
	//
	// 设计思路：
	// 高吞吐批处理或测试场景可以关闭事件开销；go-redis 命令仍正常执行并返回原始结果。
	//
	// 调用示例：
	//
	//	factory.DisableEvents()
	//	_ = conn.Client().Set(ctx, "key", "value", 0).Err()
	DisableEvents()

	// Close 关闭所有已解析连接。
	//
	// 参数说明：
	// ctx 来自 Application 关闭链路，用于控制关闭等待时间。Close 不需要创建尚未解析的连接。
	//
	// 调用示例：
	//
	//	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	//	defer cancel()
	//	return factory.Close(ctx)
	Close(context.Context) error
}

// Connection 表示一个命名 Redis 连接。
//
// 需求背景：
// Redis 命令面非常大，PrismGo 不把所有命令重写成自定义接口；Connection 保留 Client()
// 让调用方直接使用 go-redis 的完整能力。
//
// 使用方式：
// 需要 Redis 能力时使用 Client。PrismGo 只保证通过 PrismGo 管理连接返回的 client 会安装
// 事件 hook；从未交给 PrismGo 的外部 client 不在观测范围内。
//
//	if err := conn.Client().Set(ctx, "key", "value", 0).Err(); err != nil {
//	    return err
//	}
type Connection interface {
	// Name 返回配置中的连接名称。
	//
	// 调用示例：
	//
	//	log.Println("redis connection:", conn.Name())
	Name() string

	// Client 暴露底层 go-redis client。
	//
	// 设计思路：
	// 调用方需要复杂命令、pipeline、transaction、pub/sub 或 Lua script 时直接走这里，
	// 避免框架层维护不完整的 Redis 命令抽象。
	//
	// 调用示例：
	//
	//	pipe := conn.Client().Pipeline()
	//	pipe.Set(ctx, "a", "1", 0)
	//	_, err := pipe.Exec(ctx)
	Client() goredis.UniversalClient

	// Listen 注册当前连接的单命令成功监听器。
	//
	// 参数说明：
	// listener 接收 CommandExecuted；nil listener 应被实现方忽略。
	//
	// 调用示例：
	//
	//	conn.Listen(func(ctx context.Context, ev redis.CommandExecuted) {
	//	    log.Println(ev.Command, ev.Time)
	//	})
	Listen(func(context.Context, CommandExecuted))

	// ListenForFailures 注册当前连接的单命令失败监听器。
	//
	// 参数说明：
	// listener 接收 CommandFailed；nil listener 应被实现方忽略。事件不会吞掉错误，
	// 调用方仍应检查 go-redis 返回的 Err。
	//
	// 调用示例：
	//
	//	conn.ListenForFailures(func(ctx context.Context, ev redis.CommandFailed) {
	//	    log.Println(ev.Command, ev.Error)
	//	})
	ListenForFailures(func(context.Context, CommandFailed))
}

// CommandExecuted 表示 PrismGo 管理 client 成功执行了一条 Redis 命令。
//
// 逻辑说明：
// Parameters 是 go-redis 命令参数快照，不包含命令名本身；为对齐 Laravel 13，事件默认不做
// 参数脱敏，监听器若转发到日志、指标或外部监控，需要自行承担敏感参数过滤责任。
// ConnectionName 用于监听方按 Redis 连接聚合耗时或吞吐指标。
type CommandExecuted struct {
	// Command 是执行成功的 Redis 命令名，例如 get、set、del。
	Command string
	// Parameters 是命令参数快照，不包含 Command 字段本身。
	Parameters []any
	// Time 是 Redis 命令从发出到返回成功结果的耗时。
	Time time.Duration
	// Connection 是产生该事件的 Redis 连接，监听器可通过它读取连接名称或原生 client。
	Connection Connection
	// ConnectionName 是连接名称快照，便于日志和指标在不持有 Connection 时聚合。
	ConnectionName string
}

// CommandFailed 表示 PrismGo 管理 client 执行单条 Redis 命令失败。
//
// 逻辑说明：
// Error 保留 go-redis 返回的原始错误，便于监听器记录失败原因；调用方仍会从 go-redis
// 命令结果得到同一个错误。
type CommandFailed struct {
	// Command 是执行失败的 Redis 命令名，例如 get、set、del。
	Command string
	// Parameters 是命令参数快照，不包含 Command 字段本身。
	Parameters []any
	// Error 是 go-redis 返回的原始错误，调用方仍会从 Cmd.Err 得到同一个错误。
	Error error
	// Connection 是产生该事件的 Redis 连接。
	Connection Connection
	// ConnectionName 是连接名称快照，便于失败日志按 default/cache 等连接聚合。
	ConnectionName string
}

// CommandSnapshot 表示 pipeline/tx-pipeline 中单条命令的观测快照。
type CommandSnapshot struct {
	// Command 是 Redis 命令名，例如 get、set、del。
	Command string
	// Parameters 是命令参数快照，不包含 Command 字段本身。
	Parameters []any
	// Error 是该命令的 go-redis 错误；成功命令为 nil。
	Error error
}

// CommandBatchExecuted 表示 pipeline 或 tx-pipeline 成功执行。
type CommandBatchExecuted struct {
	// Commands 是批量执行中的命令快照。
	Commands []CommandSnapshot
	// Time 是批量命令从发出到返回成功结果的耗时。
	Time time.Duration
	// Connection 是产生该事件的 Redis 连接。
	Connection Connection
	// ConnectionName 是连接名称快照。
	ConnectionName string
}

// CommandBatchFailed 表示 pipeline 或 tx-pipeline 执行失败。
type CommandBatchFailed struct {
	// Commands 是批量执行中的命令快照，失败命令会带 Error。
	Commands []CommandSnapshot
	// Error 是 go-redis 为本次批量执行返回的错误。
	Error error
	// Connection 是产生该事件的 Redis 连接。
	Connection Connection
	// ConnectionName 是连接名称快照。
	ConnectionName string
}
