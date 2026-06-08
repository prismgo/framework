package redis

import (
	"context"
	"sync"

	eventcontract "github.com/prismgo/framework/contracts/event"
	rediscontract "github.com/prismgo/framework/contracts/redis"
)

const (
	// EventCommandExecuted 是 Redis 命令成功事件名。
	//
	// 逻辑说明：
	// 事件由 PrismGo 管理的 go-redis client 成功执行单条命令后产生，名称与
	// contracts/redis.CommandExecuted.Name
	// 保持一致，便于事件监听器按字符串注册。
	EventCommandExecuted = "redis.command_executed"
	// EventCommandFailed 是 Redis 命令失败事件名。
	//
	// 逻辑说明：
	// 事件由 PrismGo 管理的 go-redis client 返回单命令错误后产生，错误仍会保留在 go-redis Cmd.Err 中，
	// 监听器只做观测，不改变调用方错误处理路径。
	EventCommandFailed = "redis.command_failed"
	// EventCommandBatchExecuted 是 Redis pipeline/tx-pipeline 成功事件名。
	EventCommandBatchExecuted = "redis.command_batch_executed"
	// EventCommandBatchFailed 是 Redis pipeline/tx-pipeline 失败事件名。
	EventCommandBatchFailed = "redis.command_batch_failed"
)

// CommandExecuted 是 contracts/redis.CommandExecuted 的实现包别名。
//
// 需求背景：
// 使用 prismgo/redis facade 的调用方通常已经导入实现包；提供别名可以让本地监听器签名
// 直接写 redis.CommandExecuted，同时事件结构仍由契约包统一定义。
type CommandExecuted = rediscontract.CommandExecuted

// CommandFailed 是 contracts/redis.CommandFailed 的实现包别名。
//
// 设计思路：
// 失败事件结构保持契约层唯一来源，避免 contracts/redis 与 prismgo/redis 出现字段漂移。
type CommandFailed = rediscontract.CommandFailed

// CommandSnapshot 是 contracts/redis.CommandSnapshot 的实现包别名。
type CommandSnapshot = rediscontract.CommandSnapshot

// CommandBatchExecuted 是 contracts/redis.CommandBatchExecuted 的实现包别名。
type CommandBatchExecuted = rediscontract.CommandBatchExecuted

// CommandBatchFailed 是 contracts/redis.CommandBatchFailed 的实现包别名。
type CommandBatchFailed = rediscontract.CommandBatchFailed

// CommandExecutedEvent 是 Redis 单命令成功 DTO 的事件总线包装。
type CommandExecutedEvent struct {
	CommandExecuted
}

func (CommandExecutedEvent) Name() string { return EventCommandExecuted }

// CommandFailedEvent 是 Redis 单命令失败 DTO 的事件总线包装。
type CommandFailedEvent struct {
	CommandFailed
}

func (CommandFailedEvent) Name() string { return EventCommandFailed }

// CommandBatchExecutedEvent 是 Redis 批量命令成功 DTO 的事件总线包装。
type CommandBatchExecutedEvent struct {
	CommandBatchExecuted
}

func (CommandBatchExecutedEvent) Name() string { return EventCommandBatchExecuted }

// CommandBatchFailedEvent 是 Redis 批量命令失败 DTO 的事件总线包装。
type CommandBatchFailedEvent struct {
	CommandBatchFailed
}

func (CommandBatchFailedEvent) Name() string { return EventCommandBatchFailed }

// EventSink 是 Redis 实现包向事件系统派发事件的桥接函数。
//
// 参数说明：
// ctx 是执行 Redis 命令时的上下文；event 是 Redis 单命令或批量命令事件。
//
// 逻辑说明：
// Redis 包不直接依赖具体 event dispatcher 实例，而是通过 ServiceProvider 在 Boot 阶段
// 安装 EventSink，从而保持 Redis Manager 的可测试性和低耦合。
type EventSink func(context.Context, eventcontract.Event)

var eventSinkState struct {
	mu   sync.RWMutex
	sink EventSink
}

// UseEventSink 设置 Redis 命令事件的全局桥接函数。
//
// 参数说明：
// sink 为 nil 时表示清空桥接；非 nil 时会在每次 Redis 事件后被调用。
//
// 需求背景：
// provider 启动顺序和测试环境都可能更换当前 Application。通过可替换 sink 可以让 Redis
// 事件在不同容器上下文之间保持正确派发。
func UseEventSink(sink EventSink) {
	eventSinkState.mu.Lock()
	eventSinkState.sink = sink
	eventSinkState.mu.Unlock()
}

func dispatchEvent(ctx context.Context, ev eventcontract.Event) {
	eventSinkState.mu.RLock()
	sink := eventSinkState.sink
	eventSinkState.mu.RUnlock()
	if sink != nil {
		sink(ctx, ev)
	}
}
