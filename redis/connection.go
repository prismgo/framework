package redis

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	goexception "github.com/prismgo/framework/exception"
	goredis "github.com/redis/go-redis/v9"
)

// NamedConnection 包装 go-redis client，并为 PrismGo 管理的 client 安装事件 hook。
//
// 需求背景：
// Redis 的命令集合很大，PrismGo 不应为了事件观测而重新定义所有命令。NamedConnection 在
// 保留 go-redis 原生 client 的同时，通过 hook 观察 typed commands、Do、pipeline 和
// tx-pipeline。没有通过 PrismGo 管理连接返回的外部 client 不在观测范围内。
//
// 设计思路：
// Client 方法负责暴露完整 go-redis 能力；hook 负责计时、错误判断、成功/失败监听器派发
// 和全局事件桥接。监听器按连接实例注册，适合测试和局部观测。
//
// 使用示例：
//
//	conn := redis.NewConnectionFromClient("cache", client)
//	conn.Listen(func(ctx context.Context, ev redis.CommandExecuted) {
//	    log.Println(ev.Command, ev.ConnectionName)
//	})
//	cmd := conn.Client().Get(ctx, "cache:key")
//	if err := cmd.Err(); err != nil {
//	    return err
//	}
type NamedConnection struct {
	name   string
	client goredis.UniversalClient
	events bool

	mu               sync.RWMutex
	successListeners []func(context.Context, CommandExecuted)
	failureListeners []func(context.Context, CommandFailed)
}

func newConnection(name string, client goredis.UniversalClient, events bool) *NamedConnection {
	name = strings.TrimSpace(name)
	if name == "" {
		name = DefaultConnectionName
	}
	conn := &NamedConnection{name: name, client: client, events: events}
	if client != nil {
		client.AddHook(prismgoRedisHook{conn: conn})
	}
	return conn
}

// NewConnectionFromClient 使用外部 go-redis client 创建命名连接。
//
// 参数说明：
// name 是连接名称，空值会回退为 default；client 是调用方已经创建好的 go-redis
// UniversalClient，通常用于单元测试、已有连接池接入或高级集成。
//
// 逻辑说明：
// 该函数不会接管外部 client 的创建配置，只把 client 包装成 PrismGo Connection。连接是否
// 被外层生命周期关闭，取决于调用方如何持有和注册该连接。
func NewConnectionFromClient(name string, client goredis.UniversalClient) *NamedConnection {
	return newConnection(name, client, true)
}

// Name 返回连接名称。
//
// 逻辑说明：
// 名称来自 database.redis 的连接键，例如 default/cache；事件中的 ConnectionName 也使用
// 这个值，便于日志和指标按连接维度聚合。
func (c *NamedConnection) Name() string { return c.name }

// Client 返回底层 go-redis client。
//
// 设计思路：
// PrismGo 不复制 go-redis 的 pipeline、transaction、pub/sub、script 等完整 API。调用方
// 需要这些原生能力时直接使用 Client；PrismGo 管理的 client 会自动派发 Redis 命令事件。
func (c *NamedConnection) Client() goredis.UniversalClient { return c.client }

// Listen 注册当前连接的成功命令监听器。
//
// 参数说明：
// listener 接收 CommandExecuted 事件；传入 nil 会被忽略。监听器观察当前连接的单条命令，
// 不接收 pipeline 批量事件。
func (c *NamedConnection) Listen(listener func(context.Context, CommandExecuted)) {
	if listener == nil {
		return
	}
	c.mu.Lock()
	c.successListeners = append(c.successListeners, listener)
	c.mu.Unlock()
}

// ListenForFailures 注册当前连接的失败命令监听器。
//
// 参数说明：
// listener 接收 CommandFailed 事件；传入 nil 会被忽略。监听器用于记录失败原因或指标，
// 但不会改变 go-redis 命令返回的 Err 结果。
func (c *NamedConnection) ListenForFailures(listener func(context.Context, CommandFailed)) {
	if listener == nil {
		return
	}
	c.mu.Lock()
	c.failureListeners = append(c.failureListeners, listener)
	c.mu.Unlock()
}

func (c *NamedConnection) setEvents(enabled bool) {
	c.mu.Lock()
	c.events = enabled
	c.mu.Unlock()
}

func (c *NamedConnection) eventsEnabled() bool {
	c.mu.RLock()
	enabled := c.events
	c.mu.RUnlock()
	return enabled
}

func (c *NamedConnection) dispatchSuccess(ctx context.Context, command string, parameters []any, elapsed time.Duration) {
	ev := CommandExecuted{
		Command:        command,
		Parameters:     append([]any(nil), parameters...),
		Time:           elapsed,
		Connection:     c,
		ConnectionName: c.name,
	}
	c.mu.RLock()
	listeners := append(([]func(context.Context, CommandExecuted))(nil), c.successListeners...)
	c.mu.RUnlock()
	for _, listener := range listeners {
		c.dispatchSuccessListener(ctx, listener, ev)
	}
	dispatchEvent(ctx, CommandExecutedEvent{CommandExecuted: ev})
}

func (c *NamedConnection) dispatchFailure(ctx context.Context, command string, parameters []any, err error) {
	ev := CommandFailed{
		Command:        command,
		Parameters:     append([]any(nil), parameters...),
		Error:          err,
		Connection:     c,
		ConnectionName: c.name,
	}
	c.mu.RLock()
	listeners := append(([]func(context.Context, CommandFailed))(nil), c.failureListeners...)
	c.mu.RUnlock()
	for _, listener := range listeners {
		c.dispatchFailureListener(ctx, listener, ev)
	}
	dispatchEvent(ctx, CommandFailedEvent{CommandFailed: ev})
}

func (c *NamedConnection) dispatchSuccessListener(ctx context.Context, listener func(context.Context, CommandExecuted), ev CommandExecuted) {
	defer c.reportListenerPanic(ctx, ev.Command, "success")
	listener(ctx, ev)
}

func (c *NamedConnection) dispatchFailureListener(ctx context.Context, listener func(context.Context, CommandFailed), ev CommandFailed) {
	defer c.reportListenerPanic(ctx, ev.Command, "failure")
	listener(ctx, ev)
}

func (c *NamedConnection) reportListenerPanic(ctx context.Context, command string, listener string) {
	recovered := recover()
	if recovered == nil {
		return
	}
	err, ok := recovered.(error)
	if !ok {
		err = fmt.Errorf("%v", recovered)
	}
	goexception.Report(ctx, err, map[string]any{
		"component":  "redis",
		"connection": c.name,
		"command":    command,
		"listener":   listener,
	})
}

func (c *NamedConnection) dispatchBatchSuccess(ctx context.Context, commands []CommandSnapshot, elapsed time.Duration) {
	dispatchEvent(ctx, CommandBatchExecutedEvent{CommandBatchExecuted: CommandBatchExecuted{
		Commands:       commands,
		Time:           elapsed,
		Connection:     c,
		ConnectionName: c.name,
	}})
}

func (c *NamedConnection) dispatchBatchFailure(ctx context.Context, commands []CommandSnapshot, err error) {
	dispatchEvent(ctx, CommandBatchFailedEvent{CommandBatchFailed: CommandBatchFailed{
		Commands:       commands,
		Error:          err,
		Connection:     c,
		ConnectionName: c.name,
	}})
}

type prismgoRedisHook struct {
	conn *NamedConnection
}

func (h prismgoRedisHook) DialHook(next goredis.DialHook) goredis.DialHook {
	return next
}

func (h prismgoRedisHook) ProcessHook(next goredis.ProcessHook) goredis.ProcessHook {
	return func(ctx context.Context, cmd goredis.Cmder) error {
		if ctx == nil {
			ctx = context.Background()
		}
		start := time.Now()
		err := next(ctx, cmd)
		if h.conn == nil || !h.conn.eventsEnabled() {
			return err
		}
		snapshot := commandSnapshot(cmd)
		if shouldSkipCommandSnapshot(snapshot) {
			return err
		}
		if err != nil {
			h.conn.dispatchFailure(ctx, snapshot.Command, snapshot.Parameters, err)
			return err
		}
		if cmdErr := cmd.Err(); cmdErr != nil {
			h.conn.dispatchFailure(ctx, snapshot.Command, snapshot.Parameters, cmdErr)
			return err
		}
		h.conn.dispatchSuccess(ctx, snapshot.Command, snapshot.Parameters, time.Since(start))
		return err
	}
}

func (h prismgoRedisHook) ProcessPipelineHook(next goredis.ProcessPipelineHook) goredis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []goredis.Cmder) error {
		if ctx == nil {
			ctx = context.Background()
		}
		start := time.Now()
		err := next(ctx, cmds)
		if h.conn == nil || !h.conn.eventsEnabled() {
			return err
		}
		snapshots := commandSnapshots(observedPipelineCommands(cmds))
		snapshots = observedCommandSnapshots(snapshots)
		if len(snapshots) == 0 {
			return err
		}
		if err != nil {
			h.conn.dispatchBatchFailure(ctx, snapshots, err)
			return err
		}
		for _, snapshot := range snapshots {
			if snapshot.Error != nil {
				h.conn.dispatchBatchFailure(ctx, snapshots, snapshot.Error)
				return err
			}
		}
		h.conn.dispatchBatchSuccess(ctx, snapshots, time.Since(start))
		return err
	}
}

func commandSnapshots(cmds []goredis.Cmder) []CommandSnapshot {
	out := make([]CommandSnapshot, 0, len(cmds))
	for _, cmd := range cmds {
		out = append(out, commandSnapshot(cmd))
	}
	return out
}

func observedPipelineCommands(cmds []goredis.Cmder) []goredis.Cmder {
	if len(cmds) >= 2 && commandName(cmds[0]) == "multi" && commandName(cmds[len(cmds)-1]) == "exec" {
		return cmds[1 : len(cmds)-1]
	}
	return cmds
}

func observedCommandSnapshots(commands []CommandSnapshot) []CommandSnapshot {
	out := commands[:0]
	for _, command := range commands {
		if !shouldSkipCommandSnapshot(command) {
			out = append(out, command)
		}
	}
	return out
}

func shouldSkipCommandSnapshot(command CommandSnapshot) bool {
	switch command.Command {
	case "hello":
		return len(command.Parameters) == 1 && strings.TrimSpace(strings.ToLower(anyString(command.Parameters[0]))) == "3"
	case "client":
		if len(command.Parameters) == 0 {
			return false
		}
		subcommand := strings.TrimSpace(strings.ToLower(anyString(command.Parameters[0])))
		return subcommand == "maint_notifications" || subcommand == "setinfo"
	default:
		return false
	}
}

func commandName(cmd goredis.Cmder) string {
	if cmd == nil {
		return ""
	}
	return strings.TrimSpace(strings.ToLower(cmd.Name()))
}

func anyString(value any) string {
	if text, ok := value.(string); ok {
		return text
	}
	return fmt.Sprint(value)
}

func commandSnapshot(cmd goredis.Cmder) CommandSnapshot {
	if cmd == nil {
		return CommandSnapshot{}
	}
	args := cmd.Args()
	command := commandName(cmd)
	parameters := args
	if len(parameters) > 0 {
		parameters = parameters[1:]
	}
	return CommandSnapshot{
		Command:    command,
		Parameters: append([]any(nil), parameters...),
		Error:      cmd.Err(),
	}
}
