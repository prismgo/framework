// Package cache 提供 Laravel Cache 风格的通用缓存能力。
//
// 包的设计目标是把缓存、热点 key 刷新和分布式锁能力从业务代码中解耦出来，
// 通过 Manager 管理多 store 后端，并通过包级 facade 提供类似 Laravel Cache 的
// 便捷调用方式。当前支持进程内 memory store、Redis store、file store，
// 并可通过 Extend 注册用户自定义 driver。
//
// 数值语义 value 会直写为十进制 bytes；其他 cache value 默认使用 Payload Encoding 的
// msgpack 编码，显式 cache.encoding=json 时使用 JSON。Increment/Decrement 只接受
// 整数计数器值。
package cache
