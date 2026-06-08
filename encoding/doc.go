// Package encoding 提供 Prismgo 内置 Payload Encoding codec。
//
// 需求背景：Prismgo 的 cache value、session payload、queue envelope/job payload/
// failed/batch/chain metadata、Horizon store records 都属于内部持久化或内部传输值，
// 需要统一编码边界以支持默认 msgpack 和显式 JSON 回滚。
//
// 设计思路：本包只提供内置 json/msgpack codec 与严格名称解析，不提供 facade、
// 全局 registry、provider 或自定义 codec 注册。HTTP API JSON、CLI --json 输出、
// Dashboard API JSON、第三方 API JSON 和 README 示例 struct tag 不属于本包边界。
package encoding
