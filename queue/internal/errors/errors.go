package errors

import "errors"

// ErrEmpty 表示当前 queue transport 没有可消费任务。
//
// 设计背景：Redis/RabbitMQ driver 位于子包，不能 import 父包 queue；空队列 sentinel
// 放在 internal 包中，父包再导出别名，保证 errors.Is 在不同 driver 间保持一致。
var ErrEmpty = errors.New("queue: empty")

// ErrPoisonEnvelope 表示 driver 已取得原始消息，但无法恢复为 Prismgo Envelope。
//
// 需求背景：
// RabbitMQ 子包不能 import 父包 prismgo/queue，否则会形成循环依赖；父包和子包必须共享同一个
// sentinel，保证调用方可以通过 errors.Is(err, queue.ErrPoisonEnvelope) 判断坏消息边界错误。
var ErrPoisonEnvelope = errors.New("queue: poison envelope")
