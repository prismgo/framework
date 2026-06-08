package rabbitmq

import (
	"context"

	amqp "github.com/rabbitmq/amqp091-go"
)

// 本文件隔离 amqp091-go 的具体类型，给测试注入 fake connection/channel 留出边界。
// 业务代码只依赖下方小接口，避免在重连和拓扑测试里启动真实 RabbitMQ。

type AMQPConnection interface {
	Channel() (AMQPChannel, error)
	NotifyClose(receiver chan *amqp.Error) chan *amqp.Error
	Close() error
	IsClosed() bool
}

type AMQPChannel interface {
	Close() error
	ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error
	ExchangeDeclarePassive(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error
	QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error)
	QueueDeclarePassive(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error)
	QueueInspect(name string) (amqp.Queue, error)
	QueuePurge(name string, noWait bool) (int, error)
	QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error
	Confirm(noWait bool) error
	NotifyPublish(confirm chan amqp.Confirmation) chan amqp.Confirmation
	// NotifyReturn 注册 mandatory publish 未路由回退流；Confirm=true 发布路径用它区分
	// “broker 收到发布”和“消息进入 routing path”。
	NotifyReturn(receiver chan amqp.Return) chan amqp.Return
	PublishWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error
	Get(queue string, autoAck bool) (amqp.Delivery, bool, error)
	Qos(prefetchCount, prefetchSize int, global bool) error
	Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error)
	Cancel(consumer string, noWait bool) error
}

type rabbitMQAMQPConnectionAdapter struct {
	*amqp.Connection
}

type rabbitMQAMQPChannelAdapter struct {
	*amqp.Channel
}

type Dialer func(address string, cfg amqp.Config) (AMQPConnection, error)

var defaultRabbitMQDial Dialer = func(address string, cfg amqp.Config) (AMQPConnection, error) {
	conn, err := amqp.DialConfig(address, cfg)
	if err != nil {
		return nil, err
	}
	return &rabbitMQAMQPConnectionAdapter{Connection: conn}, nil
}

func DefaultDial(address string, cfg amqp.Config) (AMQPConnection, error) {
	return defaultRabbitMQDial(address, cfg)
}

func (o resolvedOptions) dialer() Dialer {
	if o.Dialer == nil {
		return defaultRabbitMQDial
	}
	return o.Dialer
}

func (a *rabbitMQAMQPConnectionAdapter) Channel() (AMQPChannel, error) {
	channel, err := a.Connection.Channel()
	if err != nil {
		return nil, err
	}
	return &rabbitMQAMQPChannelAdapter{Channel: channel}, nil
}

func (a *rabbitMQAMQPConnectionAdapter) NotifyClose(receiver chan *amqp.Error) chan *amqp.Error {
	return a.Connection.NotifyClose(receiver)
}

func (a *rabbitMQAMQPChannelAdapter) QueueDeclarePassive(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error) {
	return a.Channel.QueueDeclarePassive(name, durable, autoDelete, exclusive, noWait, args)
}
