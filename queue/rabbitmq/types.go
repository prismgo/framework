package rabbitmq

import (
	"container/list"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	encodingcontract "github.com/prismgo/framework/contracts/encoding"
	"github.com/prismgo/framework/internal/optional"
	queueerrors "github.com/prismgo/framework/queue/internal/errors"
	"github.com/prismgo/framework/queue/payload"
	"github.com/prismgo/framework/routine"
	amqp "github.com/rabbitmq/amqp091-go"
)

// 本文件集中定义 RabbitMQ driver 的配置、默认值和跨文件共享的内部状态结构。
// AMQP 状态、拓扑恢复状态和 RabbitMQ 专属运行细节都收敛在这个子包内。

const (
	defaultRabbitMQScheme          = "amqp"
	defaultRabbitMQHost            = "127.0.0.1"
	defaultRabbitMQPort            = "5672"
	defaultRabbitMQVHost           = "/"
	defaultRabbitMQExchange        = "prismgo.queue"
	defaultRabbitMQExchangeType    = "direct"
	defaultRabbitMQDelayMode       = "plugin"
	defaultRabbitMQHeartbeat       = 10 * time.Second
	defaultRabbitMQPublishTimeout  = 5 * time.Second
	defaultRabbitMQReconnectMin    = 100 * time.Millisecond
	defaultRabbitMQReconnectMax    = 5 * time.Second
	defaultRabbitMQRestartQueue    = "prismgo.queue.restart"
	defaultRabbitMQRestartEnabled  = true
	defaultRabbitMQRestartPoll     = time.Second
	defaultRabbitMQExchangeDurable = true
	defaultRabbitMQQueueDurable    = true
	defaultRabbitMQPersistent      = true
	defaultRabbitMQDeclare         = true
	defaultRabbitMQConfirm         = true
	defaultRabbitMQPrefetch        = 1
	// 发布 channel 池默认保持单通道，兼容既有串行发布语义，并避免低并发项目额外占用 broker channel。
	// 上限是防御性边界：配置错误不应在单个 AMQP connection 上无限创建发布 channel。
	defaultRabbitMQPublishChannels = 1
	maxRabbitMQPublishChannels     = 128

	rabbitMQContentTypeJSON    = "application/json"
	rabbitMQContentTypeMsgpack = "application/msgpack"
	rabbitMQHeaderQueue        = "x-prismgo-queue"
	rabbitMQHeaderJobID        = "x-prismgo-job-id"
	rabbitMQHeaderJobName      = "x-prismgo-job-name"
	rabbitMQHeaderDelay        = "x-delay"

	rabbitMQDelayModePlugin = "plugin"
	rabbitMQDelayModeTTLDLX = "ttl_dlx"
	rabbitMQDelayModeNone   = "none"
)

const (
	DefaultScheme          = defaultRabbitMQScheme
	DefaultHost            = defaultRabbitMQHost
	DefaultPort            = defaultRabbitMQPort
	DefaultVHost           = defaultRabbitMQVHost
	DefaultExchange        = defaultRabbitMQExchange
	DefaultExchangeType    = defaultRabbitMQExchangeType
	DefaultDelayMode       = defaultRabbitMQDelayMode
	DefaultHeartbeat       = defaultRabbitMQHeartbeat
	DefaultPublishTimeout  = defaultRabbitMQPublishTimeout
	DefaultPublishChannels = defaultRabbitMQPublishChannels
	DefaultReconnectMin    = defaultRabbitMQReconnectMin
	DefaultReconnectMax    = defaultRabbitMQReconnectMax
	DefaultRestartQueue    = defaultRabbitMQRestartQueue
	DefaultRestartPoll     = defaultRabbitMQRestartPoll
)

var (
	ErrEmpty                        = queueerrors.ErrEmpty
	ErrConnectionClosed             = errors.New("queue: connection closed")
	ErrUnsupportedOperation         = errors.New("queue: unsupported operation")
	ErrPoisonEnvelope               = queueerrors.ErrPoisonEnvelope
	ErrRabbitMQDialFailed           = errors.New("queue: rabbitmq dial failed")
	ErrRabbitMQTopologyMissing      = errors.New("queue: rabbitmq topology missing")
	ErrRabbitMQPublishNacked        = errors.New("queue: rabbitmq publish not acknowledged")
	ErrRabbitMQPublishTimeout       = errors.New("queue: rabbitmq publish confirmation timeout")
	ErrRabbitMQPublishConfirmClosed = errors.New("queue: rabbitmq publish confirmation channel closed")
	// ErrRabbitMQPublishUnrouted 表示 mandatory 发布被 broker 接收但没有路由到任何队列。
	//
	// 需求背景：publisher confirm ack 只说明 broker 接收了发布请求，不等价于消息进入业务 queue。
	// 调用方可通过 errors.Is 判断该 sentinel，并按 topology/binding 配置问题处理。
	ErrRabbitMQPublishUnrouted = errors.New("queue: rabbitmq publish unrouted")
	// ErrRabbitMQReleaseRepublishFailed 表示 release 已 ack 原 delivery，但替换发布或 confirm 失败。
	//
	// 该错误只用于 post-ack release 窗口，调用方可通过 errors.Is 同时匹配本 sentinel 和底层 publish/confirm cause。
	ErrRabbitMQReleaseRepublishFailed = errors.New("queue: rabbitmq release republish failed")
)

// defaultRabbitMQDelayBuckets 是 ttl_dlx 模式的默认固定延迟 bucket。
//
// RabbitMQ 的 TTL+DLX 模式只能按队列 TTL 近似延迟，因此这里保留一组有限 bucket，
// 避免为每条消息动态创建临时队列。
var defaultRabbitMQDelayBuckets = []time.Duration{
	5 * time.Second,
	10 * time.Second,
	30 * time.Second,
	time.Minute,
	5 * time.Minute,
	15 * time.Minute,
	time.Hour,
}

type PopOptions struct {
	BlockFor   time.Duration
	RetryAfter time.Duration
}

var rabbitMQDeliveryRegistry sync.Map

// rabbitMQDeliveryEntry 包装 delivery state 和存储时间，用于定期清理过期条目。
//
// 需求背景：全局 sync.Map 以 Envelope 指针为 key 存储 delivery 状态。
// 如果 forgetEnvelopeDelivery 因异常未被调用（如 panic 被 recover 后跳过），
// 条目会永久驻留，高吞吐场景下造成内存持续增长。
type rabbitMQDeliveryEntry struct {
	state    *rabbitMQDeliveryState
	storedAt time.Time
}

// deliveryRegistryTTL 是 delivery registry 条目的最大存活时间。
// 超过此时间的未完成条目会被定期清理，防止内存泄漏。
const deliveryRegistryTTL = 120 * time.Minute

func rememberEnvelopeDelivery(env *payload.Envelope, state *rabbitMQDeliveryState) {
	if env == nil || state == nil {
		return
	}
	rabbitMQDeliveryRegistry.Store(env, &rabbitMQDeliveryEntry{
		state:    state,
		storedAt: time.Now(),
	})
	// 首次存储时启动定期清理 goroutine，防止 registry 内存泄漏
	startDeliveryRegistryCleanup()
}

func envelopeDelivery(env *payload.Envelope) *rabbitMQDeliveryState {
	if env == nil {
		return nil
	}
	value, ok := rabbitMQDeliveryRegistry.Load(env)
	if !ok {
		return nil
	}
	if entry, ok := value.(*rabbitMQDeliveryEntry); ok {
		return entry.state
	}
	// 兼容旧格式（如果存在）
	if typed, ok := value.(*rabbitMQDeliveryState); ok {
		return typed
	}
	return nil
}

func forgetEnvelopeDelivery(env *payload.Envelope) {
	if env != nil {
		rabbitMQDeliveryRegistry.Delete(env)
	}
}

// cleanupDeliveryRegistry 扫描并清理已完成或异常的 delivery registry 条目。
//
// 设计思路：由定期 goroutine 调用（每 5 分钟），遍历 sync.Map 并删除已 ack 条目与脏数据。
// 未 ack 的活跃 delivery 仍要支持后续 Delete/Release 找回底层 delivery handle，因此不能按 TTL 强删。
func cleanupDeliveryRegistry() {
	rabbitMQDeliveryRegistry.Range(func(key, value any) bool {
		entry, ok := value.(*rabbitMQDeliveryEntry)
		if !ok {
			// 非预期类型，直接清理
			rabbitMQDeliveryRegistry.Delete(key)
			return true
		}
		// state 为 nil 的条目没有实际用途，直接清理
		if entry.state == nil {
			rabbitMQDeliveryRegistry.Delete(key)
			return true
		}
		// 已 ack 的条目可以安全清理；未 ack 条目必须保留到显式终结，不能仅因存活时间过长而删除。
		entry.state.mu.Lock()
		acked := entry.state.acked
		entry.state.mu.Unlock()
		if acked {
			rabbitMQDeliveryRegistry.Delete(key)
			return true
		}
		return true
	})
}

// deliveryRegistryCleanupOnce 保证清理 goroutine 全局只启动一次。
var deliveryRegistryCleanupOnce sync.Once

// deliveryRegistryCleanupCancel 用于停止清理 goroutine。
var deliveryRegistryCleanupCancel context.CancelFunc

// startDeliveryRegistryCleanup 启动定期清理 goroutine。
//
// 设计思路：使用 sync.Once 保证全局只有一个清理 goroutine 运行。
// 每 5 分钟扫描一次 delivery registry，清理已 ack 或超时的条目。
// goroutine 使用 routine.Task 创建，自带 recover 保护。
func startDeliveryRegistryCleanup() {
	deliveryRegistryCleanupOnce.Do(func() {
		ctx, cancel := context.WithCancel(context.Background())
		deliveryRegistryCleanupCancel = cancel

		routine.Task(ctx, func(ctx context.Context) error {
			ticker := time.NewTicker(5 * time.Minute)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					cleanupDeliveryRegistry()
				case <-ctx.Done():
					return nil
				}
			}
		}).
			Component("queue").
			Name("rabbitmq.delivery_registry_cleanup").
			Go()
	})
}

// stopDeliveryRegistryCleanup 停止清理 goroutine。
//
// 用于测试或热重载场景，允许从外部停止清理 goroutine。
func stopDeliveryRegistryCleanup() {
	if deliveryRegistryCleanupCancel != nil {
		deliveryRegistryCleanupCancel()
		deliveryRegistryCleanupCancel = nil
	}
	// 重置 once 以允许重新启动
	deliveryRegistryCleanupOnce = sync.Once{}
}

type Options struct {
	// URL 是完整 AMQP 连接地址，非空时优先于分字段配置。
	URL string
	// Dialer 创建底层 AMQP 连接；为空时使用 amqp091-go 默认 DialConfig。
	//
	// 需求背景：代理、TLS 包装、观测接入或故障注入环境需要替换建连方式。该依赖属于
	// 单条 RabbitMQ connection 的配置，不再通过包级全局变量切换，避免测试或多连接互相污染。
	Dialer Dialer
	// Scheme 是 AMQP 协议头，默认 amqp，也支持 amqps。
	Scheme string
	// Host 是 RabbitMQ broker 主机名或 IP。
	Host string
	// Port 是 RabbitMQ broker 端口。
	Port string
	// Username 是连接 RabbitMQ 时使用的用户名。
	Username string
	// Password 是连接 RabbitMQ 时使用的密码。
	Password string
	// VHost 是 RabbitMQ 虚拟主机，默认使用根 vhost。
	VHost string
	// Exchange 是默认发布使用的 exchange 名称。
	Exchange string
	// ExchangeType 是默认 exchange 类型，第一版默认 direct。
	ExchangeType string
	// Declare 控制 driver 是否自动声明 exchange、queue 与 binding。
	Declare OptionalBool
	// ExchangeDurable 控制 exchange 是否持久化。
	ExchangeDurable OptionalBool
	// QueueDurable 控制 queue 是否持久化。
	QueueDurable OptionalBool
	// QueueMaxPriority 控制队列声明时的 x-max-priority 参数，0 表示不启用优先级队列。
	QueueMaxPriority int
	// MessagePersistent 控制发布消息是否使用持久化投递模式。
	MessagePersistent OptionalBool
	// AutoDelete 控制队列或交换机在无人使用时是否自动删除。
	AutoDelete OptionalBool
	// Exclusive 控制队列是否仅允许当前连接独占使用。
	Exclusive OptionalBool
	// NoWait 控制声明资源时是否不等待 broker 确认。
	NoWait OptionalBool
	// Confirm 控制发布时是否要求 publisher confirm。
	Confirm OptionalBool
	// DelayMode 控制延迟消息实现模式，默认 plugin。
	DelayMode string
	// DelayBuckets 控制 ttl_dlx 模式下固定延迟队列的 bucket 时长。
	DelayBuckets []time.Duration
	// Prefetch 控制 consumer 预取数量，默认 1。
	Prefetch int
	// Heartbeat 控制 AMQP 连接心跳间隔。
	Heartbeat time.Duration
	// PublishTimeout 控制发布等待重连、拓扑恢复、confirm 或 return 边界的超时时间。
	// Confirm=false 时发布成功立即返回，不使用该超时等待 broker 确认。
	PublishTimeout time.Duration
	// PublishChannels 控制发布专用 AMQP channel 池大小。
	// 需求背景：publisher confirm 与 AMQP channel 绑定，单 channel 并发发布会在等待 ack/nack 时互相阻塞。
	// 设计思路：配置大于 1 时按 round-robin 分摊发布请求；每个 slot 内仍串行等待 confirm，保证确认结果不会串读。
	// 边界规则：小于等于 0 回退默认值 1，大于 128 截断为 128，避免错误配置耗尽 broker 资源。
	PublishChannels int
	// Codec 是 RabbitMQ envelope 使用的 queue Payload Encoding。
	//
	// 需求背景：Manager 注入解析后的 codec；直接构造子包连接时空值按 msgpack。
	Codec encodingcontract.Codec
	// ReconnectMinDelay 是运行期断线后的最小重连退避；初始化建连失败仍然立即返回错误。
	ReconnectMinDelay time.Duration
	// ReconnectMaxDelay 是运行期断线后的最大重连退避，避免 broker 不可用时高频自旋。
	ReconnectMaxDelay time.Duration
	// RestartQueue 是后续 queue:restart 信号使用的队列名。
	RestartQueue string
	// RestartEnabled 控制后续是否启用 queue:restart 能力。
	RestartEnabled OptionalBool
	// RestartPollInterval 控制单条 RabbitMQ connection 读取 restart queue 的最小 broker 访问间隔。
	//
	// 需求背景：
	// worker 每轮都会检查 queue:restart；如果每次都对 RabbitMQ 执行 basic.get + requeue，
	// 空闲 worker 或多 worker 场景会把 restart queue 放大成固定 broker 热点。
	//
	// 设计思路：
	// 该值只控制本地读取降载窗口，不是 RabbitMQ message TTL，也不改变 restart queue 的持久化
	// latest-value 语义。窗口内复用当前 connection 已读取到的零时间或有效 timestamp；窗口过期后
	// 再访问 broker，因此其他进程发布的新 restart 信号最多延迟该间隔被当前 connection 看见。
	//
	// 边界规则：
	// 小于等于 0 会回退到默认值 1s，避免误配置重新制造高频轮询压力。
	RestartPollInterval time.Duration
	// TopologyCacheTTL 控制当前 AMQP connection 内 topology usage cache 的 sliding TTL。
	//
	// 默认 0 表示不按时间淘汰，保持历史声明/验证缓存行为；只有显式配置正数时才启用。
	// 淘汰只删除本地缓存，不会删除 RabbitMQ broker 上的 exchange、queue、binding 或消息。
	TopologyCacheTTL time.Duration
	// TopologyCacheMaxEntries 控制当前 AMQP connection 内 topology usage cache 的合并容量上限。
	//
	// 默认 0 表示不按容量淘汰；正数按 declared/verified/delay/restart 等 topology entry 的总数
	// 做 best-effort LRU 淘汰。live Consumer Intent 依赖的 entry 会被保护，容量过小不会让操作失败。
	TopologyCacheMaxEntries int
}

// OptionalBool 是 RabbitMQ 公开配置使用的三态布尔值。
//
// 设计说明：底层类型放在 prismgo/internal/optional，便于后续其他 prismgo 子包复用；
// rabbitmq 包通过别名和 Bool 构造器提供面向调用方的稳定入口。
type OptionalBool = optional.Bool

// Bool 创建一个已设置的 RabbitMQ 三态布尔值。
func Bool(value bool) OptionalBool {
	return optional.NewBool(value)
}

type resolvedOptions struct {
	URL                     string
	Dialer                  Dialer
	Scheme                  string
	Host                    string
	Port                    string
	Username                string
	Password                string
	VHost                   string
	Exchange                string
	ExchangeType            string
	Declare                 bool
	ExchangeDurable         bool
	QueueDurable            bool
	QueueMaxPriority        int
	MessagePersistent       bool
	AutoDelete              bool
	Exclusive               bool
	NoWait                  bool
	Confirm                 bool
	DelayMode               string
	DelayBuckets            []time.Duration
	Prefetch                int
	Heartbeat               time.Duration
	PublishTimeout          time.Duration
	PublishChannels         int
	Codec                   encodingcontract.Codec
	ReconnectMinDelay       time.Duration
	ReconnectMaxDelay       time.Duration
	RestartQueue            string
	RestartEnabled          bool
	RestartPollInterval     time.Duration
	TopologyCacheTTL        time.Duration
	TopologyCacheMaxEntries int
}

type rabbitMQTTLDLXDelayTopology struct {
	Queue  string
	Bucket time.Duration
}

// rabbitMQTopologyVerificationKind 标识 declare=false 被动验证缓存中的资源类型。
//
// 需求背景：
// declare=false 不允许 driver 创建 RabbitMQ 资源，只能确认运维或 IaC 已经提前创建好资源。
// 不同资源的验证语义不同：默认 exchange、业务 queue、plugin delayed exchange 和 TTL delay queue
// 不能折叠成同一个布尔值，否则后续维护者无法判断“验证过 exchange”和“验证过某个 queue”的区别。
type rabbitMQTopologyVerificationKind string

const (
	rabbitMQVerifiedExchange      rabbitMQTopologyVerificationKind = "exchange"
	rabbitMQVerifiedQueue         rabbitMQTopologyVerificationKind = "queue"
	rabbitMQVerifiedDelayExchange rabbitMQTopologyVerificationKind = "delay_exchange"
	rabbitMQVerifiedDelayQueue    rabbitMQTopologyVerificationKind = "delay_queue"
)

// rabbitMQTopologyVerificationKey 是 declare=false 下被动存在性验证缓存的键。
//
// 设计思路：
// 1. 缓存只绑定当前 AMQP connection 生命周期，断线重连或 Close 后会被清空。
// 2. exchange 类资源把声明参数纳入 key，避免配置变化后复用旧验证结果。
// 3. queue 类资源至少按 queue name 区分；TTL delay queue 额外保留业务 queue 和 bucket，便于排查。
// 4. 该 key 不表达 binding、routing 或 publisher confirm 安全性；这些属于更高层语义，不能混入存在性缓存。
type rabbitMQTopologyVerificationKey struct {
	Kind         rabbitMQTopologyVerificationKind
	Name         string
	Exchange     string
	ExchangeType string
	Durable      bool
	AutoDelete   bool
	NoWait       bool
	DelayedType  string
	Queue        string
	Bucket       time.Duration
}

type rabbitMQTopologyCacheKind string

const (
	rabbitMQTopologyCacheDeclaredQueue rabbitMQTopologyCacheKind = "declared_queue"
	rabbitMQTopologyCachePluginDelay   rabbitMQTopologyCacheKind = "plugin_delay"
	rabbitMQTopologyCacheTTLDLXDelay   rabbitMQTopologyCacheKind = "ttl_dlx_delay"
	rabbitMQTopologyCacheVerified      rabbitMQTopologyCacheKind = "verified"
	rabbitMQTopologyCacheRestartQueue  rabbitMQTopologyCacheKind = "restart_queue"
)

// rabbitMQTopologyCacheKey 是当前 AMQP connection 内 topology usage cache 的统一淘汰键。
//
// declaredQueues、delayedQueues、ttlDelayQueues 和 verifiedTopology 仍保留各自的语义 map；
// 该 key 只提供 TTL/LRU 所需的统一计数、last used 和删除入口。
type rabbitMQTopologyCacheKey struct {
	Kind         rabbitMQTopologyCacheKind
	Name         string
	Queue        string
	Verification rabbitMQTopologyVerificationKey
}

// rabbitMQTopologyUsageEntry 记录拓扑缓存条目的 LRU 元数据。
//
// 设计原因：element 指向 container/list 中的节点，实现 O(1) 淘汰；
// key 记录对应的缓存键，使淘汰时能同步清理 map，避免 O(n) 扫描。
type rabbitMQTopologyUsageEntry struct {
	LastUsed time.Time
	element  *list.Element
	key      rabbitMQTopologyCacheKey
}

// rabbitMQPublishSlot 表示一个发布专用 AMQP channel 及其 publisher confirm 流。
//
// 设计原因：
// RabbitMQ 的 confirm 结果按 channel 返回，多个 goroutine 共享同一 channel 时必须在 slot 内串行发布并等待确认；
// 否则一个发布请求可能读到另一个请求的 ack/nack。将 channel 拆成多个 slot 后，可以提升发布并发度，
// 同时把串行边界限制在单个 slot 内。
type rabbitMQPublishSlot struct {
	// mu 保护该 slot 的发布与 confirm 等待全过程，调用方必须持锁后才能使用或重置 channel。
	mu sync.Mutex
	// channel 是绑定到该 slot 的发布专用 AMQP channel，失败后只清空当前 slot。
	channel AMQPChannel
	// confirms 是 channel.NotifyPublish 返回的确认流，与 channel 生命周期一致。
	confirms <-chan amqp.Confirmation
	// returns 是 channel.NotifyReturn 返回的未路由发布流。
	// 设计思路：Confirm=true 会读取该流；amqp.Return 没有 delivery tag，只能依赖 slot 内单次 in-flight 发布归属。
	returns <-chan amqp.Return
}

// restartSignalCache 保存当前 RabbitMQ connection 最近一次 restart signal 读取结果。
//
// 逻辑说明：
// at 可以是零时间，也可以是 broker 中的有效 restart timestamp；两者都需要缓存，否则空 restart
// queue 会继续在 worker 空轮询时每轮访问 broker。expiresAt 表示该本地读取结果的失效时间，过期后
// 下一次 RestartRequestedAt 会重新访问 RabbitMQ。
//
// 设计边界：
// 该缓存只绑定当前 Connection 对象和当前 AMQP connection 生命周期。Close、运行期断线或安装新的
// AMQP connection 时必须清空它，避免用旧连接窗口内的零时间或 timestamp 掩盖新 broker 会话事实。
type restartSignalCache struct {
	mu        sync.Mutex
	at        time.Time
	expiresAt time.Time
}

// rabbitMQDeliveryState 保存一次 Pop 取回的 delivery 句柄。
//
// 需求背景：
// 当前 queue worker 的成功路径是先 Process，再由 Delete 负责最终确认消息。
// RabbitMQ driver 需要保持这个顺序，不能在 Pop 阶段提前 ack。
//
// 设计思路：
// 1. Pop 时把 delivery 句柄挂到 Envelope 的内部状态上。
// 2. Delete 时再通过这个状态执行 ack。
// 3. 用 acked 标记保证重复 Delete 是安全 no-op，但真实 ack 失败仍然向上返回。
type rabbitMQDeliveryState struct {
	mu       sync.Mutex
	delivery amqp.Delivery
	acked    bool
}

// Ack 执行 RabbitMQ delivery 的最终确认。
//
// 这里故意把幂等控制放在状态对象内部，而不是放在 Delete 外层，
// 这样可以集中处理“重复调用直接返回 nil，但 broker ack 失败必须暴露”的规则。
func (s *rabbitMQDeliveryState) Ack() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.acked {
		return nil
	}
	if err := s.delivery.Ack(false); err != nil {
		return err
	}
	s.acked = true
	return nil
}

// Connection 实现内置 RabbitMQ driver 的基础成功链路。
//
// 当前切片只解决以下公共语义：
// 1. Push 把现有 Envelope 按 queue Payload Encoding 发布到 RabbitMQ。
// 2. Pop 通过长生命周期 push consumer 取回 delivery 并恢复 Envelope。
// 3. Delete 在成功路径执行 ack，顺序保持与现有 worker 一致。
//
// 本切片故意不处理 delay/release/reconnect 等后续议题，避免在“主链路打通”之前引入额外复杂度。
type Connection struct {
	name           string
	options        resolvedOptions
	codec          encodingcontract.Codec
	address        string
	amqpConnection AMQPConnection

	mu     sync.RWMutex
	closed bool
	// publishSlots 保存发布专用 channel 池；slot 懒加载创建，重连或主动关闭时整体清空。
	publishSlots []*rabbitMQPublishSlot
	// publishNext 是发布 slot 的 round-robin 计数器，使用 atomic 避免在选择阶段持有连接写锁。
	publishNext atomic.Uint64
	// topologyChannel 专门承载 exchange/queue/binding 声明、检查、清理等管理操作。
	// 需求背景：RabbitMQ 的 AMQP channel 不应在 publish、consume、declare 之间混用；
	// 单独拆出该 channel 可以避免并发 Dispatch 与 worker 消费时互相影响。
	topologyChannel AMQPChannel
	consumerChannel AMQPChannel
	declaredQueues  map[string]struct{}
	// delayedQueues 记录已经绑定到 delayed exchange 的业务队列，避免重复声明插件拓扑。
	delayedQueues map[string]struct{}
	// ttlDelayQueues 记录已经声明的固定 bucket delay queue，避免重复声明 TTL+DLX 拓扑。
	ttlDelayQueues map[string]struct{}
	// restartQueues 记录当前 AMQP connection 已经声明或验证过的 queue:restart 基础设施队列。
	restartQueues map[string]struct{}
	consumers     map[string]<-chan amqp.Delivery
	consumerTags  map[string]string
	// verifiedTopology 记录 declare=false 时当前 AMQP connection 已被动验证存在的 topology。
	// 它和 declared* 分离：前者不创建资源，只表示本连接已向 broker 验证过存在性。
	verifiedTopology map[rabbitMQTopologyVerificationKey]struct{}

	// known* 保存跨 AMQP connection 生命周期的拓扑意图；declared* 只表示当前连接已经声明。
	// 重连后必须清空 declared* 再用 known* 恢复，否则新 channel 会误用旧连接上的声明缓存。
	knownQueues     map[string]struct{}
	knownDelayed    map[string]struct{}
	knownTTLDelay   map[string]rabbitMQTTLDLXDelayTopology
	activeConsumers map[string]struct{}
	consumerRefs    map[string]int
	topologyUsage   map[rabbitMQTopologyCacheKey]rabbitMQTopologyUsageEntry
	// topologyLRU 维护 topologyUsage 的访问顺序链表，配合 map 实现 O(1) LRU 淘汰。
	// 链表前端是最近访问的条目，后端是最久未访问的条目（淘汰候选）。
	topologyLRU     *list.List
	topologyNow     func() time.Time
	consumerTagNext atomic.Uint64
	restartCache    restartSignalCache
	// readyCh 在重连完成时关闭，用来唤醒等待 Push/Pop 的 goroutine；ready 标记避免重复 close。
	readyCh chan struct{}
	// closedCh 在主动 Close 时关闭，让后台重连循环立即退出，不必等下一次退避计时结束。
	closedCh         chan struct{}
	ready            bool
	reconnecting     bool
	reconnectLooping bool

	topologyMu     sync.Mutex
	consumerInitMu sync.Mutex
}
