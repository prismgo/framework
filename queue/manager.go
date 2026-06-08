package queue

import (
	"context"
	"fmt"
	"sync"
	"time"

	encodingcontract "github.com/prismgo/framework/contracts/encoding"
	queuecontract "github.com/prismgo/framework/contracts/queue"
	encodingpkg "github.com/prismgo/framework/encoding"
)

// Manager 管理 Laravel 风格 queue connection 解析、连接缓存和运行时状态。
type Manager struct {
	defaultConnection string
	connectionSpecs   map[string]ConnectionConfig
	queuesMu          sync.RWMutex
	closed            bool
	queues            map[string]queuecontract.Queue
	queueBuilds       map[string]*queueBuildCall
	runtime           *Runtime
}

// queueBuildCall 表示某个 queue connection 当前正在进行的一次构建调用。
//
// 需求背景：旧实现会在持有 queuesMu 写锁时直接执行 connector.Connect(...)，
// 一旦 Redis、RabbitMQ 或自定义 connector 的初始化较慢，就会把外部 I/O 耗时放进
// 共享锁范围，放大全局锁竞争；如果简单改成锁外构建，又会让同名 connection 在并发
// 首次访问时重复执行 Connect(...)，造成重复初始化和额外副作用。
//
// 设计思路：Manager.Queue 在锁内先为同名 connection 登记一个 in-flight 占位，随后
// 在锁外执行真正的 Connect(...)。并发到来的相同 connection 解析请求不再重复构建，
// 而是等待 done 关闭后复用同一结果。这样既缩小了锁范围，也保证同名连接构建期间只会
// 有一个真正的 connector 调用在运行。
//
// 字段说明：
// - done：构建完成通知；关闭后表示 queue/err 已经稳定可读。
// - queue：成功构建出的 transport connection。
// - err：本次构建失败时返回给所有等待者的错误。
type queueBuildCall struct {
	done  chan struct{}
	queue queuecontract.Queue
	err   error
}

// NewManager 创建队列管理器。
func NewManager(cfg Config, registry *Registry) (*Manager, error) {
	if registry == nil {
		registry = NewRegistry()
	}
	codec, err := encodingpkg.ResolveWithDefault(encodingpkg.NameMsgpack, cfg.Encoding)
	if err != nil {
		return nil, fmt.Errorf("queue.encoding: %w", err)
	}
	cfg.Encoding = codec.Name()
	connectionSpecs, defaultQueueConn, failed, batch, err := buildConnections(cfg, codec)
	if err != nil {
		return nil, err
	}
	def := cfg.Default
	if def == "" {
		def = "sync"
	}
	queueName := ""
	if spec, ok := connectionSpecs[def]; ok && spec.Queue != "" {
		queueName = spec.Queue
	}
	if queueName == "" {
		queueName = "default"
	}
	manager := &Manager{
		defaultConnection: def,
		connectionSpecs:   connectionSpecs,
		queues:            make(map[string]queuecontract.Queue),
		runtime: &Runtime{
			defaultConnection: def,
			defaultQueue:      queueName,
			failed:            failed,
			batch:             batch,
			restart:           restartStoreFromConfig(cfg.Restart),
			registry:          registry,
			codec:             codec,
			payloadEncrypter:  cfg.PayloadEncrypter,
		},
	}
	if defaultQueueConn != nil {
		manager.queues[def] = defaultQueueConn
	}
	if defaultQueueConn == nil {
		if _, err := manager.Queue(def); err != nil {
			return nil, err
		}
	}
	return manager, nil
}

// Registry 返回任务注册表。
func (m *Manager) Registry() *Registry { return m.runtime.registry }

// UseMiddleware 注册全局任务 middleware。
func (m *Manager) UseMiddleware(middleware ...Middleware) {
	if m == nil {
		return
	}
	m.runtime.UseMiddleware(middleware...)
}

// Dispatch 投递任务。
func (m *Manager) Dispatch(ctx context.Context, job Job, options ...DispatchOption) (string, error) {
	return NewDispatcher(m).Dispatch(ctx, job, options...)
}

// Queue 返回 Laravel 风格 transport connection。
func (m *Manager) Queue(name string) (queuecontract.Queue, error) {
	if m == nil {
		return nil, fmt.Errorf("queue manager is nil")
	}
	if name == "" {
		name = m.defaultConnection
	}
	m.queuesMu.RLock()
	if m.closed {
		m.queuesMu.RUnlock()
		return nil, ErrManagerClosed
	}
	var queueConn queuecontract.Queue
	var build *queueBuildCall
	if m.queues != nil {
		queueConn = m.queues[name]
	}
	if m.queueBuilds != nil {
		build = m.queueBuilds[name]
	}
	spec, configured := m.connectionSpecs[name]
	m.queuesMu.RUnlock()
	if queueConn != nil {
		return queueConn, nil
	}
	if build != nil {
		<-build.done
		return build.queue, build.err
	}
	if !configured {
		return nil, fmt.Errorf("queue: connection %q is not configured", name)
	}
	driver := normalizeDriverName(spec.Driver)
	if driver == "" {
		driver = "sync"
	}
	connector := m.connector(driver)
	if connector == nil {
		return nil, fmt.Errorf("unknown driver %q", driver)
	}
	m.queuesMu.Lock()
	if m.closed {
		m.queuesMu.Unlock()
		return nil, ErrManagerClosed
	}
	if m.queues == nil {
		m.queues = make(map[string]queuecontract.Queue)
	} else if queueConn = m.queues[name]; queueConn != nil {
		m.queuesMu.Unlock()
		return queueConn, nil
	}
	if m.queueBuilds == nil {
		m.queueBuilds = make(map[string]*queueBuildCall)
	} else if build = m.queueBuilds[name]; build != nil {
		m.queuesMu.Unlock()
		<-build.done
		return build.queue, build.err
	}
	build = &queueBuildCall{done: make(chan struct{})}
	m.queueBuilds[name] = build
	m.queuesMu.Unlock()

	built, err := connector.Connect(context.Background(), name, connectorConfig(spec))
	if err != nil {
		err = fmt.Errorf("queue: build connection %s: %w", name, err)
	}

	m.queuesMu.Lock()
	if m.closed {
		if err == nil && built != nil {
			_ = built.Close()
		}
		build.queue = nil
		build.err = ErrManagerClosed
		delete(m.queueBuilds, name)
		close(build.done)
		m.queuesMu.Unlock()
		return nil, ErrManagerClosed
	}
	if err == nil {
		m.queues[name] = built
	}
	build.queue = built
	build.err = err
	delete(m.queueBuilds, name)
	close(build.done)
	m.queuesMu.Unlock()
	return build.queue, build.err
}

func (m *Manager) connector(name string) queuecontract.Connector {
	if connector := builtinConnector(name, m.runtimeCodec()); connector != nil {
		return connector
	}
	connector, ok := lookupConnector(name)
	if !ok {
		return nil
	}
	return connector
}

// runtimeCodec 返回当前 manager runtime 使用的 payload codec。
//
// 设计思路：内置 sync/redis/rabbitmq connector 仍需要 manager 创建时解析出的 codec，但
// Manager 不再持有 connector registry 字段，因此通过 runtime 读取 codec 并即时构造内置
// connector，避免把内置 driver 放入包级自定义 registry。
func (m *Manager) runtimeCodec() encodingcontract.Codec {
	if m == nil || m.runtime == nil {
		return nil
	}
	return m.runtime.codec
}

// builtinConnector 返回框架内置 driver connector。
//
// 参数 name 是 connection driver 名称，codec 是 manager 配置解析后的编码器。这里只处理
// sync、redis、rabbitmq 三个内置 driver；其他名称交给包级自定义 registry 查找。
func builtinConnector(name string, codec encodingcontract.Codec) queuecontract.Connector {
	switch normalizeDriverName(name) {
	case "sync":
		return SyncConnector{codec: codec}
	case "redis":
		return RedisConnector{codec: codec}
	case "rabbitmq":
		return RabbitMQConnector{codec: codec}
	default:
		return nil
	}
}

// Failed 返回失败任务存储。
func (m *Manager) Failed() FailedStore { return m.runtime.failed }

// RequestRestart 写入 worker 重启信号；长驻 worker 会在当前任务结束后退出。
func (m *Manager) RequestRestart(ctx context.Context) error {
	if m == nil {
		return fmt.Errorf("queue manager is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	at := time.Now()
	return m.restartStore().RequestRestart(ctx, at)
}

func (m *Manager) restartRequested(ctx context.Context, workerStartedAt time.Time) bool {
	if m == nil {
		return false
	}
	if ctx == nil {
		ctx = context.Background()
	}
	at, err := m.restartStore().RestartRequestedAt(ctx)
	return err == nil && !at.IsZero() && !at.Before(workerStartedAt)
}

// restartStore 返回 restart state store，通过 sync.Once 保证并发安全的懒初始化。
//
// 修复原因：原实现在无锁状态下读写 m.runtime.restart，worker goroutine 和测试 goroutine
// 并发调用时会触发 data race。sync.Once 确保只初始化一次且对后续调用者立即可见。
func (m *Manager) restartStore() queuecontract.RestartStore {
	m.runtime.restartOnce.Do(func() {
		if m.runtime.restart == nil {
			m.runtime.restart = NewMemoryRestartStore()
		}
	})
	return m.runtime.restart
}

// Close 关闭所有连接。
func (m *Manager) Close() error {
	var first error
	m.queuesMu.Lock()
	if m.closed {
		m.queuesMu.Unlock()
		return nil
	}
	m.closed = true
	queues := make([]queuecontract.Queue, 0, len(m.queues))
	for _, queueConn := range m.queues {
		queues = append(queues, queueConn)
	}
	m.queues = make(map[string]queuecontract.Queue)
	m.queueBuilds = nil
	m.queuesMu.Unlock()
	for _, queueConn := range queues {
		if err := queueConn.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

func (m *Manager) runtimeOrDefault() *Runtime {
	if m == nil {
		return nil
	}
	return m.runtime
}
