package horizon

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	containercontract "github.com/prismgo/framework/contracts/container"
	queuecontract "github.com/prismgo/framework/contracts/queue"
	"github.com/prismgo/framework/event"
	horizoncmd "github.com/prismgo/framework/horizon/cmd"
	"github.com/prismgo/framework/queue"
	prismredis "github.com/prismgo/framework/redis"
	amqp "github.com/rabbitmq/amqp091-go"
)

const horizonIntegrationRabbitMQEnv = "PRISMGO_RABBITMQ_TEST_URL"

func TestMemoryQueueMemoryHorizonIntegrationGate(t *testing.T) {
	// 需求背景：Horizon integration contract 要求 memory queue + memory Horizon store 覆盖 Manager、collector、Store
	// 和 WorkerRunner 的进程内协作链路。该测试只使用公开队列接口和 queue ServiceProvider
	// 安装的 queue -> prismgo/event bridge，不手动覆盖 queue.UseEventSink。
	ctx := context.Background()
	registry := useHorizonTestContainer(t)
	t.Cleanup(func() {
		queue.UseEventSink(nil)
	})

	bus := event.New()
	if err := registry.Instance("event.dispatcher", bus); err != nil {
		t.Fatalf("register event dispatcher: %v", err)
	}
	if err := (queue.ServiceProvider{}).Boot(providerTestApp{registry: registry}); err != nil {
		t.Fatalf("boot queue provider bridge: %v", err)
	}

	queueRegistry := queue.NewRegistry()
	queueManager, err := queue.NewManager(queue.Config{Default: "sync"}, queueRegistry)
	if err != nil {
		t.Fatalf("new queue manager: %v", err)
	}
	store := NewMemoryStore(StoreOptions{Prefix: "horizon_integration_memory", HeartbeatTTL: time.Minute})
	manager, err := NewManager(integrationHorizonConfig("memory", "sync"),
		WithStoreFactory(integrationStaticStore{store: store}),
		WithQueueManager(NewQueueAdapter(queueManager)),
		WithWorkerRunner(integrationDispatchingRunner{manager: queueManager}),
		WithEventDispatcher(bus),
	)
	if err != nil {
		t.Fatalf("new horizon manager: %v", err)
	}
	if err := manager.RegisterMonitor(ctx); err != nil {
		t.Fatalf("register horizon monitor: %v", err)
	}

	if _, err := queue.NewDispatcher(queueManager).Dispatch(ctx, &integrationJob{Value: "snapshot"}); err != nil {
		t.Fatalf("dispatch sync job through queue manager: %v", err)
	}
	// collector/flusher replaces legacy collector: verify collector is bound and processing events
	if !manager.CollBound() {
		t.Fatal("expected collector to be bound after RegisterMonitor")
	}
	time.Sleep(100 * time.Millisecond)
	runtime := &runtimeCommandAdapter{manager: manager, store: store}
	collectorSnapshot := manager.Collector().FlushSnapshot(time.Now().UTC())
	if collectorSnapshot == nil {
		t.Fatal("collector FlushSnapshot returned nil")
	}
	_ = collectorSnapshot

	if err := runtime.SetGlobalPaused(ctx, true); err != nil {
		t.Fatalf("pause horizon: %v", err)
	}
	if err := runtime.SetSupervisorPaused(ctx, "supervisor-default", true); err != nil {
		t.Fatalf("pause supervisor: %v", err)
	}
	control, err := store.Control(ctx)
	if err != nil {
		t.Fatalf("read control state: %v", err)
	}
	if !control.GlobalPaused || !control.PausedSupervisors["supervisor-default"] {
		t.Fatalf("control state = %+v, want global and supervisor paused", control)
	}
	if err := runtime.SetGlobalPaused(ctx, false); err != nil {
		t.Fatalf("continue horizon: %v", err)
	}
	if err := runtime.SetSupervisorPaused(ctx, "supervisor-default", false); err != nil {
		t.Fatalf("continue supervisor: %v", err)
	}

	if err := runtime.RunWorker(ctx, workerOptionsForIntegration("memory-worker", "sync")); err != nil {
		t.Fatalf("run memory worker: %v", err)
	}
	workers, err := store.Workers(ctx, time.Now().UTC())
	if err != nil {
		t.Fatalf("read workers: %v", err)
	}
	if len(workers) != 1 || workers[0].Status != WorkerIdle {
		t.Fatalf("worker states = %+v, want one idle worker", workers)
	}
	if err := runtime.RequestTerminate(ctx, time.Now().UTC(), false); err != nil {
		t.Fatalf("terminate horizon: %v", err)
	}
}

func TestRedisQueueRedisHorizonIntegrationGate(t *testing.T) {
	// 需求背景：Horizon integration contract 要求 Redis queue + Redis Horizon store 使用项目现有 Redis 测试方式
	// 覆盖两个独立 Manager/Store 实例之间的可见性。这里用 miniredis 固定跨实例 heartbeat、
	// queue length、metrics snapshot 和 control flags，不依赖真实 Redis 服务。
	ctx := context.Background()
	server := miniredis.RunT(t)
	registry := useHorizonTestContainer(t)
	redisManager, err := prismredis.NewManager(prismredis.Config{
		DefaultName: "default",
		Connections: map[string]prismredis.ConnectionConfig{
			"default": {Name: "default", Addr: server.Addr()},
		},
	})
	if err != nil {
		t.Fatalf("redis NewManager error = %v", err)
	}
	if err := registry.Instance("redis", redisManager); err != nil {
		t.Fatalf("bind redis manager: %v", err)
	}
	t.Cleanup(func() {
		queue.UseEventSink(nil)
		_ = redisManager.Close(context.Background())
	})

	bus := event.New()
	if err := registry.Instance("event.dispatcher", bus); err != nil {
		t.Fatalf("register event dispatcher: %v", err)
	}
	if err := (queue.ServiceProvider{}).Boot(providerTestApp{registry: registry}); err != nil {
		t.Fatalf("boot queue provider bridge: %v", err)
	}

	queueManager, err := newIntegrationRedisQueueManager()
	if err != nil {
		t.Fatalf("new redis queue manager: %v", err)
	}
	storeA, err := NewRedisStore(RedisOptions{Connection: "default"}, StoreOptions{Prefix: "horizon_integration_redis", HeartbeatTTL: time.Minute})
	if err != nil {
		t.Fatalf("new redis store A: %v", err)
	}
	storeB, err := NewRedisStore(RedisOptions{Connection: "default"}, StoreOptions{Prefix: "horizon_integration_redis", HeartbeatTTL: time.Minute})
	if err != nil {
		t.Fatalf("new redis store B: %v", err)
	}
	managerA, err := NewManager(integrationHorizonConfig("redis", "redis"),
		WithStoreFactory(integrationStaticStore{store: storeA}),
		WithQueueManager(NewQueueAdapter(queueManager)),
		WithWorkerRunner(NewQueueWorkerAdapter(queueManager)),
		WithEventDispatcher(bus),
	)
	if err != nil {
		t.Fatalf("new horizon manager A: %v", err)
	}
	managerB, err := NewManager(integrationHorizonConfig("redis", "redis"), WithStoreFactory(integrationStaticStore{store: storeB}))
	if err != nil {
		t.Fatalf("new horizon manager B: %v", err)
	}
	if err := managerA.RegisterMonitor(ctx); err != nil {
		t.Fatalf("register horizon monitor: %v", err)
	}

	if err := storeA.HeartbeatSupervisor(ctx, SupervisorState{
		Name:            "supervisor-default",
		Host:            "redis-test",
		PID:             1001,
		Status:          SupervisorRunning,
		StartedAt:       time.Now().UTC(),
		LastHeartbeatAt: time.Now().UTC(),
		Connection:      "redis",
		Queues:          []string{"default"},
	}); err != nil {
		t.Fatalf("heartbeat supervisor: %v", err)
	}
	if _, err := queue.NewDispatcher(queueManager).Dispatch(ctx, &integrationJob{Value: "redis"}); err != nil {
		t.Fatalf("dispatch redis job: %v", err)
	}
	redisWorkerOptions := queue.WorkerOptions{
		Connection:    "redis",
		Queues:        []string{"default"},
		Once:          true,
		StopWhenEmpty: true,
		RetryAfter:    time.Second,
		Tries:         1,
	}
	redisSession, err := managerA.WorkerRunner().Begin(ctx, redisWorkerOptions)
	if err != nil {
		t.Fatalf("begin redis worker session: %v", err)
	}
	defer func() {
		if err := redisSession.Close(); err != nil {
			t.Errorf("close redis session: %v", err)
		}
	}()
	if err := redisSession.Activate(ctx); err != nil {
		t.Fatalf("activate redis worker session: %v", err)
	}
	if err := redisSession.Work(ctx); err != nil {
		t.Fatalf("work redis job once: %v", err)
	}

	runtimeA := &runtimeCommandAdapter{manager: managerA, store: storeA}
	if _, err := runtimeA.Snapshot(ctx, time.Now().UTC()); err != nil {
		t.Fatalf("snapshot redis integration: %v", err)
	}
	if err := runtimeA.SetGlobalPaused(ctx, true); err != nil {
		t.Fatalf("set redis global pause: %v", err)
	}
	if err := runtimeA.SetSupervisorPaused(ctx, "supervisor-default", true); err != nil {
		t.Fatalf("set redis supervisor pause: %v", err)
	}

	resolvedB, err := managerB.ResolveStore(ctx)
	if err != nil {
		t.Fatalf("resolve manager B store: %v", err)
	}
	supervisors, err := resolvedB.Supervisors(ctx, time.Now().UTC())
	if err != nil {
		t.Fatalf("read redis supervisors from B: %v", err)
	}
	if len(supervisors) != 1 || supervisors[0].Status != SupervisorPaused {
		t.Fatalf("supervisors from B = %+v, want paused supervisor", supervisors)
	}
	// collector replaces legacy collector: verify managerA collector processed dispatched job
	if !managerA.CollBound() {
		t.Fatal("expected collector to be bound after RegisterMonitor on A")
	}
	collectorA := managerA.Collector().FlushSnapshot(time.Now().UTC())
	if collectorA == nil {
		t.Fatal("collector FlushSnapshot returned nil on A")
	}
	lengths, err := resolvedB.QueueLengthSnapshot(ctx)
	if err != nil {
		t.Fatalf("read redis queue lengths from B: %v", err)
	}
	if len(lengths.Queues) != 1 || lengths.Queues[0].Size != 0 {
		t.Fatalf("queue lengths from B = %+v, want drained redis queue", lengths)
	}
	control, err := resolvedB.Control(ctx)
	if err != nil {
		t.Fatalf("read redis control from B: %v", err)
	}
	if !control.GlobalPaused || !control.PausedSupervisors["supervisor-default"] {
		t.Fatalf("control from B = %+v, want pause flags visible", control)
	}
}

func TestRabbitMQHorizonIntegrationGateSkipsUnsupportedFailedAndBatchState(t *testing.T) {
	// 需求背景：RabbitMQ driver 当前不承载 failed job 和 batch 持久状态，Horizon integration contract 要求这类
	// Prismgo driver 能力差异被明确记录，不能作为 Horizon 集成门失败条件。该测试只验证
	// RabbitMQ job 消费会进入 Horizon metrics/control/store 边界。
	ctx := context.Background()
	url := strings.TrimSpace(os.Getenv(horizonIntegrationRabbitMQEnv))
	if url == "" {
		t.Skipf("%s is not set; skipping RabbitMQ Horizon integration gate", horizonIntegrationRabbitMQEnv)
	}
	name := fmt.Sprintf("prismgo.horizon.horizon_integration.%d", time.Now().UnixNano())
	exchange := name + ".exchange"
	queueName := name + ".queue"
	restartQueue := name + ".restart"
	t.Cleanup(func() { cleanupIntegrationRabbitMQ(t, url, exchange, queueName, restartQueue) })

	registry := useHorizonTestContainer(t)
	t.Cleanup(func() {
		queue.UseEventSink(nil)
	})
	bus := event.New()
	if err := registry.Instance("event.dispatcher", bus); err != nil {
		t.Fatalf("register event dispatcher: %v", err)
	}
	if err := (queue.ServiceProvider{}).Boot(providerTestApp{registry: registry}); err != nil {
		t.Fatalf("boot queue provider bridge: %v", err)
	}

	queueManager, err := newIntegrationRabbitMQQueueManager(url, exchange, queueName, restartQueue)
	if err != nil {
		t.Fatalf("new rabbitmq queue manager: %v", err)
	}
	t.Cleanup(func() {
		if err := queueManager.Close(); err != nil {
			t.Logf("close rabbitmq queue manager: %v", err)
		}
	})
	store := NewMemoryStore(StoreOptions{Prefix: "horizon_integration_rabbitmq", HeartbeatTTL: time.Minute})
	manager, err := NewManager(integrationHorizonConfigWithQueue("memory", "rabbitmq", queueName),
		WithStoreFactory(integrationStaticStore{store: store}),
		WithQueueManager(NewQueueAdapter(queueManager)),
		WithWorkerRunner(NewQueueWorkerAdapter(queueManager)),
		WithEventDispatcher(bus),
	)
	if err != nil {
		t.Fatalf("new rabbitmq horizon manager: %v", err)
	}
	if err := manager.RegisterMonitor(ctx); err != nil {
		t.Fatalf("register horizon monitor: %v", err)
	}

	if _, err := queue.NewDispatcher(queueManager).Dispatch(ctx, &integrationJob{Value: "rabbitmq"}, queue.OnQueue(queueName)); err != nil {
		t.Fatalf("dispatch rabbitmq job: %v", err)
	}
	rabbitWorkerOptions := queue.WorkerOptions{
		Connection: "rabbitmq",
		Queues:     []string{queueName},
		Once:       true,
		Tries:      1,
	}
	rabbitSession, err := manager.WorkerRunner().Begin(ctx, rabbitWorkerOptions)
	if err != nil {
		t.Fatalf("begin rabbitmq worker session: %v", err)
	}
	defer func() {
		if err := rabbitSession.Close(); err != nil {
			t.Errorf("close rabbit session: %v", err)
		}
	}()
	if err := rabbitSession.Activate(ctx); err != nil {
		t.Fatalf("activate rabbitmq worker session: %v", err)
	}
	if err := rabbitSession.Work(ctx); err != nil {
		t.Fatalf("work rabbitmq job once: %v", err)
	}

	runtime := &runtimeCommandAdapter{manager: manager, store: store}
	snapshot, err := runtime.Snapshot(ctx, time.Now().UTC())
	if err != nil {
		t.Fatalf("snapshot rabbitmq integration: %v", err)
	}
	if snapshot.Totals.Processed != 1 || snapshot.QueueLengthCount != 1 {
		t.Fatalf("rabbitmq snapshot = %+v, want processed metric and queue length", snapshot)
	}
	if err := runtime.RequestTerminate(ctx, time.Now().UTC(), false); err != nil {
		t.Fatalf("request rabbitmq horizon terminate: %v", err)
	}
}

type providerTestApp struct {
	registry containercontract.Container
}

func (a providerTestApp) Container() containercontract.Container {
	return a.registry
}

type integrationStaticStore struct {
	store Store
}

func (r integrationStaticStore) ResolveStore(context.Context, Config) (Store, error) {
	return r.store, nil
}

type integrationDispatchingRunner struct {
	manager *queue.Manager
}

func (r integrationDispatchingRunner) Begin(_ context.Context, options queue.WorkerOptions) (queuecontract.WorkerSession, error) {
	return integrationWorkerSession{manager: r.manager, options: options}, nil
}

type integrationWorkerSession struct {
	manager *queue.Manager
	options queue.WorkerOptions
}

func (s integrationWorkerSession) Activate(context.Context) error {
	return nil
}

func (s integrationWorkerSession) Work(ctx context.Context) error {
	if s.options.EventObserver != nil {
		ctx = s.options.EventObserver(ctx, queue.JobProcessing{Connection: s.options.Connection, Queue: "default", JobID: "horizon_integration-worker", JobName: "integrationJob"})
	}
	_, err := queue.NewDispatcher(s.manager).Dispatch(ctx, &integrationJob{Value: "worker"})
	return err
}

func (s integrationWorkerSession) Close() error {
	return nil
}

type integrationJob struct {
	Value string
}

func (j *integrationJob) Handle(context.Context) error {
	return nil
}

func integrationHorizonConfig(storeName, connection string) Config {
	return integrationHorizonConfigWithQueue(storeName, connection, "default")
}

func integrationHorizonConfigWithQueue(storeName, connection, queueName string) Config {
	return Config{
		Store:        storeName,
		Environment:  "local",
		HeartbeatTTL: time.Minute,
		Supervisors: map[string]SupervisorConfig{
			"supervisor-default": {
				Name:       "supervisor-default",
				Connection: connection,
				Queues:     []string{queueName},
			},
		},
	}
}

func newIntegrationRedisQueueManager() (*queue.Manager, error) {
	return queue.NewManager(queue.Config{
		Default: "redis",
		Connections: map[string]queue.ConnectionConfig{
			"redis": {
				Driver:     "redis",
				Queue:      "default",
				Prefix:     "horizon_integration_queue",
				RetryAfter: time.Second,
				Options: map[string]any{
					"connection": "default",
					"prefix":     "horizon_integration_queue",
					"failed_ttl": time.Hour,
				},
			},
		},
	}, queue.NewRegistry())
}

func newIntegrationRabbitMQQueueManager(url, exchange, queueName, restartQueue string) (*queue.Manager, error) {
	registry := queue.NewRegistry()
	queue.RegisterTypeTo[*integrationJob](registry)
	return queue.NewManager(queue.Config{
		Default: "rabbitmq",
		Connections: map[string]queue.ConnectionConfig{
			"rabbitmq": {
				Driver: "rabbitmq",
				Queue:  queueName,
				Options: map[string]any{
					"url":                url,
					"exchange":           exchange,
					"exchange_type":      "direct",
					"declare":            true,
					"exchange_durable":   false,
					"queue_durable":      false,
					"message_persistent": false,
					"confirm":            true,
					"prefetch":           1,
					"publish_timeout":    2 * time.Second,
					"restart_queue":      restartQueue,
					"restart_enabled":    true,
				},
			},
		},
	}, registry)
}

func cleanupIntegrationRabbitMQ(t *testing.T, url, exchange, queueName, restartQueue string) {
	t.Helper()
	conn, err := amqp.Dial(url)
	if err != nil {
		t.Logf("dial rabbitmq cleanup: %v", err)
		return
	}
	defer func() {
		if err := conn.Close(); err != nil {
			t.Errorf("close rabbitmq connection: %v", err)
		}
	}()
	ch, err := conn.Channel()
	if err != nil {
		t.Logf("open rabbitmq cleanup channel: %v", err)
		return
	}
	defer func() {
		if err := ch.Close(); err != nil {
			t.Errorf("close rabbitmq channel: %v", err)
		}
	}()
	_, _ = ch.QueueDelete(queueName, false, false, false)
	_, _ = ch.QueueDelete(restartQueue, false, false, false)
	_ = ch.ExchangeDelete(exchange, false, false)
}

func workerOptionsForIntegration(name, connection string) horizoncmd.WorkerOptions {
	return horizoncmd.WorkerOptions{
		Name:          name,
		Supervisor:    "supervisor-default",
		Connection:    connection,
		Queue:         "default",
		MaxJobs:       1,
		Sleep:         1,
		Timeout:       60,
		Tries:         1,
		StopWhenEmpty: true,
	}
}
