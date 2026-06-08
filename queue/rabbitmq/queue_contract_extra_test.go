package rabbitmq

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	queuecontract "github.com/prismgo/framework/contracts/queue"
	"github.com/prismgo/framework/encoding"
	queueevents "github.com/prismgo/framework/queue/internal/events"
	"github.com/prismgo/framework/queue/payload"
	amqp "github.com/rabbitmq/amqp091-go"
)

func TestRabbitMQPublishResponsibilityLivesInPublishFile(t *testing.T) {
	// 需求背景：RabbitMQ driver 按职责拆分后，publish.go 必须承载 AMQP publish pipeline，
	// connection.go 只保留连接生命周期、消费和队列管理方法，避免目录结构回退成注释壳。
	publishSource, err := os.ReadFile("publish.go")
	if err != nil {
		t.Fatalf("read publish.go: %v", err)
	}
	connectionSource, err := os.ReadFile("connection.go")
	if err != nil {
		t.Fatalf("read connection.go: %v", err)
	}
	publishText := string(publishSource)
	for _, needle := range []string{
		"func (c *Connection) Push(",
		"func (c *Connection) push(",
		"func (c *Connection) publishWithConfirm(",
		"func (c *Connection) publishWaitContext(",
		"func (c *Connection) pollPublishReturn(",
		"func rabbitMQPublishUnroutedError(",
		"func rabbitMQContentType(",
	} {
		if !strings.Contains(publishText, needle) {
			t.Fatalf("publish.go missing %q", needle)
		}
		if strings.Contains(string(connectionSource), needle) {
			t.Fatalf("connection.go still contains publish responsibility %q", needle)
		}
	}
}

func TestRabbitMQQueueContractAdapterCoversTransportMethods(t *testing.T) {
	channel := &rabbitMQTopologyTestChannel{ready: make(map[string][]amqp.Delivery), messages: map[string]int{"jobs": 0}}
	conn := newRabbitMQTopologyTestConnection(channel, Options{Declare: Bool(true), Confirm: Bool(true), RestartEnabled: Bool(true)})
	q := &RabbitMQQueue{inner: conn, codec: encoding.JSON(), blockFor: time.Millisecond}
	body := mustRabbitMQRuntimePayload(t, q, &payload.Envelope{
		ID:          "contract-1",
		Name:        "ContractJob",
		Queue:       "jobs",
		Payload:     payload.Payload(`{"ok":true}`),
		CreatedAt:   1,
		AvailableAt: 1,
	})

	if err := q.Push(context.Background(), "jobs", body); err != nil {
		t.Fatalf("push: %v", err)
	}
	if err := q.Later(context.Background(), "jobs", body, time.Millisecond); err != nil {
		t.Fatalf("later: %v", err)
	}
	if result, err := q.Bulk(context.Background(), "jobs", []queuecontract.Payload{body}); err != nil || result.Accepted != 1 {
		t.Fatalf("bulk: %v", err)
	}
	size, err := q.Size(context.Background(), "jobs")
	if err != nil {
		t.Fatalf("size: %v", err)
	}
	if size == 0 {
		t.Fatal("size = 0, want queued messages")
	}
	reserved, err := q.Pop(context.Background(), []string{"jobs"})
	if err != nil {
		t.Fatalf("pop: %v", err)
	}
	if reserved.ID() != "contract-1" || reserved.Attempts() != 1 || len(reserved.Payload()) == 0 {
		t.Fatalf("reserved = id %q attempts %d payload %d", reserved.ID(), reserved.Attempts(), len(reserved.Payload()))
	}
	if err := reserved.Delete(context.Background()); err != nil {
		t.Fatalf("delete: %v", err)
	}
	releaseJob := &RabbitMQReservedJob{queue: q, env: &payload.Envelope{ID: "release-1", Name: "Job", Queue: "jobs", CreatedAt: 1, AvailableAt: 1}, body: body}
	if releaseJob.ID() != "release-1" || releaseJob.Name() != "Job" {
		t.Fatalf("release job metadata = id %q name %q", releaseJob.ID(), releaseJob.Name())
	}
	if err := releaseJob.Release(context.Background(), 0); err != nil {
		t.Fatalf("release without delivery: %v", err)
	}
	release, err := q.AcquireConsumerIntent([]string{"jobs"})
	if err != nil {
		t.Fatalf("consumer intent: %v", err)
	}
	if err := release(); err != nil {
		t.Fatalf("consumer intent release: %v", err)
	}
	if err := q.Clear(context.Background(), "jobs"); err != nil {
		t.Fatalf("clear: %v", err)
	}
	if err := q.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func TestRabbitMQQueueSecondaryQueueHitSuppressesNextPrimaryBlock(t *testing.T) {
	channel := &rabbitMQTopologyTestChannel{ready: make(map[string][]amqp.Delivery)}
	conn := newRabbitMQTopologyTestConnection(channel, Options{Declare: Bool(true), Confirm: Bool(true)})
	q := &RabbitMQQueue{inner: conn, codec: encoding.JSON(), blockFor: 50 * time.Millisecond}
	for _, id := range []string{"secondary-1", "secondary-2"} {
		body := mustRabbitMQRuntimePayload(t, q, &payload.Envelope{
			ID:          id,
			Name:        "ExampleJob",
			Queue:       "low",
			Payload:     payload.Payload(`{"ok":true}`),
			CreatedAt:   1,
			AvailableAt: 1,
		})
		if err := q.Push(context.Background(), "low", body); err != nil {
			t.Fatalf("push low %s: %v", id, err)
		}
	}

	first, err := q.Pop(context.Background(), []string{"high", "low"}, queuecontract.PopNoWait)
	if err != nil {
		t.Fatalf("secondary first pop: %v", err)
	}
	if first.ID() != "secondary-1" {
		t.Fatalf("first secondary id = %q", first.ID())
	}
	start := time.Now()
	if _, err := q.Pop(context.Background(), []string{"high"}, queuecontract.PopNoWait); !errors.Is(err, ErrEmpty) {
		t.Fatalf("primary second pop err = %v, want empty", err)
	}
	if elapsed := time.Since(start); elapsed >= 25*time.Millisecond {
		t.Fatalf("primary second pop took %s, want no block_for wait", elapsed)
	}
	second, err := q.Pop(context.Background(), []string{"high", "low"}, queuecontract.PopNoWait)
	if err != nil {
		t.Fatalf("secondary second pop: %v", err)
	}
	if second.ID() != "secondary-2" {
		t.Fatalf("second secondary id = %q", second.ID())
	}
}

func TestRabbitMQQueuePopSessionScopesSecondaryQueueState(t *testing.T) {
	channel := &rabbitMQTopologyTestChannel{ready: make(map[string][]amqp.Delivery)}
	conn := newRabbitMQTopologyTestConnection(channel, Options{Declare: Bool(true), Confirm: Bool(true)})
	q := &RabbitMQQueue{inner: conn, codec: encoding.JSON(), blockFor: 50 * time.Millisecond}
	body := mustRabbitMQRuntimePayload(t, q, &payload.Envelope{
		ID:          "secondary-session",
		Name:        "ExampleJob",
		Queue:       "low",
		Payload:     payload.Payload(`{"ok":true}`),
		CreatedAt:   1,
		AvailableAt: 1,
	})
	if err := q.Push(context.Background(), "low", body); err != nil {
		t.Fatalf("push low: %v", err)
	}

	sessionA := q.NewPopSession()
	sessionB := q.NewPopSession()
	if _, err := sessionA.Pop(context.Background(), []string{"high"}, queuecontract.PopNoWait); !errors.Is(err, ErrEmpty) {
		t.Fatalf("session A primary pop err = %v, want empty", err)
	}
	if _, err := sessionA.Pop(context.Background(), []string{"low"}, queuecontract.PopNoWait); err != nil {
		t.Fatalf("session A secondary pop: %v", err)
	}
	start := time.Now()
	if _, err := sessionB.Pop(context.Background(), []string{"high"}, queuecontract.PopWaitAvailable); !errors.Is(err, ErrEmpty) {
		t.Fatalf("session B primary pop err = %v, want empty", err)
	}
	if elapsed := time.Since(start); elapsed < 25*time.Millisecond {
		t.Fatalf("session B primary pop took %s, want independent block_for wait", elapsed)
	}
}

func TestRabbitMQQueuePrimaryBlockWaitsForSecondaryDelivery(t *testing.T) {
	// 需求背景：多队列 worker 在整轮非阻塞探测都为空后，会允许 primary queue 阻塞等待一次。
	// RabbitMQ 是 push consumer，等待窗口内 secondary queue 新到达的 delivery 应立即被处理，
	// 不能被空 primary queue 的 block_for 放大成固定延迟。
	channel := &rabbitMQTopologyTestChannel{ready: make(map[string][]amqp.Delivery)}
	conn := newRabbitMQTopologyTestConnection(channel, Options{Declare: Bool(true), Confirm: Bool(true)})
	q := &RabbitMQQueue{inner: conn, codec: encoding.JSON(), blockFor: 80 * time.Millisecond}
	session := q.NewPopSession()

	if _, err := session.Pop(context.Background(), []string{"high", "low"}, queuecontract.PopNoWait); !errors.Is(err, ErrEmpty) {
		t.Fatalf("primary probe err = %v, want empty", err)
	}

	go func() {
		time.Sleep(10 * time.Millisecond)
		body := mustRabbitMQRuntimePayload(t, q, &payload.Envelope{
			ID:          "secondary-during-block",
			Name:        "ExampleJob",
			Queue:       "low",
			Payload:     payload.Payload(`{"ok":true}`),
			CreatedAt:   1,
			AvailableAt: 1,
		})
		if err := q.Push(context.Background(), "low", body); err != nil {
			t.Errorf("push low: %v", err)
		}
	}()

	start := time.Now()
	reserved, err := session.Pop(context.Background(), []string{"high", "low"}, queuecontract.PopWaitAvailable)
	if err != nil {
		t.Fatalf("primary block pop: %v", err)
	}
	if reserved.ID() != "secondary-during-block" {
		t.Fatalf("reserved id = %q, want secondary delivery", reserved.ID())
	}
	if elapsed := time.Since(start); elapsed >= 60*time.Millisecond {
		t.Fatalf("secondary delivery waited for primary timeout, elapsed=%s", elapsed)
	}
}

func TestRabbitMQQueueProbeGroupDoesNotLeakIntoPlainPop(t *testing.T) {
	// 需求背景：当前 Pop 调用的 queues 参数就是唯一监听范围。普通 Pop(ctx, []string{"jobs"})
	// 没有声明要监听 low queue，不能消费调用方未请求队列里的任务。
	channel := &rabbitMQTopologyTestChannel{ready: make(map[string][]amqp.Delivery)}
	conn := newRabbitMQTopologyTestConnection(channel, Options{Declare: Bool(true), Confirm: Bool(true)})
	q := &RabbitMQQueue{inner: conn, codec: encoding.JSON(), blockFor: 50 * time.Millisecond}
	session := q.NewPopSession()

	if _, err := session.Pop(context.Background(), []string{"high", "low"}, queuecontract.PopNoWait); !errors.Is(err, ErrEmpty) {
		t.Fatalf("primary probe err = %v, want empty", err)
	}

	body := mustRabbitMQRuntimePayload(t, q, &payload.Envelope{
		ID:          "leaked-secondary",
		Name:        "ExampleJob",
		Queue:       "low",
		Payload:     payload.Payload(`{"ok":true}`),
		CreatedAt:   1,
		AvailableAt: 1,
	})
	if err := q.Push(context.Background(), "low", body); err != nil {
		t.Fatalf("push low: %v", err)
	}

	if _, err := session.Pop(context.Background(), []string{"jobs"}); !errors.Is(err, ErrEmpty) {
		t.Fatalf("plain pop err = %v, want empty without consuming old secondary probe queue", err)
	}
	reserved, err := session.Pop(context.Background(), []string{"low"}, queuecontract.PopNoWait)
	if err != nil {
		t.Fatalf("secondary pop after plain pop: %v", err)
	}
	if reserved.ID() != "leaked-secondary" {
		t.Fatalf("reserved id = %q, want leaked-secondary", reserved.ID())
	}
}

func TestRabbitMQQueueProbeGroupDoesNotSurviveQueueOrderChange(t *testing.T) {
	// 需求背景：worker 配置切换队列顺序后，旧 probe group 的 primary 不再匹配当前
	// blocking Pop。RabbitMQ driver 必须丢弃旧组，只监听当前 primary queue。
	channel := &rabbitMQTopologyTestChannel{ready: make(map[string][]amqp.Delivery)}
	conn := newRabbitMQTopologyTestConnection(channel, Options{Declare: Bool(true), Confirm: Bool(true)})
	q := &RabbitMQQueue{inner: conn, codec: encoding.JSON(), blockFor: 50 * time.Millisecond}
	session := q.NewPopSession()

	if _, err := session.Pop(context.Background(), []string{"high", "low"}, queuecontract.PopNoWait); !errors.Is(err, ErrEmpty) {
		t.Fatalf("primary probe err = %v, want empty", err)
	}

	body := mustRabbitMQRuntimePayload(t, q, &payload.Envelope{
		ID:          "old-secondary",
		Name:        "ExampleJob",
		Queue:       "low",
		Payload:     payload.Payload(`{"ok":true}`),
		CreatedAt:   1,
		AvailableAt: 1,
	})
	if err := q.Push(context.Background(), "low", body); err != nil {
		t.Fatalf("push low: %v", err)
	}

	if _, err := session.Pop(context.Background(), []string{"default"}, queuecontract.PopWaitAvailable); !errors.Is(err, ErrEmpty) {
		t.Fatalf("new primary pop err = %v, want empty without reusing old probe group", err)
	}
	reserved, err := session.Pop(context.Background(), []string{"low"}, queuecontract.PopNoWait)
	if err != nil {
		t.Fatalf("old secondary pop after order change: %v", err)
	}
	if reserved.ID() != "old-secondary" {
		t.Fatalf("reserved id = %q, want old-secondary", reserved.ID())
	}
}

func TestRabbitMQQueueContractCloseReturnsStableClosedErrors(t *testing.T) {
	channel := &rabbitMQTopologyTestChannel{ready: make(map[string][]amqp.Delivery)}
	conn := newRabbitMQTopologyTestConnection(channel, Options{Declare: Bool(true), Confirm: Bool(true)})
	var queueConn queuecontract.Queue = &RabbitMQQueue{inner: conn, codec: encoding.JSON(), blockFor: time.Millisecond}
	body := mustRabbitMQRuntimePayload(t, queueConn.(*RabbitMQQueue), &payload.Envelope{
		ID:          "closed-1",
		Name:        "ClosedJob",
		Queue:       "jobs",
		Payload:     payload.Payload(`{"ok":true}`),
		CreatedAt:   1,
		AvailableAt: 1,
	})

	if err := queueConn.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := queueConn.Push(context.Background(), "jobs", body); !errors.Is(err, ErrConnectionClosed) {
		t.Fatalf("push after close err = %v, want ErrConnectionClosed", err)
	}
	if _, err := queueConn.Pop(context.Background(), []string{"jobs"}); !errors.Is(err, ErrConnectionClosed) {
		t.Fatalf("pop after close err = %v, want ErrConnectionClosed", err)
	}
	if _, err := queueConn.Size(context.Background(), "jobs"); !errors.Is(err, ErrConnectionClosed) {
		t.Fatalf("size after close err = %v, want ErrConnectionClosed", err)
	}
	if err := queueConn.Clear(context.Background(), "jobs"); !errors.Is(err, ErrConnectionClosed) {
		t.Fatalf("clear after close err = %v, want ErrConnectionClosed", err)
	}
}

func TestRabbitMQReservedJobReleaseFailureEmitsOnlyReleaseRepublishEvent(t *testing.T) {
	channel := &rabbitMQTopologyTestChannel{ready: make(map[string][]amqp.Delivery)}
	conn := newRabbitMQTopologyTestConnection(channel, Options{Declare: Bool(true), Confirm: Bool(false)})
	var queueConn queuecontract.Queue = &RabbitMQQueue{inner: conn, codec: encoding.JSON(), blockFor: time.Millisecond}
	body := mustRabbitMQRuntimePayload(t, queueConn.(*RabbitMQQueue), &payload.Envelope{
		ID:          "release-failure-1",
		Name:        "ReleaseFailureJob",
		Queue:       "jobs",
		Payload:     payload.Payload(`{"ok":true}`),
		CreatedAt:   1,
		AvailableAt: 1,
	})
	var eventNames []string
	queueevents.UseSink(func(_ context.Context, ev queueevents.Event) {
		eventNames = append(eventNames, ev.Name())
	})
	t.Cleanup(func() { queueevents.UseSink(nil) })

	if err := queueConn.Push(context.Background(), "jobs", body); err != nil {
		t.Fatalf("push: %v", err)
	}
	reserved, err := queueConn.Pop(context.Background(), []string{"jobs"})
	if err != nil {
		t.Fatalf("pop: %v", err)
	}
	beforePublishCount := len(channel.published)
	channel.publishErr = errors.New("republish failed")
	if err := reserved.Release(context.Background(), time.Second); !errors.Is(err, ErrRabbitMQReleaseRepublishFailed) {
		t.Fatalf("release err = %v, want ErrRabbitMQReleaseRepublishFailed", err)
	}
	if channel.ackCount != 1 {
		t.Fatalf("release ack count = %d, want 1", channel.ackCount)
	}
	if len(channel.published) != beforePublishCount {
		t.Fatalf("release failed publish count = %d, want unchanged %d", len(channel.published), beforePublishCount)
	}
	if !hasRabbitMQEventName(eventNames, queueevents.EventReleaseRepublishFailed) {
		t.Fatalf("events = %v, want release republish failure event", eventNames)
	}
	if hasRabbitMQEventName(eventNames, queueevents.EventPublishFailed) {
		t.Fatalf("events = %v, release failure must not emit generic publish_failed", eventNames)
	}
}

func TestRabbitMQConnectionLifecycleAndRestartBranches(t *testing.T) {
	dialErr := errors.New("dial down")
	failDialer := func(string, amqp.Config) (AMQPConnection, error) {
		return nil, dialErr
	}
	if _, err := NewRabbitMQConnection("broken", Options{Dialer: failDialer}); !errors.Is(err, ErrRabbitMQDialFailed) || !strings.Contains(err.Error(), "broken") {
		t.Fatalf("dial error = %v, want wrapped dial failure", err)
	}

	channel := &rabbitMQTopologyTestChannel{ready: make(map[string][]amqp.Delivery)}
	successDialer := func(string, amqp.Config) (AMQPConnection, error) {
		return &rabbitMQTopologyTestConnection{channels: []AMQPChannel{channel}}, nil
	}
	conn, err := NewRabbitMQConnection("live", Options{Dialer: successDialer, Declare: Bool(true), Confirm: Bool(true), RestartEnabled: Bool(true), RestartPollInterval: time.Millisecond})
	if err != nil {
		t.Fatalf("new connection: %v", err)
	}
	if err := conn.RequestRestart(context.Background(), time.Unix(0, 123)); err != nil {
		t.Fatalf("request restart: %v", err)
	}
	at, err := conn.RestartRequestedAt(context.Background())
	if err != nil {
		t.Fatalf("restart requested at: %v", err)
	}
	if at.UnixNano() != 123 {
		t.Fatalf("restart at = %d, want 123", at.UnixNano())
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
	if !conn.isClosed() {
		t.Fatal("connection should report closed")
	}
}

func TestNewRabbitMQQueueConstructorAndDecodeErrors(t *testing.T) {
	channel := &rabbitMQTopologyTestChannel{ready: make(map[string][]amqp.Delivery)}
	dialer := func(string, amqp.Config) (AMQPConnection, error) {
		return &rabbitMQTopologyTestConnection{channels: []AMQPChannel{channel}}, nil
	}
	q, err := NewRabbitMQQueue("contract", Options{Dialer: dialer, Declare: Bool(true), Confirm: Bool(true), RestartEnabled: Bool(true)}, nil, time.Millisecond)
	if err != nil {
		t.Fatalf("new queue: %v", err)
	}
	if err := q.Push(context.Background(), "jobs", []byte("{bad")); err == nil {
		t.Fatal("expected decode error from invalid runtime envelope")
	}
	if _, err := q.Pop(context.Background(), []string{"jobs"}); !errors.Is(err, ErrEmpty) {
		t.Fatalf("empty pop err = %v, want ErrEmpty", err)
	}
	if payload.QueueCodec(nil).Name() == "" {
		t.Fatal("nil queue codec should resolve to default codec")
	}
	_ = q.Close()
}

func TestRabbitMQRestartReadAndPoisonBranches(t *testing.T) {
	channel := &rabbitMQTopologyTestChannel{}
	restartDelivery := amqp.Delivery{
		Body:         []byte(`"456"`),
		Acknowledger: (*rabbitMQTopologyTestAcknowledger)(channel),
	}
	channel.getDelivery = &restartDelivery
	channel.getOK = true
	conn := newRabbitMQTopologyTestConnection(channel, Options{Declare: Bool(true), RestartEnabled: Bool(true), RestartPollInterval: time.Millisecond})
	at, err := conn.RestartRequestedAt(context.Background())
	if err != nil {
		t.Fatalf("restart requested: %v", err)
	}
	if at.UnixNano() != 456 || channel.nackCount != 1 {
		t.Fatalf("restart at=%d nack=%d, want 456/1", at.UnixNano(), channel.nackCount)
	}

	badDelivery := amqp.Delivery{
		Body:         []byte("{bad"),
		Acknowledger: (*rabbitMQTopologyTestAcknowledger)(channel),
	}
	err = conn.rejectPoisonDelivery(context.Background(), "jobs", badDelivery, errors.New("decode"))
	if !errors.Is(err, ErrPoisonEnvelope) || channel.rejectCount != 1 {
		t.Fatalf("poison err=%v reject=%d", err, channel.rejectCount)
	}
	if ParseRestartTimestamp([]byte(`789`)).UnixNano() != 789 {
		t.Fatal("numeric restart timestamp did not parse")
	}
	if !ParseRestartTimestamp([]byte(`bad`)).IsZero() {
		t.Fatal("bad restart timestamp should parse as zero")
	}

	disabled := newRabbitMQTopologyTestConnection(&rabbitMQTopologyTestChannel{}, Options{Declare: Bool(true), RestartEnabled: Bool(false)})
	if err := disabled.RequestRestart(context.Background(), time.Now()); !errors.Is(err, ErrUnsupportedOperation) {
		t.Fatalf("disabled restart request = %v", err)
	}
	if at, err := disabled.RestartRequestedAt(context.Background()); err != nil || !at.IsZero() {
		t.Fatalf("disabled restart read at=%v err=%v", at, err)
	}

	missing := newRabbitMQTopologyTestConnection(&rabbitMQTopologyTestChannel{queueInspectErr: errors.New("missing")}, Options{Declare: Bool(false), RestartEnabled: Bool(true)})
	if _, err := missing.RestartRequestedAt(context.Background()); !errors.Is(err, ErrRabbitMQTopologyMissing) {
		t.Fatalf("declare=false missing restart err = %v", err)
	}
}

func TestRabbitMQPublicHelpersAndNilBranches(t *testing.T) {
	if got := NormalizeQueues([]string{"", "jobs", "jobs"}); len(got) != 1 || got[0] != "jobs" {
		t.Fatalf("NormalizeQueues = %#v", got)
	}
	if got := NormalizeQueues([]string{"", "  "}); len(got) != 1 || got[0] != "default" {
		t.Fatalf("NormalizeQueues = %#v", got)
	}
	if NormalizeQueueName("  ", nil) != "default" {
		t.Fatal("blank queue should normalize to default")
	}
	if JoinHostPort("host", "") != "host" || JoinHostPort("host", "5672") != "host:5672" {
		t.Fatal("JoinHostPort returned unexpected value")
	}
	if VHostPath("/") != "/" || VHostPath("tenant") != "/tenant" {
		t.Fatal("VHostPath returned unexpected value")
	}
	if RedactedURL("amqp://user:secret@example.test/vh") != "amqp://user:xxxxx@example.test/vh" {
		t.Fatal("RedactedURL did not redact password")
	}
	if len(DefaultDelayBuckets()) == 0 || len(SanitizeDelayBuckets([]time.Duration{-1, time.Second})) != 1 {
		t.Fatal("delay bucket helpers returned unexpected values")
	}
	if (&RabbitMQQueue{}).Close() != nil {
		t.Fatal("nil inner close should be nil")
	}
	var job *RabbitMQReservedJob
	if job.ID() != "" || job.Name() != "" || job.Attempts() != 0 || job.Payload() != nil {
		t.Fatal("nil reserved job accessors should be zero values")
	}
	if err := job.Delete(context.Background()); err != nil {
		t.Fatalf("nil reserved delete: %v", err)
	}
	if err := job.Release(context.Background(), 0); err != nil {
		t.Fatalf("nil reserved release: %v", err)
	}
}

func TestRabbitMQReconnectStateBranches(t *testing.T) {
	oldChannel := &rabbitMQTopologyTestChannel{}
	conn := newRabbitMQTopologyTestConnection(oldChannel, Options{Declare: Bool(true)})
	oldConn := conn.amqpConnection
	conn.publishSlots = []*rabbitMQPublishSlot{{channel: oldChannel}}
	conn.topologyChannel = oldChannel
	conn.consumerChannel = oldChannel
	conn.consumers["jobs"] = make(chan amqp.Delivery)
	if conn.consumerTags == nil {
		conn.consumerTags = make(map[string]string)
	}
	conn.consumerTags["jobs"] = "tag"
	conn.activeConsumers["jobs"] = struct{}{}
	conn.consumerRefs["jobs"] = 1
	conn.reconnectLooping = true
	conn.handleConnectionClosed(oldConn, errors.New("closed"))
	if !conn.isReconnecting() || !oldChannel.closed {
		t.Fatalf("reconnect state unexpected: reconnecting=%v channelClosed=%v", conn.isReconnecting(), oldChannel.closed)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if err := conn.waitReady(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waitReady err = %v, want deadline", err)
	}
	reconnected := &rabbitMQTopologyTestConnection{channels: []AMQPChannel{&rabbitMQTopologyTestChannel{}}}
	if !conn.installReconnectedConnection(reconnected) {
		t.Fatal("install reconnected connection should succeed")
	}
	conn.markReady(reconnected)
	if err := conn.waitReady(context.Background()); err != nil {
		t.Fatalf("wait ready after markReady: %v", err)
	}
	if got := keysOfConsumerMap(map[string]<-chan amqp.Delivery{"jobs": make(chan amqp.Delivery)}); len(got) != 1 || got[0] != "jobs" {
		t.Fatalf("keysOfConsumerMap = %#v", got)
	}

	conn.closed = true
	closingReplacement := &rabbitMQTopologyTestConnection{}
	conn.markReady(closingReplacement)
	if !closingReplacement.closed {
		t.Fatal("markReady on closed connection should close replacement")
	}
}

func TestRabbitMQPublishConfirmFailureBranches(t *testing.T) {
	env := &payload.Envelope{ID: "pub-1", Name: "Job", Queue: "jobs", CreatedAt: 1, AvailableAt: 1}
	nacked := newRabbitMQTopologyTestConnection(&rabbitMQTopologyTestChannel{confirmAck: true}, Options{Declare: Bool(true), Confirm: Bool(true)})
	if err := nacked.Push(context.Background(), "jobs", env, 0); !errors.Is(err, ErrRabbitMQPublishNacked) {
		t.Fatalf("nacked publish err = %v", err)
	}

	returned := amqp.Return{Exchange: "ex", RoutingKey: "jobs", ReplyCode: 312, ReplyText: "NO_ROUTE"}
	unrouted := newRabbitMQTopologyTestConnection(&rabbitMQTopologyTestChannel{returnOnPublish: &returned}, Options{Declare: Bool(true), Confirm: Bool(true)})
	if err := unrouted.Push(context.Background(), "jobs", env, 0); !errors.Is(err, ErrRabbitMQPublishUnrouted) {
		t.Fatalf("unrouted publish err = %v", err)
	}

	timeoutCtx, cancel := context.WithCancel(context.Background())
	cancel()
	timeoutConn := newRabbitMQTopologyTestConnection(&rabbitMQTopologyTestChannel{}, Options{Declare: Bool(true), Confirm: Bool(true), PublishTimeout: time.Millisecond})
	timeoutConn.ready = false
	timeoutConn.readyCh = make(chan struct{})
	timeoutConn.reconnecting = true
	if err := timeoutConn.Push(timeoutCtx, "jobs", env, 0); !errors.Is(err, context.Canceled) {
		t.Fatalf("publish while reconnecting err = %v, want context canceled", err)
	}

	if err := nacked.Push(context.Background(), "jobs", nil, 0); err == nil || !strings.Contains(err.Error(), "envelope is nil") {
		t.Fatalf("nil envelope push err = %v", err)
	}
	publishFailed := newRabbitMQTopologyTestConnection(&rabbitMQTopologyTestChannel{publishErr: errors.New("publish failed")}, Options{Declare: Bool(true), Confirm: Bool(false)})
	if err := publishFailed.Push(context.Background(), "jobs", env, 0); err == nil || !strings.Contains(err.Error(), "publish failed") {
		t.Fatalf("publish failure err = %v", err)
	}
	bestEffort := newRabbitMQTopologyTestConnection(&rabbitMQTopologyTestChannel{}, Options{Declare: Bool(true), Confirm: Bool(false)})
	if err := bestEffort.Push(context.Background(), "jobs", env, 0); err != nil {
		t.Fatalf("confirm=false push: %v", err)
	}
}

func TestRabbitMQReconnectLoopSuccessAndMonitorBranches(t *testing.T) {
	channel := &rabbitMQTopologyTestChannel{}
	conn := newRabbitMQTopologyTestConnection(channel, Options{Declare: Bool(true), ReconnectMinDelay: time.Millisecond, ReconnectMaxDelay: time.Millisecond})
	conn.reconnecting = true
	conn.reconnectLooping = true
	conn.ready = false
	conn.readyCh = make(chan struct{})
	conn.options.Dialer = func(string, amqp.Config) (AMQPConnection, error) {
		return &rabbitMQTopologyTestConnection{channels: []AMQPChannel{&rabbitMQTopologyTestChannel{}}}, nil
	}
	conn.reconnectLoop()
	if conn.isReconnecting() {
		t.Fatal("reconnectLoop should mark connection ready after successful dial")
	}
	if err := conn.waitReady(context.Background()); err != nil {
		t.Fatalf("wait ready after reconnectLoop: %v", err)
	}

	notify := make(chan *amqp.Error, 1)
	monitored := &rabbitMQTopologyTestConnection{channels: []AMQPChannel{&rabbitMQTopologyTestChannel{}}, notify: notify}
	conn.monitorConnection(monitored)
	notify <- nil
	time.Sleep(10 * time.Millisecond)
}

func TestRabbitMQConnectionManagementErrorBranches(t *testing.T) {
	if err := (*Connection)(nil).RequestRestart(context.Background(), time.Now()); !errors.Is(err, ErrConnectionClosed) {
		t.Fatalf("nil request restart err = %v", err)
	}
	if err := (*Connection)(nil).ensureOpen(); !errors.Is(err, ErrConnectionClosed) {
		t.Fatalf("nil ensure open err = %v", err)
	}
	closed := newRabbitMQTopologyTestConnection(&rabbitMQTopologyTestChannel{}, Options{Declare: Bool(true)})
	_ = closed.Close()
	if err := closed.Delete(context.Background(), &payload.Envelope{}); !errors.Is(err, ErrConnectionClosed) {
		t.Fatalf("closed delete err = %v", err)
	}
	if err := closed.Release(context.Background(), &payload.Envelope{}, 0); !errors.Is(err, ErrConnectionClosed) {
		t.Fatalf("closed release err = %v", err)
	}
	if err := closed.Clear(context.Background(), "jobs"); !errors.Is(err, ErrConnectionClosed) {
		t.Fatalf("closed clear err = %v", err)
	}
	if _, err := closed.Size(context.Background(), "jobs"); !errors.Is(err, ErrConnectionClosed) {
		t.Fatalf("closed size err = %v", err)
	}

	channel := &rabbitMQTopologyTestChannel{queuePurgeErr: errors.New("purge failed"), queueInspectErr: errors.New("inspect failed")}
	conn := newRabbitMQTopologyTestConnection(channel, Options{Declare: Bool(true)})
	if err := conn.Clear(context.Background(), "jobs"); err == nil || !strings.Contains(err.Error(), "purge failed") {
		t.Fatalf("clear purge err = %v", err)
	}
	if _, err := conn.Size(context.Background(), "jobs"); err == nil || !strings.Contains(err.Error(), "inspect failed") {
		t.Fatalf("size inspect err = %v", err)
	}
	if _, err := conn.Pop(context.Background(), []string{"jobs"}, PopOptions{BlockFor: time.Millisecond}); !errors.Is(err, ErrEmpty) {
		t.Fatalf("blocking empty pop err = %v", err)
	}

	ackErr := errors.New("ack failed")
	ackChannel := &rabbitMQTopologyTestChannel{ackErr: ackErr}
	ackConn := newRabbitMQTopologyTestConnection(ackChannel, Options{Declare: Bool(true)})
	ackedEnv := &payload.Envelope{Queue: "jobs"}
	rememberEnvelopeDelivery(ackedEnv, &rabbitMQDeliveryState{delivery: amqp.Delivery{Acknowledger: (*rabbitMQTopologyTestAcknowledger)(ackChannel)}})
	if err := ackConn.Delete(context.Background(), ackedEnv); !errors.Is(err, ackErr) {
		t.Fatalf("delete ack err = %v", err)
	}

	releaseChannel := &rabbitMQTopologyTestChannel{publishErr: errors.New("republish failed")}
	releaseConn := newRabbitMQTopologyTestConnection(releaseChannel, Options{Declare: Bool(true), Confirm: Bool(false)})
	releaseEnv := &payload.Envelope{ID: "release", Name: "Job", Queue: "jobs", CreatedAt: 1, AvailableAt: 1}
	rememberEnvelopeDelivery(releaseEnv, &rabbitMQDeliveryState{delivery: amqp.Delivery{Acknowledger: (*rabbitMQTopologyTestAcknowledger)(releaseChannel)}})
	if err := releaseConn.Release(context.Background(), releaseEnv, 0); !errors.Is(err, ErrRabbitMQReleaseRepublishFailed) {
		t.Fatalf("release republish err = %v", err)
	}
}

func TestRabbitMQRestartTopologyCacheBranches(t *testing.T) {
	conn := newRabbitMQTopologyTestConnection(&rabbitMQTopologyTestChannel{}, Options{
		Declare:                 Bool(true),
		RestartEnabled:          Bool(true),
		TopologyCacheTTL:        time.Minute,
		TopologyCacheMaxEntries: 8,
	})
	if key := rabbitMQRestartQueueCacheKey("restart"); key.Kind != "restart_queue" || key.Name != "restart" {
		t.Fatalf("restart cache key = %#v", key)
	}
	if err := conn.ensureRestartTopology("restart"); err != nil {
		t.Fatalf("ensure restart topology: %v", err)
	}
	if !conn.isRestartQueueTopologyCached("restart") {
		t.Fatal("restart queue should be cached")
	}
	conn.markRestartQueueTopology("restart")

	passive := newRabbitMQTopologyTestConnection(&rabbitMQTopologyTestChannel{messages: map[string]int{"restart": 1}}, Options{
		Declare:                 Bool(false),
		RestartEnabled:          Bool(true),
		TopologyCacheTTL:        time.Minute,
		TopologyCacheMaxEntries: 8,
	})
	if err := passive.ensureRestartTopology("restart"); err != nil {
		t.Fatalf("passive restart topology: %v", err)
	}
	if !passive.isRestartQueueTopologyCached("restart") {
		t.Fatal("passive restart queue should be cached")
	}

	noCache := newRabbitMQTopologyTestConnection(&rabbitMQTopologyTestChannel{}, Options{Declare: Bool(true), RestartEnabled: Bool(true)})
	noCache.markRestartQueueTopology("restart")
	if noCache.isRestartQueueTopologyCached("restart") {
		t.Fatal("restart topology should not cache when topology cache disabled")
	}

	emptyChannel := &rabbitMQTopologyTestChannel{}
	empty := newRabbitMQTopologyTestConnection(emptyChannel, Options{Declare: Bool(true), RestartEnabled: Bool(true), RestartPollInterval: time.Minute})
	first, err := empty.RestartRequestedAt(context.Background())
	if err != nil || !first.IsZero() {
		t.Fatalf("empty restart read at=%v err=%v", first, err)
	}
	emptyChannel.getErr = errors.New("should be cached")
	second, err := empty.RestartRequestedAt(context.Background())
	if err != nil || !second.IsZero() {
		t.Fatalf("cached empty restart read at=%v err=%v", second, err)
	}

	nackChannel := &rabbitMQTopologyTestChannel{nackErr: errors.New("nack failed")}
	nackDelivery := amqp.Delivery{Body: []byte("123"), Acknowledger: (*rabbitMQTopologyTestAcknowledger)(nackChannel)}
	nackChannel.getDelivery = &nackDelivery
	nackChannel.getOK = true
	nackConn := newRabbitMQTopologyTestConnection(nackChannel, Options{Declare: Bool(true), RestartEnabled: Bool(true)})
	if _, err := nackConn.RestartRequestedAt(context.Background()); err == nil || !strings.Contains(err.Error(), "nack failed") {
		t.Fatalf("restart nack err = %v", err)
	}
}

func TestRabbitMQChannelAndCloseErrorBranches(t *testing.T) {
	confirmErr := errors.New("confirm failed")
	confirmChannel := &rabbitMQTopologyTestChannel{confirmErr: confirmErr}
	confirmConn := newRabbitMQTopologyTestConnection(confirmChannel, Options{Declare: Bool(true), Confirm: Bool(true)})
	if err := confirmConn.Push(context.Background(), "jobs", &payload.Envelope{ID: "confirm", Name: "Job", Queue: "jobs"}, 0); !errors.Is(err, confirmErr) {
		t.Fatalf("confirm setup err = %v, want confirm failure", err)
	}
	if !confirmChannel.closed {
		t.Fatal("publish channel should close after confirm setup failure")
	}

	qosErr := errors.New("qos failed")
	qosConn := newRabbitMQTopologyTestConnection(&rabbitMQTopologyTestChannel{qosErr: qosErr}, Options{Declare: Bool(true)})
	if _, err := qosConn.Pop(context.Background(), []string{"jobs"}, PopOptions{}); !errors.Is(err, qosErr) {
		t.Fatalf("qos pop err = %v, want qos failure", err)
	}

	closeErr := errors.New("close failed")
	publishChannel := &rabbitMQTopologyTestChannel{closeErr: closeErr}
	topologyChannel := &rabbitMQTopologyTestChannel{closeErr: errors.New("topology close failed")}
	consumerChannel := &rabbitMQTopologyTestChannel{closeErr: errors.New("consumer close failed")}
	conn := newRabbitMQTopologyTestConnection(&rabbitMQTopologyTestChannel{}, Options{Declare: Bool(true)})
	conn.publishSlots = []*rabbitMQPublishSlot{{channel: publishChannel}}
	conn.topologyChannel = topologyChannel
	conn.consumerChannel = consumerChannel
	conn.consumers["jobs"] = make(chan amqp.Delivery)
	conn.restartCache.at = time.Unix(0, 99)
	conn.restartCache.expiresAt = time.Now().Add(time.Hour)
	if err := conn.Close(); !errors.Is(err, closeErr) {
		t.Fatalf("close err = %v, want first publish channel close error", err)
	}
	if !publishChannel.closed || !topologyChannel.closed || !consumerChannel.closed {
		t.Fatal("close should close publish, topology, and consumer channels")
	}
	if !conn.restartCache.expiresAt.IsZero() {
		t.Fatal("close should clear restart signal cache")
	}
}

func TestRabbitMQPoisonAndRestartPublishErrorBranches(t *testing.T) {
	rejectErr := errors.New("reject failed")
	rejectChannel := &rabbitMQTopologyTestChannel{rejectErr: rejectErr}
	delivery := amqp.Delivery{
		Body:         []byte("{bad"),
		Acknowledger: (*rabbitMQTopologyTestAcknowledger)(rejectChannel),
	}
	conn := newRabbitMQTopologyTestConnection(rejectChannel, Options{Declare: Bool(true)})
	err := conn.rejectPoisonDelivery(context.Background(), "jobs", delivery, errors.New("decode failed"))
	if !errors.Is(err, ErrPoisonEnvelope) || !errors.Is(err, rejectErr) {
		t.Fatalf("reject poison err = %v, want poison wrapping reject failure", err)
	}
	if rejectChannel.rejectCount != 1 {
		t.Fatalf("reject count = %d, want 1", rejectChannel.rejectCount)
	}

	disabled := newRabbitMQTopologyTestConnection(&rabbitMQTopologyTestChannel{}, Options{Declare: Bool(true), RestartEnabled: Bool(false)})
	if err := disabled.RequestRestart(nil, time.Time{}); !errors.Is(err, ErrUnsupportedOperation) {
		t.Fatalf("disabled restart err = %v, want unsupported", err)
	}

	publishErr := errors.New("restart publish failed")
	failedPublish := newRabbitMQTopologyTestConnection(&rabbitMQTopologyTestChannel{publishErr: publishErr}, Options{Declare: Bool(true), Confirm: Bool(false), RestartEnabled: Bool(true)})
	if err := failedPublish.RequestRestart(context.Background(), time.Time{}); !errors.Is(err, publishErr) {
		t.Fatalf("restart publish err = %v, want publish failure", err)
	}
	if !failedPublish.restartCache.expiresAt.IsZero() {
		t.Fatal("failed restart publish must not update read-your-own-write cache")
	}
}

func TestRabbitMQOptionsBoundaryBranches(t *testing.T) {
	// 需求背景：URL 和 Dialer 只是连接输入，不能触发“调用方完整接管 bool 默认值”的旧启发式分支。
	// 未设置的可靠性开关必须继续继承默认 true。
	urlOnly := resolveRabbitMQOptions(Options{
		URL:    "amqp://guest:guest@example.test:5672/%2F",
		Dialer: func(string, amqp.Config) (AMQPConnection, error) { return nil, errors.New("unused") },
	})
	if urlOnly.URL == "" || !urlOnly.Declare || !urlOnly.ExchangeDurable || !urlOnly.QueueDurable || !urlOnly.MessagePersistent || !urlOnly.Confirm || !urlOnly.RestartEnabled {
		t.Fatalf("url-only defaults = %#v, want reliability defaults retained", urlOnly)
	}
	if urlOnly.Dialer == nil {
		t.Fatalf("url-only defaults = %#v, want custom dialer retained", urlOnly)
	}
	explicitFalse := resolveRabbitMQOptions(Options{
		URL:     "amqp://guest:guest@example.test:5672/%2F",
		Declare: Bool(false),
		Confirm: Bool(false),
	})
	if explicitFalse.Declare || explicitFalse.Confirm {
		t.Fatalf("explicit false defaults = %#v, want false retained", explicitFalse)
	}
	if resolveRabbitMQOptions(Options{PublishChannels: maxRabbitMQPublishChannels + 1}).PublishChannels != maxRabbitMQPublishChannels {
		t.Fatal("publish channels should clamp to maximum")
	}
	backoff := resolveRabbitMQOptions(Options{
		ReconnectMinDelay: 5 * time.Second,
		ReconnectMaxDelay: time.Second,
	})
	if backoff.ReconnectMaxDelay < backoff.ReconnectMinDelay {
		t.Fatalf("backoff window = min %s max %s, want max >= min", backoff.ReconnectMinDelay, backoff.ReconnectMaxDelay)
	}
	cache := resolveRabbitMQOptions(Options{TopologyCacheTTL: -time.Second, TopologyCacheMaxEntries: -1})
	if cache.TopologyCacheTTL != 0 || cache.TopologyCacheMaxEntries != 0 {
		t.Fatalf("negative topology cache options = %s/%d, want zeroed", cache.TopologyCacheTTL, cache.TopologyCacheMaxEntries)
	}
	withUser := Options{Scheme: "amqp", Host: "example.test", Port: "", VHost: "tenant/a", Username: "guest"}
	if got := resolveRabbitMQOptions(withUser).connectionURL(); !strings.Contains(got, "guest@") || !strings.Contains(got, "/tenant/a") {
		t.Fatalf("connectionURL with user/vhost = %q", got)
	}
	if redactedRabbitMQURL("://bad-url") != "://bad-url" {
		t.Fatal("invalid URL redaction should return original value")
	}
}

func TestRabbitMQTopologyCachePredicateBranches(t *testing.T) {
	noCache := newRabbitMQTopologyTestConnection(&rabbitMQTopologyTestChannel{}, Options{Declare: Bool(true)})
	noCache.delayedQueues["jobs"] = struct{}{}
	noCache.ttlDelayQueues["jobs.delay"] = struct{}{}
	if !noCache.isPluginDelayQueueDeclared("jobs") {
		t.Fatal("plugin delay declaration should be visible without topology cache")
	}
	if !noCache.isTTLDLXDelayQueueDeclared("jobs.delay", "jobs") {
		t.Fatal("ttl dlx declaration should be visible without topology cache")
	}

	cached := newRabbitMQTopologyTestConnection(&rabbitMQTopologyTestChannel{}, Options{Declare: Bool(true), TopologyCacheMaxEntries: 8})
	cached.delayedQueues["jobs"] = struct{}{}
	cached.ttlDelayQueues["jobs.delay"] = struct{}{}
	if !cached.isPluginDelayQueueDeclared("jobs") {
		t.Fatal("plugin delay declaration should be visible with topology cache")
	}
	if !cached.isTTLDLXDelayQueueDeclared("jobs.delay", "jobs") {
		t.Fatal("ttl dlx declaration should be visible with topology cache")
	}
	if cached.isPluginDelayQueueDeclared("missing") || cached.isTTLDLXDelayQueueDeclared("missing", "jobs") {
		t.Fatal("missing delay topology should not be reported as declared")
	}

	if rabbitMQContentType("msgpack") != rabbitMQContentTypeMsgpack {
		t.Fatal("non-json codec should map to msgpack content type")
	}
	if (*Connection)(nil).codecOrDefault().Name() == "" {
		t.Fatal("nil connection codec should resolve to default codec")
	}
}

func mustRabbitMQRuntimePayload(t *testing.T, q *RabbitMQQueue, env *payload.Envelope) queuecontract.Payload {
	t.Helper()
	body, err := q.codec.Marshal(env)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	return queuecontract.Payload(body)
}

func hasRabbitMQEventName(events []string, name string) bool {
	for _, event := range events {
		if event == name {
			return true
		}
	}
	return false
}
