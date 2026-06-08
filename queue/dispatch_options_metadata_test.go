package queue

import (
	"testing"
	"time"
)

func TestDispatchOptionsCarryExplicitTagsAndSilencedMetadata(t *testing.T) {
	// 测试目的：Horizon tags/silenced 必须在入队前由公开 DispatchOption 确定，
	// 并且标签清理逻辑要去空白、去重，避免展示层再读取 payload 推导。
	options := applyOptions(Tags(" tenant:1 ", "mail", "tenant:1", ""), Silenced())
	if len(options.Tags) != 2 || options.Tags[0] != "tenant:1" || options.Tags[1] != "mail" {
		t.Fatalf("unexpected tags: %#v", options.Tags)
	}
	if !options.Silenced {
		t.Fatal("Silenced option should mark dispatch metadata")
	}
}

func TestDispatchOptionFluentChainCarriesTagsAndSilencedMetadata(t *testing.T) {
	// 测试目的：流式 option API 是业务投递代码常用入口，必须和函数式入口保持同一套
	// Horizon metadata 语义。
	options := applyOptions(OnConnection("redis").OnQueue("default").Tags("quiet").Silenced())
	if options.Connection != "redis" || options.Queue != "default" {
		t.Fatalf("unexpected connection/queue: %#v", options)
	}
	if len(options.Tags) != 1 || options.Tags[0] != "quiet" || !options.Silenced {
		t.Fatalf("unexpected Horizon metadata: %#v", options)
	}
}

func TestContractDispatchOptionsConvertToFunctionalOptions(t *testing.T) {
	// 测试目的：event 等跨包调用只依赖 contracts/queue 的只读选项接口，转换层必须保留
	// connection、queue、delay、tries、timeout 和 backoff，不要求调用方 import queue 实现包。
	options := applyOptions(dispatchOptionsFromQueueOptions(contractDispatchOptions{
		connection: "redis",
		queue:      "mail",
		delay:      2 * time.Second,
		tries:      3,
		timeout:    30 * time.Second,
		backoff:    []time.Duration{time.Second, 5 * time.Second},
	})...)
	if options.Connection != "redis" || options.Queue != "mail" || options.Delay != 2*time.Second {
		t.Fatalf("basic options were not converted: %#v", options)
	}
	if options.Tries != 3 || options.Timeout != 30*time.Second || len(options.Backoff) != 2 {
		t.Fatalf("retry options were not converted: %#v", options)
	}
	if got := dispatchOptionsFromQueueOptions(nil); got != nil {
		t.Fatalf("nil contract options should return nil, got %#v", got)
	}
}

type contractDispatchOptions struct {
	connection string
	queue      string
	delay      time.Duration
	tries      int
	timeout    time.Duration
	backoff    []time.Duration
}

func (o contractDispatchOptions) QueueConnection() string     { return o.connection }
func (o contractDispatchOptions) QueueName() string           { return o.queue }
func (o contractDispatchOptions) QueueDelay() time.Duration   { return o.delay }
func (o contractDispatchOptions) QueueTries() int             { return o.tries }
func (o contractDispatchOptions) QueueTimeout() time.Duration { return o.timeout }
func (o contractDispatchOptions) QueueBackoff() []time.Duration {
	return append([]time.Duration(nil), o.backoff...)
}
func (o contractDispatchOptions) QueueMaxExceptions() int         { return 0 }
func (o contractDispatchOptions) QueueFailOnTimeout() bool        { return false }
func (o contractDispatchOptions) QueueEncrypted() bool            { return false }
func (o contractDispatchOptions) QueueRetryUntil() time.Time      { return time.Time{} }
func (o contractDispatchOptions) QueueBatchID() string            { return "" }
func (o contractDispatchOptions) QueueUniqueKey() string          { return "" }
func (o contractDispatchOptions) QueueUniqueFor() time.Duration   { return 0 }
func (o contractDispatchOptions) QueueUniqueUntil() bool          { return false }
func (o contractDispatchOptions) QueueDebounceKey() string        { return "" }
func (o contractDispatchOptions) QueueDebounceFor() time.Duration { return 0 }
func (o contractDispatchOptions) QueueTags() []string             { return nil }
func (o contractDispatchOptions) QueueSilenced() bool             { return false }
