package queue

import (
	"context"
	"fmt"
	"time"

	queuecontract "github.com/prismgo/framework/contracts/queue"
	"github.com/prismgo/framework/queue/payload"
)

func normalizeDispatchOptions(runtime *Runtime, job Job, opts DispatchOptions) DispatchOptions {
	if opts.Connection == "" {
		if provider, ok := job.(ConnectionProvider); ok {
			opts.Connection = provider.QueueConnection()
		}
	}
	if opts.Queue == "" {
		if provider, ok := job.(QueueProvider); ok {
			opts.Queue = provider.QueueName()
		}
	}
	if opts.Delay == 0 {
		if provider, ok := job.(DelayProvider); ok {
			opts.Delay = provider.QueueDelay()
		}
	}
	if opts.Tries == 0 {
		if provider, ok := job.(TriesProvider); ok {
			opts.Tries = provider.Tries()
		}
	}
	if opts.MaxExceptions == 0 {
		if provider, ok := job.(MaxExceptionsProvider); ok {
			opts.MaxExceptions = provider.MaxExceptions()
		}
	}
	if opts.Timeout == 0 {
		if provider, ok := job.(TimeoutProvider); ok {
			opts.Timeout = provider.Timeout()
		}
	}
	if !opts.FailOnTimeout {
		if provider, ok := job.(FailOnTimeoutProvider); ok {
			opts.FailOnTimeout = provider.FailOnTimeout()
		}
	}
	if !opts.Encrypted {
		if provider, ok := job.(EncryptedProvider); ok {
			opts.Encrypted = provider.ShouldEncrypt()
		}
	}
	if len(opts.Tags) == 0 {
		if provider, ok := job.(TagsProvider); ok {
			opts.Tags = normalizeOptionStrings(provider.Tags())
		}
	}
	if !opts.Silenced {
		if provider, ok := job.(SilencedProvider); ok {
			opts.Silenced = provider.Silenced()
		}
	}
	if len(opts.Backoff) == 0 {
		if provider, ok := job.(BackoffProvider); ok {
			opts.Backoff = append([]time.Duration(nil), provider.Backoff()...)
		}
	}
	if opts.RetryUntil.IsZero() {
		if provider, ok := job.(RetryUntilProvider); ok {
			opts.RetryUntil = provider.RetryUntil()
		}
	}
	if opts.UniqueKey == "" {
		if provider, ok := job.(UniqueIDProvider); ok {
			opts.UniqueKey = provider.UniqueID()
		}
	}
	if opts.UniqueFor == 0 {
		if provider, ok := job.(UniqueForProvider); ok {
			opts.UniqueFor = provider.UniqueFor()
		}
	}
	if opts.uniqueVia == nil {
		if provider, ok := job.(UniqueViaProvider); ok {
			opts.uniqueVia = provider.UniqueVia()
		}
	}
	if !opts.UniqueUntil {
		if provider, ok := job.(UniqueUntilProcessingProvider); ok {
			opts.UniqueUntil = provider.UniqueUntilProcessing()
		}
	}
	if opts.DebounceKey == "" {
		if provider, ok := job.(DebounceIDProvider); ok {
			opts.DebounceKey = provider.DebounceID()
		}
	}
	if opts.DebounceFor == 0 {
		if provider, ok := job.(DebounceForProvider); ok {
			opts.DebounceFor = provider.DebounceFor()
		}
	}
	if opts.debounceVia == nil {
		if provider, ok := job.(DebounceViaProvider); ok {
			opts.debounceVia = provider.DebounceVia()
		}
	}
	if opts.Connection == "" {
		opts.Connection = runtime.defaultConnection
	}
	if opts.Queue == "" {
		opts.Queue = runtime.defaultQueue
	}
	return opts
}

func prepareDispatchRuntime(ctx context.Context, env *payload.Envelope, opts *DispatchOptions) error {
	if env.UniqueKey != "" {
		if err := acquireUnique(ctx, env, opts.uniqueVia, opts.UniqueFor); err != nil {
			return err
		}
	}
	if env.DebounceKey != "" {
		if opts.Delay < opts.DebounceFor {
			opts.Delay = opts.DebounceFor
			env.AvailableAt = time.Now().Add(opts.Delay).Unix()
		}
		return rememberDebounce(ctx, env, opts.debounceVia, opts.DebounceFor)
	}
	return nil
}

func payloadForQueueEnvelope(runtime *Runtime, env *payload.Envelope) (payload.Payload, error) {
	if env == nil {
		return nil, fmt.Errorf("queue: envelope is nil")
	}
	if !env.Encrypted {
		return env.Payload, nil
	}
	encrypter, err := runtime.resolvePayloadEncrypter()
	if err != nil {
		return nil, err
	}
	token, err := encryptedToken(env.Payload)
	if err != nil {
		return nil, err
	}
	body, err := encrypter.Decrypt(context.Background(), []byte(token))
	if err != nil {
		return nil, err
	}
	return payload.Payload(body), nil
}

func encodeQueueEnvelope(runtime *Runtime, env *payload.Envelope) (queuecontract.Payload, error) {
	if runtime == nil {
		return nil, fmt.Errorf("queue runtime is nil")
	}
	body, err := payload.QueueCodec(runtime.codec).Marshal(env)
	if err != nil {
		return nil, err
	}
	return queuecontract.Payload(body), nil
}

func envelopeFromQueuePayload(runtime *Runtime, body queuecontract.Payload) (*payload.Envelope, error) {
	if runtime == nil {
		return nil, fmt.Errorf("queue runtime is nil")
	}
	var env payload.Envelope
	if err := payload.QueueCodec(runtime.codec).Unmarshal(body, &env); err != nil {
		return nil, err
	}
	return &env, nil
}

func newJobRunner(manager *Manager, connection string) *JobRunner {
	return newQueueJobRunner(manager, manager.runtimeOrDefault(), nil, connection)
}

func newQueueJobRunner(manager *Manager, runtime *Runtime, queueConn queuecontract.Queue, connection string) *JobRunner {
	return &JobRunner{
		manager:    manager,
		runtime:    runtime,
		registry:   runtime.registry,
		connection: connection,
		queueConn:  queueConn,
		middleware: runtime.middlewareSnapshot(),
	}
}
