package queue

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"
	"github.com/spf13/cobra"

	"github.com/prismgo/framework/console"
	"github.com/prismgo/framework/container"
	containercontract "github.com/prismgo/framework/contracts/container"
	queuecore "github.com/prismgo/framework/queue"
	"github.com/prismgo/framework/queue/payload"
	"github.com/prismgo/framework/queue/state"
	prismredis "github.com/prismgo/framework/redis"
)

type commandJob struct {
	Key string `json:"key"`
}

func (j *commandJob) Handle(context.Context) error { return nil }

func commandJobName(t *testing.T) string {
	t.Helper()
	name, err := queuecore.JobTypeName(&commandJob{})
	if err != nil {
		t.Fatalf("command job name: %v", err)
	}
	return name
}

func TestWorkCommandRunsUntilEmpty(t *testing.T) {
	useTestManager(t)
	cmd := NewWorkCommand()
	output := runCommand(t, cmd, queueInput{
		options: map[string]string{
			"queue":       "default, emails",
			"sleep":       "bad",
			"timeout":     "1",
			"tries":       "2",
			"backoff":     "1, bad, 2",
			"max-jobs":    "0",
			"max-time":    "0",
			"retry-after": "90",
		},
		bools: map[string]bool{"stop-when-empty": true},
	})
	if !strings.Contains(output, "queue worker started") {
		t.Fatalf("expected worker start output, got %q", output)
	}
	definition := cmd.Definition()
	if definition.Name != "queue" || len(definition.Aliases) != 1 || definition.Aliases[0] != "queue:work" {
		t.Fatalf("unexpected queue worker definition: %#v", definition)
	}
}

func TestFailedForgetAndFlushCommands(t *testing.T) {
	manager := useTestManager(t)
	jobName := commandJobName(t)
	failed := payload.FailedJob{
		ID:         "failed-1",
		Connection: "sync",
		Queue:      "default",
		JobName:    jobName,
		Error:      "boom",
		FailedAt:   time.Now(),
	}
	if err := manager.Failed().Record(context.Background(), failed); err != nil {
		t.Fatalf("record failed job: %v", err)
	}

	output := runCommand(t, NewFailedCommand(), queueInput{})
	if !strings.Contains(output, "failed-1") {
		t.Fatalf("expected failed job in output, got %q", output)
	}

	runCommand(t, NewForgetCommand(), queueInput{args: map[string][]string{"id": {"failed-1"}}})
	if _, err := manager.Failed().Find(context.Background(), "failed-1"); !errors.Is(err, queuecore.ErrEmpty) {
		t.Fatalf("expected forgotten failed job, got %v", err)
	}

	if err := manager.Failed().Record(context.Background(), failed); err != nil {
		t.Fatalf("record failed job again: %v", err)
	}
	runCommand(t, NewFlushCommand(), queueInput{})
	page, err := manager.Failed().Page(context.Background(), state.PageRequest{Page: 1, PageSize: 10})
	if err != nil || len(page.Items) != 0 {
		t.Fatalf("expected flushed failed jobs, got %#v, %v", page.Items, err)
	}
}

func TestFailedCommandPaginatesFailedJobs(t *testing.T) {
	manager := useTestManager(t)
	jobName := commandJobName(t)
	now := time.Now()
	for i, id := range []string{"failed-1", "failed-2", "failed-3"} {
		if err := manager.Failed().Record(context.Background(), payload.FailedJob{
			ID:         id,
			Connection: "sync",
			Queue:      "default",
			JobName:    jobName,
			Error:      "boom",
			FailedAt:   now.Add(time.Duration(i) * time.Second),
		}); err != nil {
			t.Fatalf("record failed job %s: %v", id, err)
		}
	}

	output := runCommand(t, NewFailedCommand(), queueInput{options: map[string]string{"page": "2", "page-size": "1"}})
	if !strings.Contains(output, "failed-2") || strings.Contains(output, "failed-1") || strings.Contains(output, "failed-3") {
		t.Fatalf("expected only second failed job in paged output, got %q", output)
	}
}

func TestRetryCommandRequeuesAndForgetsFailedJob(t *testing.T) {
	manager := useTestManager(t)
	jobName := commandJobName(t)
	failed := payload.FailedJob{
		ID:         "failed-2",
		Connection: "sync",
		Queue:      "default",
		JobName:    jobName,
		Envelope: payload.Envelope{
			ID:      "job-1",
			Name:    jobName,
			Queue:   "default",
			Payload: []byte(`{"key":"retry"}`),
		},
		FailedAt: time.Now(),
	}
	if err := manager.Failed().Record(context.Background(), failed); err != nil {
		t.Fatalf("record failed job: %v", err)
	}

	output := runCommand(t, NewRetryCommand(), queueInput{args: map[string][]string{"ids": {"failed-2"}}})
	if !strings.Contains(output, "retried failed job: failed-2") {
		t.Fatalf("expected retry output, got %q", output)
	}
	if _, err := manager.Failed().Find(context.Background(), "failed-2"); !errors.Is(err, queuecore.ErrEmpty) {
		t.Fatalf("expected retry to forget failed job, got %v", err)
	}
	conn, err := manager.Queue("sync")
	if err != nil {
		t.Fatalf("resolve sync connection: %v", err)
	}
	size, err := conn.Size(context.Background(), "default")
	if err != nil || size != 0 {
		t.Fatalf("expected sync retry to execute immediately, got queue size %d, %v", size, err)
	}
}

func TestRetryCommandReturnsFailedLookupAndDispatchErrors(t *testing.T) {
	manager := useTestManager(t)
	if err := NewRetryCommand().Handle(commandContext(NewRetryCommand(), queueInput{args: map[string][]string{"ids": {"missing"}}})); !errors.Is(err, queuecore.ErrEmpty) {
		t.Fatalf("expected missing failed job error, got %v", err)
	}

	jobName := commandJobName(t)
	failed := payload.FailedJob{
		ID:         "failed-missing-connection",
		Connection: "missing",
		Queue:      "default",
		JobName:    jobName,
		Envelope: payload.Envelope{
			ID:      "job-2",
			Name:    jobName,
			Queue:   "default",
			Payload: []byte(`{"key":"retry"}`),
		},
		FailedAt: time.Now(),
	}
	if err := manager.Failed().Record(context.Background(), failed); err != nil {
		t.Fatalf("record failed job: %v", err)
	}
	err := NewRetryCommand().Handle(commandContext(NewRetryCommand(), queueInput{args: map[string][]string{"ids": {"failed-missing-connection"}}}))
	if err == nil || !strings.Contains(err.Error(), "not configured") {
		t.Fatalf("expected dispatch connection error, got %v", err)
	}
}

func TestRestartCommandWritesSignal(t *testing.T) {
	useTestManager(t)
	output := runCommand(t, NewRestartCommand(), queueInput{})
	if !strings.Contains(output, "queue restart signal sent") {
		t.Fatalf("expected restart output, got %q", output)
	}
}

func TestCommandsReturnRedisStoreErrors(t *testing.T) {
	manager := useRedisTestManager(t)
	client, err := prismredis.Client("default")
	if err != nil {
		t.Fatalf("redis client: %v", err)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("close redis client: %v", err)
	}
	if err := manager.Close(); err != nil {
		t.Fatalf("close manager: %v", err)
	}

	assertCommandError(t, NewFailedCommand(), queueInput{}, "closed")
	assertCommandError(t, NewForgetCommand(), queueInput{args: map[string][]string{"id": {"missing"}}}, "closed")
	assertCommandError(t, NewFlushCommand(), queueInput{}, "closed")
}

func TestCommandsReturnResolveManagerErrors(t *testing.T) {
	registry := useQueueTestContainer(t)
	if err := registry.Singleton("queue.manager", func(containercontract.Resolver) (any, error) {
		return nil, errors.New("factory failed")
	}); err != nil {
		t.Fatalf("register failing queue manager: %v", err)
	}

	assertCommandError(t, NewFailedCommand(), queueInput{}, "resolve queue manager failed")
	assertCommandError(t, NewFlushCommand(), queueInput{}, "resolve queue manager failed")
	assertCommandError(t, NewForgetCommand(), queueInput{args: map[string][]string{"id": {"missing"}}}, "resolve queue manager failed")
	assertCommandError(t, NewRetryCommand(), queueInput{args: map[string][]string{"ids": {"failed-2"}}}, "resolve queue manager failed")
	assertCommandError(t, NewWorkCommand(), queueInput{}, "resolve queue manager failed")
	assertCommandError(t, NewRestartCommand(), queueInput{}, "resolve queue manager failed")
}

func TestResolveManagerReturnsErrorWhenFactoryFails(t *testing.T) {
	registry := useQueueTestContainer(t)
	if err := registry.Singleton("queue.manager", func(containercontract.Resolver) (any, error) {
		return nil, errors.New("factory failed")
	}); err != nil {
		t.Fatalf("register failing queue manager: %v", err)
	}
	manager, err := resolveManager()
	if err == nil || !strings.Contains(err.Error(), "resolve queue manager failed") {
		t.Fatalf("expected resolve manager error, got manager=%v err=%v", manager, err)
	}
}

func TestHelperParsingBranches(t *testing.T) {
	if got := splitQueueNames(" default, ,emails "); strings.Join(got, "|") != "default|emails" {
		t.Fatalf("splitQueueNames = %v", got)
	}
	if got := splitQueueNames(""); got != nil {
		t.Fatalf("expected nil queue list, got %v", got)
	}
	if got := parseBackoff("0"); got != nil {
		t.Fatalf("expected nil backoff, got %v", got)
	}
	backoff := parseBackoff("1, bad, 2")
	if len(backoff) != 2 || backoff[0] != time.Second || backoff[1] != 2*time.Second {
		t.Fatalf("unexpected backoff = %v", backoff)
	}
	ctx := commandContext(NewWorkCommand(), queueInput{options: map[string]string{"bad": "nan", "ok": "3"}})
	if got := intOption(commandContext(NewWorkCommand(), queueInput{}), "missing", 9); got != 9 {
		t.Fatalf("empty int option = %d", got)
	}
	if got := intOption(ctx, "bad", 7); got != 7 {
		t.Fatalf("invalid int option = %d", got)
	}
	if got := secondsOption(ctx, "ok", 1); got != 3*time.Second {
		t.Fatalf("seconds option = %v", got)
	}
}

func useTestManager(t *testing.T) *queuecore.Manager {
	t.Helper()
	registryContainer := useQueueTestContainer(t)
	registry := queuecore.NewRegistry()
	queuecore.RegisterTypeTo[*commandJob](registry)
	manager, err := queuecore.NewManager(queuecore.Config{Default: "sync", Encoding: "json"}, registry)
	if err != nil {
		t.Fatalf("new queue manager: %v", err)
	}
	if err := registryContainer.Instance("queue.manager", manager); err != nil {
		t.Fatalf("bind queue manager: %v", err)
	}
	return manager
}

func useRedisTestManager(t *testing.T) *queuecore.Manager {
	t.Helper()
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	containerRegistry := useQueueTestContainer(t)
	redisManager, err := prismredis.NewManager(prismredis.Config{
		DefaultName: "default",
		Connections: map[string]prismredis.ConnectionConfig{
			"default": {Name: "default", Addr: srv.Addr()},
		},
	})
	if err != nil {
		t.Fatalf("new redis manager: %v", err)
	}
	if err := containerRegistry.Instance("redis", redisManager); err != nil {
		t.Fatalf("bind redis manager: %v", err)
	}
	registry := queuecore.NewRegistry()
	queuecore.RegisterTypeTo[*commandJob](registry)
	manager, err := queuecore.NewManager(queuecore.Config{
		Default:  "redis",
		Encoding: "json",
		Failed:   queuecore.StateStoreConfig{Driver: "redis", Store: "default", Prefix: "command_queue_test"},
		Batching: queuecore.StateStoreConfig{Driver: "redis", Store: "default", Prefix: "command_queue_test"},
		Connections: map[string]queuecore.ConnectionConfig{
			"redis": {
				Driver: "redis",
				Options: map[string]any{
					"connection": "default",
					"prefix":     "command_queue_test",
				},
			},
		},
	}, registry)
	if err != nil {
		t.Fatalf("new redis queue manager: %v", err)
	}
	if err := containerRegistry.Instance("queue.manager", manager); err != nil {
		t.Fatalf("bind queue manager: %v", err)
	}
	t.Cleanup(func() {
		_ = manager.Close()
		_ = redisManager.Close(context.Background())
		srv.Close()
	})
	return manager
}

func useQueueTestContainer(t *testing.T) *container.Container {
	t.Helper()
	registry := container.NewContainer()
	container.SetProvider(func() *container.Container { return registry })
	t.Cleanup(func() { container.SetProvider(nil) })
	return registry
}

func runCommand(t *testing.T, cmd console.Command, input queueInput) string {
	t.Helper()
	stdout := &bytes.Buffer{}
	ctx := console.NewCommandContext(context.Background(), cmd, *cmd.Definition(), input, console.NewIO(strings.NewReader(""), stdout, io.Discard), nil, &cobra.Command{Use: cmd.Definition().Name})
	if err := cmd.Handle(ctx); err != nil {
		t.Fatalf("%s returned error: %v", cmd.Definition().Name, err)
	}
	return stdout.String()
}

func assertCommandError(t *testing.T, cmd console.Command, input queueInput, contains string) {
	t.Helper()
	err := cmd.Handle(commandContext(cmd, input))
	if err == nil || !strings.Contains(err.Error(), contains) {
		t.Fatalf("expected %s error containing %q, got %v", cmd.Definition().Name, contains, err)
	}
}

func commandContext(cmd console.Command, input queueInput) console.CommandContext {
	return console.NewCommandContext(context.Background(), cmd, *cmd.Definition(), input, console.NewIO(strings.NewReader(""), io.Discard, io.Discard), nil, &cobra.Command{Use: cmd.Definition().Name})
}

type queueInput struct {
	args    map[string][]string
	options map[string]string
	bools   map[string]bool
}

func (i queueInput) Argument(name string) string {
	values := i.args[name]
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

func (i queueInput) Arguments(name string) []string { return append([]string(nil), i.args[name]...) }
func (i queueInput) Option(name string) string      { return i.options[name] }
func (i queueInput) OptionStrings(name string) []string {
	value := i.options[name]
	if value == "" {
		return nil
	}
	return []string{value}
}
func (i queueInput) OptionBool(name string) bool { return i.bools[name] }
func (i queueInput) OptionInt(name string) int   { return 0 }
func (i queueInput) HasOption(name string) bool  { return i.options[name] != "" || i.bools[name] }
