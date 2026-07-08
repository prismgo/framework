package cmd

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/prismgo/framework/console"
)

func TestCommandDefinitionsExposeLaravelAlignedSurface(t *testing.T) {
	// 需求背景：batch bulk dispatch contract 收口 Horizon CLI 表面，测试只验证公开 Definition，
	// 避免后续实现重构时悄悄改变 Laravel 对齐的命令参数契约。
	commands := []console.Command{
		NewMasterCommand(nil),
		NewSupervisorProcessCommand(nil),
		NewWorkCommand(nil),
		NewTerminateCommand(nil),
		NewClearCommand(nil),
		NewForgetCommand(nil),
		NewPurgeCommand(nil),
		NewTimeoutCommand(nil),
	}

	defs := map[string]*console.Definition{}
	for _, command := range commands {
		defs[command.Definition().Name] = command.Definition()
	}

	assertOption(t, defs["horizon"], "environment")

	supervisor := defs["horizon:supervisor"]
	assertArgument(t, supervisor, "name", true, "")
	assertArgument(t, supervisor, "connection", true, "")
	for _, option := range []string{"balance", "backoff", "max-jobs", "max-time", "force", "max-processes", "min-processes", "memory", "nice", "paused", "queue", "sleep", "timeout", "tries", "workers-name", "parent-id"} {
		assertOption(t, supervisor, option)
	}

	work := defs["horizon:work"]
	assertArgument(t, work, "connection", false, "")
	for _, option := range []string{"name", "backoff", "max-jobs", "max-time", "force", "memory", "once", "stop-when-empty", "queue", "sleep", "rest", "supervisor", "timeout", "tries", "json"} {
		assertOption(t, work, option)
	}

	clear := defs["horizon:clear"]
	assertArgument(t, clear, "connection", false, "")
	assertOption(t, clear, "queue")
	assertOption(t, clear, "force")

	forget := defs["horizon:forget"]
	assertArgument(t, forget, "id", false, "")
	assertOption(t, forget, "all")

	assertOption(t, defs["horizon:terminate"], "wait")
	assertOptionDefault(t, defs["horizon:purge"], "signal", "SIGTERM")
	assertArgument(t, defs["horizon:timeout"], "environment", false, "production")
}

func TestCommandBindingsReachRuntime(t *testing.T) {
	// 设计思路：命令层只负责稳定解析参数并传给 Runtime 窄接口；
	// 行为细节由 horizon 包 adapter 处理，因此这里使用 fake runtime 验证绑定不丢失。
	runtime := &surfaceRuntime{}
	load := func(context.Context) (Runtime, error) { return runtime, nil }

	supervisor := NewSupervisorProcessCommand(load)
	err := supervisor.Handle(surfaceCommandContext(supervisor, surfaceInput{
		args: map[string][]string{"name": {"supervisor-1"}, "connection": {"redis"}},
		options: map[string]string{
			"queue": "default,emails", "balance": "auto", "backoff": "1,5", "max-jobs": "10", "max-time": "20",
			"max-processes": "5", "min-processes": "2", "memory": "128", "nice": "3", "sleep": "4",
			"timeout": "90", "tries": "2", "workers-name": "worker", "parent-id": "master-1",
		},
		bools: map[string]bool{"force": true, "paused": true},
	}))
	if err != nil {
		t.Fatalf("supervisor command: %v", err)
	}
	if runtime.supervisor.Name != "supervisor-1" || runtime.supervisor.Connection != "redis" || runtime.supervisor.Queue != "default,emails" || !runtime.supervisor.Force || !runtime.supervisor.Paused {
		t.Fatalf("supervisor options were not bound: %#v", runtime.supervisor)
	}

	worker := NewWorkCommand(load)
	err = worker.Handle(surfaceCommandContext(worker, surfaceInput{
		args: map[string][]string{"connection": {"redis"}},
		options: map[string]string{
			"name": "worker-1", "queue": "default", "backoff": "1", "max-jobs": "3", "max-time": "4",
			"memory": "256", "sleep": "2", "rest": "1", "supervisor": "supervisor-1", "timeout": "60", "tries": "5",
		},
		bools: map[string]bool{"force": true, "once": true, "stop-when-empty": true, "json": true},
	}))
	if err != nil {
		t.Fatalf("work command: %v", err)
	}
	if runtime.worker.Connection != "redis" || runtime.worker.Queue != "default" || !runtime.worker.Once || !runtime.worker.JSON || runtime.worker.Rest != 1 {
		t.Fatalf("worker options were not bound: %#v", runtime.worker)
	}

	terminate := NewTerminateCommand(load)
	_ = terminate.Handle(surfaceCommandContext(terminate, surfaceInput{bools: map[string]bool{"wait": true}}))
	if !runtime.terminateWait {
		t.Fatal("terminate --wait was not passed to runtime")
	}

	purge := NewPurgeCommand(load)
	_ = purge.Handle(surfaceCommandContext(purge, surfaceInput{options: map[string]string{"signal": "SIGKILL"}}))
	if runtime.purgeSignal != "SIGKILL" {
		t.Fatalf("purge signal = %q", runtime.purgeSignal)
	}
}

func TestForgetAllAndClearForceBehaviors(t *testing.T) {
	// 测试目的：覆盖两个容易被签名收口遗漏的安全边界：
	// forget --all 走批量删除；clear 在非交互命令入口必须显式 --force。
	runtime := &surfaceRuntime{}
	load := func(context.Context) (Runtime, error) { return runtime, nil }

	forget := NewForgetCommand(load)
	err := forget.Handle(surfaceCommandContext(forget, surfaceInput{bools: map[string]bool{"all": true}}))
	if err != nil {
		t.Fatalf("forget --all: %v", err)
	}
	if !runtime.forgetAll {
		t.Fatal("forget --all did not call runtime bulk delete")
	}

	clear := NewClearCommand(load)
	err = clear.Handle(surfaceCommandContext(clear, surfaceInput{args: map[string][]string{"connection": {"redis"}}, options: map[string]string{"queue": "default"}}))
	if err == nil || !strings.Contains(err.Error(), "--force") {
		t.Fatalf("clear without force should be rejected, got %v", err)
	}
	err = clear.Handle(surfaceCommandContext(clear, surfaceInput{args: map[string][]string{"connection": {"redis"}}, options: map[string]string{"queue": "default"}, bools: map[string]bool{"force": true}}))
	if err != nil {
		t.Fatalf("clear with force: %v", err)
	}
	if runtime.clearTarget.Connection != "redis" || runtime.clearTarget.Queue != "default" {
		t.Fatalf("clear target not bound: %#v", runtime.clearTarget)
	}
}

func TestSupervisorStatusShowsAllSameNameInstances(t *testing.T) {
	// 需求背景：多机部署下同名 supervisor 会有多个运行实例；supervisor-status 不能只展示
	// Runtime.Supervisor(name) 的兼容单实例视图，否则排障时会漏掉同名机器。
	now := time.Date(2026, 5, 19, 13, 0, 0, 0, time.UTC)
	runtime := &surfaceRuntime{supervisors: []SupervisorState{
		{Name: "fixed", Host: "host-new", PID: 202, Status: "running", LastHeartbeatAt: now.Add(time.Second), WorkerCount: 2, Connection: "redis", Queues: []string{"default"}},
		{Name: "other", Host: "host-other", PID: 303, Status: "running", LastHeartbeatAt: now, WorkerCount: 1},
		{Name: "fixed", Host: "host-old", PID: 101, Status: "running", LastHeartbeatAt: now, WorkerCount: 1, Connection: "redis", Queues: []string{"default"}},
	}}
	load := func(context.Context) (Runtime, error) { return runtime, nil }
	command := NewSupervisorStatusCommand(load)
	ctx, output := surfaceCommandContextWithOutput(command, surfaceInput{args: map[string][]string{"name": {"fixed"}}})

	if err := command.Handle(ctx); err != nil {
		t.Fatalf("supervisor status: %v", err)
	}
	text := output.String()
	if !strings.Contains(text, "host-new") || !strings.Contains(text, "host-old") || strings.Contains(text, "host-other") {
		t.Fatalf("supervisor status should show only all matching instances, got:\n%s", text)
	}
	if strings.Index(text, "host-new") > strings.Index(text, "host-old") {
		t.Fatalf("supervisor instances should keep heartbeat-desc order, got:\n%s", text)
	}
}

func TestSupervisorStatusReportsRuntimeErrorAndMissingSupervisor(t *testing.T) {
	// 设计目的：命令层需要区分 Store/Runtime 读取失败和确实没有同名实例，便于脚本调用方
	// 根据错误类型判断是重试还是提示 not found。
	runtimeErr := errors.New("runtime unavailable")
	errorCommand := NewSupervisorStatusCommand(func(context.Context) (Runtime, error) {
		return &surfaceRuntime{supervisorsErr: runtimeErr}, nil
	})
	err := errorCommand.Handle(surfaceCommandContext(errorCommand, surfaceInput{args: map[string][]string{"name": {"fixed"}}}))
	if !errors.Is(err, runtimeErr) {
		t.Fatalf("expected runtime supervisors error, got %v", err)
	}

	missingCommand := NewSupervisorStatusCommand(func(context.Context) (Runtime, error) {
		return &surfaceRuntime{supervisors: []SupervisorState{{Name: "other", Host: "host-1"}}}, nil
	})
	err = missingCommand.Handle(surfaceCommandContext(missingCommand, surfaceInput{args: map[string][]string{"name": {"fixed"}}}))
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("expected not found error, got %v", err)
	}
}

func assertArgument(t *testing.T, def *console.Definition, name string, required bool, defaultValue string) {
	t.Helper()
	for _, arg := range def.Arguments {
		if arg.Name != name {
			continue
		}
		if arg.Required != required {
			t.Fatalf("%s argument %s required=%v, want %v", def.Name, name, arg.Required, required)
		}
		if defaultValue != "" {
			if arg.DefaultValue == nil || *arg.DefaultValue != defaultValue {
				t.Fatalf("%s argument %s default=%v, want %q", def.Name, name, arg.DefaultValue, defaultValue)
			}
		}
		return
	}
	t.Fatalf("%s missing argument %s", def.Name, name)
}

func assertOption(t *testing.T, def *console.Definition, name string) {
	t.Helper()
	for _, option := range def.Options {
		if option.Name == name {
			return
		}
	}
	t.Fatalf("%s missing option %s", def.Name, name)
}

func assertOptionDefault(t *testing.T, def *console.Definition, name string, defaultValue string) {
	t.Helper()
	for _, option := range def.Options {
		if option.Name != name {
			continue
		}
		if option.DefaultValue == nil || *option.DefaultValue != defaultValue {
			t.Fatalf("%s option %s default=%v, want %q", def.Name, name, option.DefaultValue, defaultValue)
		}
		return
	}
	t.Fatalf("%s missing option %s", def.Name, name)
}

type surfaceRuntime struct {
	memory              bool
	statusSnapshot      StatusSnapshot
	statusSnapshotErr   error
	masters             []MasterState
	mastersErr          error
	supervisor          SupervisorProcessOptions
	supervisors         []SupervisorState
	supervisorsErr      error
	workers             []WorkerState
	workersErr          error
	worker              WorkerOptions
	globalPaused        bool
	supervisorPauseName string
	supervisorPaused    bool
	terminateErr        error
	terminateWait       bool
	timeout             int
	timeoutErr          error
	snapshot            SnapshotSummary
	snapshotErr         error
	clearMetricsCalled  bool
	queueTargets        []QueueTarget
	clearTarget         QueueTarget
	forgetID            string
	forgetAll           bool
	purgeSummary        PurgeSummary
	purgeSignal         string
	master              MasterOptions
	listen              ListenOptions
}

func (r *surfaceRuntime) UsesMemoryStore() bool { return r.memory }
func (r *surfaceRuntime) StatusSnapshot(context.Context, time.Time) (StatusSnapshot, error) {
	if r.statusSnapshotErr != nil {
		return StatusSnapshot{}, r.statusSnapshotErr
	}
	return r.statusSnapshot, nil
}
func (r *surfaceRuntime) Masters(context.Context, time.Time) ([]MasterState, error) {
	if r.mastersErr != nil {
		return nil, r.mastersErr
	}
	return append([]MasterState(nil), r.masters...), nil
}
func (r *surfaceRuntime) Supervisors(context.Context, time.Time) ([]SupervisorState, error) {
	if r.supervisorsErr != nil {
		return nil, r.supervisorsErr
	}
	return append([]SupervisorState(nil), r.supervisors...), nil
}
func (r *surfaceRuntime) Supervisor(context.Context, string, time.Time) (SupervisorState, bool, error) {
	return SupervisorState{}, false, nil
}
func (r *surfaceRuntime) Workers(context.Context, time.Time) ([]WorkerState, error) {
	if r.workersErr != nil {
		return nil, r.workersErr
	}
	return append([]WorkerState(nil), r.workers...), nil
}
func (r *surfaceRuntime) SetGlobalPaused(_ context.Context, paused bool) error {
	r.globalPaused = paused
	return nil
}
func (r *surfaceRuntime) SetSupervisorPaused(_ context.Context, name string, paused bool) error {
	r.supervisorPauseName = name
	r.supervisorPaused = paused
	return nil
}
func (r *surfaceRuntime) RequestTerminate(_ context.Context, _ time.Time, wait bool) error {
	r.terminateWait = wait
	return r.terminateErr
}
func (r *surfaceRuntime) MaxWorkerTimeout(string) (int, error) {
	if r.timeoutErr != nil {
		return 0, r.timeoutErr
	}
	if r.timeout != 0 {
		return r.timeout, nil
	}
	return 60, nil
}
func (r *surfaceRuntime) Snapshot(context.Context, time.Time) (SnapshotSummary, error) {
	if r.snapshotErr != nil {
		return SnapshotSummary{}, r.snapshotErr
	}
	return r.snapshot, nil
}
func (r *surfaceRuntime) ClearMetrics(context.Context) error {
	r.clearMetricsCalled = true
	return nil
}
func (r *surfaceRuntime) QueueTargets() []QueueTarget {
	if r.queueTargets != nil {
		return append([]QueueTarget(nil), r.queueTargets...)
	}
	return []QueueTarget{{Connection: "redis", Queue: "default"}}
}
func (r *surfaceRuntime) ClearQueue(_ context.Context, target QueueTarget) error {
	r.clearTarget = target
	return nil
}
func (r *surfaceRuntime) ForgetFailedJob(_ context.Context, id string) error {
	r.forgetID = id
	return nil
}
func (r *surfaceRuntime) ForgetAllFailedJobs(context.Context) error {
	r.forgetAll = true
	return nil
}
func (r *surfaceRuntime) Purge(_ context.Context, _ time.Time, signal string) (PurgeSummary, error) {
	r.purgeSignal = signal
	return r.purgeSummary, nil
}
func (r *surfaceRuntime) RunMaster(_ context.Context, options MasterOptions) error {
	r.master = options
	return nil
}
func (r *surfaceRuntime) RunSupervisor(_ context.Context, options SupervisorProcessOptions) error {
	r.supervisor = options
	return nil
}
func (r *surfaceRuntime) RunWorker(_ context.Context, options WorkerOptions) error {
	r.worker = options
	return nil
}
func (r *surfaceRuntime) Listen(_ context.Context, options ListenOptions) (ListenSummary, error) {
	r.listen = options
	return ListenSummary{WatchPathCount: 2, Starts: 1}, nil
}

type surfaceInput struct {
	args    map[string][]string
	options map[string]string
	bools   map[string]bool
	ints    map[string]int
}

func (i surfaceInput) Argument(name string) string {
	values := i.args[name]
	if len(values) == 0 {
		return ""
	}
	return values[0]
}
func (i surfaceInput) Arguments(name string) []string { return append([]string(nil), i.args[name]...) }
func (i surfaceInput) Option(name string) string      { return i.options[name] }
func (i surfaceInput) OptionStrings(string) []string  { return nil }
func (i surfaceInput) OptionBool(name string) bool    { return i.bools[name] }
func (i surfaceInput) OptionInt(name string) (int, error)      { return i.ints[name], nil }
func (i surfaceInput) HasOption(name string) bool {
	if _, ok := i.options[name]; ok {
		return true
	}
	return i.bools[name]
}

func surfaceCommandContext(command console.Command, input surfaceInput) console.CommandContext {
	return console.NewCommandContext(context.Background(), command, *command.Definition(), input, console.NewIO(strings.NewReader(""), io.Discard, io.Discard), nil, nil)
}

func surfaceCommandContextWithOutput(command console.Command, input surfaceInput) (console.CommandContext, *bytes.Buffer) {
	output := &bytes.Buffer{}
	ctx := console.NewCommandContext(context.Background(), command, *command.Definition(), input, console.NewIO(strings.NewReader(""), output, output), nil, nil)
	return ctx, output
}
