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
	"github.com/spf13/cobra"
)

func TestStaleCommandReportsEmptyResult(t *testing.T) {
	// 需求背景：没有 heartbeat stale 对象时，诊断命令要给出明确空结果，而不是静默成功。
	cmd := NewStaleCommand(func(context.Context) (Runtime, error) {
		return fakeRuntime{}, nil
	})
	output := &bytes.Buffer{}
	if err := cmd.Handle(testCommandContext(cmd, output)); err != nil {
		t.Fatalf("stale command: %v", err)
	}
	if !strings.Contains(output.String(), "No stale Horizon processes.") {
		t.Fatalf("unexpected output:\n%s", output.String())
	}
}

func TestStaleCommandPropagatesRuntimeErrors(t *testing.T) {
	cmd := NewStaleCommand(func(context.Context) (Runtime, error) {
		return fakeRuntime{mastersErr: errors.New("masters failed")}, nil
	})
	err := cmd.Handle(testCommandContext(cmd, io.Discard))
	if err == nil || !strings.Contains(err.Error(), "masters failed") {
		t.Fatalf("expected masters error, got %v", err)
	}
}

func TestFormatDurationSinceBoundaries(t *testing.T) {
	now := time.Date(2026, 5, 12, 10, 0, 0, 0, time.UTC)
	if got := formatDurationSince(time.Time{}, now); got != "" {
		t.Fatalf("zero start = %q", got)
	}
	if got := formatDurationSince(now.Add(time.Second), now); got != "0s" {
		t.Fatalf("future start = %q", got)
	}
	if got := formatDurationSince(now.Add(-90*time.Second), now); got != "1m30s" {
		t.Fatalf("duration = %q", got)
	}
}

func TestFormatProcessPoolsCoversEmptyAndMultiQueuePools(t *testing.T) {
	if got := formatProcessPools(nil); got != "" {
		t.Fatalf("empty pools = %q", got)
	}
	got := formatProcessPools([]ProcessPoolState{
		{Name: "fixed:all", Queues: []string{"high", "default"}, CurrentWorkers: 1, TargetWorkers: 3},
		{Name: "simple:emails", Queue: "emails", Queues: []string{"emails"}, CurrentWorkers: 2, TargetWorkers: 2},
	})
	if !strings.Contains(got, "high,default:1/3") || !strings.Contains(got, "emails:2/2") {
		t.Fatalf("formatted pools = %q", got)
	}
}

func testCommandContext(cmd console.Command, out io.Writer) console.CommandContext {
	return console.NewCommandContext(context.Background(), cmd, *cmd.Definition(), fakeInput{}, console.NewIO(strings.NewReader(""), out, out), nil, &cobra.Command{Use: cmd.Definition().Name})
}

type fakeRuntime struct {
	mastersErr error
}

func (r fakeRuntime) UsesMemoryStore() bool { return false }
func (r fakeRuntime) StatusSnapshot(context.Context, time.Time) (StatusSnapshot, error) {
	return StatusSnapshot{}, nil
}
func (r fakeRuntime) Masters(context.Context, time.Time) ([]MasterState, error) {
	return nil, r.mastersErr
}
func (r fakeRuntime) Supervisors(context.Context, time.Time) ([]SupervisorState, error) {
	return nil, nil
}
func (r fakeRuntime) Supervisor(context.Context, string, time.Time) (SupervisorState, bool, error) {
	return SupervisorState{}, false, nil
}
func (r fakeRuntime) Workers(context.Context, time.Time) ([]WorkerState, error) { return nil, nil }
func (r fakeRuntime) SetGlobalPaused(context.Context, bool) error               { return nil }
func (r fakeRuntime) SetSupervisorPaused(context.Context, string, bool) error   { return nil }
func (r fakeRuntime) RequestTerminate(context.Context, time.Time, bool) error   { return nil }
func (r fakeRuntime) MaxWorkerTimeout(string) (int, error)                      { return 60, nil }
func (r fakeRuntime) Snapshot(context.Context, time.Time) (SnapshotSummary, error) {
	return SnapshotSummary{}, nil
}
func (r fakeRuntime) ClearMetrics(context.Context) error            { return nil }
func (r fakeRuntime) QueueTargets() []QueueTarget                   { return nil }
func (r fakeRuntime) ClearQueue(context.Context, QueueTarget) error { return nil }
func (r fakeRuntime) ForgetFailedJob(context.Context, string) error { return nil }
func (r fakeRuntime) ForgetAllFailedJobs(context.Context) error     { return nil }
func (r fakeRuntime) Purge(context.Context, time.Time, string) (PurgeSummary, error) {
	return PurgeSummary{}, nil
}
func (r fakeRuntime) RunMaster(context.Context, MasterOptions) error { return nil }
func (r fakeRuntime) RunSupervisor(context.Context, SupervisorProcessOptions) error {
	return nil
}
func (r fakeRuntime) RunWorker(context.Context, WorkerOptions) error { return nil }
func (r fakeRuntime) Listen(context.Context, ListenOptions) (ListenSummary, error) {
	return ListenSummary{}, nil
}

type fakeInput struct{}

func (fakeInput) Argument(string) string        { return "" }
func (fakeInput) Arguments(string) []string     { return nil }
func (fakeInput) Option(string) string          { return "" }
func (fakeInput) OptionStrings(string) []string { return nil }
func (fakeInput) OptionBool(string) bool        { return false }
func (fakeInput) OptionInt(string) (int, error) { return 0, nil }
func (fakeInput) HasOption(string) bool         { return false }
