package cmd

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestRuntimeCommandConfigurationAndMemoryWarningBranches(t *testing.T) {
	command := NewStatusCommand(func(context.Context) (Runtime, error) {
		return nil, nil
	})
	err := command.Handle(surfaceCommandContext(command, surfaceInput{}))
	if !errors.Is(err, ErrRuntimeNotConfigured) {
		t.Fatalf("nil runtime error = %v", err)
	}

	loadErr := errors.New("load failed")
	command = NewStatusCommand(func(context.Context) (Runtime, error) {
		return nil, loadErr
	})
	err = command.Handle(surfaceCommandContext(command, surfaceInput{}))
	if !errors.Is(err, loadErr) {
		t.Fatalf("loader error = %v", err)
	}

	runtime := &surfaceRuntime{memory: true, statusSnapshot: StatusSnapshot{Status: "running"}}
	command = NewStatusCommand(func(context.Context) (Runtime, error) {
		return runtime, nil
	})
	ctx, output := surfaceCommandContextWithOutput(command, surfaceInput{})
	if err := command.Handle(ctx); err != nil {
		t.Fatalf("memory status command: %v", err)
	}
	if !strings.Contains(output.String(), MemoryStoreWarning) {
		t.Fatalf("expected memory warning, got:\n%s", output.String())
	}

	var nilCommand *runtimeCommand
	if err := nilCommand.Handle(ctx); !errors.Is(err, ErrRuntimeNotConfigured) {
		t.Fatalf("nil command error = %v", err)
	}
}

func TestStatusSnapshotOutputsCountsAndFailsInactiveStates(t *testing.T) {
	runtime := &surfaceRuntime{statusSnapshot: StatusSnapshot{
		Status:               "running",
		GlobalPaused:         true,
		TerminateRequested:   true,
		SupervisorCount:      2,
		WorkerCount:          6,
		StaleSupervisorCount: 1,
		StaleWorkerCount:     3,
	}}
	command := NewStatusCommand(func(context.Context) (Runtime, error) { return runtime, nil })
	ctx, output := surfaceCommandContextWithOutput(command, surfaceInput{})

	if err := command.Handle(ctx); err != nil {
		t.Fatalf("status command: %v", err)
	}
	for _, want := range []string{"Status: running", "Global Paused: true", "Terminate Requested: true", "Supervisors: 2", "Workers: 6", "Stale Supervisors: 1", "Stale Workers: 3"} {
		if !strings.Contains(output.String(), want) {
			t.Fatalf("status output missing %q:\n%s", want, output.String())
		}
	}

	runtime.statusSnapshot.Status = "paused"
	err := command.Handle(surfaceCommandContext(command, surfaceInput{}))
	if err != nil {
		t.Fatalf("paused status should succeed, got %v", err)
	}
}

func TestPauseContinueAndSupervisorPauseCommandsBindRuntimeFlags(t *testing.T) {
	runtime := &surfaceRuntime{}
	load := func(context.Context) (Runtime, error) { return runtime, nil }

	pause := NewPauseCommand(load)
	if err := pause.Handle(surfaceCommandContext(pause, surfaceInput{})); err != nil {
		t.Fatalf("pause: %v", err)
	}
	if !runtime.globalPaused {
		t.Fatal("pause did not set global paused")
	}

	cont := NewContinueCommand(load)
	if err := cont.Handle(surfaceCommandContext(cont, surfaceInput{})); err != nil {
		t.Fatalf("continue: %v", err)
	}
	if runtime.globalPaused {
		t.Fatal("continue did not clear global paused")
	}

	pauseSupervisor := NewPauseSupervisorCommand(load)
	if err := pauseSupervisor.Handle(surfaceCommandContext(pauseSupervisor, surfaceInput{args: map[string][]string{"name": {" supervisor-a "}}})); err != nil {
		t.Fatalf("pause supervisor: %v", err)
	}
	if runtime.supervisorPauseName != "supervisor-a" || !runtime.supervisorPaused {
		t.Fatalf("pause supervisor flags = name %q paused %v", runtime.supervisorPauseName, runtime.supervisorPaused)
	}

	continueSupervisor := NewContinueSupervisorCommand(load)
	if err := continueSupervisor.Handle(surfaceCommandContext(continueSupervisor, surfaceInput{args: map[string][]string{"name": {"supervisor-a"}}})); err != nil {
		t.Fatalf("continue supervisor: %v", err)
	}
	if runtime.supervisorPauseName != "supervisor-a" || runtime.supervisorPaused {
		t.Fatalf("continue supervisor flags = name %q paused %v", runtime.supervisorPauseName, runtime.supervisorPaused)
	}
}

func TestMaintenanceCommandsBindRuntimeAndOutputSummaries(t *testing.T) {
	runtime := &surfaceRuntime{
		timeout:      91,
		purgeSummary: PurgeSummary{OrphansDiscovered: 3, TerminateRequests: 2, OrphansForgotten: 1},
	}
	load := func(context.Context) (Runtime, error) { return runtime, nil }

	master := NewMasterCommand(load)
	if err := master.Handle(surfaceCommandContext(master, surfaceInput{options: map[string]string{"environment": "staging"}})); err != nil {
		t.Fatalf("master: %v", err)
	}
	if runtime.master.Environment != "staging" {
		t.Fatalf("master environment = %q", runtime.master.Environment)
	}

	clearMetrics := NewClearMetricsCommand(load)
	if err := clearMetrics.Handle(surfaceCommandContext(clearMetrics, surfaceInput{})); err != nil {
		t.Fatalf("clear metrics: %v", err)
	}
	if !runtime.clearMetricsCalled {
		t.Fatal("clear metrics did not call runtime")
	}

	timeoutCommand := NewTimeoutCommand(load)
	ctx, timeoutOutput := surfaceCommandContextWithOutput(timeoutCommand, surfaceInput{args: map[string][]string{"environment": {"production"}}})
	if err := timeoutCommand.Handle(ctx); err != nil {
		t.Fatalf("timeout: %v", err)
	}
	if !strings.Contains(timeoutOutput.String(), "91") {
		t.Fatalf("timeout output = %q", timeoutOutput.String())
	}

	forget := NewForgetCommand(load)
	if err := forget.Handle(surfaceCommandContext(forget, surfaceInput{args: map[string][]string{"id": {" failed-1 "}}})); err != nil {
		t.Fatalf("forget one: %v", err)
	}
	if runtime.forgetID != "failed-1" {
		t.Fatalf("forget id = %q", runtime.forgetID)
	}
	err := forget.Handle(surfaceCommandContext(forget, surfaceInput{}))
	if err == nil || !strings.Contains(err.Error(), "id is required") {
		t.Fatalf("forget without id error = %v", err)
	}

	purge := NewPurgeCommand(load)
	ctx, purgeOutput := surfaceCommandContextWithOutput(purge, surfaceInput{options: map[string]string{"signal": "SIGQUIT"}})
	if err := purge.Handle(ctx); err != nil {
		t.Fatalf("purge: %v", err)
	}
	for _, want := range []string{"Orphans Discovered: 3", "Terminate Requests: 2", "Orphans Forgotten: 1"} {
		if !strings.Contains(purgeOutput.String(), want) {
			t.Fatalf("purge output missing %q:\n%s", want, purgeOutput.String())
		}
	}
}

func TestSnapshotCommandOutputsFlushDetailsAndSkippedCounts(t *testing.T) {
	runtime := &surfaceRuntime{snapshot: SnapshotSummary{
		CapturedAt:             time.Date(2026, 5, 25, 12, 0, 0, 0, time.UTC),
		QueueLengthStatus:      SnapshotStatusSkipped,
		MetricsStatus:          SnapshotStatusEnabled,
		BucketCount:            7,
		FlushStatus:            "ok",
		FlushWindowCount:       4,
		FlushDetailCount:       5,
		FlushDiagnosticCount:   6,
		FlushBatchSummaryCount: 8,
		FlushDropCount:         9,
		FlushQuality:           "complete",
		FlushDegraded:          true,
		Totals:                 MetricsCounters{Processed: 10, Failed: 1, Released: 2, PoisonEnvelopes: 3},
	}}
	command := NewSnapshotCommand(func(context.Context) (Runtime, error) { return runtime, nil })
	ctx, output := surfaceCommandContextWithOutput(command, surfaceInput{})

	if err := command.Handle(ctx); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	for _, want := range []string{"Snapshot At: 2026-05-25T12:00:00Z", "Queue Lengths: skipped", "Flush Status: ok", "Flush Windows: 4", "Flush Quality: complete", "Flush Degraded: true", "Buckets: 7", "Processed: 10", "Poison Envelopes: 3"} {
		if !strings.Contains(output.String(), want) {
			t.Fatalf("snapshot output missing %q:\n%s", want, output.String())
		}
	}
}

func TestSupervisorsCommandSortsRowsAndReportsEmpty(t *testing.T) {
	runtime := &surfaceRuntime{supervisors: []SupervisorState{
		{Name: "zeta", PID: 20, Status: "running", WorkerCount: 2, Host: "host-z", Connection: "redis", Queues: []string{"low"}, LastHeartbeatAt: time.Date(2026, 5, 25, 12, 1, 0, 0, time.UTC)},
		{Name: "alpha", PID: 10, Status: "paused", WorkerCount: 1, Host: "host-a", Connection: "redis", Queues: []string{"default"}, Pools: []ProcessPoolState{{Queue: "default", CurrentWorkers: 1, TargetWorkers: 3}}},
	}}
	command := NewSupervisorsCommand(func(context.Context) (Runtime, error) { return runtime, nil })
	ctx, output := surfaceCommandContextWithOutput(command, surfaceInput{})

	if err := command.Handle(ctx); err != nil {
		t.Fatalf("supervisors: %v", err)
	}
	text := output.String()
	if !strings.Contains(text, "alpha") || !strings.Contains(text, "default:1/3") || strings.Index(text, "alpha") > strings.Index(text, "zeta") {
		t.Fatalf("supervisors output not sorted/formatted:\n%s", text)
	}

	runtime.supervisors = nil
	ctx, output = surfaceCommandContextWithOutput(command, surfaceInput{})
	if err := command.Handle(ctx); err != nil {
		t.Fatalf("empty supervisors: %v", err)
	}
	if !strings.Contains(output.String(), "No supervisors are running.") {
		t.Fatalf("empty supervisors output:\n%s", output.String())
	}
}

func TestStaleRowsIncludesAllStaleProcessTypesAndPropagatesWorkerError(t *testing.T) {
	now := time.Date(2026, 5, 25, 12, 0, 0, 0, time.UTC)
	runtime := &surfaceRuntime{
		masters:     []MasterState{{ID: "master-b", PID: 11, Host: "host-m", Status: "stale", LastHeartbeatAt: now.Add(-2 * time.Minute)}},
		supervisors: []SupervisorState{{Name: "super-a", PID: 22, Host: "host-s", Status: "stale", LastHeartbeatAt: now.Add(-3 * time.Minute)}},
		workers:     []WorkerState{{ID: "worker-c", PID: 33, Host: "host-w", Status: "stale", LastHeartbeatAt: now.Add(-4 * time.Minute)}},
	}
	rows, err := staleRows(surfaceCommandContext(NewStaleCommand(nil), surfaceInput{}), runtime, now)
	if err != nil {
		t.Fatalf("stale rows: %v", err)
	}
	got := strings.Join([]string{strings.Join(rows[0], "|"), strings.Join(rows[1], "|"), strings.Join(rows[2], "|")}, "\n")
	for _, want := range []string{"master|master-b|11|host-m|stale", "supervisor|super-a|22|host-s|stale", "worker|worker-c|33|host-w|stale"} {
		if !strings.Contains(got, want) {
			t.Fatalf("stale rows missing %q:\n%s", want, got)
		}
	}

	workerErr := errors.New("workers failed")
	runtime.workersErr = workerErr
	if _, err := staleRows(surfaceCommandContext(NewStaleCommand(nil), surfaceInput{}), runtime, now); !errors.Is(err, workerErr) {
		t.Fatalf("worker error = %v", err)
	}
}

func TestTerminateCommandCoversNoProcessAndRuntimeNoProcessSentinel(t *testing.T) {
	runtime := &surfaceRuntime{}
	command := NewTerminateCommand(func(context.Context) (Runtime, error) { return runtime, nil })
	ctx, output := surfaceCommandContextWithOutput(command, surfaceInput{})

	if err := command.Handle(ctx); err != nil {
		t.Fatalf("terminate without processes: %v", err)
	}
	if !strings.Contains(output.String(), "No processes to terminate.") {
		t.Fatalf("terminate no-process output:\n%s", output.String())
	}

	runtime.masters = []MasterState{{ID: "master-1", PID: 101, Status: "running"}}
	ctx, output = surfaceCommandContextWithOutput(command, surfaceInput{options: map[string]string{"wait": "yes"}})
	if err := command.Handle(ctx); err != nil {
		t.Fatalf("terminate with process: %v", err)
	}
	if !runtime.terminateWait || !strings.Contains(output.String(), "Horizon termination requested.") {
		t.Fatalf("terminate success wait=%v output:\n%s", runtime.terminateWait, output.String())
	}

	runtime.terminateErr = ErrNoProcessesToTerminate
	ctx, output = surfaceCommandContextWithOutput(command, surfaceInput{})
	if err := command.Handle(ctx); err != nil {
		t.Fatalf("terminate sentinel: %v", err)
	}
	if !strings.Contains(output.String(), "No processes to terminate.") {
		t.Fatalf("terminate sentinel output:\n%s", output.String())
	}
}

func TestQueueTargetResolutionBranches(t *testing.T) {
	targets := []QueueTarget{
		{Connection: "redis", Queue: "default"},
		{Connection: "redis", Queue: "emails"},
		{Connection: "sqs", Queue: "default"},
	}
	if got := formatQueueTargets(targets); got != "redis:default,redis:emails,sqs:default" {
		t.Fatalf("formatted targets = %q", got)
	}
	if _, err := resolveQueueTarget(nil, "", ""); err == nil || !strings.Contains(err.Error(), "no monitored") {
		t.Fatalf("empty targets error = %v", err)
	}
	if _, err := resolveQueueTarget(targets, "missing", ""); err == nil || !strings.Contains(err.Error(), "connection") {
		t.Fatalf("missing connection error = %v", err)
	}
	if _, err := resolveQueueTarget(targets, "redis", "missing"); err == nil || !strings.Contains(err.Error(), "queue") {
		t.Fatalf("missing queue error = %v", err)
	}
	if _, err := resolveQueueTarget(targets, "redis", ""); err == nil || !strings.Contains(err.Error(), "ambiguous") {
		t.Fatalf("ambiguous connection error = %v", err)
	}
	target, err := resolveQueueTarget(targets, "redis", "emails")
	if err != nil {
		t.Fatalf("exact target: %v", err)
	}
	if target.Connection != "redis" || target.Queue != "emails" {
		t.Fatalf("exact target = %#v", target)
	}
}

func TestOutputHelpersAndOptionFallbacks(t *testing.T) {
	if got := supervisorStateHeaders(); len(got) == 0 || got[0] != "Name" {
		t.Fatalf("headers = %#v", got)
	}
	rows := supervisorStateRows([]SupervisorState{{Name: "sup", PID: 1, Queues: []string{"a", "b"}}})
	if rows[0][0] != "sup" || rows[0][8] != "a,b" {
		t.Fatalf("supervisor rows = %#v", rows)
	}

	command := NewStatusCommand(func(context.Context) (Runtime, error) {
		return &surfaceRuntime{statusSnapshot: StatusSnapshot{Status: "running"}}, nil
	})
	ctx := surfaceCommandContext(command, surfaceInput{options: map[string]string{"bad": "not-int", "good": "42", "second": "value"}})
	if got := intCommandOption(ctx, "missing", 7); got != 7 {
		t.Fatalf("missing int option = %d", got)
	}
	if got := intCommandOption(ctx, "bad", 7); got != 7 {
		t.Fatalf("bad int option = %d", got)
	}
	if got := intCommandOption(ctx, "good", 7); got != 42 {
		t.Fatalf("good int option = %d", got)
	}
	if got := firstCommandOption(ctx, "first", "second"); got != "value" {
		t.Fatalf("first command option = %q", got)
	}
	if boolCommandOption(surfaceCommandContext(command, surfaceInput{options: map[string]string{"flag": "on"}}), "flag") != true {
		t.Fatal("bool string option was not true")
	}
}

func TestCommandRuntimeErrorsPropagate(t *testing.T) {
	statusErr := errors.New("status failed")
	command := NewStatusCommand(func(context.Context) (Runtime, error) {
		return &surfaceRuntime{statusSnapshotErr: statusErr}, nil
	})
	if err := command.Handle(surfaceCommandContext(command, surfaceInput{})); !errors.Is(err, statusErr) {
		t.Fatalf("status error = %v", err)
	}

	snapshotErr := errors.New("snapshot failed")
	snapshot := NewSnapshotCommand(func(context.Context) (Runtime, error) {
		return &surfaceRuntime{snapshotErr: snapshotErr}, nil
	})
	if err := snapshot.Handle(surfaceCommandContext(snapshot, surfaceInput{})); !errors.Is(err, snapshotErr) {
		t.Fatalf("snapshot error = %v", err)
	}

	timeoutErr := errors.New("timeout failed")
	timeout := NewTimeoutCommand(func(context.Context) (Runtime, error) {
		return &surfaceRuntime{timeoutErr: timeoutErr}, nil
	})
	if err := timeout.Handle(surfaceCommandContext(timeout, surfaceInput{})); !errors.Is(err, timeoutErr) {
		t.Fatalf("timeout error = %v", err)
	}

	mastersErr := errors.New("masters failed")
	terminate := NewTerminateCommand(func(context.Context) (Runtime, error) {
		return &surfaceRuntime{mastersErr: mastersErr}, nil
	})
	if err := terminate.Handle(surfaceCommandContext(terminate, surfaceInput{})); !errors.Is(err, mastersErr) {
		t.Fatalf("terminate masters error = %v", err)
	}

	supervisorsErr := errors.New("supervisors failed")
	if _, err := staleRows(surfaceCommandContext(NewStaleCommand(nil), surfaceInput{}), &surfaceRuntime{supervisorsErr: supervisorsErr}, time.Now()); !errors.Is(err, supervisorsErr) {
		t.Fatalf("stale supervisors error = %v", err)
	}

	if got := formatDurationSince(time.Now().Add(-time.Second), time.Time{}); got == "" {
		t.Fatal("duration with zero now should use current time")
	}
	if got := formatSnapshotCount(SnapshotStatusEnabled, 12); got != "12" {
		t.Fatalf("snapshot enabled count = %q", got)
	}
}
