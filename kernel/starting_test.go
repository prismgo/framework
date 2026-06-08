package kernel

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/prismgo/framework/console"
	"github.com/prismgo/framework/timer"
)

func TestKernelRunsApplicationStartingCallbacksBeforeCommandExecution(t *testing.T) {
	starts := 0
	k := newApplicationKernelForTest(t, "test", nil)
	if err := k.Starting(func(k *Kernel) error {
		starts++
		return k.ResolveCommand(&startingRegisteredCommand{})
	}); err != nil {
		t.Fatalf("Starting() error = %v", err)
	}
	k.rootCmd.SetArgs([]string{"starting:registered"})
	if err := k.RunContext(context.Background()); err != nil {
		t.Fatalf("RunContext() error = %v", err)
	}
	if starts != 1 {
		t.Fatalf("starting callbacks = %d, want 1", starts)
	}

	k.rootCmd.SetArgs([]string{"starting:registered"})
	if err := k.RunContext(context.Background()); err != nil {
		t.Fatalf("second RunContext() error = %v", err)
	}
	if starts != 1 {
		t.Fatalf("starting callbacks after reuse = %d, want 1", starts)
	}
}

func TestKernelResolveCommandsRegistersExplicitCommandsAndFactories(t *testing.T) {
	k := New("test")
	if err := k.ResolveCommands(
		&startingRegisteredCommand{},
		console.CommandFactory(func() console.Command { return namedRegistryCommand("starting:factory") }),
	); err != nil {
		t.Fatalf("ResolveCommands() error = %v", err)
	}

	if _, err := k.resolveRegisteredCommand("starting:registered", nil); err != nil {
		t.Fatalf("registered command was not resolvable: %v", err)
	}
	if _, err := k.resolveRegisteredCommand("starting:factory", nil); err != nil {
		t.Fatalf("factory command was not resolvable: %v", err)
	}
}

func TestKernelResolveCommandReturnsRegistrationErrors(t *testing.T) {
	k := New("test")
	var nilKernel *Kernel
	if err := nilKernel.ResolveCommand(&startingRegisteredCommand{}); err == nil || !strings.Contains(err.Error(), "kernel is nil") {
		t.Fatalf("nil kernel resolve error = %v, want kernel is nil", err)
	}
	if err := k.ResolveCommand(123); err == nil || !strings.Contains(err.Error(), "unsupported type") {
		t.Fatalf("unsupported resolve error = %v, want unsupported type", err)
	}
	if err := k.ResolveCommand(console.CommandFactory(nil)); err == nil || !strings.Contains(err.Error(), "factory is nil") {
		t.Fatalf("nil factory error = %v, want factory is nil", err)
	}
	if err := k.ResolveCommand(console.CommandFactory(func() console.Command { return nil })); err == nil || !strings.Contains(err.Error(), "factory returned nil") {
		t.Fatalf("nil command error = %v, want factory returned nil", err)
	}

	if err := k.ResolveCommand(&startingRegisteredCommand{}); err != nil {
		t.Fatalf("first ResolveCommand() error = %v", err)
	}
	if err := k.ResolveCommand(&startingRegisteredCommand{}); err == nil || !strings.Contains(err.Error(), "already registered") {
		t.Fatalf("duplicate command error = %v, want already registered", err)
	}
	if err := k.ResolveCommands(console.CommandFactory(func() console.Command { return nil }), &startingRegisteredCommand{}); err == nil || !strings.Contains(err.Error(), "factory returned nil") {
		t.Fatalf("ResolveCommands failure error = %v, want factory returned nil", err)
	}
}

func TestKernelStartingCommandAppearsInListAndProgrammaticCall(t *testing.T) {
	k := newApplicationKernelForTest(t, "test", nil)
	if err := k.Starting(func(k *Kernel) error {
		return k.ResolveCommand(&startingRegisteredCommand{})
	}); err != nil {
		t.Fatalf("Starting() error = %v", err)
	}
	buffer := bytes.NewBuffer(nil)
	k.rootCmd.SetOut(buffer)
	k.rootCmd.SetErr(buffer)
	k.rootCmd.SetArgs([]string{"list"})
	if err := k.RunContext(context.Background()); err != nil {
		t.Fatalf("RunContext list error = %v", err)
	}
	if !strings.Contains(buffer.String(), "starting:registered") {
		t.Fatalf("list output missing starting command: %q", buffer.String())
	}

	if err := k.Call(context.Background(), "starting:registered"); err != nil {
		t.Fatalf("Call starting command error = %v", err)
	}
}

func TestKernelApplicationStartingCallbacksRunOnce(t *testing.T) {
	starts := 0
	source := startingRegistryTestSource{starting: []StartingCallback{
		nil,
		func(k *Kernel) error {
			starts++
			return k.ResolveCommand(&startingRegisteredCommand{})
		},
	}}

	k := newApplicationKernelForTest(t, "test", source)
	if err := k.Call(context.Background(), "starting:registered"); err != nil {
		t.Fatalf("Call starting callback error = %v", err)
	}
	if err := k.Call(context.Background(), "starting:registered"); err != nil {
		t.Fatalf("second Call starting callback error = %v", err)
	}
	if starts != 1 {
		t.Fatalf("application starts = %d, want 1", starts)
	}
}

func TestKernelStartingErrorStopsCommandExecution(t *testing.T) {
	startErr := errors.New("starting failed")
	command := &applicationLifecycleCommand{}
	source := startingRegistryTestSource{starting: []StartingCallback{func(*Kernel) error { return startErr }}}

	k := newApplicationKernelForTest(t, "test", source)
	k.Register(command)
	k.rootCmd.SetArgs([]string{"application:lifecycle"})
	if err := k.RunContext(context.Background()); !errors.Is(err, startErr) {
		t.Fatalf("RunContext starting error = %v, want %v", err, startErr)
	}
	if command.ran {
		t.Fatal("command should not run after starting error")
	}
}

func TestKernelStartingCallbackFailureCanRetry(t *testing.T) {
	attempts := 0
	command := &applicationLifecycleCommand{}
	k := newApplicationKernelForTest(t, "test", nil)
	if err := k.Starting(func(k *Kernel) error {
		attempts++
		if attempts == 1 {
			return errors.New("transient starting failure")
		}
		return k.ResolveCommand(&startingRegisteredCommand{})
	}); err != nil {
		t.Fatalf("Starting() error = %v", err)
	}
	k.Register(command)

	if err := k.Call(context.Background(), "starting:registered"); err == nil || !strings.Contains(err.Error(), "transient starting failure") {
		t.Fatalf("first Call() error = %v, want transient starting failure", err)
	}
	if attempts != 1 {
		t.Fatalf("starting attempts after first call = %d, want 1", attempts)
	}

	if err := k.Call(context.Background(), "starting:registered"); err != nil {
		t.Fatalf("second Call() error = %v", err)
	}
	if attempts != 2 {
		t.Fatalf("starting attempts after retry = %d, want 2", attempts)
	}
	if err := k.Starting(func(*Kernel) error { return nil }); err == nil || !strings.Contains(err.Error(), "callbacks already ran") {
		t.Fatalf("Starting() after successful retry error = %v, want callbacks already ran", err)
	}
}

func TestKernelScheduleCommandSupportsStartingRegisteredCommand(t *testing.T) {
	executed := make(chan struct{}, 1)
	k := New("test")
	if err := k.Starting(func(k *Kernel) error {
		return k.ResolveCommand(scheduleStartingCommand{executed: executed})
	}); err != nil {
		t.Fatalf("Starting() error = %v", err)
	}

	k.Schedule().Command("schedule:starting").EveryMinute()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go k.Start(ctx)

	select {
	case <-executed:
		k.Stop()
	case <-time.After(2 * time.Second):
		k.Stop()
		t.Fatal("scheduled command registered during starting did not execute")
	}
}

func TestKernelScheduleCommandStillRejectsUnknownCommandWithoutStartingFallback(t *testing.T) {
	k := New("test")
	defer func() {
		recovered := recover()
		if recovered == nil {
			t.Fatal("expected schedule command registration to panic")
		}
		if !strings.Contains(recovered.(string), "not registered") {
			t.Fatalf("panic = %v, want not registered", recovered)
		}
	}()
	k.Schedule().Command("schedule:missing")
}

type startingRegistryTestSource struct {
	starting []StartingCallback
}

func (s startingRegistryTestSource) CommandFactories() []console.CommandFactory {
	return nil
}

func (s startingRegistryTestSource) StartingCallbacks() []StartingCallback {
	return append([]StartingCallback(nil), s.starting...)
}

func (s startingRegistryTestSource) ScheduleRegistrars() []func(*timer.Schedule) {
	return nil
}

func (s startingRegistryTestSource) MigrationPaths() []string {
	return nil
}

func (s startingRegistryTestSource) SeedPaths() []string {
	return nil
}

type startingRegisteredCommand struct{}

func (c *startingRegisteredCommand) Definition() *console.Definition {
	return console.MustDefinition("starting:registered", "registered during starting")
}

func (c *startingRegisteredCommand) Handle(console.CommandContext) error {
	return nil
}

type scheduleStartingCommand struct {
	executed chan<- struct{}
}

func (c scheduleStartingCommand) Definition() *console.Definition {
	return console.MustDefinition("schedule:starting", "registered during starting for schedule")
}

func (c scheduleStartingCommand) Handle(console.CommandContext) error {
	select {
	case c.executed <- struct{}{}:
	default:
	}
	return nil
}
