package kernel

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prismgo/framework/console"
)

func TestKernelScheduleStartStopAndRun(t *testing.T) {
	useKernelTestContainer(t)
	k := New("test")
	var calls atomic.Int32
	k.Schedule().Call(func(context.Context) error {
		calls.Add(1)
		return nil
	}).Every(10 * time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	k.Start(ctx)
	time.Sleep(25 * time.Millisecond)
	cancel()
	k.Stop()
	if calls.Load() == 0 {
		t.Fatal("expected scheduled callback to run at least once")
	}

	k2 := New("test")
	k2.Register(&runOnlyCommand{})
	k2.rootCmd.SetArgs([]string{"run:only"})
	if err := k2.Run(); err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
}

func TestKernelRegisterClosureRejectsNilAndDefinitionValidation(t *testing.T) {
	k := New("test")
	defer func() {
		if recover() == nil {
			t.Fatal("expected RegisterClosure to panic for nil handler")
		}
	}()
	k.RegisterClosure(console.Definition{Name: "broken"}, nil)
}

func TestKernelRegisterClosureRejectsOptionalArgumentBeforeRequired(t *testing.T) {
	k := New("test")
	defer func() {
		recovered := recover()
		if recovered == nil {
			t.Fatal("expected RegisterClosure to panic for invalid argument order")
		}
		err, ok := recovered.(error)
		if !ok || !strings.Contains(err.Error(), "required argument") {
			t.Fatalf("panic = %v, want required argument validation", recovered)
		}
	}()

	k.RegisterClosure(console.Definition{
		Name: "broken:arguments",
		Arguments: []console.Argument{
			{Name: "first"},
			{Name: "second", Required: true},
		},
	}, func(console.CommandContext) error { return nil })
}

func TestKernelArgumentValidatorRejectsMissingRequiredArgument(t *testing.T) {
	useKernelTestContainer(t)
	k := New("test")
	cmd := &validatorCommand{}
	k.Register(cmd)
	k.rootCmd.SetArgs([]string{"validator:run"})
	if err := k.RunContext(context.Background()); err == nil {
		t.Fatal("expected RunContext to reject missing required argument")
	}
}

type runOnlyCommand struct{}

func (c *runOnlyCommand) Definition() *console.Definition {
	return console.MustDefinition("run:only", "run only")
}

func (c *runOnlyCommand) Handle(_ console.CommandContext) error { return nil }

type validatorCommand struct{}

func (c *validatorCommand) Definition() *console.Definition {
	return console.MustDefinition("validator:run {tenant}", "validator")
}

func (c *validatorCommand) Handle(_ console.CommandContext) error { return nil }
