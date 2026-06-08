package kernel

import (
	"context"
	"strings"
	"testing"

	"github.com/prismgo/framework/console"
)

func TestKernelRunApplicationDelegatesToApplicationLifecycle(t *testing.T) {
	k := New("test")
	command := &applicationLifecycleCommand{}
	k.Register(command)
	k.rootCmd.SetArgs([]string{"application:lifecycle"})

	application := &kernelLifecycleApplication{}
	if err := k.RunApplication(application); err != nil {
		t.Fatalf("RunApplication() error = %v", err)
	}

	if application.runs != 1 {
		t.Fatalf("application runs = %d, want 1", application.runs)
	}
	if !command.ran {
		t.Fatal("expected command to run inside application lifecycle")
	}
}

func TestKernelRunApplicationValidatesInputsAndPassesExternalContext(t *testing.T) {
	var nilKernel *Kernel
	if err := nilKernel.RunApplication(&kernelLifecycleApplication{}); err == nil || !strings.Contains(err.Error(), "kernel is not initialized") {
		t.Fatalf("nil kernel error = %v, want kernel is not initialized", err)
	}

	k := New("test")
	if err := k.RunApplication(nil); err == nil || !strings.Contains(err.Error(), "application is not initialized") {
		t.Fatalf("nil application error = %v, want application is not initialized", err)
	}

	application := &kernelLifecycleApplication{}
	ctx := context.WithValue(context.Background(), testContextKey{}, "external")
	k.Register(&applicationLifecycleCommand{})
	k.rootCmd.SetArgs([]string{"application:lifecycle"})
	if err := k.RunApplication(application, ctx); err != nil {
		t.Fatalf("RunApplication with context error = %v", err)
	}
	if application.ctx.Value(testContextKey{}) != "external" {
		t.Fatalf("application context value = %v, want external", application.ctx.Value(testContextKey{}))
	}
}

type kernelLifecycleApplication struct {
	runs int
	ctx  context.Context
}

func (a *kernelLifecycleApplication) RunContext(run func(context.Context) error, contexts ...context.Context) error {
	a.runs++
	ctx := context.Background()
	if len(contexts) > 0 && contexts[0] != nil {
		ctx = contexts[0]
	}
	a.ctx = ctx
	return run(ctx)
}

type applicationLifecycleCommand struct {
	ran bool
}

func (c *applicationLifecycleCommand) Definition() *console.Definition {
	return console.MustDefinition("application:lifecycle", "application lifecycle")
}

func (c *applicationLifecycleCommand) Handle(_ console.CommandContext) error {
	c.ran = true
	return nil
}
