package kernel

import (
	"context"
	"strings"
	"testing"

	"github.com/prismgo/framework/container"
	"github.com/prismgo/framework/event"
	goexception "github.com/prismgo/framework/exception"
	"github.com/prismgo/framework/logger"
)

func TestWithBuiltinsRegistersBuiltinCommandsOnNew(t *testing.T) {
	k := New("test", WithBuiltins(BuiltinDependencies{}))

	assertBuiltinCommandsRegistered(t, k)
}

func TestNewApplicationKernelAcceptsOptionalBuiltinDependencies(t *testing.T) {
	for _, tt := range []struct {
		name string
		k    *Kernel
	}{
		{name: "default dependencies", k: newApplicationKernelForTest(t, "test", nil)},
		{name: "explicit dependencies", k: newApplicationKernelForTest(t, "test", nil, BuiltinDependencies{})},
	} {
		t.Run(tt.name, func(t *testing.T) {
			assertBuiltinCommandsRegistered(t, tt.k)
		})
	}
}

func TestBuiltinCommandFactoriesIncludesStorageLinkCommands(t *testing.T) {
	k := New("test")
	names := map[string]bool{}

	// Built-in factories are the source list used by WithBuiltins and application kernels.
	for _, factory := range BuiltinCommandFactories(k, BuiltinDependencies{}) {
		names[factory().Definition().Name] = true
	}

	for _, name := range []string{"storage:link", "storage:unlink"} {
		if !names[name] {
			t.Fatalf("expected builtin command factory %q to be registered", name)
		}
	}
}

func assertBuiltinCommandsRegistered(t *testing.T, k *Kernel) {
	t.Helper()

	definitions := k.Commands()
	names := map[string]bool{}
	for _, definition := range definitions {
		names[definition.Name] = true
	}

	for _, name := range []string{
		"list",
		"serve",
		"migrate",
		"migrate:install",
		"migrate:status",
		"migrate:rollback",
		"migrate:reset",
		"migrate:refresh",
		"migrate:fresh",
		"db:seed",
		"key:generate",
		"queue",
		"cron",
		"stub:publish",
		"make:command",
		"make:controller",
		"make:model",
		"make:event",
		"make:job",
		"make:listener",
		"make:migration",
		"make:middleware",
		"make:provider",
		"make:resource",
		"make:seeder",
	} {
		if !names[name] {
			t.Fatalf("expected builtin command %q to be registered", name)
		}
	}
}

func TestBuiltInGeneratorRejectsUnsupportedOption(t *testing.T) {
	registry := container.NewContainer()
	container.SetProvider(func() *container.Container { return registry })
	t.Cleanup(func() { container.SetProvider(nil) })
	if err := registry.Instance("event.dispatcher", event.New()); err != nil {
		t.Fatalf("bind event dispatcher: %v", err)
	}
	if err := registry.Instance("exception.handler", goexception.New(goexception.WithPanicStack(false)), container.WithCloseGroup(container.CloseGroupReporting)); err != nil {
		t.Fatalf("bind exception handler: %v", err)
	}
	manager, err := logger.NewManager(logger.Config{
		Default:  "null",
		Channels: map[string]logger.ChannelOptions{"null": {Driver: "null", Level: "debug"}},
	})
	if err != nil {
		t.Fatalf("new logger manager: %v", err)
	}
	if err := registry.Instance("logger.manager", manager, container.WithCloseGroup(container.CloseGroupReporting)); err != nil {
		t.Fatalf("bind logger manager: %v", err)
	}
	k := New("test", WithBuiltins(BuiltinDependencies{}))

	err = k.Call(context.Background(), "make:model User --tenant")
	if err == nil || !strings.Contains(err.Error(), "unknown flag: --tenant") {
		t.Fatalf("Call error = %v, want unknown flag", err)
	}

	err = k.Call(context.Background(), "make:model User --all")
	if err == nil || !strings.Contains(err.Error(), "unsupported option --all") || !strings.Contains(err.Error(), "--migration/-m") {
		t.Fatalf("Call error = %v, want unsupported option with available combinations", err)
	}
}
