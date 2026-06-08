package kernel

import (
	"context"
	"strings"
	"testing"
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
	k := New("test", WithBuiltins(BuiltinDependencies{}))

	err := k.Call(context.Background(), "make:model User --tenant")
	if err == nil || !strings.Contains(err.Error(), "unknown flag: --tenant") {
		t.Fatalf("Call error = %v, want unknown flag", err)
	}

	err = k.Call(context.Background(), "make:model User --all")
	if err == nil || !strings.Contains(err.Error(), "unsupported option --all") || !strings.Contains(err.Error(), "--migration/-m") {
		t.Fatalf("Call error = %v, want unsupported option with available combinations", err)
	}
}
