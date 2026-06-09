package provider

import (
	"context"
	"strings"
	"testing"

	"github.com/prismgo/framework/console"
	"github.com/prismgo/framework/container"
	contractprovider "github.com/prismgo/framework/contracts/provider"
	"github.com/prismgo/framework/event"
	"github.com/prismgo/framework/kernel"
	"github.com/prismgo/framework/timer"
)

func TestDefaultProvidersOrderAndFreshSlice(t *testing.T) {
	got := DefaultProviders()
	want := []string{"redis", "cache", "queue", "cookie", "session", "filesystem", "database", "database.schema", "route"}
	if len(got) != len(want) {
		t.Fatalf("DefaultProviders length = %d, want %d", len(got), len(want))
	}
	for i, provider := range got {
		named, ok := provider.(interface{ Name() string })
		if !ok {
			t.Fatalf("provider[%d] does not expose Name()", i)
		}
		if named.Name() != want[i] {
			t.Fatalf("provider[%d] name = %q, want %q", i, named.Name(), want[i])
		}
	}

	got[0] = nil
	next := DefaultProviders()
	if next[0] == nil {
		t.Fatal("DefaultProviders should return a fresh slice")
	}
}

func TestProviderContractAliasesCompile(t *testing.T) {
	var _ ServiceProvider = aliasProbe{}
	var _ NamedProvider = aliasProbe{}
	var _ DeferrableProvider = aliasProbe{}
	var _ TerminableProvider = aliasProbe{}
	var _ contractprovider.TerminableProvider = aliasProbe{}
}

func TestCommandsRegistersProviderCommandsDuringKernelStarting(t *testing.T) {
	// 需求背景：historical scenario 10 要求 provider 可用 Laravel commands([...]) 风格声明 console 命令，
	// 并在 Console Kernel starting 阶段复用 ResolveCommands 的 Definition、重复命令和 alias 校验。
	source := providerKernelSource{}
	bindProviderStartingRegistrar(t, &source)

	if err := Commands(
		providerCommand("provider:instance"),
		console.CommandFactory(func() console.Command { return providerCommand("provider:factory") }),
	); err != nil {
		t.Fatalf("provider Commands() error = %v", err)
	}

	appKernel := kernel.New("test", kernel.WithApplicationRegistry(&source))
	if err := appKernel.Call(context.Background(), "provider:instance"); err != nil {
		t.Fatalf("provider instance command should run after starting: %v", err)
	}
	if err := appKernel.Call(context.Background(), "provider:factory"); err != nil {
		t.Fatalf("provider factory command should run after starting: %v", err)
	}
}

func TestCommandsRegisteredAfterKernelCreationStillRunDuringStarting(t *testing.T) {
	source := providerKernelSource{}
	bindProviderStartingRegistrar(t, &source)

	appKernel := kernel.New("test", kernel.WithApplicationRegistry(&source))
	if err := Commands(
		providerCommand("provider:late"),
		console.CommandFactory(func() console.Command { return providerCommand("provider:late-factory") }),
	); err != nil {
		t.Fatalf("late provider Commands() error = %v", err)
	}

	if err := appKernel.Call(context.Background(), "provider:late"); err != nil {
		t.Fatalf("late provider instance command should run after starting: %v", err)
	}
	if err := appKernel.Call(context.Background(), "provider:late-factory"); err != nil {
		t.Fatalf("late provider factory command should run after starting: %v", err)
	}
}

func TestCommandsRejectsNilAndUnsupportedDeclarations(t *testing.T) {
	if err := Commands(nil); err == nil || !strings.Contains(err.Error(), "command is nil") {
		t.Fatalf("Commands(nil) error = %v, want command is nil", err)
	}
	var nilCommand *nilProviderCommand
	if err := Commands(nilCommand); err == nil || !strings.Contains(err.Error(), "command is nil") {
		t.Fatalf("Commands(typed nil command) error = %v, want command is nil", err)
	}
	if err := Commands("provider:class-name"); err == nil || !strings.Contains(err.Error(), "unsupported type string") {
		t.Fatalf("Commands(string) error = %v, want unsupported type", err)
	}
	var factory console.CommandFactory
	if err := Commands(factory); err == nil || !strings.Contains(err.Error(), "factory is nil") {
		t.Fatalf("Commands(nil factory) error = %v, want factory is nil", err)
	}
}

type providerCommand string

type providerKernelSource struct {
	starting []kernel.StartingCallback
}

func (s *providerKernelSource) CommandFactories() []console.CommandFactory { return nil }

func (s *providerKernelSource) StartingCallbacks() []kernel.StartingCallback {
	return append([]kernel.StartingCallback(nil), s.starting...)
}

func (s *providerKernelSource) ScheduleRegistrars() []func(*timer.Schedule) { return nil }

func (s *providerKernelSource) MigrationPaths() []string { return nil }

func (s *providerKernelSource) SeedPaths() []string { return nil }

func bindProviderStartingRegistrar(t *testing.T, source *providerKernelSource) {
	t.Helper()
	registry := container.NewContainer()
	container.SetProvider(func() *container.Container { return registry })
	t.Cleanup(func() { container.SetProvider(nil) })
	if err := registry.Instance("event.dispatcher", event.New()); err != nil {
		t.Fatalf("bind event dispatcher: %v", err)
	}
	if err := registry.Instance(kernel.StartingRegistrarKey, kernel.StartingRegistrar(func(callbacks ...kernel.StartingCallback) error {
		source.starting = append(source.starting, callbacks...)
		return nil
	})); err != nil {
		t.Fatalf("bind starting registrar: %v", err)
	}
}

type aliasProbe struct{}

func (aliasProbe) Name() string { return "alias.probe" }

func (aliasProbe) Provides() []string { return []string{"alias.probe"} }

func (aliasProbe) Register(contractprovider.Application) error { return nil }

func (aliasProbe) Boot(contractprovider.Application) error { return nil }

func (aliasProbe) Terminate(context.Context) error { return nil }

func (c providerCommand) Definition() *console.Definition {
	return console.MustDefinition(string(c), "provider command")
}

func (c providerCommand) Handle(console.CommandContext) error {
	return nil
}

type nilProviderCommand struct{}

func (c *nilProviderCommand) Definition() *console.Definition {
	return console.MustDefinition("provider:nil", "provider nil")
}

func (c *nilProviderCommand) Handle(console.CommandContext) error {
	return nil
}
