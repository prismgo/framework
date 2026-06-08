package kernel

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/prismgo/framework/console"
	"github.com/prismgo/framework/timer"
)

func TestNewApplicationKernelMountsExplicitApplicationRegistry(t *testing.T) {
	source := kernelRegistryTestSource{
		commands: []console.CommandFactory{func() console.Command { return &applicationRegistryCommand{} }},
		schedules: []func(*timer.Schedule){
			func(schedule *timer.Schedule) {
				schedule.Command("registry:run").EveryMinute().Name("registry:run")
			},
		},
		migrationPaths: []string{"database/migrations"},
		seedPaths:      []string{"database/seeders"},
	}

	k := newApplicationKernelForTest(t, "test", source)
	if _, err := k.resolveRegisteredCommand("registry:run", nil); err != nil {
		t.Fatalf("expected application command to be registered: %v", err)
	}
	if summary := k.Schedule().Summary(); !strings.Contains(summary, "registry:run") {
		t.Fatalf("expected application schedule in summary, got %q", summary)
	}
	if got := source.MigrationPaths(); len(got) != 1 || got[0] != "database/migrations" {
		t.Fatalf("migration paths = %v, want [database/migrations]", got)
	}
	if got := source.SeedPaths(); len(got) != 1 || got[0] != "database/seeders" {
		t.Fatalf("seed paths = %v, want [database/seeders]", got)
	}
}

func TestApplicationRegistryIsExplicitPerKernel(t *testing.T) {
	first := kernelRegistryTestSource{
		commands: []console.CommandFactory{func() console.Command { return namedRegistryCommand("first:run") }},
	}
	second := kernelRegistryTestSource{
		commands: []console.CommandFactory{func() console.Command { return namedRegistryCommand("second:run") }},
	}

	k := newApplicationKernelForTest(t, "test", second)
	if _, err := k.resolveRegisteredCommand("second:run", nil); err != nil {
		t.Fatalf("expected active source command to be registered: %v", err)
	}
	if _, err := k.resolveRegisteredCommand("first:run", nil); err == nil {
		t.Fatal("expected unrelated source command to stay out of the kernel")
	}

	k = newApplicationKernelForTest(t, "test", first)
	if _, err := k.resolveRegisteredCommand("first:run", nil); err != nil {
		t.Fatalf("expected first source command after explicit construction: %v", err)
	}
	if _, err := k.resolveRegisteredCommand("second:run", nil); err == nil {
		t.Fatal("expected second source command to stay out")
	}
}

func TestRegisterApplicationPanicsOnNilKernel(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()
	RegisterApplication(nil, nil)
}

func TestApplicationHTTPServerSource(t *testing.T) {
	source := kernelHTTPRegistryTestSource{}

	server, err := ApplicationNewHTTPServer(source, context.Background(), "8051")
	if err != nil {
		t.Fatalf("ApplicationNewHTTPServer returned error: %v", err)
	}
	if server.Addr != ":8051" {
		t.Fatalf("server Addr = %q, want :8051", server.Addr)
	}
	if err := ApplicationLoadHTTPRoutes(source); err != nil {
		t.Fatalf("ApplicationLoadHTTPRoutes returned error: %v", err)
	}

	if _, err := ApplicationNewHTTPServer(nil, context.Background(), "8051"); err == nil {
		t.Fatal("ApplicationNewHTTPServer without source should fail")
	}
	if err := ApplicationLoadHTTPRoutes(nil); err == nil {
		t.Fatal("ApplicationLoadHTTPRoutes without source should fail")
	}
}

type applicationRegistryCommand struct{}

func (c *applicationRegistryCommand) Definition() *console.Definition {
	return console.MustDefinition("registry:run", "registry")
}

func (c *applicationRegistryCommand) Handle(_ console.CommandContext) error {
	return nil
}

type kernelRegistryTestSource struct {
	commands       []console.CommandFactory
	starting       []StartingCallback
	schedules      []func(*timer.Schedule)
	migrationPaths []string
	seedPaths      []string
}

type kernelHTTPRegistryTestSource struct {
	kernelRegistryTestSource
}

func (kernelHTTPRegistryTestSource) NewHTTPServer(_ context.Context, port string) (*http.Server, error) {
	return &http.Server{Addr: ":" + port}, nil
}

func (kernelHTTPRegistryTestSource) LoadHTTPRoutes() error {
	return nil
}

func (s kernelRegistryTestSource) CommandFactories() []console.CommandFactory {
	return append([]console.CommandFactory(nil), s.commands...)
}

func (s kernelRegistryTestSource) StartingCallbacks() []StartingCallback {
	return append([]StartingCallback(nil), s.starting...)
}

func (s kernelRegistryTestSource) ScheduleRegistrars() []func(*timer.Schedule) {
	return append([]func(*timer.Schedule){}, s.schedules...)
}

func (s kernelRegistryTestSource) MigrationPaths() []string {
	return append([]string(nil), s.migrationPaths...)
}

func (s kernelRegistryTestSource) SeedPaths() []string {
	return append([]string(nil), s.seedPaths...)
}

type namedRegistryCommand string

func (c namedRegistryCommand) Definition() *console.Definition {
	return console.MustDefinition(string(c), "registry")
}

func (c namedRegistryCommand) Handle(_ console.CommandContext) error {
	return nil
}
