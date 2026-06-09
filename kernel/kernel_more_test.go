package kernel

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/prismgo/framework/cache"
	"github.com/prismgo/framework/console"
)

func TestKernelRegisterLazyAndCommandsSnapshot(t *testing.T) {
	k := New("test")
	k.RegisterLazy(func() console.Command { return &lazyCommand{} })

	definitions := k.Commands()
	found := false
	for _, definition := range definitions {
		if definition.Name == "lazy:run" {
			found = true
		}
	}
	if !found {
		t.Fatal("expected lazy:run to appear in Commands snapshot")
	}
}

func TestKernelAllRunsStartingCallbacks(t *testing.T) {
	useKernelTestContainer(t)
	k := New("test")
	starts := 0
	if err := k.Starting(func(k *Kernel) error {
		starts++
		return k.ResolveCommand(allStartingCommand{})
	}); err != nil {
		t.Fatalf("Starting() error = %v", err)
	}

	definitions, err := k.All()
	if err != nil {
		t.Fatalf("All() error = %v", err)
	}
	if starts != 1 {
		t.Fatalf("starting callbacks = %d, want 1", starts)
	}
	if _, ok := findKernelDefinition(definitions, "all:starting"); !ok {
		t.Fatalf("All() missing command registered during starting: %#v", definitions)
	}

	if _, err := k.All(); err != nil {
		t.Fatalf("second All() error = %v", err)
	}
	if starts != 1 {
		t.Fatalf("starting callbacks after second All = %d, want 1", starts)
	}
}

func TestKernelAllReturnsCompleteClonedDefinitionSnapshot(t *testing.T) {
	useKernelTestContainer(t)
	k := New("test")
	k.Register(allRichCommand{})

	definitions, err := k.All()
	if err != nil {
		t.Fatalf("All() error = %v", err)
	}
	definition, ok := findKernelDefinition(definitions, "all:rich")
	if !ok {
		t.Fatalf("All() missing all:rich command: %#v", definitions)
	}

	defaultValue := "default"
	want := console.Definition{
		Name:        "all:rich",
		Description: "rich all command",
		Arguments: []console.Argument{{
			Name:         "name",
			Description:  "target name",
			Required:     false,
			DefaultValue: &defaultValue,
		}},
		Options: []console.Option{{
			Name:        "force",
			Description: "force run",
			ValueMode:   console.OptionValueNone,
		}, {
			Name:         "tag",
			Shortcut:     "t",
			Description:  "tag values",
			ValueMode:    console.OptionValueRequired,
			IsArray:      true,
			DefaultValue: &defaultValue,
		}},
		Aliases:  []string{"all:r"},
		Hidden:   true,
		Examples: []string{"all:rich demo --force"},
	}
	if !reflect.DeepEqual(definition, want) {
		t.Fatalf("All() definition = %#v, want %#v", definition, want)
	}

	definition.Arguments[0].Name = "mutated"
	definition.Options[0].Name = "mutated"
	definition.Aliases[0] = "mutated"
	definition.Examples[0] = "mutated"

	nextDefinitions, err := k.All()
	if err != nil {
		t.Fatalf("second All() error = %v", err)
	}
	nextDefinition, ok := findKernelDefinition(nextDefinitions, "all:rich")
	if !ok {
		t.Fatal("second All() missing all:rich command")
	}
	if !reflect.DeepEqual(nextDefinition, want) {
		t.Fatalf("All() leaked mutable snapshot changes: %#v, want %#v", nextDefinition, want)
	}
}

func TestKernelAllReturnsStartingErrorWithoutPartialResult(t *testing.T) {
	useKernelTestContainer(t)
	k := New("test")
	startErr := errors.New("all starting failed")
	if err := k.Starting(func(*Kernel) error { return startErr }); err != nil {
		t.Fatalf("Starting() error = %v", err)
	}

	definitions, err := k.All()
	if !errors.Is(err, startErr) {
		t.Fatalf("All() error = %v, want %v", err, startErr)
	}
	if definitions != nil {
		t.Fatalf("All() definitions after starting error = %#v, want nil", definitions)
	}
}

func TestKernelRegisterClosureAndCall(t *testing.T) {
	useKernelTestContainer(t)
	k := New("test")
	called := false
	k.RegisterClosure(console.Definition{Name: "closure:run", Description: "closure"}, func(ctx console.CommandContext) error {
		called = ctx.CommandName() == "closure:run"
		return nil
	})

	if err := k.Call(context.Background(), "closure:run"); err != nil {
		t.Fatalf("Call returned error: %v", err)
	}
	if !called {
		t.Fatal("expected closure command to run")
	}
}

func TestKernelScheduleResolverRunsRegisteredCommand(t *testing.T) {
	useKernelTestContainer(t)
	k := New("test")
	cmd := &resolverCommand{}
	k.Register(cmd)

	resolved, err := k.resolveRegisteredCommand("resolver:run", []string{"tenant-a"})
	if err != nil {
		t.Fatalf("resolveCommand returned error: %v", err)
	}
	if err := resolved.Fn(context.Background()); err != nil {
		t.Fatalf("resolved command returned error: %v", err)
	}
	if cmd.tenant != "tenant-a" {
		t.Fatalf("cmd.tenant = %q, want tenant-a", cmd.tenant)
	}
}

func TestKernelRegisterAcceptsCommandContextIsolationSignature(t *testing.T) {
	useKernelTestContainer(t)
	k := New("test")
	k.Register(&legacyIsolationCommand{})

	if _, err := k.All(); err != nil {
		t.Fatalf("All returned error: %v", err)
	}
}

func TestKernelResolveCommandAcceptsCommandContextIsolationSignature(t *testing.T) {
	useKernelTestContainer(t)
	k := New("test")
	if err := k.ResolveCommand(&legacyIsolationCommand{}); err != nil {
		t.Fatalf("ResolveCommand returned error: %v", err)
	}
}

func TestAcquireIsolationLockRejectsConcurrentRun(t *testing.T) {
	bindKernelCacheManagerForTest(t)

	ctx := console.NewCommandContext(context.Background(), &resolverCommand{}, console.Definition{Name: "resolver:run"}, nil, nil, nil, nil)
	release, err := acquireIsolationLock(isolationCommand{key: "same"}, ctx)
	if err != nil {
		t.Fatalf("first acquireIsolationLock returned error: %v", err)
	}
	defer release()
	if _, err := acquireIsolationLock(isolationCommand{key: "same"}, ctx); err == nil {
		t.Fatal("expected second acquireIsolationLock to fail")
	}
}

func TestAcquireIsolationLockUsesCacheLock(t *testing.T) {
	bindKernelCacheManagerForTest(t)

	ctx := console.NewCommandContext(context.Background(), &resolverCommand{}, console.Definition{Name: "resolver:run"}, nil, nil, nil, nil)
	key := "cache-lock-shared-" + t.Name()
	held := cache.Lock("prismgo-command-"+sanitizeLockKey(key), commandIsolationLockTTL)
	ok, err := held.Get(context.Background())
	if err != nil {
		t.Fatalf("cache lock get returned error: %v", err)
	}
	if !ok {
		t.Fatal("expected cache lock acquisition")
	}
	t.Cleanup(func() { _ = held.Release(context.Background()) })

	if _, err := acquireIsolationLock(isolationCommand{key: key}, ctx); err == nil || !strings.Contains(err.Error(), "already running") {
		t.Fatalf("acquireIsolationLock error = %v, want already running", err)
	}
}

func TestAcquireIsolationLockAllowsDynamicKeyFromCommandInput(t *testing.T) {
	bindKernelCacheManagerForTest(t)

	input := isolationInput{
		args:    map[string][]string{"tenant": {"tenant-a"}, "ids": {"1", "2"}},
		options: map[string]string{"queue": "exports", "take": "25"},
		bools:   map[string]bool{"force": true},
	}
	ctx := console.NewCommandContext(context.Background(), &resolverCommand{}, console.Definition{Name: "resolver:run"}, input, nil, nil, nil)
	command := &dynamicIsolationCommand{}

	release, err := acquireIsolationLock(command, ctx)
	if err != nil {
		t.Fatalf("acquireIsolationLock returned error: %v", err)
	}
	defer release()

	want := "tenant-a:exports:true:25:1,2"
	if command.key != want {
		t.Fatalf("dynamic isolation key = %q, want %q", command.key, want)
	}
}

type lazyCommand struct{}

func (c *lazyCommand) Definition() *console.Definition {
	return console.MustDefinition("lazy:run", "lazy")
}

func (c *lazyCommand) Handle(_ console.CommandContext) error { return nil }

type allStartingCommand struct{}

func (allStartingCommand) Definition() *console.Definition {
	return console.MustDefinition("all:starting", "registered during all")
}

func (allStartingCommand) Handle(_ console.CommandContext) error { return nil }

type allRichCommand struct{}

func (allRichCommand) Definition() *console.Definition {
	defaultValue := " default "
	definition := console.Definition{
		Name:        " all:rich ",
		Description: " rich all command ",
		Arguments: []console.Argument{{
			Name:         " name ",
			Description:  " target name ",
			Required:     true,
			DefaultValue: &defaultValue,
		}},
		Options: []console.Option{{
			Name:        " force ",
			Description: " force run ",
			ValueMode:   console.OptionValueNone,
		}, {
			Name:         " tag ",
			Shortcut:     " t ",
			Description:  " tag values ",
			ValueMode:    console.OptionValueRequired,
			IsArray:      true,
			DefaultValue: &defaultValue,
		}},
		Aliases:  []string{" all:r ", "all:r"},
		Hidden:   true,
		Examples: []string{" all:rich demo --force ", "all:rich demo --force"},
	}
	return &definition
}

func (allRichCommand) Handle(_ console.CommandContext) error { return nil }

type resolverCommand struct{ tenant string }

func (c *resolverCommand) Definition() *console.Definition {
	return console.MustDefinition("resolver:run {tenant}", "resolver")
}

func (c *resolverCommand) Handle(ctx console.CommandContext) error {
	c.tenant = ctx.Input().Argument("tenant")
	return nil
}

type isolationCommand struct{ key string }

func (c isolationCommand) IsolationKey(_ console.IsolationContext) string { return c.key }

type legacyIsolationCommand struct{}

func (c *legacyIsolationCommand) Definition() *console.Definition {
	return console.MustDefinition("legacy:isolation", "legacy isolation")
}

func (c *legacyIsolationCommand) Handle(_ console.CommandContext) error { return nil }

func (c *legacyIsolationCommand) IsolationKey(_ console.CommandContext) string { return "legacy" }

type dynamicIsolationCommand struct{ key string }

func (c *dynamicIsolationCommand) IsolationKey(ctx console.IsolationContext) string {
	c.key = strings.Join([]string{
		ctx.Argument("tenant"),
		ctx.Option("queue"),
		boolString(ctx.OptionBool("force")),
		strconv.Itoa(ctx.OptionInt("take")),
		strings.Join(ctx.Arguments("ids"), ","),
	}, ":")
	return c.key
}

func boolString(value bool) string {
	if value {
		return "true"
	}
	return "false"
}

type isolationInput struct {
	args    map[string][]string
	options map[string]string
	bools   map[string]bool
}

func (i isolationInput) Argument(name string) string {
	values := i.Arguments(name)
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

func (i isolationInput) Arguments(name string) []string {
	return append([]string(nil), i.args[name]...)
}

func (i isolationInput) Option(name string) string { return i.options[name] }

func (i isolationInput) OptionStrings(name string) []string {
	value := i.options[name]
	if value == "" {
		return nil
	}
	return []string{value}
}

func (i isolationInput) OptionBool(name string) bool { return i.bools[name] }

func (i isolationInput) OptionInt(name string) int {
	value, _ := strconv.Atoi(i.options[name])
	return value
}

func (i isolationInput) HasOption(name string) bool {
	_, ok := i.options[name]
	return ok || i.bools[name]
}

func findKernelDefinition(definitions []console.Definition, name string) (console.Definition, bool) {
	for _, definition := range definitions {
		if definition.Name == name {
			return definition, true
		}
	}
	return console.Definition{}, false
}
