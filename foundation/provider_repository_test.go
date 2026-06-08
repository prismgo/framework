package foundation

import (
	"context"
	"errors"
	"strings"
	"testing"

	containercontract "github.com/prismgo/framework/contracts/container"
	"github.com/prismgo/framework/contracts/provider"
	"github.com/prismgo/framework/event"
)

func TestApplicationProviderRepositoryRegistersEverythingBeforeBooting(t *testing.T) {
	var calls []string
	bus := event.New()
	base := repositoryProvider{
		name: "base",
		register: func(app *Application) error {
			calls = append(calls, "register:base")
			return app.Container().Singleton("event.dispatcher", func(containercontract.Resolver) (any, error) {
				return bus, nil
			})
		},
		boot: func(*Application) error {
			calls = append(calls, "boot:base")
			return nil
		},
	}
	withBaseProvidersForTest(t, base)

	bus.Listen("*", event.ListenerFunc(func(_ context.Context, ev event.Event) error {
		switch payload := ev.(type) {
		case event.ProviderRegistering:
			calls = append(calls, "event:registering:"+payload.Provider)
		case event.ProviderRegistered:
			calls = append(calls, "event:registered:"+payload.Provider)
		case event.ProviderBooting:
			calls = append(calls, "event:booting:"+payload.Provider)
		case event.ProviderBooted:
			calls = append(calls, "event:booted:"+payload.Provider)
		case event.AppBooting:
			calls = append(calls, "event:app.booting")
		case event.AppBooted:
			calls = append(calls, "event:app.booted")
		}
		return nil
	}))

	app := NewApplication()
	if err := app.RegisterProvider(repositoryProvider{name: "app-a", callLog: &calls}); err != nil {
		t.Fatalf("register app-a: %v", err)
	}
	if err := app.RegisterProvider(repositoryProvider{name: "app-b", callLog: &calls}); err != nil {
		t.Fatalf("register app-b: %v", err)
	}

	if err := app.Boot(); err != nil {
		t.Fatalf("Boot failed: %v", err)
	}

	assertSubsequence(t, calls, []string{
		"register:base",
		"event:registering:app-a",
		"register:app-a",
		"event:registered:app-a",
		"event:registering:app-b",
		"register:app-b",
		"event:registered:app-b",
		"event:app.booting",
		"event:booting:base",
		"boot:base",
		"event:booted:base",
		"event:booting:app-a",
		"boot:app-a",
		"event:booted:app-a",
		"event:booting:app-b",
		"boot:app-b",
		"event:booted:app-b",
		"event:app.booted",
	})
}

func TestApplicationProviderRepositoryDeduplicatesByIdentity(t *testing.T) {
	withBaseProvidersForTest(t, repositoryProvider{name: "base"})
	app := NewApplication()
	first := &countingProvider{name: "shared"}
	second := &countingProvider{name: "shared"}
	typed := &typedCountingProvider{}

	if err := app.RegisterProvider(first); err != nil {
		t.Fatalf("register first named provider: %v", err)
	}
	if err := app.RegisterProvider(second); err != nil {
		t.Fatalf("register duplicate named provider: %v", err)
	}
	if err := app.RegisterProvider(typed); err != nil {
		t.Fatalf("register typed provider: %v", err)
	}
	if err := app.RegisterProvider(&typedCountingProvider{}); err != nil {
		t.Fatalf("register duplicate typed provider: %v", err)
	}

	if err := app.Boot(); err != nil {
		t.Fatalf("Boot failed: %v", err)
	}
	if first.registered != 1 || first.booted != 1 {
		t.Fatalf("first provider lifecycle = register:%d boot:%d, want 1/1", first.registered, first.booted)
	}
	if second.registered != 0 || second.booted != 0 {
		t.Fatalf("duplicate named provider ran: register:%d boot:%d", second.registered, second.booted)
	}
	if typed.registered != 1 || typed.booted != 1 {
		t.Fatalf("typed provider lifecycle = register:%d boot:%d, want 1/1", typed.registered, typed.booted)
	}
}

func TestApplicationBootRetryDoesNotRepeatCompletedProviderStages(t *testing.T) {
	withBaseProvidersForTest(t)
	app := NewApplication()
	stable := &countingProvider{name: "stable"}
	flaky := &countingProvider{name: "flaky", bootErr: errors.New("temporary boot failure")}

	if err := app.RegisterProvider(stable); err != nil {
		t.Fatalf("register stable provider: %v", err)
	}
	if err := app.RegisterProvider(flaky); err != nil {
		t.Fatalf("register flaky provider: %v", err)
	}

	err := app.Boot()
	if err == nil || !strings.Contains(err.Error(), "provider flaky boot") {
		t.Fatalf("first Boot error = %v, want provider identity and boot phase", err)
	}
	if stable.registered != 1 || stable.booted != 1 || flaky.registered != 1 || flaky.booted != 1 {
		t.Fatalf("first boot counts stable=%d/%d flaky=%d/%d, want stable 1/1 flaky 1/1", stable.registered, stable.booted, flaky.registered, flaky.booted)
	}

	flaky.bootErr = nil
	if err := app.Boot(); err != nil {
		t.Fatalf("retry Boot failed: %v", err)
	}
	if stable.registered != 1 || stable.booted != 1 {
		t.Fatalf("retry repeated stable provider: register:%d boot:%d", stable.registered, stable.booted)
	}
	if flaky.registered != 1 || flaky.booted != 2 {
		t.Fatalf("retry counts for flaky = register:%d boot:%d, want 1/2", flaky.registered, flaky.booted)
	}
}

func TestApplicationRegisterProviderAfterBootRunsOnlyThatProvider(t *testing.T) {
	withBaseProvidersForTest(t)
	app := NewApplication()
	first := &countingProvider{name: "first"}
	later := &countingProvider{name: "later"}

	if err := app.RegisterProvider(first); err != nil {
		t.Fatalf("register first provider: %v", err)
	}
	if err := app.Boot(); err != nil {
		t.Fatalf("Boot failed: %v", err)
	}
	if err := app.RegisterProvider(later); err != nil {
		t.Fatalf("register provider after boot: %v", err)
	}
	if first.registered != 1 || first.booted != 1 {
		t.Fatalf("first provider repeated after boot: register:%d boot:%d", first.registered, first.booted)
	}
	if later.registered != 1 || later.booted != 1 {
		t.Fatalf("later provider lifecycle = register:%d boot:%d, want 1/1", later.registered, later.booted)
	}
}

func TestApplicationRejectsProviderRegistrationDuringBoot(t *testing.T) {
	withBaseProvidersForTest(t)
	app := NewApplication()
	var registerErr error
	if err := app.RegisterProvider(repositoryProvider{
		name: "reentrant",
		boot: func(app *Application) error {
			registerErr = app.RegisterProvider(repositoryProvider{name: "late"})
			return nil
		},
	}); err != nil {
		t.Fatalf("register reentrant provider: %v", err)
	}

	if err := app.Boot(); err != nil {
		t.Fatalf("Boot failed: %v", err)
	}
	if registerErr == nil || !strings.Contains(registerErr.Error(), "boot is in progress") {
		t.Fatalf("RegisterProvider during Boot error = %v, want boot-in-progress error", registerErr)
	}
}

func TestApplicationMarksBootedBeforeAppBootedEvent(t *testing.T) {
	app := NewApplication()
	var bootedDuringEvent bool
	bus := event.Resolve()
	bus.Listen(event.EventAppBooted, event.ListenerFunc(func(context.Context, event.Event) error {
		bootedDuringEvent = app.booted
		return nil
	}))

	if err := app.Boot(); err != nil {
		t.Fatalf("Boot failed: %v", err)
	}
	if !bootedDuringEvent {
		t.Fatal("AppBooted listener should observe application as booted")
	}
}

type repositoryProvider struct {
	name     string
	callLog  *[]string
	register func(*Application) error
	boot     func(*Application) error
}

func (p repositoryProvider) Name() string { return p.name }

func (p repositoryProvider) Register(app provider.Application) error {
	if p.callLog != nil {
		*p.callLog = append(*p.callLog, "register:"+p.name)
	}
	if p.register != nil {
		return p.register(app.(*Application))
	}
	return nil
}

func (p repositoryProvider) Boot(app provider.Application) error {
	if p.callLog != nil {
		*p.callLog = append(*p.callLog, "boot:"+p.name)
	}
	if p.boot != nil {
		return p.boot(app.(*Application))
	}
	return nil
}

type countingProvider struct {
	name        string
	registered  int
	booted      int
	registerErr error
	bootErr     error
}

func (p *countingProvider) Name() string { return p.name }

func (p *countingProvider) Register(provider.Application) error {
	p.registered++
	return p.registerErr
}

func (p *countingProvider) Boot(provider.Application) error {
	p.booted++
	return p.bootErr
}

type typedCountingProvider struct {
	registered int
	booted     int
}

func (p *typedCountingProvider) Register(provider.Application) error {
	p.registered++
	return nil
}

func (p *typedCountingProvider) Boot(provider.Application) error {
	p.booted++
	return nil
}

func assertSubsequence(t *testing.T, got, want []string) {
	t.Helper()
	pos := 0
	for _, item := range got {
		if pos < len(want) && item == want[pos] {
			pos++
		}
	}
	if pos != len(want) {
		t.Fatalf("subsequence %v not found in calls %v", want, got)
	}
}

func withBaseProvidersForTest(t *testing.T, items ...provider.ServiceProvider) {
	t.Helper()
	previous := applicationBaseProviders
	applicationBaseProviders = func() []provider.ServiceProvider {
		return append([]provider.ServiceProvider(nil), items...)
	}
	t.Cleanup(func() {
		applicationBaseProviders = previous
	})
}
