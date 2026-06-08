package filesystem

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/prismgo/framework/container"
	containercontract "github.com/prismgo/framework/contracts/container"
	fscontract "github.com/prismgo/framework/contracts/filesystem"
	"github.com/prismgo/framework/exception"
)

func TestContractsCompile(t *testing.T) {
	var _ fscontract.Manager = (*Manager)(nil)
	var _ fscontract.Repository = (*Repository)(nil)

	manager := newTestManager(t, t.TempDir())
	t.Cleanup(func() { _ = manager.Close() })

	var defaultDisk fscontract.Repository = manager.Default()
	var namedDisk fscontract.Repository = manager.Disk("public")
	var cloudDisk fscontract.Cloud = manager.Cloud()

	if defaultDisk == nil || namedDisk == nil || cloudDisk == nil {
		t.Fatal("expected contract repositories from manager")
	}
	if err := namedDisk.PutReader(context.Background(), "contract-reader.txt", strings.NewReader("ok"), PutOptions{ContentType: "text/plain"}); err != nil {
		t.Fatalf("contract repository PutReader failed: %v", err)
	}
}

func useIsolatedFacadeRegistry(t *testing.T) *container.Container {
	t.Helper()
	return useFilesystemTestContainer(t)
}

func assertPanics(t *testing.T, fn func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()
	fn()
}

func TestResolveRequiresCurrentRegistry(t *testing.T) {
	container.SetProvider(nil)
	t.Cleanup(func() { container.SetProvider(nil) })

	assertPanics(t, func() { _ = Resolve() })
}

func TestDefaultDiskResolvesRegisteredFactoryBeforeErrorRepository(t *testing.T) {
	registry := useIsolatedFacadeRegistry(t)

	manager := newTestManager(t, t.TempDir())
	calls := 0
	if err := registry.Singleton(serviceKey, func(containercontract.Resolver) (any, error) {
		calls++
		return manager, nil
	}); err != nil {
		t.Fatalf("register factory: %v", err)
	}

	if got := Resolve(); got == nil {
		t.Fatal("Resolve before default access returned nil")
	}
	if err := Default().Put(context.Background(), "contract.txt", "ok"); err != nil {
		t.Fatalf("Default put via factory manager: %v", err)
	}
	if err := Disk("public").Put(context.Background(), "contract.txt", "ok"); err != nil {
		t.Fatalf("Disk put via factory manager: %v", err)
	}
	var repo fscontract.Repository = Default()
	if err := repo.Put(context.Background(), "typed-contract.txt", "ok"); err != nil {
		t.Fatalf("Default contract repository put: %v", err)
	}
	if calls != 1 {
		t.Fatalf("factory calls = %d, want 1", calls)
	}
}

func TestDefaultReportsFactoryErrorBeforeErrorRepository(t *testing.T) {
	registry := useIsolatedFacadeRegistry(t)

	factoryErr := errors.New("filesystem factory failed")
	if err := registry.Instance("exception.handler", exception.New(
		exception.WithPanicStack(false),
		exception.WithReporter(func(ctx any, err error, got map[string]any) {
		}),
	), container.WithCloseGroup(container.CloseGroupReporting)); err != nil {
		t.Fatalf("bind exception handler: %v", err)
	}

	if err := registry.Singleton(serviceKey, func(containercontract.Resolver) (any, error) {
		return nil, factoryErr
	}); err != nil {
		t.Fatalf("register factory: %v", err)
	}

	if _, err := container.Make[*Manager](serviceKey); !errors.Is(err, factoryErr) {
		t.Fatalf("Make error = %v, want factory error", err)
	}
	assertPanics(t, func() { _ = Resolve() })
}
