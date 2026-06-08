package cookie

import (
	"errors"
	"testing"

	"github.com/prismgo/framework/container"
	containercontract "github.com/prismgo/framework/contracts/container"
)

func useIsolatedFacadeRegistry(t *testing.T) *container.Container {
	t.Helper()
	registry := container.NewContainer()
	container.SetProvider(func() *container.Container { return registry })
	t.Cleanup(func() { container.SetProvider(nil) })
	return registry
}

func TestResolvePanicsWhenQueueFactoryFails(t *testing.T) {
	registry := useIsolatedFacadeRegistry(t)

	factoryErr := errors.New("cookie factory failed")
	if err := registry.Singleton("cookie.queue", func(containercontract.Resolver) (any, error) {
		return nil, factoryErr
	}); err != nil {
		t.Fatalf("register factory: %v", err)
	}

	defer func() {
		recovered := recover()
		if !errors.Is(recoveredAsError(recovered), factoryErr) {
			t.Fatalf("Resolve panic = %v, want factory error", recovered)
		}
	}()
	_ = Resolve()
}

// recoveredAsError 把 recover 捕获值转换为 error，便于断言 facade 透传的解析失败原因。
func recoveredAsError(recovered any) error {
	if err, ok := recovered.(error); ok {
		return err
	}
	return nil
}
