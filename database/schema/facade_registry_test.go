package schema

import (
	"os"
	"testing"

	"github.com/prismgo/framework/container"
)

var testFacadeRegistry = container.NewContainer()

func TestMain(m *testing.M) {
	container.SetProvider(func() *container.Container { return testFacadeRegistry })
	code := m.Run()
	container.SetProvider(nil)
	os.Exit(code)
}

func useIsolatedFacadeRegistry(t *testing.T) *container.Container {
	t.Helper()
	registry := container.NewContainer()
	container.SetProvider(func() *container.Container { return registry })
	t.Cleanup(func() { container.SetProvider(nil) })
	return registry
}
