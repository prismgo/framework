package config

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

func useConfigTestContainer(t *testing.T) *container.Container {
	t.Helper()
	registry := container.NewContainer()
	container.SetProvider(func() *container.Container { return registry })
	t.Cleanup(func() { container.SetProvider(nil) })
	return registry
}

func bindConfigForTest(t *testing.T, cfg *Config) *container.Container {
	t.Helper()
	registry := useConfigTestContainer(t)
	if cfg != nil {
		if err := registry.Instance(serviceKey, cfg); err != nil {
			t.Fatalf("bind config: %v", err)
		}
	}
	return registry
}
