package event

import (
	"os"
	"testing"

	"github.com/prismgo/framework/container"
	eventcontract "github.com/prismgo/framework/contracts/event"
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
	resetQueuedStateForTest()
	t.Cleanup(resetQueuedStateForTest)
	registry := container.NewContainer()
	container.SetProvider(func() *container.Container { return registry })
	t.Cleanup(func() { container.SetProvider(nil) })
	return registry
}

func bindEventDispatcherForTest(t *testing.T, dispatcher eventcontract.Dispatcher) *container.Container {
	t.Helper()
	registry := useIsolatedFacadeRegistry(t)
	if err := registry.Instance(serviceKey, dispatcher); err != nil {
		t.Fatalf("bind event dispatcher: %v", err)
	}
	return registry
}

func resetQueuedStateForTest() {
	eventFactoriesMu.Lock()
	eventFactories = map[string]func() Event{}
	eventFactoriesMu.Unlock()
	queuedMu.Lock()
	queuedListeners = map[string]Listener{}
	queuedMu.Unlock()
	queuedSequence.Store(0)
	UseQueuedDispatcher(nil)
}
