package session

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
