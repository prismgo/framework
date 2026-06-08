package facade

import (
	"testing"

	"github.com/prismgo/framework/container"
	containercontract "github.com/prismgo/framework/contracts/container"
)

type testService struct {
	name string
}

func TestResolveReturnsServiceFromCurrentContainer(t *testing.T) {
	c := container.NewContainer()
	container.SetProvider(func() *container.Container { return c })
	defer container.SetProvider(nil)

	svc := &testService{name: "test"}
	if err := c.Instance("test.service", svc); err != nil {
		t.Fatalf("container instance: %v", err)
	}

	got := Resolve[*testService]("test.service")
	if got == nil {
		t.Fatal("Resolve returned nil")
	}
	if got.name != "test" {
		t.Fatalf("name = %q, want %q", got.name, "test")
	}
}

func TestResolvePanicsWhenNoCurrentContainer(t *testing.T) {
	container.SetProvider(nil)

	// Resolve 是 facade 便捷入口，容器解析失败时必须立刻暴露装配错误，避免调用方拿到零值后继续执行。
	defer func() {
		if recovered := recover(); recovered == nil {
			t.Fatal("Resolve did not panic")
		}
	}()

	_ = Resolve[*testService]("missing.service")
}

func TestResolvePanicsWhenKeyNotFound(t *testing.T) {
	c := container.NewContainer()
	container.SetProvider(func() *container.Container { return c })
	defer container.SetProvider(nil)

	// 未绑定 key 是真实装配错误，facade.Resolve 必须 panic，不能用零值掩盖问题。
	defer func() {
		if recovered := recover(); recovered == nil {
			t.Fatal("Resolve did not panic")
		}
	}()

	_ = Resolve[*testService]("nonexistent.key")
}

func TestResolveWithFactoryRegistration(t *testing.T) {
	c := container.NewContainer()
	container.SetProvider(func() *container.Container { return c })
	defer container.SetProvider(nil)

	if err := c.Singleton("factory.service", func(containercontract.Resolver) (any, error) {
		return &testService{name: "factory"}, nil
	}); err != nil {
		t.Fatalf("container singleton: %v", err)
	}

	got := Resolve[*testService]("factory.service")
	if got == nil {
		t.Fatal("Resolve returned nil")
	}
	if got.name != "factory" {
		t.Fatalf("name = %q, want %q", got.name, "factory")
	}
}
