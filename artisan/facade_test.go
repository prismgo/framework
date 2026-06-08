package artisan_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/prismgo/framework/artisan"
	"github.com/prismgo/framework/console"
	"github.com/prismgo/framework/container"
	containercontract "github.com/prismgo/framework/contracts/container"
	"github.com/prismgo/framework/foundation"
	"github.com/prismgo/framework/kernel"
	"github.com/prismgo/framework/timer"
)

func TestArtisanUseResolveAndCallUseCurrentApplicationRegistry(t *testing.T) {
	// 测试意图：显式容器绑定后，Resolve 和 Call 必须命中当前 Application registry 里的同一个 Kernel。
	registry := container.NewContainer()
	container.SetProvider(func() *container.Container { return registry })
	t.Cleanup(func() { container.SetProvider(nil) })

	k := kernel.New("test")
	ran := false
	k.RegisterClosure(*console.MustDefinition("artisan:run", "run through facade"), func(console.CommandContext) error {
		ran = true
		return nil
	})

	if err := registry.Instance(kernel.ArtisanFacadeKey, k); err != nil {
		t.Fatalf("bind kernel: %v", err)
	}
	resolved, err := artisan.Resolve()
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if resolved != k {
		t.Fatal("Resolve() did not return the kernel bound to the current registry")
	}

	if err := artisan.Call(context.Background(), "artisan:run"); err != nil {
		t.Fatalf("Call() error = %v", err)
	}
	if !ran {
		t.Fatal("Call() did not execute the command through the bound kernel")
	}
}

func TestArtisanStartingRegistersCallbackOnBoundKernel(t *testing.T) {
	// 测试意图：artisan.Starting 只把 callback 注册到当前绑定的 Kernel，命令仍通过 ResolveCommand 进入统一校验。
	registry := container.NewContainer()
	container.SetProvider(func() *container.Container { return registry })
	t.Cleanup(func() { container.SetProvider(nil) })

	k := kernel.New("test")
	if err := registry.Instance(kernel.ArtisanFacadeKey, k); err != nil {
		t.Fatalf("bind kernel: %v", err)
	}
	if err := artisan.Starting(func(k *kernel.Kernel) error {
		return k.ResolveCommand(startingCommand{})
	}); err != nil {
		t.Fatalf("Starting() error = %v", err)
	}

	if err := artisan.Call(context.Background(), "artisan:starting"); err != nil {
		t.Fatalf("Call() should execute command registered during starting: %v", err)
	}
}

func TestArtisanAllRunsStartingCallbacksAndReturnsDefinitions(t *testing.T) {
	// 测试意图：artisan.All 对齐 Laravel all() 枚举语义，先执行 starting 注册延迟命令，再返回完整 Definition。
	registry := container.NewContainer()
	container.SetProvider(func() *container.Container { return registry })
	t.Cleanup(func() { container.SetProvider(nil) })

	k := kernel.New("test")
	if err := registry.Instance(kernel.ArtisanFacadeKey, k); err != nil {
		t.Fatalf("bind kernel: %v", err)
	}
	if err := artisan.Starting(func(k *kernel.Kernel) error {
		return k.ResolveCommand(artisanAllCommand{})
	}); err != nil {
		t.Fatalf("Starting() error = %v", err)
	}

	definitions, err := artisan.All()
	if err != nil {
		t.Fatalf("All() error = %v", err)
	}
	definition, ok := findDefinition(definitions, "artisan:all")
	if !ok {
		t.Fatalf("All() missing command registered during starting: %#v", definitions)
	}
	if definition.Description != "enumerated through artisan all" {
		t.Fatalf("Definition.Description = %q, want enumerated through artisan all", definition.Description)
	}
	if len(definition.Arguments) != 1 || definition.Arguments[0].Name != "name" || !definition.Arguments[0].Required {
		t.Fatalf("Definition.Arguments = %#v, want required name argument", definition.Arguments)
	}
	if len(definition.Options) != 1 || definition.Options[0].Name != "force" || definition.Options[0].ValueMode != console.OptionValueNone {
		t.Fatalf("Definition.Options = %#v, want force boolean option", definition.Options)
	}
}

func TestArtisanCallSilentlyDelegatesToBoundKernel(t *testing.T) {
	// 测试意图：silent 调用路径也只做 facade 解析，并委托 Kernel.CallSilently 执行实际命令。
	registry := container.NewContainer()
	container.SetProvider(func() *container.Container { return registry })
	t.Cleanup(func() { container.SetProvider(nil) })

	k := kernel.New("test")
	ran := false
	k.RegisterClosure(*console.MustDefinition("artisan:silent", "run silently"), func(console.CommandContext) error {
		ran = true
		return nil
	})
	if err := registry.Instance(kernel.ArtisanFacadeKey, k); err != nil {
		t.Fatalf("bind kernel: %v", err)
	}

	if err := artisan.CallSilently(context.Background(), "artisan:silent"); err != nil {
		t.Fatalf("CallSilently() error = %v", err)
	}
	if !ran {
		t.Fatal("CallSilently() did not execute the command through the bound kernel")
	}
}

func TestArtisanCallInputDelegatesToBoundKernel(t *testing.T) {
	registry := container.NewContainer()
	container.SetProvider(func() *container.Container { return registry })
	t.Cleanup(func() { container.SetProvider(nil) })

	k := kernel.New("test")
	var got string
	k.RegisterClosure(*console.MustDefinition("artisan:with {name}", "run with input"), func(ctx console.CommandContext) error {
		got = ctx.Argument("name")
		return nil
	})
	if err := registry.Instance(kernel.ArtisanFacadeKey, k); err != nil {
		t.Fatalf("bind kernel: %v", err)
	}

	if err := artisan.Call(context.Background(), "artisan:with", console.CallInput{Arguments: map[string]any{"name": "demo"}}); err != nil {
		t.Fatalf("Call() with input error = %v", err)
	}
	if got != "demo" {
		t.Fatalf("Call with input argument = %q, want demo", got)
	}
	got = ""
	if err := artisan.CallSilently(context.Background(), "artisan:with", console.CallInput{Arguments: map[string]any{"name": "quiet"}}); err != nil {
		t.Fatalf("CallSilently() with input error = %v", err)
	}
	if got != "quiet" {
		t.Fatalf("CallSilently with input argument = %q, want quiet", got)
	}
}

type startingCommand struct{}

func (startingCommand) Definition() *console.Definition {
	return console.MustDefinition("artisan:starting", "registered during artisan starting")
}

func (startingCommand) Handle(console.CommandContext) error {
	return nil
}

type artisanAllCommand struct{}

func (artisanAllCommand) Definition() *console.Definition {
	definition := console.MustDefinition("artisan:all {name} {--force}", "enumerated through artisan all")
	definition.Aliases = []string{"artisan:enumerate"}
	definition.Hidden = true
	definition.Examples = []string{"artisan:all demo --force"}
	return definition
}

func (artisanAllCommand) Handle(console.CommandContext) error {
	return nil
}

type artisanTestApplicationSource struct {
	registry containercontract.Container
}

func (s artisanTestApplicationSource) Container() containercontract.Container { return s.registry }
func (s artisanTestApplicationSource) CommandFactories() []console.CommandFactory {
	return nil
}
func (s artisanTestApplicationSource) StartingCallbacks() []kernel.StartingCallback {
	return nil
}
func (s artisanTestApplicationSource) ScheduleRegistrars() []func(*timer.Schedule) {
	return nil
}
func (s artisanTestApplicationSource) MigrationPaths() []string { return nil }
func (s artisanTestApplicationSource) SeedPaths() []string      { return nil }

func TestArtisanReportsRegistryAndBindingErrors(t *testing.T) {
	// 测试意图：区分“没有当前 Application registry”和“registry 中没有 Kernel 绑定”两类装配错误。
	container.SetProvider(nil)
	t.Cleanup(func() { container.SetProvider(nil) })

	if _, err := artisan.Resolve(); !errors.Is(err, container.ErrNoCurrentContainer) {
		t.Fatalf("Resolve() without registry error = %v, want ErrNoCurrentRegistry", err)
	}
	if _, err := artisan.All(); !errors.Is(err, container.ErrNoCurrentContainer) {
		t.Fatalf("All() without registry error = %v, want ErrNoCurrentRegistry", err)
	}
	registry := container.NewContainer()
	container.SetProvider(func() *container.Container { return registry })
	if _, err := artisan.Resolve(); !errors.Is(err, artisan.ErrKernelNotBound) {
		t.Fatalf("Resolve() without kernel error = %v, want ErrKernelNotBound", err)
	}
	if _, err := artisan.All(); !errors.Is(err, artisan.ErrKernelNotBound) {
		t.Fatalf("All() without kernel error = %v, want ErrKernelNotBound", err)
	}
	if err := artisan.Call(context.Background(), "missing"); !errors.Is(err, artisan.ErrKernelNotBound) {
		t.Fatalf("Call() without kernel error = %v, want ErrKernelNotBound", err)
	}
	if err := artisan.CallSilently(context.Background(), "missing"); !errors.Is(err, artisan.ErrKernelNotBound) {
		t.Fatalf("CallSilently() without kernel error = %v, want ErrKernelNotBound", err)
	}
	if err := artisan.Starting(func(*kernel.Kernel) error { return nil }); !errors.Is(err, artisan.ErrKernelNotBound) {
		t.Fatalf("Starting() without kernel error = %v, want ErrKernelNotBound", err)
	}
}

func TestNewApplicationKernelBindsCurrentApplicationRegistry(t *testing.T) {
	// 测试意图：标准应用 Kernel 构造流程会自动绑定 artisan facade，main.go 不需要手写 Use。
	app := foundation.NewApplication()
	t.Cleanup(func() { _ = app.Close() })

	k := kernel.NewApplicationKernel("test", artisanTestApplicationSource{registry: app.Container()})
	resolved, err := artisan.Resolve()
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if resolved != k {
		t.Fatal("NewApplicationKernel() did not bind the created kernel to the current application registry")
	}
}

func TestNewApplicationKernelPanicsWithoutCurrentApplicationRegistry(t *testing.T) {
	// 测试意图：应用 Kernel 自动绑定失败时必须立刻暴露启动顺序错误，不能降级为进程级默认 Kernel。
	container.SetProvider(nil)
	t.Cleanup(func() { container.SetProvider(nil) })

	defer func() {
		recovered := recover()
		if recovered == nil {
			t.Fatal("expected NewApplicationKernel to panic without current registry")
		}
		if !strings.Contains(recovered.(string), "Artisan Kernel binding") {
			t.Fatalf("panic = %v, want Artisan Kernel binding context", recovered)
		}
	}()
	_ = kernel.NewApplicationKernel("test", nil)
}

func TestKernelNewDoesNotBindArtisanFacade(t *testing.T) {
	// 测试意图：普通 kernel.New 仍保持通用 CLI Kernel 语义，不假设存在 Application registry。
	registry := container.NewContainer()
	container.SetProvider(func() *container.Container { return registry })
	t.Cleanup(func() { container.SetProvider(nil) })

	_ = kernel.New("test")
	if _, err := artisan.Resolve(); !errors.Is(err, artisan.ErrKernelNotBound) {
		t.Fatalf("Resolve() after kernel.New error = %v, want ErrKernelNotBound", err)
	}
}

func TestArtisanBindingDoesNotSurviveApplicationCloseOrPolluteNextApplication(t *testing.T) {
	// 测试意图：关闭 Application 后 facade registry 会被清理，后续新 Application 绑定自己的 Kernel。
	first := foundation.NewApplication()
	firstKernel := kernel.NewApplicationKernel("first", artisanTestApplicationSource{registry: first.Container()})
	if resolved, err := artisan.Resolve(); err != nil || resolved != firstKernel {
		t.Fatalf("first Resolve() = %v, %v; want first kernel", resolved, err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("first Close() error = %v", err)
	}
	if _, err := artisan.Resolve(); !errors.Is(err, container.ErrNoCurrentContainer) {
		t.Fatalf("Resolve() after first close error = %v, want ErrNoCurrentRegistry", err)
	}

	second := foundation.NewApplication()
	t.Cleanup(func() { _ = second.Close() })
	secondKernel := kernel.NewApplicationKernel("second", artisanTestApplicationSource{registry: second.Container()})
	resolved, err := artisan.Resolve()
	if err != nil {
		t.Fatalf("second Resolve() error = %v", err)
	}
	if resolved != secondKernel {
		t.Fatal("second application did not bind its own kernel")
	}
	if resolved == firstKernel {
		t.Fatal("second application reused the first application's kernel")
	}
}

func TestArtisanStartingReturnsKernelLifecycleError(t *testing.T) {
	// 测试意图：Kernel 已经执行过 starting callbacks 后，artisan.Starting 应透传 Kernel 的生命周期错误。
	registry := container.NewContainer()
	container.SetProvider(func() *container.Container { return registry })
	t.Cleanup(func() { container.SetProvider(nil) })

	k := kernel.New("test")
	if err := registry.Instance(kernel.ArtisanFacadeKey, k); err != nil {
		t.Fatalf("bind kernel: %v", err)
	}
	if err := artisan.Call(context.Background(), "list"); err != nil {
		t.Fatalf("Call(list) error = %v", err)
	}
	if err := artisan.Starting(func(*kernel.Kernel) error { return nil }); err == nil || !strings.Contains(err.Error(), "callbacks already ran") {
		t.Fatalf("Starting() after run error = %v, want callbacks already ran", err)
	}
}

func findDefinition(definitions []console.Definition, name string) (console.Definition, bool) {
	for _, definition := range definitions {
		if definition.Name == name {
			return definition, true
		}
	}
	return console.Definition{}, false
}
