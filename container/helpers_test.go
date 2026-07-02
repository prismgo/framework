package container

import (
	"context"
	"errors"
	"reflect"
	"testing"

	containercontract "github.com/prismgo/framework/contracts/container"
)

type helperProbe struct {
	name string
}

func TestCurrentHelpersAndTypedAccess(t *testing.T) {
	SetProvider(nil)
	if got := List(); got != nil {
		t.Fatalf("list without current = %#v, want nil", got)
	}
	if _, err := Make[*helperProbe]("missing.current"); !errors.Is(err, ErrNoCurrentContainer) {
		t.Fatalf("make without current error = %v", err)
	}

	c := NewContainer()
	SetProvider(func() *Container { return c })
	t.Cleanup(func() { SetProvider(nil) })

	value := &helperProbe{name: "explicit"}
	if err := c.Instance("probe", value); err != nil {
		t.Fatalf("instance current: %v", err)
	}
	got, err := Make[*helperProbe]("probe")
	if err != nil {
		t.Fatalf("make current: %v", err)
	}
	if got != value {
		t.Fatal("make current did not return explicit value")
	}
	if current := Value[*helperProbe]("probe"); current != value {
		t.Fatal("value did not return stored value")
	}

	SetProvider(nil)
}

func TestTypedFactoryAndOptions(t *testing.T) {
	c := NewContainer()
	SetProvider(func() *Container { return c })
	t.Cleanup(func() { SetProvider(nil) })

	// Low #7: Make[T] 应该返回类型不匹配的明确错误，而不是 isNilValue 检查
	if err := c.Instance("string.service", "hello"); err != nil {
		t.Fatalf("instance: %v", err)
	}
	if _, err := Make[*helperProbe]("string.service"); err == nil {
		t.Fatal("Make[T] should return error when type mismatch")
	}

	var closed []string
	err := c.Singleton("probe", func(containercontract.Resolver) (any, error) {
		return &helperProbe{name: "factory"}, nil
	}, WithContextCloser(func(ctx context.Context, p *helperProbe) error {
		if ctx == nil {
			t.Fatal("close context should not be nil")
		}
		closed = append(closed, p.name)
		return nil
	}), WithCloseGroup(CloseGroupReporting))
	if err != nil {
		t.Fatalf("register factory: %v", err)
	}

	got, err := Make[*helperProbe]("probe")
	if err != nil {
		t.Fatalf("typed make: %v", err)
	}
	if got.name != "factory" {
		t.Fatalf("probe name = %q", got.name)
	}
	if err := c.CloseGroup(context.Background(), CloseGroupReporting); err != nil {
		t.Fatalf("close reporting: %v", err)
	}
	if !reflect.DeepEqual(closed, []string{"factory"}) {
		t.Fatalf("closed = %#v", closed)
	}
}

func TestBoundDoesNotResolveFactoriesOrLoadDeferredProviders(t *testing.T) {
	c := NewContainer()
	SetProvider(func() *Container { return c })
	t.Cleanup(func() { SetProvider(nil) })

	var factoryCalls int
	if err := c.Singleton("probe", func(containercontract.Resolver) (any, error) {
		factoryCalls++
		return &helperProbe{name: "factory"}, nil
	}); err != nil {
		t.Fatalf("register factory: %v", err)
	}
	if !c.Bound("probe") {
		t.Fatal("registered factory should be bound")
	}
	if factoryCalls != 0 {
		t.Fatalf("Bound executed factory %d times, want 0", factoryCalls)
	}
	if c.Resolved("probe") {
		t.Fatal("Bound should not mark factory as resolved")
	}

	var deferredLoads int
	c.SetMissingFactoryLoader(func(key string) error {
		deferredLoads++
		if key == "deferred.probe" {
			return c.Singleton(key, func(containercontract.Resolver) (any, error) {
				return &helperProbe{name: "deferred"}, nil
			})
		}
		return nil
	})
	if c.Bound("deferred.probe") {
		t.Fatal("Bound should not trigger deferred provider loading")
	}
	if deferredLoads != 0 {
		t.Fatalf("Bound loaded deferred provider %d times, want 0", deferredLoads)
	}
	if !c.Has("deferred.probe") {
		t.Fatal("Has should trigger deferred provider loading")
	}
	if deferredLoads != 1 {
		t.Fatalf("Has loaded deferred provider %d times, want 1", deferredLoads)
	}
}

func TestTypedHelpersErrorsAndNilUse(t *testing.T) {
	c := NewContainer()
	SetProvider(func() *Container { return c })
	t.Cleanup(func() { SetProvider(nil) })

	if c.Bound("nil.probe") {
		t.Fatal("nil probe should not be bound")
	}
	if _, err := Make[*helperProbe]("missing"); !errors.Is(err, ErrFactoryNotRegistered) {
		t.Fatalf("missing typed make error = %v", err)
	}
	if err := c.Instance("wrong", "not probe"); err != nil {
		t.Fatalf("wrong instance: %v", err)
	}
	if _, err := Make[*helperProbe]("wrong"); err == nil {
		t.Fatal("type mismatch should fail")
	}
}

func TestPackageRegisterFactory(t *testing.T) {
	c := NewContainer()
	SetProvider(func() *Container { return c })
	t.Cleanup(func() { SetProvider(nil) })

	if err := c.Singleton("current.factory", func(containercontract.Resolver) (any, error) {
		return &helperProbe{name: "current"}, nil
	}, WithCloser(func(p *helperProbe) error { return nil })); err != nil {
		t.Fatalf("register current factory: %v", err)
	}
	got, err := Make[*helperProbe]("current.factory")
	if err != nil {
		t.Fatalf("make current factory: %v", err)
	}
	if got.name != "current" {
		t.Fatalf("current factory name = %q", got.name)
	}

	infos := List()
	if len(infos) != 1 || infos[0].Key != "current.factory" || !infos[0].Closable {
		t.Fatalf("list = %#v", infos)
	}
	if err := Close(context.Background()); err != nil {
		t.Fatalf("close current: %v", err)
	}
	if Value[*helperProbe]("current.factory") != nil {
		t.Fatal("close should clear current value")
	}

	if err := Close(context.Background()); err != nil {
		t.Fatalf("package close: %v", err)
	}
}

func TestValueDoesNotResolveSingletonFactory(t *testing.T) {
	c := NewContainer()
	SetProvider(func() *Container { return c })
	t.Cleanup(func() { SetProvider(nil) })

	var factoryCalls int
	if err := c.Singleton("current.lazy", func(containercontract.Resolver) (any, error) {
		factoryCalls++
		return &helperProbe{name: "lazy"}, nil
	}); err != nil {
		t.Fatalf("register current factory: %v", err)
	}
	if got := Value[*helperProbe]("current.lazy"); got != nil {
		t.Fatalf("Value resolved singleton factory, got %#v", got)
	}
	if factoryCalls != 0 {
		t.Fatalf("Value executed factory %d times, want 0", factoryCalls)
	}
	if c.Resolved("current.lazy") {
		t.Fatal("Value should not mark singleton factory as resolved")
	}
}

func TestContainerCloseCanceledKeepsRegisteredService(t *testing.T) {
	c := NewContainer()
	if err := c.Instance("kept", &helperProbe{name: "kept"}); err != nil {
		t.Fatalf("instance: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := c.Close(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("close canceled error = %v", err)
	}
	if !c.Resolved("kept") {
		t.Fatal("canceled close should not clear service")
	}
}

func TestContainerMakeAndCallErrorBranches(t *testing.T) {
	c := NewContainer()
	if err := c.Bind("", func(containercontract.Resolver) (any, error) { return nil, nil }); err == nil {
		t.Fatal("empty bind key should fail")
	}
	if err := c.Bind("nil.factory", nil); err == nil {
		t.Fatal("nil factory should fail")
	}
	if _, err := c.Make(""); err == nil {
		t.Fatal("empty make key should fail")
	}
	if _, err := c.Make("missing"); !errors.Is(err, ErrFactoryNotRegistered) {
		t.Fatalf("missing make error = %v", err)
	}
	if _, err := c.Factory("missing"); !errors.Is(err, ErrFactoryNotRegistered) {
		t.Fatalf("missing factory error = %v", err)
	}
	if err := c.Alias("", "bad"); err == nil {
		t.Fatal("empty alias target should fail")
	}
	if _, err := c.Call(42); err == nil {
		t.Fatal("non-function call should fail")
	}
	if _, err := c.Call(func(int) {}, "not-int"); err == nil {
		t.Fatal("incompatible explicit argument should fail")
	}
	if _, err := c.Call(func(*helperProbe) {}); err == nil {
		t.Fatal("missing implicit argument should fail")
	}
	if err := c.Instance(reflect.TypeOf((*helperProbe)(nil)).String(), "not probe"); err != nil {
		t.Fatalf("implicit mismatch instance: %v", err)
	}
	if _, err := c.Call(func(*helperProbe) {}); err == nil {
		t.Fatal("resolved implicit argument type mismatch should fail")
	}
	out, err := c.Call(func(value int64, probe *helperProbe) (int64, bool) {
		return value, probe == nil
	}, int(7), nil)
	if err != nil {
		t.Fatalf("call convertible and nil args: %v", err)
	}
	if !reflect.DeepEqual(out, []any{int64(7), true}) {
		t.Fatalf("call output = %#v", out)
	}
}

func TestContainerNilReceiversAndCurrentErrorBranches(t *testing.T) {
	// 逻辑说明：nil container 和未安装 current provider 是 facade 装配错误；这些入口应返回
	// 稳定错误或零值，便于上层用 errors.Is 做明确诊断。
	SetProvider(nil)
	var c *Container
	c.Forget("missing")
	c.SetMissingFactoryLoader(func(string) error { return nil })

	if err := c.Bind("service", func(containercontract.Resolver) (any, error) { return nil, nil }); !errors.Is(err, ErrNoCurrentContainer) {
		t.Fatalf("nil bind error = %v", err)
	}
	if err := c.Singleton("service", func(containercontract.Resolver) (any, error) { return nil, nil }); !errors.Is(err, ErrNoCurrentContainer) {
		t.Fatalf("nil singleton error = %v", err)
	}
	if err := c.Instance("service", &helperProbe{}); !errors.Is(err, ErrNoCurrentContainer) {
		t.Fatalf("nil instance error = %v", err)
	}
	if _, err := c.Factory("service"); !errors.Is(err, ErrNoCurrentContainer) {
		t.Fatalf("nil factory error = %v", err)
	}
	if err := c.Alias("service", "alias"); !errors.Is(err, ErrNoCurrentContainer) {
		t.Fatalf("nil alias error = %v", err)
	}
	if c.Has("service") || c.Bound("service") || c.Resolved("service") {
		t.Fatal("nil container should not report service state")
	}
	if err := Close(context.Background()); !errors.Is(err, ErrNoCurrentContainer) {
		t.Fatalf("package close without current error = %v", err)
	}
	if _, err := Make[*helperProbe](""); !errors.Is(err, ErrNoCurrentContainer) {
		t.Fatalf("empty package make error = %v", err)
	}
	if got := newNoCurrentContainerError(""); !errors.Is(got, ErrNoCurrentContainer) {
		t.Fatalf("empty no-current error = %v", got)
	}
}

func TestContainerAliasCyclesResolveSafely(t *testing.T) {
	// 设计原因：alias 可以由多个 provider 注册，循环别名不能让 canonical 解析陷入死循环。
	c := NewContainer()
	if err := c.Alias("alpha", "beta"); err != nil {
		t.Fatalf("alias beta: %v", err)
	}
	if err := c.Alias("beta", "alpha"); err != nil {
		t.Fatalf("alias alpha: %v", err)
	}
	if c.Has("alpha") || c.Resolved("beta") {
		t.Fatal("alias cycle without binding should remain unresolved")
	}
}

func TestInternalFactoryRegistrationCompatibility(t *testing.T) {
	c := NewContainer()
	c.register("internal", "string", nil, "")
	c.setFactory("internal", func() (any, error) {
		return "value", nil
	})
	got, err := c.Make("internal")
	if err != nil {
		t.Fatalf("make internal: %v", err)
	}
	if got != "value" {
		t.Fatalf("internal value = %v", got)
	}
	if !c.Resolved("internal") {
		t.Fatal("internal factory should mark service resolved")
	}
	c.clear("internal")
	if c.Resolved("internal") {
		t.Fatal("clear should reset resolved state")
	}

	c.register("group.default", "string", nil, "")
	infos := c.List()
	var found bool
	for _, info := range infos {
		if info.Key == "group.default" {
			found = true
			if info.CloseGroup != CloseGroupNormal {
				t.Fatalf("default close group = %q", info.CloseGroup)
			}
		}
	}
	if !found {
		t.Fatal("registered entry not listed")
	}
}

func TestHelperNilAndMismatchBranches(t *testing.T) {
	SetProvider(nil)
	if got := Value[*helperProbe]("missing"); got != nil {
		t.Fatalf("value without current container = %#v, want nil", got)
	}
	c := NewContainer()
	SetProvider(func() *Container { return c })
	t.Cleanup(func() { SetProvider(nil) })
	if got := Value[*helperProbe](""); got != nil {
		t.Fatalf("empty key value = %#v, want nil", got)
	}
	if err := c.Instance("nil.option", &helperProbe{name: "nil"}, WithCloser[*helperProbe](nil)); err != nil {
		t.Fatalf("nil closer option: %v", err)
	}
	if err := c.Instance("wrong.closer", "value", WithCloser(func(*helperProbe) error {
		t.Fatal("typed closer should ignore non-matching value")
		return nil
	})); err != nil {
		t.Fatalf("wrong closer instance: %v", err)
	}
	var nilProbe *helperProbe
	if err := c.Instance("nil.closer", nilProbe, WithContextCloser(func(context.Context, *helperProbe) error {
		t.Fatal("typed context closer should ignore nil pointer values")
		return nil
	})); err != nil {
		t.Fatalf("nil closer instance: %v", err)
	}
	if err := c.Close(context.Background()); err != nil {
		t.Fatalf("close ignored closers: %v", err)
	}
	var nilBinding *containercontract.Binding
	WithCloseGroup(CloseGroupReporting)(nilBinding)
	WithCloser(func(*helperProbe) error { return nil })(nilBinding)
	WithContextCloser(func(context.Context, *helperProbe) error { return nil })(nilBinding)
}
