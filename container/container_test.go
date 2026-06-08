package container

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	containercontract "github.com/prismgo/framework/contracts/container"
)

type closeProbe struct {
	name string
}

func TestContainerBindingLifecycles(t *testing.T) {
	c := NewContainer()
	transientCalls := 0
	if err := c.Bind("transient", func(containercontract.Resolver) (any, error) {
		transientCalls++
		return transientCalls, nil
	}); err != nil {
		t.Fatalf("bind transient: %v", err)
	}

	first, err := c.Make("transient")
	if err != nil {
		t.Fatalf("make transient first: %v", err)
	}
	second, err := c.Make("transient")
	if err != nil {
		t.Fatalf("make transient second: %v", err)
	}
	if first == second {
		t.Fatalf("transient reused value %v", first)
	}
	if !c.Resolved("transient") {
		t.Fatal("transient should be marked resolved after Make")
	}

	singletonCalls := 0
	if err := c.Singleton("singleton", func(containercontract.Resolver) (any, error) {
		singletonCalls++
		return &closeProbe{name: "singleton"}, nil
	}); err != nil {
		t.Fatalf("bind singleton: %v", err)
	}
	one, err := c.Make("singleton")
	if err != nil {
		t.Fatalf("make singleton first: %v", err)
	}
	two, err := c.Make("singleton")
	if err != nil {
		t.Fatalf("make singleton second: %v", err)
	}
	if one != two {
		t.Fatal("singleton returned different instances")
	}
	if singletonCalls != 1 {
		t.Fatalf("singleton factory calls = %d, want 1", singletonCalls)
	}

	explicit := &closeProbe{name: "instance"}
	if err := c.Instance("instance", explicit); err != nil {
		t.Fatalf("instance: %v", err)
	}
	got, err := c.Make("instance")
	if err != nil {
		t.Fatalf("make instance: %v", err)
	}
	if got != explicit {
		t.Fatal("instance did not return explicit value")
	}
}

func TestContainerAliasBoundHasResolvedAndFactory(t *testing.T) {
	c := NewContainer()
	if err := c.Singleton("cache.manager", func(containercontract.Resolver) (any, error) {
		return "manager", nil
	}); err != nil {
		t.Fatalf("singleton: %v", err)
	}
	if err := c.Alias("cache.manager", "cache"); err != nil {
		t.Fatalf("alias: %v", err)
	}

	if !c.Bound("cache") || !c.Bound("cache.manager") {
		t.Fatal("alias and canonical key should be bound")
	}
	if !c.Has("cache") {
		t.Fatal("alias should be resolvable")
	}
	if c.Resolved("cache") {
		t.Fatal("service should not be resolved before Make")
	}

	factory, err := c.Factory("cache")
	if err != nil {
		t.Fatalf("factory: %v", err)
	}
	value, err := factory()
	if err != nil {
		t.Fatalf("factory call: %v", err)
	}
	if value != "manager" {
		t.Fatalf("factory value = %v, want manager", value)
	}
	if !c.Resolved("cache.manager") || !c.Resolved("cache") {
		t.Fatal("canonical key and alias should both report resolved")
	}
}

func TestContainerDeferredLoaderSemantics(t *testing.T) {
	c := NewContainer()
	loads := 0
	c.SetMissingFactoryLoader(func(key string) error {
		loads++
		if key != "deferred" {
			return nil
		}
		return c.Singleton("deferred", func(containercontract.Resolver) (any, error) {
			return "loaded", nil
		})
	})

	if c.Bound("deferred") {
		t.Fatal("Bound should not trigger deferred loading")
	}
	if loads != 0 {
		t.Fatalf("loader calls after Bound = %d, want 0", loads)
	}
	if !c.Has("deferred") {
		t.Fatal("Has should trigger deferred loading")
	}
	got, err := c.Make("deferred")
	if err != nil {
		t.Fatalf("make deferred: %v", err)
	}
	if got != "loaded" {
		t.Fatalf("deferred value = %v, want loaded", got)
	}
}

func TestContainerCallResolvesMissingArguments(t *testing.T) {
	c := NewContainer()
	if err := c.Instance(reflect.TypeOf((*closeProbe)(nil)).String(), &closeProbe{name: "resolved"}); err != nil {
		t.Fatalf("instance: %v", err)
	}

	out, err := c.Call(func(prefix string, probe *closeProbe) string {
		return prefix + ":" + probe.name
	}, "value")
	if err != nil {
		t.Fatalf("call: %v", err)
	}
	if len(out) != 1 || out[0] != "value:resolved" {
		t.Fatalf("call output = %#v", out)
	}
}

func TestContainerCloseGroupOrderAndRetry(t *testing.T) {
	c := NewContainer()
	var calls []string
	closeErr := errors.New("close failed")
	failOnce := true
	closeOption := func(group CloseGroup, name string) containercontract.BindingOption {
		return func(binding *containercontract.Binding) {
			binding.CloseGroup = group
			binding.Closer = func(context.Context, any) error {
				calls = append(calls, name)
				if name == "normal-a" && failOnce {
					failOnce = false
					return closeErr
				}
				return nil
			}
		}
	}

	if err := c.Instance("normal-a", &closeProbe{name: "a"}, closeOption(CloseGroupNormal, "normal-a")); err != nil {
		t.Fatalf("instance normal-a: %v", err)
	}
	if err := c.Instance("reporting", &closeProbe{name: "reporting"}, closeOption(CloseGroupReporting, "reporting")); err != nil {
		t.Fatalf("instance reporting: %v", err)
	}
	if err := c.Instance("normal-b", &closeProbe{name: "b"}, closeOption(CloseGroupNormal, "normal-b")); err != nil {
		t.Fatalf("instance normal-b: %v", err)
	}

	err := c.CloseGroup(context.Background(), CloseGroupNormal)
	if !errors.Is(err, closeErr) {
		t.Fatalf("close normal error = %v, want %v", err, closeErr)
	}
	if !reflect.DeepEqual(calls, []string{"normal-b", "normal-a"}) {
		t.Fatalf("close order = %#v", calls)
	}
	if !c.Resolved("normal-a") {
		t.Fatal("failed close should keep service resolved for retry")
	}

	calls = nil
	if err := c.CloseGroup(context.Background(), CloseGroupNormal); err != nil {
		t.Fatalf("close normal retry: %v", err)
	}
	if !reflect.DeepEqual(calls, []string{"normal-a"}) {
		t.Fatalf("retry order = %#v", calls)
	}

	calls = nil
	if err := c.CloseGroup(context.Background(), CloseGroupReporting); err != nil {
		t.Fatalf("close reporting: %v", err)
	}
	if !reflect.DeepEqual(calls, []string{"reporting"}) {
		t.Fatalf("reporting order = %#v", calls)
	}
}

func TestContainerCloseWaitsForRunningSingletonFactory(t *testing.T) {
	c := NewContainer()
	factoryStarted := make(chan struct{})
	releaseFactory := make(chan struct{})
	makeDone := make(chan error, 1)
	closeDone := make(chan error, 1)
	closed := make(chan struct{}, 1)

	// 需求背景：Close 与 lazy singleton 初始化并发时，Close 必须等待已开始的同 key
	// factory 稳定后再读取关闭快照，否则 factory 成功注册的资源会漏掉 closer。
	if err := c.Singleton("shared", func(containercontract.Resolver) (any, error) {
		close(factoryStarted)
		<-releaseFactory
		return &closeProbe{name: "shared"}, nil
	}, func(binding *containercontract.Binding) {
		binding.Closer = func(context.Context, any) error {
			closed <- struct{}{}
			return nil
		}
	}); err != nil {
		t.Fatalf("singleton: %v", err)
	}

	go func() {
		_, err := c.Make("shared")
		makeDone <- err
	}()
	<-factoryStarted

	go func() {
		closeDone <- c.Close(context.Background())
	}()

	select {
	case err := <-closeDone:
		close(releaseFactory)
		t.Fatalf("close returned before running singleton factory completed: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(releaseFactory)
	if err := <-makeDone; err != nil {
		t.Fatalf("make: %v", err)
	}
	if err := <-closeDone; err != nil {
		t.Fatalf("close: %v", err)
	}
	select {
	case <-closed:
	default:
		t.Fatal("close did not call singleton closer")
	}
	if c.Resolved("shared") {
		t.Fatal("close should clear singleton after closer succeeds")
	}
}

func TestContainerCloseWaitsForLazyFactoryContextCancel(t *testing.T) {
	c := NewContainer()
	factoryStarted := make(chan struct{})
	releaseFactory := make(chan struct{})
	makeDone := make(chan error, 1)
	closeDone := make(chan error, 1)
	closed := make(chan struct{}, 1)

	// 需求背景：Close(ctx) 等待 lazy singleton 初始化属于关闭流程的一部分，ctx
	// 取消时必须返回取消错误，并保留尚未完成注册的资源给后续 Close 重试。
	if err := c.Singleton("shared", func(containercontract.Resolver) (any, error) {
		close(factoryStarted)
		<-releaseFactory
		return &closeProbe{name: "shared"}, nil
	}, func(binding *containercontract.Binding) {
		binding.Closer = func(context.Context, any) error {
			closed <- struct{}{}
			return nil
		}
	}); err != nil {
		t.Fatalf("singleton: %v", err)
	}

	go func() {
		_, err := c.Make("shared")
		makeDone <- err
	}()
	<-factoryStarted

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		closeDone <- c.Close(ctx)
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()

	if err := <-closeDone; !errors.Is(err, context.Canceled) {
		close(releaseFactory)
		t.Fatalf("close error = %v, want context canceled", err)
	}
	select {
	case <-closed:
		close(releaseFactory)
		t.Fatal("close should not call closer after context cancellation")
	default:
	}

	close(releaseFactory)
	if err := <-makeDone; err != nil {
		t.Fatalf("make: %v", err)
	}
	if !c.Resolved("shared") {
		t.Fatal("canceled close should keep completed singleton resolved for retry")
	}
	if err := c.Close(context.Background()); err != nil {
		t.Fatalf("close retry: %v", err)
	}
	select {
	case <-closed:
	default:
		t.Fatal("close retry did not call singleton closer")
	}
	if c.Resolved("shared") {
		t.Fatal("close retry should clear singleton after closer succeeds")
	}
}

func TestContainerCloseGroupDoesNotWaitForOtherGroupLazyFactory(t *testing.T) {
	c := NewContainer()
	factoryStarted := make(chan struct{})
	releaseFactory := make(chan struct{})
	makeDone := make(chan error, 1)
	closedNormal := make(chan struct{}, 1)

	// 设计原因：CloseGroup 是分阶段关闭机制，只应等待目标分组的 lazy 初始化；
	// 非目标分组可能是错误上报资源，不能拖住当前分组的关闭和重试流程。
	if err := c.Instance("normal", &closeProbe{name: "normal"}, func(binding *containercontract.Binding) {
		binding.CloseGroup = CloseGroupNormal
		binding.Closer = func(context.Context, any) error {
			closedNormal <- struct{}{}
			return nil
		}
	}); err != nil {
		t.Fatalf("instance normal: %v", err)
	}
	if err := c.Singleton("reporting", func(containercontract.Resolver) (any, error) {
		close(factoryStarted)
		<-releaseFactory
		return &closeProbe{name: "reporting"}, nil
	}, func(binding *containercontract.Binding) {
		binding.CloseGroup = CloseGroupReporting
		binding.Closer = func(context.Context, any) error { return nil }
	}); err != nil {
		t.Fatalf("singleton reporting: %v", err)
	}

	go func() {
		_, err := c.Make("reporting")
		makeDone <- err
	}()
	<-factoryStarted

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- c.CloseGroup(context.Background(), CloseGroupNormal)
	}()
	select {
	case err := <-closeDone:
		if err != nil {
			close(releaseFactory)
			t.Fatalf("close normal group: %v", err)
		}
	case <-time.After(200 * time.Millisecond):
		close(releaseFactory)
		t.Fatal("close normal group waited for reporting singleton factory")
	}
	select {
	case <-closedNormal:
	default:
		close(releaseFactory)
		t.Fatal("close normal group did not close normal resource")
	}

	close(releaseFactory)
	if err := <-makeDone; err != nil {
		t.Fatalf("make reporting: %v", err)
	}
	if !c.Resolved("reporting") {
		t.Fatal("non-target reporting singleton should remain resolved for its own close group")
	}
}

func TestContainerSingletonConcurrentMake(t *testing.T) {
	c := NewContainer()
	calls := 0
	if err := c.Singleton("shared", func(containercontract.Resolver) (any, error) {
		calls++
		return &closeProbe{name: "shared"}, nil
	}); err != nil {
		t.Fatalf("singleton: %v", err)
	}

	var wg sync.WaitGroup
	results := make(chan any, 16)
	errs := make(chan error, 16)
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, err := c.Make("shared")
			if err != nil {
				errs <- err
				return
			}
			results <- got
		}()
	}
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		t.Fatalf("make shared: %v", err)
	}
	var first any
	for got := range results {
		if first == nil {
			first = got
			continue
		}
		if got != first {
			t.Fatal("singleton concurrent Make returned different instances")
		}
	}
	if calls != 1 {
		t.Fatalf("singleton factory calls = %d, want 1", calls)
	}
}

func TestContainerSingletonRetriesAfterFactoryError(t *testing.T) {
	// 需求背景：Singleton 的 factory 可能依赖启动期资源，第一次解析失败不应冻结容器状态。
	// 逻辑说明：第一次 Make 返回 factory 错误后，第二次 Make 必须重新执行 factory；
	// 第二次成功后再验证第三次 Make 复用同一实例，覆盖“失败不缓存、成功才缓存”的公开契约。
	c := NewContainer()
	factoryErr := errors.New("factory failed")
	calls := 0
	if err := c.Singleton("shared", func(containercontract.Resolver) (any, error) {
		calls++
		if calls == 1 {
			return nil, factoryErr
		}
		return &closeProbe{name: "shared"}, nil
	}); err != nil {
		t.Fatalf("singleton: %v", err)
	}

	if _, err := c.Make("shared"); !errors.Is(err, factoryErr) {
		t.Fatalf("first make error = %v, want %v", err, factoryErr)
	}
	first, err := c.Make("shared")
	if err != nil {
		t.Fatalf("second make: %v", err)
	}
	second, err := c.Make("shared")
	if err != nil {
		t.Fatalf("third make: %v", err)
	}
	if first != second {
		t.Fatal("singleton did not cache successful retry")
	}
	if calls != 2 {
		t.Fatalf("factory calls = %d, want 2", calls)
	}
}

func TestContainerSingletonConcurrentFailuresRetryUntilOneSuccess(t *testing.T) {
	// 设计思路：并发 Make 只能串行进入同一个 singleton factory，但失败尝试不能写入最终实例。
	// 该测试通过前 3 次失败、第 4 次成功验证重试语义；成功后所有后续 goroutine 必须拿到同一实例。
	c := NewContainer()
	factoryErr := errors.New("factory failed")
	calls := 0
	shared := &closeProbe{name: "shared"}
	if err := c.Singleton("shared", func(containercontract.Resolver) (any, error) {
		calls++
		if calls <= 3 {
			return nil, factoryErr
		}
		return shared, nil
	}); err != nil {
		t.Fatalf("singleton: %v", err)
	}

	var wg sync.WaitGroup
	results := make(chan any, 12)
	errs := make(chan error, 12)
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, err := c.Make("shared")
			if err != nil {
				errs <- err
				return
			}
			results <- got
		}()
	}
	wg.Wait()
	close(results)
	close(errs)

	if len(errs) != 3 {
		t.Fatalf("error count = %d, want 3", len(errs))
	}
	for err := range errs {
		if !errors.Is(err, factoryErr) {
			t.Fatalf("make error = %v, want %v", err, factoryErr)
		}
	}
	for got := range results {
		if got != shared {
			t.Fatal("successful concurrent Make returned a different singleton instance")
		}
	}
	if calls != 4 {
		t.Fatalf("factory calls = %d, want 4", calls)
	}
}

func TestContainerConcurrentMakeAndRebindHasNoRace(t *testing.T) {
	// 需求背景：运行期热替换会让 Make 与 Singleton、Instance、Forget 交错执行。
	// 行为断言保持在公开接口层面：race 检测负责发现数据竞争，最终状态只要求可解析或明确未注册。
	c := NewContainer()
	if err := c.Singleton("service", func(containercontract.Resolver) (any, error) {
		return &closeProbe{name: "initial"}, nil
	}); err != nil {
		t.Fatalf("singleton: %v", err)
	}

	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for j := 0; j < 50; j++ {
				_, err := c.Make("service")
				if err != nil && !errors.Is(err, ErrFactoryNotRegistered) {
					t.Errorf("make service: %v", err)
				}
			}
		}()
	}
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			<-start
			for j := 0; j < 30; j++ {
				switch j % 4 {
				case 0:
					if err := c.Singleton("service", func(containercontract.Resolver) (any, error) {
						return &closeProbe{name: "singleton"}, nil
					}); err != nil {
						t.Errorf("singleton: %v", err)
					}
				case 1:
					if err := c.Instance("service", &closeProbe{name: "instance"}); err != nil {
						t.Errorf("instance: %v", err)
					}
				case 2:
					c.Forget("service")
				default:
					if err := c.Bind("service", func(containercontract.Resolver) (any, error) {
						return &closeProbe{name: "bound"}, nil
					}); err != nil {
						t.Errorf("bind: %v", err)
					}
				}
			}
			_ = worker
		}(i)
	}
	close(start)
	wg.Wait()

	_, err := c.Make("service")
	if err != nil && !errors.Is(err, ErrFactoryNotRegistered) {
		t.Fatalf("final make error = %v, want nil or ErrFactoryNotRegistered", err)
	}
}
