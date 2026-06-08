package cmd

import (
	"bytes"
	"context"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	queuecommand "github.com/prismgo/framework/cmd/queue"
	"github.com/prismgo/framework/console"
	"github.com/prismgo/framework/container"
	containercontract "github.com/prismgo/framework/contracts/container"
	queuecore "github.com/prismgo/framework/queue"
	"github.com/prismgo/framework/route"
	"github.com/prismgo/framework/timer"

	"github.com/gin-gonic/gin"
	"github.com/spf13/cobra"
)

func TestQueueCommandRunStartsWorkerAndStopsWhenEmpty(t *testing.T) {
	registry := container.NewContainer()
	container.SetProvider(func() *container.Container { return registry })
	t.Cleanup(func() { container.SetProvider(nil) })
	manager, err := queuecore.NewManager(queuecore.Config{Default: "sync"}, queuecore.DefaultRegistry())
	if err != nil {
		t.Fatalf("new queue manager: %v", err)
	}
	if err := registry.Instance("queue.manager", manager); err != nil {
		t.Fatalf("bind queue manager: %v", err)
	}

	cmd := queuecommand.NewWorkCommand()
	stdout := &bytes.Buffer{}
	commandCtx := console.NewCommandContext(context.Background(), cmd, *cmd.Definition(), fakeInput{
		options: map[string]string{"queue": "default"},
		bools:   map[string]bool{"stop-when-empty": true},
	}, console.NewIO(strings.NewReader(""), stdout, io.Discard), nil, &cobra.Command{Use: "queue"})
	if err := cmd.Handle(commandCtx); err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if !strings.Contains(stdout.String(), "queue worker started") {
		t.Fatalf("expected queue worker start output, got %q", stdout.String())
	}
}

func TestCronCommandRunRegistersAndStopsOnCanceledContext(t *testing.T) {
	fakeKernel := &fakeCronKernel{schedule: timer.NewSchedule()}
	registered := false
	cmd := NewCronCommand(fakeKernel, func(schedule *timer.Schedule) {
		registered = schedule != nil
	})
	parent, cancel := context.WithCancel(context.Background())
	cancel()
	commandCtx := console.NewCommandContext(parent, cmd, *cmd.Definition(), fakeInput{}, console.NewIO(strings.NewReader(""), io.Discard, io.Discard), nil, &cobra.Command{Use: "cron"})
	if err := cmd.Handle(commandCtx); err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if !registered {
		t.Fatal("expected cron register callback to be called")
	}
	if !fakeKernel.started {
		t.Fatal("expected cron command to start scheduler")
	}
	if !fakeKernel.stopped {
		t.Fatal("expected cron command to stop scheduler")
	}
}

func TestServeCommandRunUsesProcessControlBranch(t *testing.T) {
	cmd := NewServeCommand(fakeHTTPServerFactory(&fakeHTTPRegistrars{}))
	ctx := console.NewCommandContext(context.Background(), cmd, *cmd.Definition(), fakeInput{
		options: map[string]string{"port": "7001"},
		bools:   map[string]bool{"restart": true},
	}, console.NewIO(strings.NewReader(""), io.Discard, io.Discard), nil, &cobra.Command{Use: "serve"})

	err := cmd.Handle(ctx)
	if err == nil || !strings.Contains(err.Error(), "read pid file failed") {
		t.Fatalf("expected process control error, got %v", err)
	}
}

func TestServeCommandGetPIDFileUsesPort(t *testing.T) {
	path := NewServeCommand(fakeHTTPServerFactory(&fakeHTTPRegistrars{})).getPIDFile("8051")
	if !strings.Contains(path, "8051") {
		t.Fatalf("expected pid file path to include port, got %q", path)
	}
}

func TestServeCommandProcessControlMethods(t *testing.T) {
	manager := &fakeProcessManager{pid: 100, reloadedPID: 101, restartedPID: 102}
	cmd := NewServeCommand(fakeHTTPServerFactory(&fakeHTTPRegistrars{}))
	cmd.newProcessManager = func(string) processManager { return manager }
	ioo := console.NewIO(strings.NewReader(""), io.Discard, io.Discard)

	definition := cmd.Definition()
	if definition.Name == "" || definition.Description == "" {
		t.Fatal("expected serve command definition to be non-empty")
	}

	if err := cmd.handleProcessControl("8051", false, false, false, true, ioo); err != nil {
		t.Fatalf("kill process control returned error: %v", err)
	}
	if !manager.killed {
		t.Fatal("expected kill branch to call Kill")
	}

	manager.killed = false
	if err := cmd.handleProcessControl("8051", false, false, true, false, ioo); err != nil {
		t.Fatalf("stop process control returned error: %v", err)
	}
	if !manager.stopped {
		t.Fatal("expected stop branch to call Stop")
	}

	if err := cmd.handleProcessControl("8051", false, true, false, false, ioo); err != nil {
		t.Fatalf("reload process control returned error: %v", err)
	}
	if !manager.reloaded {
		t.Fatal("expected reload branch to call Reload")
	}

	if err := cmd.handleProcessControl("8051", true, false, false, false, ioo); err != nil {
		t.Fatalf("restart process control returned error: %v", err)
	}
	if !manager.restarted {
		t.Fatal("expected restart branch to call Restart")
	}
}

func TestRouteListCommandShowsAndFiltersRegisteredRoutes(t *testing.T) {
	setupRouteContainer(t)

	route.Get("/admin/users", func(*gin.Context) {}).Name("admin.users.index")
	route.Post("/admin/users", func(*gin.Context) {}).Name("admin.users.store")

	cmd := NewRouteListCommand(func() error { return nil })
	stdout := &bytes.Buffer{}
	ctx := console.NewCommandContext(context.Background(), cmd, *cmd.Definition(), fakeInput{
		options: map[string]string{"path": "/admin", "name": "index", "method": "GET"},
	}, console.NewIO(strings.NewReader(""), stdout, io.Discard), nil, &cobra.Command{Use: "route:list"})

	if err := cmd.Handle(ctx); err != nil {
		t.Fatalf("Handle returned error: %v", err)
	}
	output := stdout.String()
	if !strings.Contains(output, "admin.users.index") {
		t.Fatalf("route list output missing matching route: %q", output)
	}
	if strings.Contains(output, "admin.users.store") {
		t.Fatalf("route list output included filtered route: %q", output)
	}
	if containsMethod([]string{"POST"}, "GET") {
		t.Fatal("expected containsMethod to report missing method as false")
	}
}

func TestListCommandSupportsSymfonyFormatsAndNamespaceFiltering(t *testing.T) {
	t.Setenv("FORCE_COLOR", "1")
	t.Setenv("NO_COLOR", "")
	cmd := NewListCommand(func() []console.Definition {
		return []console.Definition{
			{Name: "list", Description: "List commands", Aliases: []string{"ls"}},
			{Name: "route:list", Description: "List routes"},
			{Name: "queue:work", Description: "Work queues"},
			{Name: "hidden", Description: "Hidden", Hidden: true},
		}
	})
	stdout := &bytes.Buffer{}
	cobraCmd := &cobra.Command{Use: "list"}
	cobraCmd.SetOut(stdout)
	ctx := console.NewCommandContext(context.Background(), cmd, *cmd.Definition(), fakeInput{
		args:    map[string][]string{"namespace": {"route"}},
		options: map[string]string{"format": "json"},
	}, console.NewIO(strings.NewReader(""), stdout, io.Discard), nil, cobraCmd)

	if err := cmd.Handle(ctx); err != nil {
		t.Fatalf("Handle returned error: %v", err)
	}
	output := stdout.String()
	if !strings.Contains(output, `"name": "route:list"`) || strings.Contains(output, "queue:work") || strings.Contains(output, "hidden") {
		t.Fatalf("unexpected json list output: %s", output)
	}

	stdout.Reset()
	ctx = console.NewCommandContext(context.Background(), cmd, *cmd.Definition(), fakeInput{
		options: map[string]string{"format": "txt"},
		bools:   map[string]bool{"raw": true},
	}, console.NewIO(strings.NewReader(""), stdout, io.Discard), nil, cobraCmd)
	if err := cmd.Handle(ctx); err != nil {
		t.Fatalf("Handle raw returned error: %v", err)
	}
	if !strings.Contains(stdout.String(), "list List commands") || strings.Contains(stdout.String(), "Available commands:") {
		t.Fatalf("unexpected raw list output: %s", stdout.String())
	}

	stdout.Reset()
	ctx = console.NewCommandContext(context.Background(), cmd, *cmd.Definition(), fakeInput{
		options: map[string]string{"format": "md"},
	}, console.NewIO(strings.NewReader(""), stdout, io.Discard), nil, cobraCmd)
	if err := cmd.Handle(ctx); err != nil {
		t.Fatalf("Handle markdown returned error: %v", err)
	}
	if !strings.Contains(stdout.String(), "# PrismGo") || !strings.Contains(stdout.String(), "`route:list`") {
		t.Fatalf("unexpected markdown list output: %s", stdout.String())
	}

	stdout.Reset()
	ctx = console.NewCommandContext(context.Background(), cmd, *cmd.Definition(), fakeInput{
		options: map[string]string{"format": "txt"},
	}, console.NewIO(strings.NewReader(""), stdout, io.Discard), nil, cobraCmd)
	if err := cmd.Handle(ctx); err != nil {
		t.Fatalf("Handle colored text returned error: %v", err)
	}
	if !strings.Contains(stdout.String(), "\x1b[33mUsage:\x1b[0m") || !strings.Contains(stdout.String(), "\x1b[32mlist\x1b[0m") {
		t.Fatalf("expected list output to auto-enable ANSI with FORCE_COLOR: %q", stdout.String())
	}
}

func TestRouteListCommandLaravelOutputFiltersSortsAndJSON(t *testing.T) {
	setupRouteContainer(t)
	auth := route.NamedMiddleware("auth", func(*gin.Context) {})
	route.Domain("api.example.com").Middleware(auth).Get("/users/{user}", func(*gin.Context) {}).Name("users.show")
	route.Post("/users", func(*gin.Context) {}).Name("users.store")

	cmd := NewRouteListCommand(func() error { return nil })
	stdout := &bytes.Buffer{}
	cobraCmd := &cobra.Command{Use: "route:list"}
	cobraCmd.SetOut(stdout)
	ctx := console.NewCommandContext(context.Background(), cmd, *cmd.Definition(), fakeInput{
		options: map[string]string{"method": "GET", "sort": "name"},
		bools:   map[string]bool{"ansi": true},
	}, console.NewIOWithOutputOptions(strings.NewReader(""), stdout, io.Discard, console.OutputOptions{ANSI: true}), nil, cobraCmd)

	if err := cmd.Handle(ctx); err != nil {
		t.Fatalf("Handle returned error: %v", err)
	}
	output := stdout.String()
	for _, want := range []string{"\n  ", "GET", "/users/", "{user}", "users.show ›", "Showing [1] routes", "\x1b["} {
		if !strings.Contains(output, want) {
			t.Fatalf("route output missing %q:\n%s", want, output)
		}
	}
	if strings.Contains(output, "users.store") {
		t.Fatalf("route output included filtered POST route:\n%s", output)
	}

	stdout.Reset()
	ctx = console.NewCommandContext(context.Background(), cmd, *cmd.Definition(), fakeInput{
		options: map[string]string{"middleware": "auth"},
		bools:   map[string]bool{"json": true, "no-ansi": true},
	}, console.NewIO(strings.NewReader(""), stdout, io.Discard), nil, cobraCmd)
	if err := cmd.Handle(ctx); err != nil {
		t.Fatalf("Handle json returned error: %v", err)
	}
	if !strings.Contains(stdout.String(), `"name": "users.show"`) || strings.Contains(stdout.String(), "\x1b[") {
		t.Fatalf("unexpected route json output: %s", stdout.String())
	}
}

func TestRouteListCommandLoadsRoutesEvenWhenProviderRoutesAlreadyExist(t *testing.T) {
	setupRouteContainer(t)
	route.Get("/horizon", func(*gin.Context) {})

	cmd := NewRouteListCommand(func() error {
		route.Get("/horizon", func(*gin.Context) {})
		route.Get("/api/v1/workorders", func(*gin.Context) {}).Name("workorders.index")
		return nil
	})
	stdout := &bytes.Buffer{}
	ctx := console.NewCommandContext(context.Background(), cmd, *cmd.Definition(), fakeInput{}, console.NewIO(strings.NewReader(""), stdout, io.Discard), nil, &cobra.Command{Use: "route:list"})

	if err := cmd.Handle(ctx); err != nil {
		t.Fatalf("Handle returned error: %v", err)
	}
	output := stdout.String()
	if !strings.Contains(output, "/api/v1/workorders") {
		t.Fatalf("route list output missing application route after loader ran: %q", output)
	}
}

func TestRouteListHelpersCoverFiltersSortsAndFallbacks(t *testing.T) {
	routes := []route.RouteInfo{
		{Methods: []string{"POST"}, URI: "/admin/users", Name: "users.store", Domain: "admin.example.com", Handler: "App.Store", Middleware: []string{"auth"}, SourcePath: "/app/routes.go"},
		{Methods: []string{"GET"}, URI: "/api/users/{user}", Name: "users.show", Domain: "api.example.com", Handler: "App.Show", Middleware: []string{"auth", "throttle"}, SourcePath: "/go/pkg/mod/vendor/routes.go"},
		{Methods: []string{"DELETE"}, URI: "", GinPath: "/fallback", Name: "fallback", Handler: "App.Delete"},
	}

	filtered := filterRouteList(routes, routeListFilters{
		action:       "show",
		domain:       "api.",
		middleware:   "throttle",
		path:         "users",
		exceptPath:   "admin",
		onlyVendor:   true,
		exceptVendor: false,
	})
	if len(filtered) != 1 || filtered[0].Name != "users.show" {
		t.Fatalf("filtered routes = %#v", filtered)
	}
	if got := filterRouteList(routes, routeListFilters{exceptVendor: true}); len(got) != 2 {
		t.Fatalf("except vendor count = %d, want 2", len(got))
	}
	if got := displayRouteURI(routes[2]); got != "/fallback" {
		t.Fatalf("display fallback URI = %q", got)
	}

	for _, key := range []string{"domain", "method", "name", "action", "middleware", "definition", "uri"} {
		copied := append([]route.RouteInfo(nil), routes...)
		sortRouteList(copied, key, key == "method")
		if len(copied) != len(routes) {
			t.Fatalf("sort %s changed route count", key)
		}
	}

	t.Setenv("COLUMNS", "120")
	if terminalWidth() != 120 {
		t.Fatalf("terminalWidth did not read COLUMNS")
	}
	if !strings.Contains(colorMethod("DELETE", console.OutputOptions{ANSI: true}), "\x1b[") {
		t.Fatal("expected colored DELETE method")
	}
	if got := routeListLine(routes[2], 40, console.OutputOptions{}); !strings.Contains(got, "/fallback") {
		t.Fatalf("routeListLine fallback = %q", got)
	}
}

func TestRouteListCommandErrorsWhenLoaderMissingAndRoutesEmpty(t *testing.T) {
	setupRouteContainer(t)

	cmd := NewRouteListCommand(nil)
	ctx := console.NewCommandContext(context.Background(), cmd, *cmd.Definition(), fakeInput{}, console.NewIO(strings.NewReader(""), io.Discard, io.Discard), nil, &cobra.Command{Use: "route:list"})

	err := cmd.Handle(ctx)
	if err == nil || !strings.Contains(err.Error(), "route loader is not configured") {
		t.Fatalf("expected missing loader error, got %v", err)
	}
}

func setupRouteContainer(t *testing.T) {
	t.Helper()
	c := container.NewContainer()
	container.SetProvider(func() *container.Container { return c })
	t.Cleanup(func() { container.SetProvider(nil) })
	_ = route.ServiceProvider{}.Register(fakeContainerApp{container: c})
}

type fakeContainerApp struct {
	container *container.Container
}

func (a fakeContainerApp) Container() containercontract.Container { return a.container }

type fakeInput struct {
	args    map[string][]string
	options map[string]string
	bools   map[string]bool
}

type fakeProcessManager struct {
	pid          int
	reloadedPID  int
	restartedPID int
	killed       bool
	stopped      bool
	reloaded     bool
	restarted    bool
}

type fakeCronKernel struct {
	schedule *timer.Schedule
	started  bool
	stopped  bool
}

func (pm *fakeProcessManager) SavePID() error        { return nil }
func (pm *fakeProcessManager) RemovePID() error      { return nil }
func (pm *fakeProcessManager) ReadPID() (int, error) { return pm.pid, nil }
func (pm *fakeProcessManager) Kill(pid int) error {
	pm.killed = pid == pm.pid
	return nil
}
func (pm *fakeProcessManager) Stop(pid int, _ time.Duration) error {
	pm.stopped = pid == pm.pid
	return nil
}
func (pm *fakeProcessManager) Reload(pid int, _ string, _ []string, _ time.Duration) (int, error) {
	pm.reloaded = pid == pm.pid
	return pm.reloadedPID, nil
}
func (pm *fakeProcessManager) Restart(pid int, _ string, _ []string) (int, error) {
	pm.restarted = pid == pm.pid
	return pm.restartedPID, nil
}

func (k *fakeCronKernel) Schedule() *timer.Schedule { return k.schedule }
func (k *fakeCronKernel) Start(context.Context)     { k.started = true }
func (k *fakeCronKernel) Stop()                     { k.stopped = true }

func (i fakeInput) Argument(name string) string {
	values := i.args[name]
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

func (i fakeInput) Arguments(name string) []string { return append([]string(nil), i.args[name]...) }
func (i fakeInput) Option(name string) string      { return i.options[name] }
func (i fakeInput) OptionStrings(name string) []string {
	value := i.options[name]
	if value == "" {
		return nil
	}
	return []string{value}
}
func (i fakeInput) OptionBool(name string) bool { return i.bools[name] }
func (i fakeInput) OptionInt(name string) int {
	value := i.options[name]
	if value == "" {
		return 0
	}
	if value == "7001" {
		return 7001
	}
	return 0
}
func (i fakeInput) HasOption(name string) bool {
	_, ok := i.options[name]
	if ok {
		return true
	}
	return i.bools[name]
}

func captureStdout(t *testing.T, run func()) string {
	t.Helper()
	originalStdout := os.Stdout
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe failed: %v", err)
	}
	os.Stdout = writer
	defer func() {
		os.Stdout = originalStdout
	}()

	outputCh := make(chan string, 1)
	go func() {
		buffer := &bytes.Buffer{}
		_, _ = io.Copy(buffer, reader)
		outputCh <- buffer.String()
	}()

	run()
	_ = writer.Close()

	select {
	case output := <-outputCh:
		return output
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for captured stdout")
		return ""
	}
}
