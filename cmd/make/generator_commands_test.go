package make

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/prismgo/framework/console"

	"github.com/spf13/cobra"
)

func TestCommandFactoriesReturnsGeneratorCommands(t *testing.T) {
	factories := CommandFactories()
	if len(factories) == 0 {
		t.Fatal("CommandFactories should return generator command factories")
	}
	for i, factory := range factories {
		if factory == nil {
			t.Fatalf("factory %d is nil", i)
		}
		if command := factory(); command == nil {
			t.Fatalf("factory %d returned nil command", i)
		}
	}
}

func TestStubPublishWritesBuiltInTemplatesAndHonorsForce(t *testing.T) {
	root := testProjectRoot(t)
	cmd := NewStubPublishCommand()
	stdout := &bytes.Buffer{}

	runMakeCommand(t, cmd, makeInput{}, stdout)

	modelStub := filepath.Join(root, "stubs", "model.stub")
	content := readFile(t, modelStub)
	if !strings.Contains(content, "type {{ .TypeName }} struct") {
		t.Fatalf("model stub content = %q, want Go template", content)
	}
	if !strings.Contains(stdout.String(), "Created: stubs/model.stub") {
		t.Fatalf("output = %q, want Created line", stdout.String())
	}

	if err := os.WriteFile(modelStub, []byte("custom model stub"), 0o644); err != nil {
		t.Fatalf("write custom stub: %v", err)
	}
	stdout.Reset()
	runMakeCommand(t, cmd, makeInput{}, stdout)
	if got := readFile(t, modelStub); got != "custom model stub" {
		t.Fatalf("stub was overwritten without force: %q", got)
	}
	if !strings.Contains(stdout.String(), "Skipped: stubs/model.stub") {
		t.Fatalf("output = %q, want Skipped line", stdout.String())
	}

	stdout.Reset()
	runMakeCommand(t, cmd, makeInput{bools: map[string]bool{"force": true}}, stdout)
	if got := readFile(t, modelStub); !strings.Contains(got, "type {{ .TypeName }} struct") {
		t.Fatalf("stub was not overwritten with force: %q", got)
	}
	if !strings.Contains(stdout.String(), "Overwritten: stubs/model.stub") {
		t.Fatalf("output = %q, want Overwritten line", stdout.String())
	}
}

func TestModelGeneratorCreatesFrameworkNeutralFiles(t *testing.T) {
	root := testProjectRoot(t)
	cmd := NewArtifactCommand(ModelArtifact)
	stdout := &bytes.Buffer{}

	runMakeCommand(t, cmd, makeInput{
		args:    map[string][]string{"name": {"Admin/UserProfile"}},
		options: map[string]string{"table": "users"},
	}, stdout)

	generated := filepath.Join(root, "app", "models", "admin", "user_profile.go")
	content := readFile(t, generated)
	for _, want := range []string{
		"package admin",
		"type UserProfile struct",
		"ID        uint",
		"CreatedAt time.Time",
		"func (UserProfile) TableName() string",
		"return \"users\"",
	} {
		if !strings.Contains(content, want) {
			t.Fatalf("generated model missing %q:\n%s", want, content)
		}
	}
	if strings.Contains(content, "TenantBase") {
		t.Fatalf("model should not include business tenant base:\n%s", content)
	}
	if !strings.Contains(stdout.String(), "Created: app/models/admin/user_profile.go") {
		t.Fatalf("output = %q, want relative Created path", stdout.String())
	}
}

func TestGeneratorUsesProjectStubOverrideAndRejectsUnsafeNames(t *testing.T) {
	root := testProjectRoot(t)
	if err := os.MkdirAll(filepath.Join(root, "stubs"), 0o755); err != nil {
		t.Fatalf("mkdir stubs: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "stubs", "event.stub"), []byte("package {{ .PackageName }}\n\ntype {{ .TypeName }} struct { Custom bool }\n"), 0o644); err != nil {
		t.Fatalf("write event stub: %v", err)
	}

	cmd := NewArtifactCommand(EventArtifact)
	runMakeCommand(t, cmd, makeInput{args: map[string][]string{"name": {"Billing.InvoicePaid"}}}, &bytes.Buffer{})

	content := readFile(t, filepath.Join(root, "app", "events", "billing", "invoice_paid.go"))
	if !strings.Contains(content, "Custom bool") || !strings.Contains(content, "type InvoicePaid struct") {
		t.Fatalf("event did not use project stub override:\n%s", content)
	}

	err := cmd.Handle(newMakeContext(cmd, makeInput{args: map[string][]string{"name": {"../Secret"}}}, &bytes.Buffer{}))
	if err == nil || !strings.Contains(err.Error(), "illegal path") {
		t.Fatalf("Handle error = %v, want illegal path", err)
	}
}

func TestBasicArtifactGeneratorsCreateExpectedOutputs(t *testing.T) {
	root := testProjectRoot(t)

	cases := []struct {
		name     string
		artifact Artifact
		input    makeInput
		path     string
		contains []string
	}{
		{
			name:     "event",
			artifact: EventArtifact,
			input:    makeInput{args: map[string][]string{"name": {"OrderCreated"}}},
			path:     "app/events/order_created.go",
			contains: []string{
				"package events",
				`const EventOrderCreated = "order.created"`,
				"type OrderCreated struct",
				"func (OrderCreated) Name() string",
				"return EventOrderCreated",
			},
		},
		{
			name:     "job",
			artifact: JobArtifact,
			input:    makeInput{args: map[string][]string{"name": {"SendReceipt"}}},
			path:     "app/jobs/send_receipt.go",
			contains: []string{
				"package jobs",
				`"github.com/prismgo/framework/queue"`,
				"func init()",
				"queue.RegisterType[*SendReceipt]()",
				"func (j SendReceipt) Handle(ctx context.Context) error",
			},
		},
		{
			name:     "middleware",
			artifact: MiddlewareArtifact,
			input:    makeInput{args: map[string][]string{"name": {"EnsureUser"}}},
			path:     "app/http/middleware/ensure_user.go",
			contains: []string{"package middleware", "func EnsureUser() gin.HandlerFunc"},
		},
		{
			name:     "listener",
			artifact: ListenerArtifact,
			input:    makeInput{args: map[string][]string{"name": {"SendInvoiceNotification"}}},
			path:     "app/listeners/send_invoice_notification.go",
			contains: []string{"package listeners", "func (l SendInvoiceNotification) Handle(ctx context.Context, ev event.Event) error"},
		},
		{
			name:     "provider",
			artifact: ProviderArtifact,
			input:    makeInput{args: map[string][]string{"name": {"ReportServiceProvider"}}},
			path:     "app/providers/report_service_provider.go",
			contains: []string{"package providers", `providercontract "github.com/prismgo/framework/contracts/provider"`, "func (p ReportServiceProvider) Register(app providercontract.Application) error", "func (p ReportServiceProvider) Boot(app providercontract.Application) error"},
		},
		{
			name:     "resource",
			artifact: ResourceArtifact,
			input:    makeInput{args: map[string][]string{"name": {"UserResource"}}},
			path:     "app/http/resources/user_resource.go",
			contains: []string{"package resources", "func (r UserResource) ToMap() map[string]any"},
		},
		{
			name:     "seeder",
			artifact: SeederArtifact,
			input:    makeInput{args: map[string][]string{"name": {"UserSeeder"}}},
			path:     "database/seeders/user_seeder.go",
			contains: []string{"package seeders", "func init() {", "database.RegisterSeeder(Seed)", "func Seed(db *gorm.DB) error"},
		},
		{
			name:     "controller",
			artifact: ControllerArtifact,
			input:    makeInput{args: map[string][]string{"name": {"Admin/UserController"}}, options: map[string]string{"model": "User"}},
			path:     "app/http/controllers/admin/user_controller.go",
			contains: []string{"package admin", "func (c UserController) Index(ctx *gin.Context)", "TODO: connect User manually"},
		},
		{
			name:     "command",
			artifact: CommandArtifact,
			input:    makeInput{args: map[string][]string{"name": {"SyncOrders"}}},
			path:     "app/cmd/sync_orders.go",
			contains: []string{"package cmd", "func (c SyncOrdersCommand) Definition() *console.Definition"},
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			runMakeCommand(t, NewArtifactCommand(tt.artifact), tt.input, &bytes.Buffer{})
			content := readFile(t, filepath.Join(root, filepath.FromSlash(tt.path)))
			for _, want := range tt.contains {
				if !strings.Contains(content, want) {
					t.Fatalf("%s missing %q:\n%s", tt.path, want, content)
				}
			}
		})
	}
}

func TestModelGeneratorChainingCreatesRelatedArtifacts(t *testing.T) {
	root := testProjectRoot(t)
	runMakeCommand(t, NewArtifactCommand(ModelArtifact), makeInput{
		args:  map[string][]string{"name": {"Invoice"}},
		bools: map[string]bool{"migration": true, "controller": true, "resource": true, "seed": true, "api": true},
	}, &bytes.Buffer{})

	for _, path := range []string{
		"app/models/invoice.go",
		"app/http/controllers/invoice_controller.go",
		"app/http/resources/invoice_resource.go",
		"database/seeders/invoice_seeder.go",
	} {
		if _, err := os.Stat(filepath.Join(root, filepath.FromSlash(path))); err != nil {
			t.Fatalf("expected chained file %s: %v", path, err)
		}
	}
	matches, err := filepath.Glob(filepath.Join(root, "database", "migrations", "*_create_invoices_table.go"))
	if err != nil {
		t.Fatalf("glob chained migration: %v", err)
	}
	if len(matches) != 1 {
		t.Fatalf("expected one chained migration, got %v", matches)
	}
}

func TestComplexGeneratorDefinitionsExposeLaravelAliases(t *testing.T) {
	model := NewArtifactCommand(ModelArtifact).Definition()
	assertOption(t, model, "migration", "m")
	assertOption(t, model, "controller", "c")
	assertOption(t, model, "resource", "r")
	assertOption(t, model, "seeder", "s")
	assertOption(t, model, "seed", "")

	controller := NewArtifactCommand(ControllerArtifact).Definition()
	assertOption(t, controller, "model", "m")
	assertOption(t, controller, "resource", "r")
}

func TestMigrationGeneratorUsesLaravelTimestampAndSchemaIntent(t *testing.T) {
	root := testProjectRoot(t)
	cmd := NewArtifactCommand(MigrationArtifact)
	stdout := &bytes.Buffer{}

	runMakeCommand(t, cmd, makeInput{
		args:    map[string][]string{"name": {"create_users_table"}},
		options: map[string]string{"create": "users"},
	}, stdout)

	matches, err := filepath.Glob(filepath.Join(root, "database", "migrations", "*_create_users_table.go"))
	if err != nil {
		t.Fatalf("glob migration: %v", err)
	}
	if len(matches) != 1 {
		t.Fatalf("migration files = %v, want one timestamped create migration", matches)
	}
	if base := filepath.Base(matches[0]); !regexp.MustCompile(`^\d{14}_create_users_table\.go$`).MatchString(base) {
		t.Fatalf("migration filename = %q, want Laravel-style timestamp", base)
	}
	content := readFile(t, matches[0])
	for _, want := range []string{
		`"github.com/prismgo/framework/database"`,
		"func init() {",
		"database.RegisterMigration(Up, Down)",
		"schema.Bind(db).Create(\"users\"",
		"schema.Bind(db).DropIfExists(\"users\")",
	} {
		if !strings.Contains(content, want) {
			t.Fatalf("migration missing %q:\n%s", want, content)
		}
	}
	if !strings.Contains(stdout.String(), "Created: database/migrations/") {
		t.Fatalf("output = %q, want relative migration path", stdout.String())
	}

	runMakeCommand(t, cmd, makeInput{
		args: map[string][]string{"name": {"create_posts_table"}},
	}, &bytes.Buffer{})
	inferred := readOnlyGlob(t, filepath.Join(root, "database", "migrations", "*_create_posts_table.go"))
	if !strings.Contains(readFile(t, inferred), "schema.Bind(db).Create(\"posts\"") {
		t.Fatalf("create migration did not infer table:\n%s", readFile(t, inferred))
	}

	runMakeCommand(t, cmd, makeInput{
		args: map[string][]string{"name": {"rebuild_indexes"}},
	}, &bytes.Buffer{})
	blank := readOnlyGlob(t, filepath.Join(root, "database", "migrations", "*_rebuild_indexes.go"))
	if !strings.Contains(readFile(t, blank), "TODO: add schema changes") {
		t.Fatalf("blank migration did not keep skeleton:\n%s", readFile(t, blank))
	}
}

func TestMigrationGeneratorInfersTableIntentAndHonorsPathOptions(t *testing.T) {
	root := testProjectRoot(t)
	cmd := NewArtifactCommand(MigrationArtifact)
	customPath := filepath.Join(root, "custom", "migrations")

	stdout := &bytes.Buffer{}
	runMakeCommand(t, cmd, makeInput{
		args:    map[string][]string{"name": {"add_email_to_users_table"}},
		options: map[string]string{"path": customPath},
		bools:   map[string]bool{"realpath": true, "fullpath": true},
	}, stdout)

	matches, err := filepath.Glob(filepath.Join(customPath, "*_add_email_to_users_table.go"))
	if err != nil {
		t.Fatalf("glob migration: %v", err)
	}
	if len(matches) != 1 {
		t.Fatalf("migration files = %v, want one timestamped table migration", matches)
	}
	content := readFile(t, matches[0])
	for _, want := range []string{
		"schema.Bind(db).Table(\"users\"",
		`// table.DropColumn("email")`,
	} {
		if !strings.Contains(content, want) {
			t.Fatalf("migration missing %q:\n%s", want, content)
		}
	}
	if !strings.Contains(stdout.String(), filepath.ToSlash(matches[0])) {
		t.Fatalf("output = %q, want full path %q", stdout.String(), matches[0])
	}
}

func TestCommandGeneratorDerivesSignatureAndAcceptsOverride(t *testing.T) {
	root := testProjectRoot(t)
	cmd := NewArtifactCommand(CommandArtifact)

	runMakeCommand(t, cmd, makeInput{args: map[string][]string{"name": {"Report/SendEmails"}}}, &bytes.Buffer{})
	nested := readFile(t, filepath.Join(root, "app", "cmd", "report", "send_emails.go"))
	if !strings.Contains(nested, `console.MustDefinition("report:send-emails"`) {
		t.Fatalf("nested command did not derive signature:\n%s", nested)
	}

	runMakeCommand(t, cmd, makeInput{
		args:    map[string][]string{"name": {"send_emails"}},
		options: map[string]string{"command": "mail:send"},
	}, &bytes.Buffer{})
	content := readFile(t, filepath.Join(root, "app", "cmd", "send_emails.go"))
	for _, want := range []string{
		"type SendEmailsCommand struct{}",
		`console.MustDefinition("mail:send"`,
		"func (c SendEmailsCommand) Handle(ctx console.CommandContext) error",
	} {
		if !strings.Contains(content, want) {
			t.Fatalf("command missing %q:\n%s", want, content)
		}
	}
}

func TestControllerAndListenerComplexOptionsShapeOutput(t *testing.T) {
	root := testProjectRoot(t)

	runMakeCommand(t, NewArtifactCommand(ControllerArtifact), makeInput{
		args:    map[string][]string{"name": {"UserController"}},
		options: map[string]string{"model": "User"},
		bools:   map[string]bool{"api": true, "resource": true},
	}, &bytes.Buffer{})
	controller := readFile(t, filepath.Join(root, "app", "http", "controllers", "user_controller.go"))
	for _, want := range []string{
		"func (c UserController) Index(ctx *gin.Context)",
		"func (c UserController) Show(ctx *gin.Context)",
		"func (c UserController) Store(ctx *gin.Context)",
		"func (c UserController) Update(ctx *gin.Context)",
		"func (c UserController) Destroy(ctx *gin.Context)",
		"TODO: connect User manually",
	} {
		if !strings.Contains(controller, want) {
			t.Fatalf("controller missing %q:\n%s", want, controller)
		}
	}
	if strings.Contains(controller, "Create(ctx") || strings.Contains(controller, "Edit(ctx") {
		t.Fatalf("api resource controller should not contain web-only methods:\n%s", controller)
	}

	stdout := &bytes.Buffer{}
	runMakeCommand(t, NewArtifactCommand(ListenerArtifact), makeInput{
		args:    map[string][]string{"name": {"SendWelcomeEmail"}},
		options: map[string]string{"event": "UserRegistered"},
		bools:   map[string]bool{"queued": true},
	}, stdout)
	listener := readFile(t, filepath.Join(root, "app", "listeners", "send_welcome_email.go"))
	for _, want := range []string{
		`event "github.com/prismgo/framework/contracts/event"`,
		"func (l SendWelcomeEmail) Handle(ctx context.Context, ev event.Event) error",
		"func (l SendWelcomeEmail) ShouldQueue() bool",
		"return true",
		"UserRegistered",
	} {
		if !strings.Contains(listener, want) {
			t.Fatalf("listener missing %q:\n%s", want, listener)
		}
	}
	if !strings.Contains(stdout.String(), "queued listener requires event factory registration") {
		t.Fatalf("output = %q, want queued listener registration warning", stdout.String())
	}
}

func TestListenerRejectsAsyncAndQueuedTogether(t *testing.T) {
	testProjectRoot(t)
	cmd := NewArtifactCommand(ListenerArtifact)
	err := cmd.Handle(newMakeContext(cmd, makeInput{
		args:  map[string][]string{"name": {"NotifyUser"}},
		bools: map[string]bool{"async": true, "queued": true},
	}, &bytes.Buffer{}))
	if err == nil || !strings.Contains(err.Error(), "--async and --queued are mutually exclusive") {
		t.Fatalf("Handle error = %v, want mutually exclusive options", err)
	}
}

func TestGeneratedFileExistsRequiresForceAndFullpathControlsOutput(t *testing.T) {
	root := testProjectRoot(t)
	cmd := NewArtifactCommand(EventArtifact)
	runMakeCommand(t, cmd, makeInput{args: map[string][]string{"name": {"OrderPaid"}}}, &bytes.Buffer{})

	err := cmd.Handle(newMakeContext(cmd, makeInput{args: map[string][]string{"name": {"OrderPaid"}}}, &bytes.Buffer{}))
	if err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("Handle error = %v, want already exists", err)
	}

	stdout := &bytes.Buffer{}
	runMakeCommand(t, cmd, makeInput{args: map[string][]string{"name": {"OrderPaid"}}, bools: map[string]bool{"force": true, "fullpath": true}}, stdout)
	if !strings.Contains(stdout.String(), filepath.Join(root, "app", "events", "order_paid.go")) {
		t.Fatalf("output = %q, want absolute path", stdout.String())
	}
}

func testProjectRoot(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	previous, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	if err := os.Chdir(root); err != nil {
		t.Fatalf("chdir temp project: %v", err)
	}
	t.Cleanup(func() {
		if err := os.Chdir(previous); err != nil {
			t.Fatalf("restore cwd: %v", err)
		}
	})
	return root
}

func runMakeCommand(t *testing.T, cmd console.Command, input makeInput, stdout *bytes.Buffer) {
	t.Helper()
	if err := cmd.Handle(newMakeContext(cmd, input, stdout)); err != nil {
		t.Fatalf("Handle returned error: %v", err)
	}
}

func newMakeContext(cmd console.Command, input makeInput, stdout *bytes.Buffer) console.CommandContext {
	return console.NewCommandContext(
		context.Background(),
		cmd,
		*cmd.Definition(),
		input,
		console.NewIO(strings.NewReader(""), stdout, io.Discard),
		nil,
		&cobra.Command{Use: cmd.Definition().Name},
	)
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(content)
}

func readOnlyGlob(t *testing.T, pattern string) string {
	t.Helper()
	matches, err := filepath.Glob(pattern)
	if err != nil {
		t.Fatalf("glob %s: %v", pattern, err)
	}
	if len(matches) != 1 {
		t.Fatalf("glob %s matched %v, want one file", pattern, matches)
	}
	return matches[0]
}

func assertOption(t *testing.T, definition *console.Definition, name string, shortcut string) {
	t.Helper()
	for _, option := range definition.Options {
		if option.Name == name {
			if option.Shortcut != shortcut {
				t.Fatalf("option %s shortcut = %q, want %q", name, option.Shortcut, shortcut)
			}
			return
		}
	}
	t.Fatalf("definition %s missing option %s", definition.Name, name)
}

type makeInput struct {
	args    map[string][]string
	options map[string]string
	bools   map[string]bool
}

func (i makeInput) Argument(name string) string {
	values := i.Arguments(name)
	if len(values) == 0 {
		return ""
	}
	return values[0]
}
func (i makeInput) Arguments(name string) []string { return append([]string(nil), i.args[name]...) }
func (i makeInput) Option(name string) string      { return i.options[name] }
func (i makeInput) OptionStrings(name string) []string {
	value := i.options[name]
	if value == "" {
		return nil
	}
	return []string{value}
}
func (i makeInput) OptionBool(name string) bool { return i.bools[name] }
func (i makeInput) OptionInt(name string) int   { return 0 }
func (i makeInput) HasOption(name string) bool {
	if _, ok := i.options[name]; ok {
		return true
	}
	return i.bools[name]
}
