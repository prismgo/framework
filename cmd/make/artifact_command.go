package make

import (
	"bytes"
	"fmt"
	"go/format"
	"os"
	"path/filepath"
	"strings"
	"text/template"
	"time"

	"github.com/prismgo/framework/console"
)

// ArtifactCommand creates one Prismgo artifact type through shared generator plumbing.
type ArtifactCommand struct {
	artifact Artifact
	spec     artifactSpec
}

// NewArtifactCommand creates a make:* command for the requested artifact.
func NewArtifactCommand(artifact Artifact) *ArtifactCommand {
	spec, ok := artifactSpecs[artifact]
	if !ok {
		panic(fmt.Sprintf("make command: unknown artifact %q", artifact))
	}
	return &ArtifactCommand{artifact: artifact, spec: spec}
}

// Definition returns the Laravel-style command metadata for this artifact.
func (c *ArtifactCommand) Definition() *console.Definition {
	signature := c.spec.CommandName + " {name} {--force} {--fullpath}"
	switch c.artifact {
	case ModelArtifact:
		signature += " {--m|migration} {--c|controller} {--r|resource} {--s|seeder} {--seed} {--api} {--table=}"
		signature += " {--factory} {--policy} {--requests} {--all} {--test} {--pest} {--pivot} {--morph-pivot}"
	case ControllerArtifact:
		signature += " {--m|model=} {--api} {--r|resource}"
	case CommandArtifact:
		signature += " {--command=}"
	case ListenerArtifact:
		signature += " {--queued} {--async} {--event=}"
	case MigrationArtifact:
		signature += " {--create=} {--table=} {--path=} {--realpath}"
	}
	return console.MustDefinition(signature, c.spec.Description)
}

// Handle generates the requested artifact and any explicit chained artifacts.
func (c *ArtifactCommand) Handle(ctx console.CommandContext) error {
	if err := c.rejectUnsupportedOptions(ctx); err != nil {
		return err
	}
	if c.artifact == ListenerArtifact && ctx.Input().OptionBool("async") && ctx.Input().OptionBool("queued") {
		return fmt.Errorf("--async and --queued are mutually exclusive")
	}
	name, err := normalizeName(ctx.Input().Argument("name"), c.spec)
	if err != nil {
		return err
	}
	result, err := c.writeArtifact(ctx, name, ctx.Input().Argument("name"))
	if err != nil {
		return err
	}
	c.writeResult(ctx, result)
	for _, chained := range c.chainedArtifacts(ctx, name) {
		result, err := c.writeChainedArtifact(ctx, chained.artifact, chained.name, chained.inputName)
		if err != nil {
			return err
		}
		c.writeResult(ctx, result)
	}
	if c.spec.Hint != "" {
		ctx.IO().Info(c.spec.Hint)
	}
	return nil
}

func (c *ArtifactCommand) rejectUnsupportedOptions(ctx console.CommandContext) error {
	if c.artifact != ModelArtifact {
		return nil
	}
	for _, option := range []string{"factory", "policy", "requests", "all", "test", "pest", "pivot", "morph-pivot"} {
		if ctx.Input().OptionBool(option) {
			return fmt.Errorf("unsupported option --%s for make:model; available chaining options: --migration/-m, --seeder/-s, --controller/-c, --resource/-r, --api", option)
		}
	}
	return nil
}

type writeResult struct {
	Path        string
	Overwritten bool
}

type chainedArtifact struct {
	artifact  Artifact
	name      normalizedName
	inputName string
}

func (c *ArtifactCommand) writeArtifact(ctx console.CommandContext, name normalizedName, inputName string) (writeResult, error) {
	return writeGeneratedFile(ctx, c.spec, name, inputName)
}

func (c *ArtifactCommand) writeChainedArtifact(ctx console.CommandContext, artifact Artifact, name normalizedName, inputName string) (writeResult, error) {
	spec := artifactSpecs[artifact]
	return writeGeneratedFile(ctx, spec, name, inputName)
}

func writeGeneratedFile(ctx console.CommandContext, spec artifactSpec, name normalizedName, inputName string) (writeResult, error) {
	relPath, err := targetPath(spec, name, inputName, ctx)
	if err != nil {
		return writeResult{}, err
	}
	exists := fileExists(relPath)
	if exists && !ctx.Input().OptionBool("force") {
		return writeResult{}, fmt.Errorf("generated file already exists: %s; rerun with --force to overwrite", filepath.ToSlash(relPath))
	}
	content, err := renderStub(spec, name, inputName, ctx)
	if err != nil {
		return writeResult{}, err
	}
	if err := os.MkdirAll(filepath.Dir(relPath), 0o755); err != nil {
		return writeResult{}, err
	}
	if err := os.WriteFile(relPath, []byte(content), 0o644); err != nil {
		return writeResult{}, err
	}
	return writeResult{Path: relPath, Overwritten: exists}, nil
}

func targetPath(spec artifactSpec, name normalizedName, inputName string, ctx console.CommandContext) (string, error) {
	directory, err := targetDirectory(spec, ctx)
	if err != nil {
		return "", err
	}
	parts := []string{directory}
	parts = append(parts, name.Directories...)
	fileName := name.FileName + ".go"
	if spec.Artifact == MigrationArtifact {
		fileName = time.Now().Format("20060102150405") + "_" + snakeCase(inputName) + ".go"
	}
	parts = append(parts, fileName)
	return filepath.Join(parts...), nil
}

func targetDirectory(spec artifactSpec, ctx console.CommandContext) (string, error) {
	if spec.Artifact != MigrationArtifact {
		return filepath.FromSlash(spec.Directory), nil
	}
	custom := strings.TrimSpace(ctx.Input().Option("path"))
	if custom == "" {
		return filepath.FromSlash(spec.Directory), nil
	}
	if ctx.Input().OptionBool("realpath") {
		return filepath.Clean(custom), nil
	}
	if filepath.IsAbs(custom) {
		return "", fmt.Errorf("illegal path %q: absolute paths require --realpath", custom)
	}
	cleaned := filepath.Clean(custom)
	if cleaned == "." || strings.HasPrefix(filepath.ToSlash(cleaned), "../") || cleaned == ".." {
		return "", fmt.Errorf("illegal path %q: traversal segments are not supported", custom)
	}
	return cleaned, nil
}

func renderStub(spec artifactSpec, name normalizedName, inputName string, ctx console.CommandContext) (string, error) {
	stub, err := resolveStub(spec.Stub)
	if err != nil {
		return "", err
	}
	intent := migrationIntentFor(inputName, ctx)
	modelTodo := ""
	if model := strings.TrimSpace(ctx.Input().Option("model")); model != "" {
		modelTodo = fmt.Sprintf("// TODO: connect %s manually; imports are not inferred by make:controller.", model)
	}
	eventHint := ""
	if event := strings.TrimSpace(ctx.Input().Option("event")); event != "" {
		eventHint = fmt.Sprintf("// Event hint: %s", event)
	}
	data := map[string]any{
		"PackageName":      name.PackageName,
		"TypeName":         name.TypeName,
		"TableName":        intent.TableName,
		"MigrationKind":    intent.Kind,
		"MigrationColumn":  intent.ColumnName,
		"KebabName":        strings.ReplaceAll(name.FileName, "_", "-"),
		"ModelTodo":        modelTodo,
		"EventHint":        eventHint,
		"EventName":        eventName(name),
		"CommandSignature": commandSignature(name, ctx),
		"Resource":         ctx.Input().OptionBool("resource") || ctx.Input().OptionBool("api"),
		"API":              ctx.Input().OptionBool("api"),
		"Queued":           ctx.Input().OptionBool("queued"),
		"Async":            ctx.Input().OptionBool("async"),
	}
	tpl, err := template.New(spec.Stub).Parse(stub)
	if err != nil {
		return "", err
	}
	var out bytes.Buffer
	if err := tpl.Execute(&out, data); err != nil {
		return "", err
	}
	formatted, err := format.Source([]byte(strings.TrimSpace(out.String()) + "\n"))
	if err != nil {
		return strings.TrimSpace(out.String()) + "\n", nil
	}
	return string(formatted), nil
}

func resolveStub(name string) (string, error) {
	projectStub := filepath.Join("stubs", name)
	if content, err := os.ReadFile(projectStub); err == nil {
		return string(content), nil
	}
	return readBuiltInStub(name)
}

func (c *ArtifactCommand) chainedArtifacts(ctx console.CommandContext, name normalizedName) []chainedArtifact {
	if c.artifact != ModelArtifact {
		return nil
	}
	var chained []chainedArtifact
	base := name.TypeName
	appendChained := func(artifact Artifact, input string) {
		spec := artifactSpecs[artifact]
		normalized, err := normalizeName(input, spec)
		if err != nil {
			return
		}
		chained = append(chained, chainedArtifact{artifact: artifact, name: normalized, inputName: input})
	}
	if ctx.Input().OptionBool("migration") {
		appendChained(MigrationArtifact, "create_"+snakeCase(base)+"s_table")
	}
	if ctx.Input().OptionBool("controller") {
		appendChained(ControllerArtifact, base+"Controller")
	}
	if ctx.Input().OptionBool("resource") {
		appendChained(ResourceArtifact, base+"Resource")
	}
	if ctx.Input().OptionBool("seeder") || ctx.Input().OptionBool("seed") {
		appendChained(SeederArtifact, base+"Seeder")
	}
	if ctx.Input().OptionBool("api") && !ctx.Input().OptionBool("controller") {
		appendChained(ControllerArtifact, base+"Controller")
	}
	return chained
}

func (c *ArtifactCommand) writeResult(ctx console.CommandContext, result writeResult) {
	path := filepath.ToSlash(result.Path)
	if ctx.Input().OptionBool("fullpath") {
		if absolute, err := filepath.Abs(result.Path); err == nil {
			path = filepath.ToSlash(absolute)
		}
	}
	if result.Overwritten {
		ctx.IO().Info("Overwritten: " + path)
		return
	}
	ctx.IO().Info("Created: " + path)
	if c.artifact == CommandArtifact {
		ctx.IO().Info("Register business commands in foundation.WithRouting(... Commands ...); provider/module commands may use provider.Commands(...) or Builder.WithCommands(...).")
	}
	if c.artifact == ListenerArtifact && ctx.Input().OptionBool("queued") {
		ctx.IO().Info("queued listener requires event factory registration for cross-process workers; otherwise payload restoration may be raw or fail strong typing.")
	}
}

type migrationIntent struct {
	Kind       string
	TableName  string
	ColumnName string
}

func migrationIntentFor(inputName string, ctx console.CommandContext) migrationIntent {
	if table := strings.TrimSpace(ctx.Input().Option("create")); table != "" {
		return migrationIntent{Kind: "create", TableName: table}
	}
	if table := strings.TrimSpace(ctx.Input().Option("table")); table != "" {
		return migrationIntent{Kind: "table", TableName: table, ColumnName: inferredColumnName(inputName)}
	}

	name := snakeCase(inputName)
	if table, ok := strings.CutPrefix(name, "create_"); ok && strings.HasSuffix(table, "_table") {
		return migrationIntent{Kind: "create", TableName: strings.TrimSuffix(table, "_table")}
	}
	if before, after, ok := strings.Cut(name, "_to_"); ok && strings.HasSuffix(after, "_table") {
		return migrationIntent{Kind: "table", TableName: strings.TrimSuffix(after, "_table"), ColumnName: strings.TrimPrefix(before, "add_")}
	}
	return migrationIntent{Kind: "blank"}
}

func inferredColumnName(inputName string) string {
	name := snakeCase(inputName)
	before, _, ok := strings.Cut(name, "_to_")
	if !ok {
		return "column_name"
	}
	return strings.TrimPrefix(before, "add_")
}

func commandSignature(name normalizedName, ctx console.CommandContext) string {
	if override := strings.TrimSpace(ctx.Input().Option("command")); override != "" {
		return override
	}
	segments := append([]string(nil), name.Directories...)
	words := strings.Split(name.FileName, "_")
	if len(words) == 0 || words[0] == "" {
		return strings.ReplaceAll(name.FileName, "_", "-")
	}
	if len(segments) == 0 {
		if len(words) == 1 {
			return words[0]
		}
		return words[0] + ":" + strings.Join(words[1:], "-")
	}
	return strings.Join(kebabSegments(segments), ":") + ":" + strings.Join(words, "-")
}

func kebabSegments(segments []string) []string {
	result := make([]string, 0, len(segments))
	for _, segment := range segments {
		result = append(result, strings.ReplaceAll(segment, "_", "-"))
	}
	return result
}

func eventName(name normalizedName) string {
	segments := append([]string(nil), name.Directories...)
	for _, word := range splitWords(name.TypeName) {
		segments = append(segments, strings.ToLower(word))
	}
	return strings.Join(segments, ".")
}

func fileExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}

// CommandFactories returns all Prismgo generator command factories.
func CommandFactories() []console.CommandFactory {
	factories := []console.CommandFactory{
		func() console.Command { return NewStubPublishCommand() },
	}
	for _, artifact := range allArtifacts() {
		artifact := artifact
		factories = append(factories, func() console.Command {
			return NewArtifactCommand(artifact)
		})
	}
	return factories
}
