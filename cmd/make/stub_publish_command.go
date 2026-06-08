package make

import (
	"os"
	"path/filepath"

	"github.com/prismgo/framework/console"
)

// StubPublishCommand publishes the built-in Prismgo generator stubs to ./stubs.
type StubPublishCommand struct{}

// NewStubPublishCommand creates the stub:publish command.
func NewStubPublishCommand() *StubPublishCommand { return &StubPublishCommand{} }

// Definition returns the Laravel-style command metadata.
func (c *StubPublishCommand) Definition() *console.Definition {
	return console.MustDefinition("stub:publish {--force}", "Publish Prismgo generator stubs")
}

// Handle publishes all generator stubs, skipping existing files unless --force is set.
func (c *StubPublishCommand) Handle(ctx console.CommandContext) error {
	for _, artifact := range allArtifacts() {
		spec := artifactSpecs[artifact]
		content, err := readBuiltInStub(spec.Stub)
		if err != nil {
			return err
		}
		target := filepath.Join("stubs", spec.Stub)
		exists := fileExists(target)
		if exists && !ctx.Input().OptionBool("force") {
			ctx.IO().Info("Skipped: " + filepath.ToSlash(target))
			continue
		}
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return err
		}
		if err := os.WriteFile(target, []byte(content), 0o644); err != nil {
			return err
		}
		if exists {
			ctx.IO().Info("Overwritten: " + filepath.ToSlash(target))
			continue
		}
		ctx.IO().Info("Created: " + filepath.ToSlash(target))
	}
	return nil
}
