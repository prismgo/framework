package migration

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/spf13/cobra"

	"github.com/prismgo/framework/config"
	"github.com/prismgo/framework/console"
)

func testCmdCtx(cmd console.Command, input fakeInput, use string) console.CommandContext {
	return console.NewCommandContext(
		context.Background(),
		cmd,
		*cmd.Definition(),
		input,
		console.NewIO(strings.NewReader(""), io.Discard, io.Discard),
		nil,
		&cobra.Command{Use: use},
	)
}

func TestFreshAndSeedForceGuards(t *testing.T) {
	cfg := config.New()
	if err := useMigrationTestContainer(t).Instance("config.default", cfg); err != nil {
		t.Fatalf("bind config.default error = %v, want nil", err)
	}
	t.Setenv("APP_ENV", "production")

	fresh := NewMigrateFreshCommand()
	if err := fresh.Handle(testCmdCtx(fresh, fakeInput{}, "migrate:fresh")); err == nil || !strings.Contains(err.Error(), "--force") {
		t.Fatalf("migrate:fresh error = %v, want --force guard error", err)
	}

	seed := NewDBSeedCommand()
	if err := seed.Handle(testCmdCtx(seed, fakeInput{}, "db:seed")); err == nil || !strings.Contains(err.Error(), "--force") {
		t.Fatalf("db:seed error = %v, want --force guard error", err)
	}
}
