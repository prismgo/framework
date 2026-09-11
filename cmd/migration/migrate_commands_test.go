package migration

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"gorm.io/gorm"

	"github.com/prismgo/framework/config"
	"github.com/prismgo/framework/console"
)

func TestForceProtectionInProduction(t *testing.T) {
	cfg := config.New()
	if err := useMigrationTestContainer(t).Instance("config.default", cfg); err != nil {
		t.Fatalf("bind config: %v", err)
	}
	t.Setenv("APP_ENV", "production")

	cmd := NewMigrateCommand(MigrationDependencies{})
	cmd.openDB = func(string) (dbSession, error) { return dbSession{DB: &gorm.DB{}}, nil }
	ctx := console.NewCommandContext(context.Background(), cmd, *cmd.Definition(), fakeInput{}, console.NewIO(strings.NewReader(""), io.Discard, io.Discard), nil, &cobra.Command{Use: "migrate"})
	if err := cmd.Handle(ctx); err == nil || !strings.Contains(err.Error(), "--force") {
		t.Fatalf("expected production force guard error, got %v", err)
	}
}
