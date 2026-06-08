package migration

import (
	"fmt"
	"slices"

	"github.com/prismgo/framework/console"
)

// MigrateResetCommand 对应 `migrate:reset` 命令。
//
// 用途：回滚所有已执行迁移。
type MigrateResetCommand struct {
	deps   MigrationDependencies
	openDB func(connection string) (dbSession, error)
}

// NewMigrateResetCommand 创建 `migrate:reset` 命令实例。
func NewMigrateResetCommand(dependencies ...MigrationDependencies) *MigrateResetCommand {
	return &MigrateResetCommand{
		deps:   firstMigrationDependencies(dependencies...),
		openDB: openDatabaseSession,
	}
}

func (c *MigrateResetCommand) Definition() *console.Definition {
	definition := console.MustDefinition(
		"migrate:reset {--database= : The database connection to use} {--force : Force the operation to run when in production} {--path=* : The path(s) to the migrations files to be executed} {--realpath : Indicate any provided migration file paths are pre-resolved absolute paths} {--pretend : Dump the SQL queries that would be run}",
		"Rollback all database migrations",
	)
	definition.Aliases = []string{"migration:reset"}
	return definition
}

// Run 执行全量回滚。
//
// 回滚顺序按执行记录反序，保证依赖关系与迁移执行顺序相反。
func (c *MigrateResetCommand) Handle(ctx console.CommandContext) error {
	force := ctx.Input().OptionBool("force")
	if err := requireForceInProduction(force, "migrate:reset"); err != nil {
		return err
	}

	session, err := c.openDB(ctx.Input().Option("database"))
	if err != nil {
		return err
	}
	defer session.Close()

	store := newMigrationStore(session.DB)
	if !store.hasTable() {
		ctx.IO().Info("Nothing to rollback.")
		return nil
	}

	paths := ctx.Input().OptionStrings("path")
	if len(paths) == 0 {
		paths = c.deps.paths()
	}
	migrations, err := collectMigrations(paths, ctx.Input().OptionBool("realpath"))
	if err != nil {
		return err
	}
	index := migrationIndex(migrations)

	records, err := store.listAll()
	if err != nil {
		return err
	}
	if len(records) == 0 {
		ctx.IO().Info("Nothing to rollback.")
		return nil
	}
	slices.Reverse(records)

	pretend := ctx.Input().OptionBool("pretend")
	for _, record := range records {
		migration, exists := index[record.Migration]
		if !exists {
			return fmt.Errorf("migration %s is recorded but source file is missing", record.Migration)
		}
		if pretend {
			ctx.IO().Info("Pretend: " + describeMigrationOperation(migration, false))
			continue
		}
		if err := applyMigrationDown(session.DB, migration, false); err != nil {
			return fmt.Errorf("reset %s failed: %w", migration.Name, err)
		}
		if err := store.deleteApplied(migration.Name); err != nil {
			return err
		}
		ctx.IO().Success("Rolled back: " + migration.Name)
	}
	return nil
}
