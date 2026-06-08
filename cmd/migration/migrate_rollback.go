package migration

import (
	"fmt"

	"github.com/prismgo/framework/console"
)

// MigrateRollbackCommand 对应 `migrate:rollback` 命令。
//
// 用途：按 step/batch 规则回滚最近一批或指定批次迁移。
type MigrateRollbackCommand struct {
	deps   MigrationDependencies
	openDB func(connection string) (dbSession, error)
}

// NewMigrateRollbackCommand 创建 `migrate:rollback` 命令实例。
func NewMigrateRollbackCommand(dependencies ...MigrationDependencies) *MigrateRollbackCommand {
	return &MigrateRollbackCommand{
		deps:   firstMigrationDependencies(dependencies...),
		openDB: openDatabaseSession,
	}
}

func (c *MigrateRollbackCommand) Definition() *console.Definition {
	definition := console.MustDefinition(
		"migrate:rollback {--database= : The database connection to use} {--force : Force the operation to run when in production} {--path=* : The path(s) to the migrations files to be executed} {--realpath : Indicate any provided migration file paths are pre-resolved absolute paths} {--pretend : Dump the SQL queries that would be run} {--step= : The number of migrations to be reverted} {--batch= : The batch of migrations (identified by their batch number) to be reverted}",
		"Rollback the last database migration",
	)
	definition.Aliases = []string{"migration:rollback"}
	return definition
}

// Run 执行回滚流程。
//
// 设计说明：只回滚 migrations 表中有记录的迁移；若记录存在但源文件缺失，直接报错中止。
func (c *MigrateRollbackCommand) Handle(ctx console.CommandContext) error {
	force := ctx.Input().OptionBool("force")
	if err := requireForceInProduction(force, "migrate:rollback"); err != nil {
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

	step := parsePositiveInt(ctx.Input().Option("step"))
	batch := parsePositiveInt(ctx.Input().Option("batch"))
	candidates, err := store.rollbackCandidates(step, batch)
	if err != nil {
		return err
	}
	if len(candidates) == 0 {
		ctx.IO().Info("Nothing to rollback.")
		return nil
	}

	pretend := ctx.Input().OptionBool("pretend")
	for _, record := range candidates {
		migration, exists := index[record.Migration]
		if !exists {
			return fmt.Errorf("migration %s is recorded but source file is missing", record.Migration)
		}
		if pretend {
			ctx.IO().Info("Pretend: " + describeMigrationOperation(migration, false))
			continue
		}
		if err := rollbackMigrationAndTrack(session.DB, migration); err != nil {
			return fmt.Errorf("rollback %s failed: %w", migration.Name, err)
		}
		ctx.IO().Success("Rolled back: " + migration.Name)
	}
	return nil
}
