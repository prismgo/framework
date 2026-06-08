package migration

import (
	"fmt"
	"slices"
	"strings"

	"github.com/prismgo/framework/console"
)

// MigrateRefreshCommand 对应 `migrate:refresh` 命令。
//
// 用途：先回滚再重跑迁移，可选 step 与 seed。
type MigrateRefreshCommand struct {
	deps   MigrationDependencies
	openDB func(connection string) (dbSession, error)
}

// NewMigrateRefreshCommand 创建 `migrate:refresh` 命令实例。
func NewMigrateRefreshCommand(dependencies ...MigrationDependencies) *MigrateRefreshCommand {
	return &MigrateRefreshCommand{
		deps:   firstMigrationDependencies(dependencies...),
		openDB: openDatabaseSession,
	}
}

func (c *MigrateRefreshCommand) Definition() *console.Definition {
	definition := console.MustDefinition(
		"migrate:refresh {--database= : The database connection to use} {--force : Force the operation to run when in production} {--path=* : The path(s) to the migrations files to be executed} {--realpath : Indicate any provided migration file paths are pre-resolved absolute paths} {--pretend : Dump the SQL queries that would be run} {--seed : Indicates if the seed task should be re-run} {--seeder= : The class name of the root seeder} {--step= : The number of migrations to be reverted}",
		"Reset and re-run all migrations",
	)
	definition.Aliases = []string{"migration:refresh"}
	return definition
}

// Run 执行 refresh 流程。
//
// 规则：
// - step>0 时仅回滚指定条数；
// - 其余情况回滚全部；
// - 回滚后重新执行 pending 迁移。
func (c *MigrateRefreshCommand) Handle(ctx console.CommandContext) error {
	force := ctx.Input().OptionBool("force")
	if err := requireForceInProduction(force, "migrate:refresh"); err != nil {
		return err
	}

	session, err := c.openDB(ctx.Input().Option("database"))
	if err != nil {
		return err
	}
	defer session.Close()

	paths := ctx.Input().OptionStrings("path")
	if len(paths) == 0 {
		paths = c.deps.paths()
	}
	migrations, err := collectMigrations(paths, ctx.Input().OptionBool("realpath"))
	if err != nil {
		return err
	}
	index := migrationIndex(migrations)
	store := newMigrationStore(session.DB)
	if err := store.ensureTable(); err != nil {
		return err
	}

	pretend := ctx.Input().OptionBool("pretend")
	step := parsePositiveInt(ctx.Input().Option("step"))
	var rollbackRecords []migrationRecord
	if step > 0 {
		rollbackRecords, err = store.rollbackCandidates(step, 0)
		if err != nil {
			return err
		}
	} else {
		rollbackRecords, err = store.listAll()
		if err != nil {
			return err
		}
		slices.Reverse(rollbackRecords)
	}

	for _, record := range rollbackRecords {
		migration, exists := index[record.Migration]
		if !exists {
			return fmt.Errorf("migration %s is recorded but source file is missing", record.Migration)
		}
		if pretend {
			ctx.IO().Info("Pretend: " + describeMigrationOperation(migration, false))
			continue
		}
		if err := applyMigrationDown(session.DB, migration, false); err != nil {
			return fmt.Errorf("refresh rollback %s failed: %w", migration.Name, err)
		}
		if err := store.deleteApplied(migration.Name); err != nil {
			return err
		}
		ctx.IO().Success("Rolled back: " + migration.Name)
	}

	applied, err := store.appliedMap()
	if err != nil {
		return err
	}
	pending := make([]migrationSpec, 0, len(migrations))
	for _, migration := range migrations {
		if _, exists := applied[migration.Name]; exists {
			continue
		}
		pending = append(pending, migration)
	}
	if len(pending) == 0 {
		ctx.IO().Info("Nothing to migrate.")
		return nil
	}

	batch, err := store.nextBatch()
	if err != nil {
		return err
	}
	for _, migration := range pending {
		if pretend {
			ctx.IO().Info("Pretend: " + describeMigrationOperation(migration, true))
			continue
		}
		if err := applyMigrationUp(session.DB, migration, false); err != nil {
			return fmt.Errorf("refresh migrate %s failed: %w", migration.Name, err)
		}
		if err := store.markApplied(migration.Name, batch); err != nil {
			return err
		}
		ctx.IO().Success("Migrated: " + migration.Name)
	}

	if ctx.Input().OptionBool("seed") && !pretend {
		className := strings.TrimSpace(ctx.Input().Option("seeder"))
		if className == "" {
			className = defaultSeederClass
		}
		if _, err := resolveSourcePaths(c.deps.seedPaths(), false, "database/seeders", "seeder"); err != nil {
			return err
		}
		if err := runSeederClass(session.DB, className); err != nil {
			return err
		}
		ctx.IO().Success("Seeded: " + className)
	}
	return nil
}
