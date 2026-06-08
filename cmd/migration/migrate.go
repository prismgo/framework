package migration

import (
	"fmt"
	"strings"

	"github.com/prismgo/framework/console"
)

// MigrateCommand 对应 `migrate` 命令。
//
// 用途：执行未运行迁移，并支持可选 seed。
type MigrateCommand struct {
	deps   MigrationDependencies
	openDB func(connection string) (dbSession, error)
}

// NewMigrateCommand 创建 `migrate` 命令。
//
// 用途：执行所有未运行迁移，并可按参数触发 seeder。
func NewMigrateCommand(dependencies ...MigrationDependencies) *MigrateCommand {
	return &MigrateCommand{
		deps:   firstMigrationDependencies(dependencies...),
		openDB: openDatabaseSession,
	}
}

func (c *MigrateCommand) Definition() *console.Definition {
	definition := console.MustDefinition(
		"migrate {--database= : The database connection to use} {--force : Force the operation to run when in production} {--path=* : The path(s) to the migrations files to be executed} {--realpath : Indicate any provided migration file paths are pre-resolved absolute paths} {--pretend : Dump the SQL queries that would be run} {--seed : Indicates if the seed task should be re-run} {--seeder= : The class name of the root seeder} {--step : Force the migrations to be run so they can be rolled back individually}",
		"Run the database migrations",
	)
	definition.Aliases = []string{"migration"}
	return definition
}

// Run 执行迁移主流程。
//
// 执行步骤：
// 1. 环境保护：生产环境未带 --force 时拒绝执行；
// 2. 解析路径并扫描迁移文件；
// 3. 建立迁移仓库并计算 pending；
// 4. 按顺序执行 up，并记录 batch；
// 5. 按需执行 seed。
func (c *MigrateCommand) Handle(ctx console.CommandContext) error {
	force := ctx.Input().OptionBool("force")
	if err := requireForceInProduction(force, "migrate"); err != nil {
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

	store := newMigrationStore(session.DB)
	if err := store.ensureTable(); err != nil {
		return err
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

	pretend := ctx.Input().OptionBool("pretend")
	stepMode := ctx.Input().OptionBool("step")
	batch, err := store.nextBatch()
	if err != nil {
		return err
	}

	for index, migration := range pending {
		if pretend {
			ctx.IO().Info("Pretend: " + describeMigrationOperation(migration, true))
			continue
		}
		migrationBatch := batch
		if stepMode {
			migrationBatch = batch + index
		}
		if err := applyMigrationAndTrack(session.DB, migration, migrationBatch); err != nil {
			return fmt.Errorf("migrate %s failed: %w", migration.Name, err)
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
