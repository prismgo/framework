package migration

import (
	"fmt"
	"strings"

	"gorm.io/gorm"

	"github.com/prismgo/framework/console"
)

// MigrateFreshCommand 对应 `migrate:fresh` 命令。
//
// 用途：清空数据库对象后重新执行全部迁移，可选 seed。
type MigrateFreshCommand struct {
	deps   MigrationDependencies
	openDB func(connection string) (dbSession, error)
}

// NewMigrateFreshCommand 创建 `migrate:fresh` 命令实例。
func NewMigrateFreshCommand(dependencies ...MigrationDependencies) *MigrateFreshCommand {
	return &MigrateFreshCommand{
		deps:   firstMigrationDependencies(dependencies...),
		openDB: openDatabaseSession,
	}
}

func (c *MigrateFreshCommand) Definition() *console.Definition {
	definition := console.MustDefinition(
		"migrate:fresh {--database= : The database connection to use} {--force : Force the operation to run when in production} {--path=* : The path(s) to the migrations files to be executed} {--realpath : Indicate any provided migration file paths are pre-resolved absolute paths} {--seed : Indicates if the seed task should be re-run} {--seeder= : The class name of the root seeder} {--drop-views : Drop all tables and views} {--drop-types : Drop all tables and types (Postgres only)}",
		"Drop all tables and re-run all migrations",
	)
	definition.Aliases = []string{"migration:fresh"}
	return definition
}

// Run 执行 fresh 主流程。
//
// 执行顺序：drop tables -> (optional) drop views/types -> re-run migrations -> (optional) seed。
func (c *MigrateFreshCommand) Handle(ctx console.CommandContext) error {
	force := ctx.Input().OptionBool("force")
	if err := requireForceInProduction(force, "migrate:fresh"); err != nil {
		return err
	}

	session, err := c.openDB(ctx.Input().Option("database"))
	if err != nil {
		return err
	}
	defer session.Close()

	if err := dropAllTables(session.DB); err != nil {
		return err
	}
	if ctx.Input().OptionBool("drop-views") {
		if err := dropAllViews(session.DB); err != nil {
			return err
		}
	}
	if ctx.Input().OptionBool("drop-types") {
		if err := dropAllTypes(session.DB); err != nil {
			return err
		}
	}

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
	batch, err := store.nextBatch()
	if err != nil {
		return err
	}
	for _, migration := range migrations {
		if err := applyMigrationUp(session.DB, migration, false); err != nil {
			return fmt.Errorf("fresh migrate %s failed: %w", migration.Name, err)
		}
		if err := store.markApplied(migration.Name, batch); err != nil {
			return err
		}
		ctx.IO().Success("Migrated: " + migration.Name)
	}

	if ctx.Input().OptionBool("seed") {
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

func dropAllTables(db *gorm.DB) error {
	tables, err := db.Migrator().GetTables()
	if err != nil {
		return err
	}
	for _, table := range tables {
		if strings.HasPrefix(strings.ToLower(table), "sqlite_") {
			continue
		}
		if dropErr := db.Migrator().DropTable(table); dropErr != nil {
			return dropErr
		}
	}
	return nil
}

// dropAllViews 按当前数据库方言删除视图。
func dropAllViews(db *gorm.DB) error {
	dialect := strings.ToLower(strings.TrimSpace(db.Name()))
	switch dialect {
	case "sqlite", "sqlite3":
		type viewRow struct {
			Name string
		}
		var rows []viewRow
		if err := db.Raw("SELECT name FROM sqlite_master WHERE type='view'").Scan(&rows).Error; err != nil {
			return err
		}
		for _, row := range rows {
			if err := db.Exec(fmt.Sprintf("DROP VIEW IF EXISTS `%s`", row.Name)).Error; err != nil {
				return err
			}
		}
	case "mysql":
		var names []string
		sql := "SELECT table_name FROM information_schema.views WHERE table_schema = DATABASE()"
		if err := db.Raw(sql).Scan(&names).Error; err != nil {
			return err
		}
		for _, name := range names {
			if err := db.Exec(fmt.Sprintf("DROP VIEW IF EXISTS `%s`", name)).Error; err != nil {
				return err
			}
		}
	}
	return nil
}

// dropAllTypes 删除 Postgres enum/type 对象。
//
// 说明：仅在 postgres 方言执行，其他方言直接跳过。
func dropAllTypes(db *gorm.DB) error {
	if strings.ToLower(strings.TrimSpace(db.Name())) != "postgres" {
		return nil
	}
	type typeRow struct {
		Name string
	}
	var rows []typeRow
	sql := "SELECT typname FROM pg_type WHERE typnamespace IN (SELECT oid FROM pg_namespace WHERE nspname = current_schema()) AND typtype = 'e'"
	if err := db.Raw(sql).Scan(&rows).Error; err != nil {
		return err
	}
	for _, row := range rows {
		if err := db.Exec(fmt.Sprintf("DROP TYPE IF EXISTS \"%s\" CASCADE", row.Name)).Error; err != nil {
			return err
		}
	}
	return nil
}
