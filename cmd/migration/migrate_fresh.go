package migration

import (
	"errors"
	"fmt"
	"strings"

	"gorm.io/gorm"

	"github.com/prismgo/framework/console"
	dbschema "github.com/prismgo/framework/database/schema"
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
	return dbschema.New(db).DropAllTables()
}

// dropAllViews delegates view discovery and deletion to Schema.
func dropAllViews(db *gorm.DB) error {
	err := dbschema.New(db).DropAllViews()
	if errors.Is(err, dbschema.ErrUnsupportedFeature) {
		return nil
	}
	return err
}

// dropAllTypes delegates custom type discovery and deletion to Schema.
func dropAllTypes(db *gorm.DB) error {
	err := dbschema.New(db).DropAllTypes()
	if err == nil || !errors.Is(err, dbschema.ErrUnsupportedFeature) {
		return err
	}
	if strings.ToLower(strings.TrimSpace(db.Name())) != "postgres" {
		return nil
	}
	var rows []struct {
		Name string `gorm:"column:typname"`
	}
	query := "SELECT typname FROM pg_type WHERE typnamespace IN (SELECT oid FROM pg_namespace WHERE nspname = current_schema()) AND typtype = 'e'"
	if err := db.Raw(query).Scan(&rows).Error; err != nil {
		return err
	}
	for _, row := range rows {
		name := strings.ReplaceAll(row.Name, `"`, `""`)
		if err := db.Exec(fmt.Sprintf(`DROP TYPE IF EXISTS "%s" CASCADE`, name)).Error; err != nil {
			return err
		}
	}
	return nil
}
