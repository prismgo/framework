package migration

import (
	"strings"

	"gorm.io/gorm"

	"github.com/prismgo/framework/console"
	dbregistry "github.com/prismgo/framework/database"
)

// DBSeedCommand 对应 Laravel 风格 `db:seed` 命令。
//
// 用途：执行指定 seeder class（默认 DatabaseSeeder）。
type DBSeedCommand struct {
	deps   MigrationDependencies
	openDB func(connection string) (dbSession, error)
}

// NewDBSeedCommand 创建 `db:seed` 命令实例。
func NewDBSeedCommand(dependencies ...MigrationDependencies) *DBSeedCommand {
	return &DBSeedCommand{
		deps:   firstMigrationDependencies(dependencies...),
		openDB: openDatabaseSession,
	}
}

// Definition 返回命令签名定义。
func (c *DBSeedCommand) Definition() *console.Definition {
	return console.MustDefinition(
		"db:seed {--database= : The database connection to seed} {--class= : The class name of the root seeder} {--force : Force the operation to run when in production}",
		"Seed the database with records",
	)
}

// Run 执行 seeder。
//
// 执行前会校验生产环境 --force 保护，并验证 seeder 目录配置有效。
func (c *DBSeedCommand) Handle(ctx console.CommandContext) error {
	force := ctx.Input().OptionBool("force")
	if err := requireForceInProduction(force, "db:seed"); err != nil {
		return err
	}

	session, err := c.openDB(ctx.Input().Option("database"))
	if err != nil {
		return err
	}
	defer session.Close()

	className := strings.TrimSpace(ctx.Input().Option("class"))
	if className == "" {
		className = defaultSeederClass
	}
	if _, err := resolveSourcePaths(c.deps.seedPaths(), false, "database/seeders", "seeder"); err != nil {
		return err
	}
	if err := runSeederClass(session.DB, className); err != nil {
		return err
	}
	ctx.IO().Success("seed completed")
	return nil
}

func runSeederClass(db *gorm.DB, className string) error {
	if err := dbregistry.EnsureSeederRegistered(className); err != nil {
		return err
	}
	seeder, ok := dbregistry.SeederByClass(className)
	if !ok || seeder == nil {
		return dbregistry.EnsureSeederRegistered(className)
	}
	return seeder(db)
}
