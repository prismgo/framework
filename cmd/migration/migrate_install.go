package migration

import "github.com/prismgo/framework/console"

// MigrateInstallCommand 对应 `migrate:install` 命令。
//
// 用途：初始化 migrations 元数据表。
type MigrateInstallCommand struct {
	deps   MigrationDependencies
	openDB func(connection string) (dbSession, error)
}

// NewMigrateInstallCommand 创建 `migrate:install` 命令实例。
func NewMigrateInstallCommand(dependencies ...MigrationDependencies) *MigrateInstallCommand {
	return &MigrateInstallCommand{
		deps:   firstMigrationDependencies(dependencies...),
		openDB: openDatabaseSession,
	}
}

func (c *MigrateInstallCommand) Definition() *console.Definition {
	definition := console.MustDefinition(
		"migrate:install {--database= : The database connection to use}",
		"Create the migration repository",
	)
	definition.Aliases = []string{"migration:install"}
	return definition
}

// Run 执行迁移仓库初始化。
func (c *MigrateInstallCommand) Handle(ctx console.CommandContext) error {
	session, err := c.openDB(ctx.Input().Option("database"))
	if err != nil {
		return err
	}
	defer session.Close()

	if err := newMigrationStore(session.DB).ensureTable(); err != nil {
		return err
	}
	ctx.IO().Success("migration table created")
	return nil
}
