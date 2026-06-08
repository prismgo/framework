package migration

import (
	"slices"
	"strconv"

	"github.com/prismgo/framework/console"
)

// MigrateStatusCommand 对应 `migrate:status` 命令。
//
// 用途：展示每个迁移文件是否已执行，以及对应批次。
type MigrateStatusCommand struct {
	deps   MigrationDependencies
	openDB func(connection string) (dbSession, error)
}

// NewMigrateStatusCommand 创建 `migrate:status` 命令实例。
func NewMigrateStatusCommand(dependencies ...MigrationDependencies) *MigrateStatusCommand {
	return &MigrateStatusCommand{
		deps:   firstMigrationDependencies(dependencies...),
		openDB: openDatabaseSession,
	}
}

func (c *MigrateStatusCommand) Definition() *console.Definition {
	definition := console.MustDefinition(
		"migrate:status {--database= : The database connection to use} {--path=* : The path(s) to the migrations files to be executed} {--realpath : Indicate any provided migration file paths are pre-resolved absolute paths}",
		"Show the status of each migration",
	)
	definition.Aliases = []string{"migration:status"}
	return definition
}

// Run 执行状态查询并输出表格。
//
// 额外行为：数据库中存在但文件缺失的记录会标记为 `[missing]`。
func (c *MigrateStatusCommand) Handle(ctx console.CommandContext) error {
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
	if !store.hasTable() {
		ctx.IO().Warn("migration table not found, run migrate:install first")
		return nil
	}

	applied, err := store.appliedMap()
	if err != nil {
		return err
	}

	rows := make([][]string, 0, len(migrations))
	seen := make(map[string]struct{}, len(migrations))
	for _, migration := range migrations {
		seen[migration.Name] = struct{}{}
		record, exists := applied[migration.Name]
		status := "N"
		batch := "-"
		if exists {
			status = "Y"
			batch = strconv.Itoa(record.Batch)
		}
		rows = append(rows, []string{status, batch, migration.Name})
	}

	missing := make([]string, 0)
	for migration := range applied {
		if _, exists := seen[migration]; !exists {
			missing = append(missing, migration)
		}
	}
	slices.Sort(missing)
	for _, migration := range missing {
		rows = append(rows, []string{"Y", strconv.Itoa(applied[migration].Batch), migration + " [missing]"})
	}
	return ctx.IO().Table([]string{"Ran?", "Batch", "Migration"}, rows)
}
