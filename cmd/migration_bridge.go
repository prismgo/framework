package cmd

import migrationcmd "github.com/prismgo/framework/cmd/migration"

type (
	MigrationDependencies  = migrationcmd.MigrationDependencies
	MigrateInstallCommand  = migrationcmd.MigrateInstallCommand
	MigrateStatusCommand   = migrationcmd.MigrateStatusCommand
	MigrateCommand         = migrationcmd.MigrateCommand
	MigrateRollbackCommand = migrationcmd.MigrateRollbackCommand
	MigrateResetCommand    = migrationcmd.MigrateResetCommand
	MigrateRefreshCommand  = migrationcmd.MigrateRefreshCommand
	MigrateFreshCommand    = migrationcmd.MigrateFreshCommand
	DBSeedCommand          = migrationcmd.DBSeedCommand
)

func NewMigrateInstallCommand(dependencies ...MigrationDependencies) *MigrateInstallCommand {
	return migrationcmd.NewMigrateInstallCommand(dependencies...)
}

func NewMigrateStatusCommand(dependencies ...MigrationDependencies) *MigrateStatusCommand {
	return migrationcmd.NewMigrateStatusCommand(dependencies...)
}

func NewMigrateCommand(dependencies ...MigrationDependencies) *MigrateCommand {
	return migrationcmd.NewMigrateCommand(dependencies...)
}

func NewMigrateRollbackCommand(dependencies ...MigrationDependencies) *MigrateRollbackCommand {
	return migrationcmd.NewMigrateRollbackCommand(dependencies...)
}

func NewMigrateResetCommand(dependencies ...MigrationDependencies) *MigrateResetCommand {
	return migrationcmd.NewMigrateResetCommand(dependencies...)
}

func NewMigrateRefreshCommand(dependencies ...MigrationDependencies) *MigrateRefreshCommand {
	return migrationcmd.NewMigrateRefreshCommand(dependencies...)
}

func NewMigrateFreshCommand(dependencies ...MigrationDependencies) *MigrateFreshCommand {
	return migrationcmd.NewMigrateFreshCommand(dependencies...)
}

func NewDBSeedCommand(dependencies ...MigrationDependencies) *DBSeedCommand {
	return migrationcmd.NewDBSeedCommand(dependencies...)
}
