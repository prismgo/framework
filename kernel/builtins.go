package kernel

import (
	"context"
	"net/http"

	"github.com/prismgo/framework/cmd"
	queuecommand "github.com/prismgo/framework/cmd/queue"
	"github.com/prismgo/framework/console"
)

// BuiltinDependencies 描述内置命令注册所需的外部依赖。
type BuiltinDependencies struct {
	Application ApplicationRegistrySource
}

// WithBuiltins 在 Kernel 初始化阶段自动注册默认内置命令。
func WithBuiltins(deps BuiltinDependencies) Option {
	return func(k *Kernel) {
		RegisterBuiltins(k, deps)
	}
}

func (k *Kernel) registerCoreCommands() {
	k.RegisterLazy(func() console.Command {
		return cmd.NewListCommand(k.Commands)
	}, func() console.Command {
		return newCompletionCommand(k)
	})
}

// BuiltinCommandFactories 返回默认内置命令工厂。
func BuiltinCommandFactories(k *Kernel, deps BuiltinDependencies) []console.CommandFactory {
	application := deps.Application
	migrationDeps := cmd.MigrationDependencies{
		MigrationPaths: func() []string {
			if application == nil {
				return nil
			}
			return application.MigrationPaths()
		},
		SeedPaths: func() []string {
			if application == nil {
				return nil
			}
			return application.SeedPaths()
		},
	}
	factories := []console.CommandFactory{
		func() console.Command {
			return cmd.NewServeCommand(func(ctx context.Context, port string) (*http.Server, error) {
				return ApplicationNewHTTPServer(application, ctx, port)
			})
		},
		func() console.Command { return cmd.NewMigrateInstallCommand(migrationDeps) },
		func() console.Command { return cmd.NewMigrateStatusCommand(migrationDeps) },
		func() console.Command { return cmd.NewMigrateCommand(migrationDeps) },
		func() console.Command { return cmd.NewMigrateRollbackCommand(migrationDeps) },
		func() console.Command { return cmd.NewMigrateResetCommand(migrationDeps) },
		func() console.Command { return cmd.NewMigrateRefreshCommand(migrationDeps) },
		func() console.Command { return cmd.NewMigrateFreshCommand(migrationDeps) },
		func() console.Command { return cmd.NewDBSeedCommand(migrationDeps) },
		func() console.Command { return cmd.NewKeyGenerateCommand() },
		func() console.Command { return cmd.NewStorageLinkCommand() },
		func() console.Command { return cmd.NewStorageUnlinkCommand() },
		func() console.Command { return queuecommand.NewWorkCommand() },
		func() console.Command { return queuecommand.NewFailedCommand() },
		func() console.Command { return queuecommand.NewRetryCommand() },
		func() console.Command { return queuecommand.NewForgetCommand() },
		func() console.Command { return queuecommand.NewFlushCommand() },
		func() console.Command { return queuecommand.NewRestartCommand() },
		func() console.Command { return cmd.NewCronCommand(k, nil) },
		func() console.Command {
			return cmd.NewRouteListCommand(func() error {
				return ApplicationLoadHTTPRoutes(application)
			})
		},
		func() console.Command { return cmd.NewVendorPublishCommand() },
	}
	return append(factories, cmd.MakeCommandFactories()...)
}

// RegisterBuiltins 统一注册默认内置命令。
func RegisterBuiltins(k *Kernel, deps BuiltinDependencies) {
	if k == nil {
		panic("kernel register builtins: kernel is nil")
	}
	k.RegisterLazy(BuiltinCommandFactories(k, deps)...)
}
