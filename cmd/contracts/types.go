package contracts

import "github.com/prismgo/framework/database"

// MigrationFunc 描述一个可被迁移命令执行的 Go 迁移函数。
type MigrationFunc = database.MigrationFunc

// MigrationFuncMap 描述 migration key 到执行函数的映射关系。
type MigrationFuncMap = database.MigrationFuncMap

// SeedFunc 描述一个 Seeder 执行函数。
type SeedFunc = database.SeedFunc

// SeedFuncMap 描述 seeder class 到执行函数的映射关系。
type SeedFuncMap = database.SeedFuncMap

const DefaultSeederClass = database.DefaultSeederClass
