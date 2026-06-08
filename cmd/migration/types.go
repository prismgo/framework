package migration

import "github.com/prismgo/framework/database"

type (
	// MigrationFunc 描述单个迁移步骤执行函数。
	MigrationFunc = database.MigrationFunc
	// MigrationFuncMap 描述迁移名到执行函数映射。
	MigrationFuncMap = database.MigrationFuncMap
	// SeedFunc 描述单个 seeder 执行函数。
	SeedFunc = database.SeedFunc
	// SeedFuncMap 描述 seeder class 到执行函数映射。
	SeedFuncMap = database.SeedFuncMap
)

const defaultSeederClass = database.DefaultSeederClass
