package cmd

import "github.com/prismgo/framework/cmd/contracts"

// MigrationFunc 描述一个可被迁移命令执行的 Go 迁移函数。
//
// 使用方式：在 migration 文件（如 20260428xxx_xxx.go）的 init 中注册执行函数。
// 设计原因：将迁移命令与业务层彻底解耦，命令本身只负责调度，不直接依赖业务包。
type MigrationFunc = contracts.MigrationFunc

// MigrationFuncMap 描述 migration key 到执行函数的映射关系。
type MigrationFuncMap = contracts.MigrationFuncMap

// SeedFuncMap 描述 seeder class 到执行函数的映射关系。
//
// 示例：`DatabaseSeeder -> seeders.Seed`。
type SeedFuncMap = contracts.SeedFuncMap
