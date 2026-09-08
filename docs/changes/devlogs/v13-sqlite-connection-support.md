# v13 SQLite 连接支持

## 功能概览

为数据库组件补齐 SQLite 生产连接路径，使 starter 默认的 SQLite 配置、Hermetic 测试策略与框架真实行为一致。此前 Schema 层和测试依赖已经支持 SQLite，但 `database.Open` 仍会把 `sqlite` 判定为未知驱动，导致应用首次解析默认数据库连接时失败。

## 影响范围

- `database/connection.go`：`Open` 接受 `sqlite` / `sqlite3`，使用纯 Go SQLite driver 创建 GORM 连接；SQLite DSN 可从显式 `dsn` 或 `database` 路径读取。
- `database/*_test.go`：原 SQLite 拒绝用例改为未知 PostgreSQL driver，并增加真实临时 SQLite 文件的建表、写入、查询和默认连接测试。
- `CODE_INDEX.md`：修正已过期的 database 文件与符号索引，并登记 MySQL/SQLite 支持。
- 对外数据库文档在独立 `docs/` 仓库同步更新中英文驱动说明、配置项和打开连接示例。

## 实际检查

- `GOCACHE=/www/code/prismgo-dev/framework/tmp/gocache go test ./database/...`：通过。
- `GOCACHE=/www/code/prismgo-dev/framework/tmp/gocache go test ./...`：在允许 localhost 监听的环境中通过；受限沙箱内的首次执行因 miniredis/httptest 无法绑定端口而失败。
- `make covdata PACKAGES=./database`：通过，database 包语句覆盖率 `91.2%`。
- `golangci-lint run --verbose ./database/...`：通过，0 issues。
- `gofmt`：已应用于所有修改的 Go 文件。

## 测试范围与复杂逻辑

单元/组件测试覆盖 SQLite driver 分派、DSN 优先级、未知 driver 错误，以及使用真实临时 SQLite 文件执行 DDL、参数化写入和聚合查询。默认连接测试同时覆盖配置读取与连接池装配路径。本次没有启动 MySQL 等外部服务。

## 风险与兼容性

- 已有 MySQL 分支及连接初始化逻辑未改变。
- `sqlite` 原先返回 unsupported error，现在返回可用连接；这是对既有 starter 配置和文档承诺的补齐。
- 继续对 PostgreSQL、SQL Server 和其他未知 driver 返回明确错误，不增加静默回退。
- 没有新增依赖：`github.com/glebarez/sqlite` 已是框架直接依赖，此前主要用于测试。

## 未完成事项

- PostgreSQL 与 SQL Server 仍不受支持。
- 本变更不扩展 SQLite 不具备的 MySQL 专属迁移操作；相关 Schema API 继续返回既有的不支持错误或采用既有 SQLite 语义。
