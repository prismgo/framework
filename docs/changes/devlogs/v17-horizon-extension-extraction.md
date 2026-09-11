# v17 Horizon 扩展拆分

## 功能概览

将 Horizon 从 framework 内置包迁移到独立模块 `github.com/prismgo/horizon`。应用通过 Extension Provider 层注册 `horizon.ServiceProvider{}`；命令名、配置键、Dashboard/API、事件与 Store 语义保持不变。

## 影响范围

- 删除 framework 内的 `horizon/` 实现、测试、Dashboard 资源和安装模板。
- `CODE_INDEX.md` 不再声明内置 Horizon 包及其容器键。
- framework README 中英文入口改为指向独立 Horizon 扩展。
- Horizon 实现、测试和资源在独立模块中保留，内部 import 改为 `github.com/prismgo/horizon[/cmd]`。

## 测试范围与复杂逻辑

独立模块保留原 Horizon 根包及 `cmd` 包测试，并新增公开 Provider 契约测试。迁移使用格式化后的逐文件等价比较，保证除 module/import 调整外未改写实现或既有测试断言。

## 实际检查

- `framework`：`GOWORK=off go test -count=1 ./...`、`GOWORK=off go vet ./...` 和 `GOWORK=off golangci-lint run --verbose ./...` 通过；`foundation`、`provider` 目标覆盖率合计 93.1%。
- `ext/horizon`：`GOWORK=off go test -count=1 ./...`、`go test -race`、`go vet` 和 `golangci-lint` 通过；根包与 `cmd` 合并覆盖率 90.3%。真实 Redis gate 已执行且未跳过。
- `ext/rabbitmq`：独立 module 全量测试和 race 通过，覆盖率 90.1%；未安装 Horizon 时 gate 明确跳过，工作区安装 Horizon 后真实 RabbitMQ gate 在 30 秒父级期限内通过。
- `docs`：中英文文件配对、旧 import 迁移说明、Horizon 交叉链接与 `git diff --check` 通过。

## 风险、兼容性与未完成事项

- 使用 Horizon 的应用必须新增 `github.com/prismgo/horizon` module，并在 `WithExtensionProviders` 中注册 `horizon.ServiceProvider{}`。
- 旧 `github.com/prismgo/framework/horizon[/cmd]` import path 不再存在；不保留反向兼容 shim，以避免 framework 与扩展形成 module 环。
- 独立模块在 framework 新版本发布前依赖 origin/main 的可解析 pseudo-version；正式版本发布后应切换到对应 tag。
