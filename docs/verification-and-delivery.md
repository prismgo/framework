# 验证与交付

## 按范围验证

| 目标 | 命令 |
|---|---|
| 目标包测试 | `go test ./<package>/...` |
| 目标包覆盖率 | `make covdata PACKAGES=./<package>` |
| 目标包静态检查 | `golangci-lint run --verbose ./<package>/...` |
| 全量测试 | `go test ./...` 或 `make test` |
| 竞态检查 | `make test-race` |
| 格式检查 | `make fmt-check` |
| Vet | `make vet` |
| 完整 CI | `make ci` |

`make covdata` 写入 `.coverage/` 并使用隔离的 `tmp/gocache`。框架 Go 代码变更的相关范围语句覆盖率目标为不低于 90%。遇到疑似 flaky 失败时，单独重跑失败包并说明结果。

## Go 代码交付顺序

1. 检查本次变更产生的死代码、孤立代码和兼容回退；发现后先报告。
2. 对改动文件运行 `gofmt`。
3. 运行改动包的测试、覆盖率和 `golangci-lint run --verbose ./<changed>/...`。
4. 根据跨组件风险补充全量测试、竞态检查或 `make ci`。
5. 创建开发日志并复核最终差异。

## 开发日志

功能变更后创建 `docs/changes/devlogs/v{next}-{function-description}.md`；若 `docs/changes` 被忽略，仍按仓库规则生成本地记录。文件序号递增，语言与本次回复一致，并包含：

- 功能概览、目标与业务背景；
- 影响范围、修改文件和行为变化；
- 实际检查命令与结果；
- 单元/集成测试范围、覆盖率和复杂逻辑说明；
- 风险、优化建议、死代码和兼容回退；
- 未完成事项。

## 最终报告

每次 Go 代码变更必须报告实际覆盖率命令、测试类型（单元、集成或两者）、语句总覆盖率和明显低覆盖的包或函数。覆盖率未执行或只覆盖部分范围时，说明准确原因。
