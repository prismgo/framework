---
comet_change: add-github-project-automation
role: technical-design
canonical_spec: openspec
---

# GitHub 项目自动化流程设计

## 背景

本变更为 `github.com/prismgo/framework` 设计一套与 gin-gonic/gin `.github` 功能对齐、但按本项目实际情况适配的 GitHub 项目流程。gin 的方案覆盖 Issue 模板、PR 模板、Dependabot、CI、CodeQL、Trivy 和 GoReleaser。用户已确认本项目采用以下边界：

- 范围采用适配版，不做字节级照搬。
- CI 强度采用 gin 同款矩阵：lint 前置，测试覆盖 Ubuntu、macOS、多 Go 版本、race 和 Codecov。
- 安全发布采用安全优先：启用 Dependabot、CodeQL、Trivy；GoReleaser 只设计，不默认启用 tag 自动发布。
- 安全问题入口使用 GitHub Security Advisory。
- 深度设计文档使用中文。

本地事实：

- 当前分支为 `main`，workflow 分支过滤应使用 `main`，不是 gin 的 `master`。
- 远端为 `git@github.com:prismgo/framework.git`。
- 项目是 Go module，模块名为 `github.com/prismgo/framework`。
- 当前无 `.github` 目录。
- 当前存在未提交的用户改动：`.gitignore` 修改和新增 `codecov.yml`，实现阶段不得覆盖这些改动。

## 目标

本设计要让项目获得一套可维护、可本地复现、权限收敛的 GitHub 自动化流程：

- 贡献入口清晰：bug、feature、PR 模板描述维护者需要的信息。
- CI 可复现：本地命令和 GitHub Actions 使用同一组 Make target。
- 安全扫描稳定：CodeQL 和 Trivy 上传 GitHub Security 结果，并在必要时失败。
- 依赖更新自动化：Dependabot 覆盖 Go module 和 GitHub Actions。
- 发布保持保守：不因本变更引入 tag push 自动发布行为。

非目标：

- 不修改 Go 包 API 或运行时行为。
- 不新增 gin 专属 build tags，例如 `nomsgpack`、`sonic`、`go_json`。
- 不启用 GoReleaser 自动发布。
- 不覆盖现有 `codecov.yml`。

## 方案总览

采用“适配 gin 的完整治理流水线”：

```text
.github/
├── ISSUE_TEMPLATE/
│   ├── bug-report.yaml
│   ├── feature-request.yaml
│   └── config.yml
├── PULL_REQUEST_TEMPLATE.md
├── dependabot.yml
└── workflows/
    ├── ci.yml
    ├── codeql.yml
    └── trivy-scan.yml

Makefile
.golangci.yml      # 仅在实现阶段确认需要时新增
```

文件命名不完全照搬 gin。gin 的主 CI 文件名是 `gin.yml`，本项目使用 `ci.yml`，因为它表达的是通用 CI 职责，避免未来维护者误以为文件属于 gin。

## 贡献入口设计

### Bug Report

Bug 模板使用英文，保持开源协作友好。字段应覆盖：

- 安全提醒：安全问题通过 GitHub Security Advisory 私下报告，不要开公开 issue。
- Description：问题描述。
- Prismgo Version：版本号或 commit。
- Can you reproduce the bug?：是否可复现。
- Minimal Reproduction：最小复现代码、命令或配置。
- Expected Behavior：期望行为。
- Actual Behavior：实际行为。
- Logs or Error Output：相关日志。
- Go Version：Go 版本。
- Operating System：操作系统。

标签建议使用 `type/bug`，与 gin 的标签风格一致。

### Feature Request

Feature 模板保持轻量，但比 gin 原版略多字段，以降低需求歧义：

- Feature Description：功能描述，必填。
- Use Case：解决的问题或使用场景。
- Proposed API or Behavior：期望 API 或行为。
- Alternatives Considered：考虑过的替代方案。

标签建议使用 `type/proposal`，与 gin 一致。

### Issue Template Config

默认禁用 blank issue，避免维护者收到缺少上下文的公开问题。contact links 应至少包含：

- Go package documentation：`https://pkg.go.dev/github.com/prismgo/framework`
- Repository README 或 docs：指向本仓库文档入口。
- Discussions：如果仓库未启用 Discussions，实现阶段可先不放该链接，或放 GitHub 仓库 discussions URL 并由维护者后续启用。

## PR 模板设计

PR 模板沿用 gin 的短 checklist，不引入复杂流程：

- PR 目标分支是 `main`。
- CI 必须通过。
- 代码变更需要新增或更新测试。
- 用户可见变更需要更新 `README.md` 或 `docs/`。
- Public API 变更需要在 PR 描述中说明意图。

该模板不强制 changelog，因为本仓库当前未确认 changelog 维护策略。

## CI 设计

### 工作流触发

`ci.yml` 在以下事件运行：

- push 到 `main`
- pull_request 目标为 `main`

权限使用最小集：

```yaml
permissions:
  contents: read
```

### Job 拆分

CI 使用两个 job：

1. `lint`
   - `actions/checkout`
   - `actions/setup-go`
   - `golangci/golangci-lint-action`

2. `test`
   - `needs: lint`
   - matrix 覆盖 OS、Go 版本、测试模式
   - 缓存 Go build cache 和 module cache
   - 调用 Make target
   - 上传 coverage 到 Codecov

### Matrix

gin 的矩阵包含 Ubuntu、macOS、Go 1.25/1.26，以及多个 gin 特有 test tags。本项目应保留 OS 和 Go 版本强度，但不添加无关 tags。

建议实现矩阵：

```yaml
os: [ubuntu-latest, macos-latest]
go: ["1.26"]
test-mode: ["normal", "race"]
```

实现备注：`go.mod` 当前声明 `go 1.26.2`，因此 Go 1.25 job 不能真实验证 Go 1.25 兼容性，可能失败或触发 toolchain 自动下载。实现阶段保留 gin 同款 OS 和 normal/race 强度，但将 Go version matrix 收敛为 Go 1.26。

### Makefile

由于本项目当前没有 Makefile，建议新增统一入口：

- `make test`：运行 `go test ./...`，并输出 `coverage.out`。
- `make test-race`：运行 `go test -race ./...`。
- `make vet`：运行 `go vet ./...`。
- `make fmt-check`：检查 `gofmt` diff。
- `make lint`：运行 `golangci-lint run`。
- `make ci`：组合本地 CI 常用检查。

CI workflow 只调用 Make target，避免复杂 shell 分散在 YAML 中。这样本地失败和 CI 失败更容易对齐。

### Coverage

已有 `codecov.yml` 不应被覆盖。CI 只负责生成 `coverage.out` 并使用 `codecov/codecov-action` 上传。

为兼容 matrix，可用 flags 标识：

- OS，例如 `ubuntu-latest`
- Go 版本，例如 `go-1.26`
- 测试模式，例如 `normal` 或 `race`

## Dependabot 设计

Dependabot 与 gin 对齐：

- `gomod`，目录 `/`，daily。
- `github-actions`，目录 `/`，daily。
- GitHub Actions 更新分组为 `actions`，patterns 为 `*`。

初版不对 Go module 做额外分组。Go 依赖数量较多，后续如果 PR 噪声过大，可以单独设计分组策略。

## CodeQL 设计

CodeQL 与 gin 对齐：

- push 到 `main`
- pull_request 到 `main`
- 每周 schedule
- language 只启用 `go`

权限：

```yaml
permissions:
  security-events: write
  contents: read
```

CodeQL 不需要手写 build 步骤，除非实现阶段发现自动构建不能覆盖本项目。若需要手写 build，应优先调用 Make target。

## Trivy 设计

Trivy 与 gin 对齐：

- push 到 `main`
- pull_request 到 `main`
- daily schedule
- workflow_dispatch

扫描参数：

- scan-type: `fs`
- scan-ref: `.`
- scanners: `vuln,secret,misconfig`
- format: `sarif` 用于上传 GitHub Security
- table output 用于日志
- severity: `CRITICAL,HIGH,MEDIUM`
- ignore-unfixed: `true`

执行顺序必须保证 SARIF 上传优先于失败：

1. 生成 SARIF。
2. `if: always()` 上传 SARIF。
3. `if: always()` 运行 table 输出并设置 `exit-code: "1"`。

本仓库存在 `storage/logs` 这类运行时文件。实现阶段不能用广泛 ignore 掩盖真实风险；只能对明确不应纳入仓库或已经被 `.gitignore` 排除的本地运行产物做有边界处理。

## 发布设计

gin 在 tag push 时直接运行 GoReleaser，并触发 pkg.go.dev/proxy reindex。本项目本轮不启用该行为。

原因：

- 尚未确认 `.goreleaser.yaml`。
- 尚未确认 tag 命名策略。
- 尚未确认维护者是否希望 tag push 自动创建 GitHub Release。
- 自动发布属于高影响动作，应单独评审。

本轮只在设计和任务中记录未来启用条件。实现阶段不得新增 `goreleaser.yml`，除非该 workflow 默认不触发发布且维护者再次确认。

## 错误处理和边界

- GitHub Actions Go 版本不可用：调整到可用版本并记录偏差，不静默降级。
- Codecov 上传失败：不应影响本地测试结果判断，但是否 fail CI 取决于 Codecov action 默认行为；初版保持 action 默认。
- Trivy 误报：先确认是否为仓库应清理文件，再考虑 `.trivyignore`；不得 broad ignore。
- macOS race 慢：这是用户选择 gin 同款矩阵的成本，初版接受。
- 缺少 Discussions：contact links 可先只放 pkg.go.dev 和 README/docs。

## 验证策略

实现完成后应验证：

- YAML 文件语法有效。
- `.github` 文件路径符合 GitHub 约定。
- Make target 可本地运行，至少 `make test` 或等价命令可执行。
- `go test ./...` 在本地运行，若因环境或外部依赖失败，需要记录失败原因。
- workflow 权限最小化：
  - CI: `contents: read`
  - CodeQL/Trivy: `contents: read` + `security-events: write`
- 分支过滤全部使用 `main`。
- 没有新增 tag-triggered release workflow。
- 未覆盖 `.gitignore` 和 `codecov.yml` 的用户改动。

## 实施顺序

1. 新增 `.github` 目录和 Issue/PR 模板。
2. 新增 Dependabot。
3. 新增 Makefile 和必要 lint 配置。
4. 新增 CI workflow。
5. 新增 CodeQL workflow。
6. 新增 Trivy workflow。
7. 本地验证 Make target、Go tests 和 YAML 结构。
8. 更新 OpenSpec tasks 状态。

## 设计自检

- 无未决占位符。
- OpenSpec delta spec 是需求事实源，本文件只描述实现方案。
- 方案没有引入自动发布，符合安全优先边界。
- CI 覆盖 gin 同款 OS/Go/race 强度，但排除了本项目不存在的 gin build tags。
- 文件命名按 Prismgo 语义适配，不牺牲可维护性。
