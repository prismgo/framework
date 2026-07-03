# Repository Guidelines

<karpathy-guidelines>

## Karpathy Guidelines

These behavioral rules reduce common LLM coding mistakes. They favor caution over speed; use judgment for trivial tasks.

### 1. Think Before Coding

Do not assume or hide uncertainty. Before implementing:
- State assumptions; ask when uncertain.
- Surface multiple interpretations instead of silently choosing.
- Mention simpler approaches and push back when appropriate.
- If unclear, stop, name the confusion, and ask.

### 2. Simplicity First

Write the minimum code that solves the request:
- No unrequested features, abstractions, flexibility, configurability, or impossible-scenario error handling.
- If a solution is much longer than necessary, simplify it.

### 3. Surgical Changes

Touch only what the request requires:
- Do not improve, refactor, reformat, or delete adjacent code unless needed.
- Match existing style.
- Remove unused imports, variables, or functions created by your change.
- Report unrelated dead code; do not delete it unless asked.

Every changed line must trace directly to the request.

### 4. Goal-Driven Execution

Turn tasks into verifiable goals and loop until verified:
- Validation change: test invalid inputs, then pass.
- Bug fix: reproduce with a test, then pass.
- Refactor: ensure tests pass before and after.

For multi-step work, state a brief plan:
```text
1. [Step] -> verify: [check]
2. [Step] -> verify: [check]
3. [Step] -> verify: [check]
```

These rules work when diffs shrink, rewrites decrease, and clarifying questions come before implementation mistakes.

</karpathy-guidelines>

<general-guidelines>

## General Agent Rules

### Response Rules

- Reply in the same language as the user's question.
- When clarifying requirements or boundaries, ask one question at a time and offer 3-5 concrete options with enough detail to choose, including example code when useful.
- Keep responses concise and avoid filler.

### Hard Rules

1. Never modify production code solely for tests.
   - No test-only logic, APIs, helpers, fallbacks, or workarounds.
   - Production code may change only to fix genuine production bugs.
2. Never keep compatibility code after feature changes without approval.
3. Never ignore errors.
   - Return errors whenever possible.
   - Otherwise report them with `exception.Report(...)`; if no `Report` API exists, log the error.
4. Never change public APIs unless explicitly requested.

### Security

Do not commit secrets, local credentials, coverage files, or temporary runtime data.

### Completion Checklist

After completing a feature:

1. Check for orphaned or dead code. Report findings first and confirm before deleting.
2. Check for compatibility or fallback code. Report findings first and confirm before deleting.
3. Run static analysis only for changed packages, for example `golangci-lint run --verbose ./cache/...`.
4. Run `gofmt`.
5. Create `docs/changes/devlogs/v{next}-{function-description}.md` unless `docs/changes` is ignored. Match the document language to the agent response language, increment `{next}` numerically, and include:
   - Feature overview and implementation goals.
   - Requirements / business background.
   - Impact scope.
   - Modified files.
   - Behavioral changes.
   - Checks executed and result summary.
   - Unit-test coverage details, with complex logic explained.
   - Risks and optimization suggestions.
   - Orphaned/dead code.
   - Compatibility/fallback code.
   - Outstanding/incomplete items.
6. For every Go code change, the final response must report:
   - Actual coverage command executed.
   - Whether coverage came from unit tests, integration tests, or both.
   - Total statement coverage.
   - Packages or functions with significantly low coverage.
   - If coverage was skipped or partial, the exact reason.

</general-guidelines>

<go-guidelines>

## Go Project Rules

### Coding Style

Follow standard Go conventions: `gofmt`, tabs, short package names, exported `PascalCase`, unexported `camelCase`, idiomatic APIs, and naming consistent with nearby components. Prefer package-level tests and local naming patterns such as `facade_registry_test.go`, `service_provider_test.go`, and focused behavior names like `redis_lifecycle_test.go`.

#### Principles

- Prefer reuse over reimplementation; refactor only when boundaries are unclear or existing code does not fit.
- Keep functions, structs, interfaces, files, and packages single-purpose with clear boundaries.
- Prefer the Go standard library; adding a third-party dependency requires user approval.
- Favor explicit logic over implicit behavior.
- Use clear, consistent names for classes, functions, variables, tables, and fields.

#### Comments

Modified and newly added code, including tests, must include useful comments that explain logic, design rationale, complex function internals, and parameter purposes.

### Testing

Add or update colocated `*_test.go` files for behavior changes. Use focused unit tests for package behavior and integration-style tests when behavior spans components.

### Test Failure Boundary

When fixing tests, first decide whether the failure is bad test setup or a real production bug.

Do not change production code to tolerate incomplete tests. Forbidden without explicit approval:
- Adding fallback config or service behavior.
- Making required services optional.
- Replacing required resolution with silent defaults.
- Adding nil checks only to avoid test panics.
- Adding test-only branches, helpers, or alternate runtime paths.
- Weakening validation, lifecycle, or provider requirements.

Production code may change only when a failure proves a real production bug under valid runtime setup. If unsure, stop and ask. A panic from missing required test setup is a test bug, not a production bug.

</go-guidelines>

<project-guidelines>

## PrismGo Framework — Project Rules

### Overview

**PrismGo** (`github.com/prismgo/framework`) is a Laravel-style Go web framework (Go 1.25). Stack: [Gin](https://github.com/gin-gonic/gin) (HTTP), [GORM](https://github.com/go-gorm/gorm) (ORM), [Cobra](https://github.com/spf13/cobra) (CLI), [Viper](https://github.com/spf13/viper) (config), [Logrus](https://github.com/sirupsen/logrus) (logging), [go-redis](https://github.com/redis/go-redis/v9) (Redis).

### Component Packages

Each component lives in a root-level package. Every component typically has a `service_provider.go` (registers into container), `facade.go` (public convenience API), `manager.go` / core logic, and a `config.go` / `types.go` for configuration.

| Package | Role |
|---|---|
| `foundation` | Application bootstrap, provider lifecycle, cleanup |
| `container` | DI container (Singleton, Instance, Bind, Make) |
| `config` | Dot-path config access via `config.GetString("app.name")`, Viper-backed |
| `kernel` | Cobra-based CLI kernel, command registration & dispatch |
| `route` | Gin-based routing: named routes, resource routes, groups |
| `http` | HTTP server lifecycle, middleware, request ID, process management |
| `cache` | Multi-driver cache (memory, Redis, file) with Remember/WithoutOverlapping/Funnel/Lock |
| `queue` | Job dispatcher + worker, Redis / RabbitMQ / sync connectors |
| `event` | Event dispatcher with listener/subscriber/queue support |
| `console` | Artisan-style CLI command base & IO |
| `database` | ORM integration, schema migrations, factories, seeders |
| `filesystem` | Multi-disk filesystem abstraction (local, OSS, S3) |
| `session` | Session driver, lifecycle, lock management |
| `encryption` | Encrypter interface (AES, etc.) |
| `encoding` | Codec interface (JSON, etc.) |
| `cookie` | Cookie signing and management |
| `logger` | Multi-channel logging, Logrus-backed |
| `translation` | i18n: loader, translator, selector |
| `redis` | Redis manager, connection pool |
| `ratelimit` | Rate limiter |
| `routine` | Goroutine task builder/runner |
| `support` | Helpers: env, collection, etc. |
| `storage` | Storage link management |
| `process` | Process supervision & signals |
| `responsekit` | Response builder helpers |
| `exception` | Exception handler interface |
| `timer` | Cron / scheduled task management |
| `provider` | ServiceProvider contract + vendor publish support |
| `horizon` | Horizon dashboard (queue monitoring) |
| `contracts/` | Public component interfaces (no implementations) |
| `facade/` | Generic facade: `facade.Resolve[T](key string) T` |
| `cmd/` | CLI entry points: `serve`, `make`, `migration`, `queue`, `cron`, `storage:link`, `vendor:publish` |
| `internal/` | Shared helpers: `fmtx/`, `optional/`, `path/`, `stackx/` |

### Three-Layer Pattern

Every capability follows three layers:
1. **`contracts/<component>/`** — interfaces only, no state, no constructors.
2. **`<component>/`** — concrete implementation + `service_provider.go` + `facade.go`.
3. **`facade/`** — generic typed resolver `Resolve[T]` backed by `container.Make`.

Components are registered via **ServiceProvider** (implements `Register(app)`, `Boot(ap)`) and wired through the `foundation.Application` lifecycle.

### Commands

| Command | What it does |
|---|---|
| `go test ./...` | Run full test suite |
| `make test` | Verbose tests with `-covermode=count -coverprofile=coverage.out` |
| `make covdata` | Full coverage via `.github/scripts/coverage.sh` (uses `tmp/gocache`, writes `.coverage/`) |
| `make covdata PACKAGES=./cache` | Narrow-scope coverage |
| `make test-race` | Race detector via `.github/scripts/test_with_summary.sh` |
| `make vet` | `go vet ./...` |
| `make fmt` | `gofmt` excluding `./tmp` |
| `make fmt-check` | Format diff check (CI gate) |
| `make lint` | `golangci-lint run` (linters: govet, ineffassign, staticcheck, unused) |
| `make ci` | CI gate: `fmt-check` → `vet` → `test` |

### Coding Conventions

- **Comments**: Chinese preferred for explanatory comments; English for doc comments on exported symbols. Every non-trivial function/struct must have a doc comment.
- **Tests**: Package-level `*_test.go` files. Test helpers always call `t.Helper()`. Use `t.Cleanup()` for resource teardown. Common patterns: `newTestManager()`, `useTestContainer()`, `bindXxxForTest()`.
- **ServiceProvider**: Each component has a `ServiceProvider` struct with `Name() string`, `Register(providerApplication) error`, `Boot(providerApplication) error`.
- **Facade**: Component `facade.go` files use `facade.Resolve[T](key)` from the `facade` package; returns typed instance or panics on resolution failure.
- **Container**: Use `container.Container` methods `Singleton`, `Instance`, `Bound`, `Make` for DI. Container key convention: `"<component>.default"`.
- **Error handling**: Return errors to callers. Use panic only for unrecoverable misconfiguration (facade resolution, bootstrap).
- **Configuration**: Via `config` package: `config.Get[T]("path")`, `config.GetString("app.name")`. Backed by Viper + `.env`.
- **Code Location**: Before modifying any Go code, agents MUST first read `CODE_INDEX.md` to locate the relevant files, symbols, and package structure for the code being worked on.
- **Code Index Maintenance**: When adding new packages, files, or exported symbols, update `CODE_INDEX.md` accordingly. Follow the three-layer index pattern (meta → Quick Lookup → per-package details → conventions) to preserve navigation efficiency. Avoid "etc." in table descriptions—list key symbols or reference the source file.

### Testing & Coverage

- Unit tests for package behavior; integration-style tests for cross-component flows (queue+Redis, Horizon, filesystem).
- Every Go code change must run tests and report coverage.
- Required coverage for changed scope: ≥90%.
- Coverage command: `make covdata` (full) or `make covdata PACKAGES=./<pkg>` (narrow).
  - Output: `.coverage/coverage.out`, `.coverage/coverage.percent.txt`, `.coverage/coverage.func.txt`.
  - Uses `tmp/gocache` to isolate cache.
- On flaky failures: rerun failing packages in isolation and explain.
- Before final submission run `gofmt`, run `golangci-lint run --verbose ./<changed>/...`.
- Create `docs/changes/devlogs/v{next}-{function-description}.md` after each feature change.

</project-guidelines>
