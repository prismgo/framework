# Repository Guidelines

## Karpathy Guidelines

Behavioral guidelines to reduce common LLM coding mistakes. Merge with project-specific instructions as needed.

**Tradeoff:** These guidelines bias toward caution over speed. For trivial tasks, use judgment.

## 1. Think Before Coding

**Don't assume. Don't hide confusion. Surface tradeoffs.**

Before implementing:
- State your assumptions explicitly. If uncertain, ask.
- If multiple interpretations exist, present them - don't pick silently.
- If a simpler approach exists, say so. Push back when warranted.
- If something is unclear, stop. Name what's confusing. Ask.

## 2. Simplicity First

**Minimum code that solves the problem. Nothing speculative.**

- No features beyond what was asked.
- No abstractions for single-use code.
- No "flexibility" or "configurability" that wasn't requested.
- No error handling for impossible scenarios.
- If you write 200 lines and it could be 50, rewrite it.

Ask yourself: "Would a senior engineer say this is overcomplicated?" If yes, simplify.

## 3. Surgical Changes

**Touch only what you must. Clean up only your own mess.**

When editing existing code:
- Don't "improve" adjacent code, comments, or formatting.
- Don't refactor things that aren't broken.
- Match existing style, even if you'd do it differently.
- If you notice unrelated dead code, mention it - don't delete it.

When your changes create orphans:
- Remove imports/variables/functions that YOUR changes made unused.
- Don't remove pre-existing dead code unless asked.

The test: Every changed line should trace directly to the user's request.

## 4. Goal-Driven Execution

**Define success criteria. Loop until verified.**

Transform tasks into verifiable goals:
- "Add validation" → "Write tests for invalid inputs, then make them pass"
- "Fix the bug" → "Write a test that reproduces it, then make it pass"
- "Refactor X" → "Ensure tests pass before and after"

For multi-step tasks, state a brief plan:
```
1. [Step] → verify: [check]
2. [Step] → verify: [check]
3. [Step] → verify: [check]
```

Strong success criteria let you loop independently. Weak criteria ("make it work") require constant clarification.

---

**These guidelines are working if:** fewer unnecessary changes in diffs, fewer rewrites due to overcomplication, and clarifying questions come before implementation rather than after mistakes.

## Project Structure & Module Organization

Framework packages live at the repository root, with one directory per component, such as `foundation`, `route`, `cache`, `queue`, `database`, `filesystem`, `session`, and `console`. Shared helpers under `internal/`, CLI commands under `cmd/`, and Horizon dashboard assets under `horizon/dashboard/resources/`. Tests are colocated as `*_test.go`.  automation lives in `.github/`.

### Contracts

Contracts are public interfaces that define the behavior of a component. They are used to communicate between components, and to validate user input.The `contracts` directory must not contain concrete implementations.

### Facades
Facades are public interfaces that wrap a component to provide a simpler, more convenient API.

- The `facade.go` in the component package must be implemented based on the `facade` package.
- APIs exposed by `facade` should preferentially return interface types from the `contracts` package.

### Service Providers

Service providers are responsible for registering components and their lifecycle hooks.Services provided by a component package must be registered via a `service provider`.


## Non-Negotiable Rules
1. Never modify production code solely for testing.
  - No test-only logic, APIs, helpers, fallbacks, or workarounds.
  - Production code may only change to fix genuine production bugs.
2. Never keep compatibility code after feature changes without approval.
3. Never ignore errors.
  - Return errors whenever possible.
  - Otherwise report them via `exception.Report(...)`.
4. Never change public APIs unless explicitly requested.

## Build, Test, and Development Commands

- `go test ./...`: run the full Go test suite.
- `make test`: run verbose tests with count coverage and write `coverage.out`.
- `make covdata`: run `./scripts/coverage.sh` with `PACKAGES` support and write coverage artifacts under `.coverage/`.
- `make test-race`: run all tests with the race detector.
- `make vet`: run `go vet ./...`.
- `make fmt`: format all Go files with `gofmt`, excluding `./tmp`.
- `make fmt-check`: verify formatting without modifying files.
- `make lint`: run `golangci-lint run`.
- `make ci`: run the local CI gate.

## Coding Style & Naming Conventions

Follow standard Go conventions: `gofmt` formatting, tabs for indentation, short package names, exported identifiers in `PascalCase`, and unexported identifiers in `camelCase`. Keep package APIs idiomatic and consistent with nearby components. Prefer package-level tests and helpers that mirror existing naming patterns, such as `facade_registry_test.go`, `service_provider_test.go`, or focused behavior names like `redis_lifecycle_test.go`.

## Testing Guidelines

Add or update colocated `*_test.go` files for behavior changes. Use focused unit tests for package-level contracts and integration-style tests where external behavior crosses components, such as queue, Redis, RabbitMQ, Horizon, or filesystem flows. Run `make test` before submitting; run `make test-race` for concurrency, worker, lifecycle, or connection-management changes. Coverage is uploaded from `coverage.out` in CI, so avoid bypassing `make test` for final verification.

### Testing and Coverage
- Any changes to Go code, go.mod, go.sum, test files, or code generation logic must run tests and compute coverage.
- Coverage must be collected via the project script, selecting the script based on the OS:
  - Linux/macOS/Git Bash: `make covdata`
  - For narrow-scope validation, pass `PACKAGES`, e.g., `make covdata PACKAGES=./cache`, or pass a package path to the script, e.g., `./scripts/coverage.sh ./cache`
- Coverage output is placed in `.coverage/`; Go build cache is fixed to `tmp/gocache` by the script to avoid writing to the user's global cache.
- Before final delivery, run the appropriate OS script based on the scope of changes, e.g., `make covdata PACKAGES=./cache`
- Required coverage for the changed scope is > `95%`. If not met, additional test code must be added.
- If full covdata is blocked by existing flaky tests (e.g., timer-sensitive tests), you must rerun the failing package(s) in isolation and explain in the results which tests failed and whether they are related to the current changes. You cannot treat a failed full coverage run as passing.

## Security & Configuration Tips

Do not commit secrets, local credentials, coverage files, or temporary runtime data. 

### 9. Checklist
After completing a feature, the following must be performed:

1. Check for orphaned (dead) code
   - Report any findings first, then confirm whether to delete.

2. Check for compatibility/fallback code
   - Report any findings first, then confirm whether to delete.

3. Run static analysis: `golangci-lint run --verbose`

4. Run formatting: `gofmt`

5. Output a summary document: `docs/changes/v{next}-{function-description}.md`, with the following requirements:
- Written in Chinese (the document content is in Chinese)
- `{next}` increments numerically
- Contains the following sections:
  - Feature overview and implementation goals
  - Requirements / business background
  - Impact scope
  - Which files were modified
  - What behavioral changes were made
  - Which checks were executed and a summary of the results
  - What logic is covered by unit tests (complex logic requires detailed explanation)
  - Risks and optimization suggestions
  - Orphaned/dead code
  - Compatibility/fallback code
  - Outstanding/incomplete items

6. Final response requirements

For every Go code change task, the final response must report:

- The actual coverage command executed.
- Whether the collected coverage is from unit tests, integration tests, or both.
- Total statement coverage.
- Packages or functions with significantly low coverage.
- If coverage is skipped or only partially run, the exact reason must be stated.
