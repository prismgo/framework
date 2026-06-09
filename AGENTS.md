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

This repository is the Go module `github.com/prismgo/framework`. Framework packages live at the repository root, with one directory per component, such as `foundation`, `route`, `cache`, `queue`, `horizon`, `database`, `filesystem`, `session`, and `console`. Public contracts are under `contracts/`, shared helpers under `internal/`, CLI commands under `cmd/`, and Horizon dashboard assets under `horizon/dashboard/resources/`. Tests are colocated as `*_test.go`. Documentation starts in `README.md` and `docs/README_en.md`; automation lives in `.github/`.

## Build, Test, and Development Commands

- `go test ./...`: run the full Go test suite.
- `make test`: run verbose tests with count coverage and write `coverage.out`.
- `make test-race`: run all tests with the race detector.
- `make vet`: run `go vet ./...`.
- `make fmt`: format all Go files with `gofmt`, excluding `./tmp`.
- `make fmt-check`: verify formatting without modifying files.
- `make lint`: run `golangci-lint run`.
- `make ci`: run the local CI gate.

Use Go 1.26.x, matching `go.mod` and GitHub Actions.

## Coding Style & Naming Conventions

Follow standard Go conventions: `gofmt` formatting, tabs for indentation, short package names, exported identifiers in `PascalCase`, and unexported identifiers in `camelCase`. Keep package APIs idiomatic and consistent with nearby components. Prefer package-level tests and helpers that mirror existing naming patterns, such as `facade_registry_test.go`, `service_provider_test.go`, or focused behavior names like `redis_lifecycle_test.go`.

## Testing Guidelines

Add or update colocated `*_test.go` files for behavior changes. Use focused unit tests for package-level contracts and integration-style tests where external behavior crosses components, such as queue, Redis, RabbitMQ, Horizon, or filesystem flows. Run `make test` before submitting; run `make test-race` for concurrency, worker, lifecycle, or connection-management changes. Coverage is uploaded from `coverage.out` in CI, so avoid bypassing `make test` for final verification.

## Commit & Pull Request Guidelines

Recent history uses short, imperative commit subjects such as `fix golangci-lint`, plus merge commits from Dependabot. Keep commits focused and describe the changed behavior. Open pull requests against `main`, ensure CI passes, add or update tests as needed, document user-facing changes in `README.md` or `docs/`, and call out intentional public API changes in the PR description.

## Security & Configuration Tips

Do not commit secrets, local credentials, coverage files, or temporary runtime data. Treat Redis, RabbitMQ, OSS, database, and Horizon credentials as environment-specific. Verify dependency changes with `go mod tidy`, `make lint`, and `make ci`.
