# Comet Design Handoff

- Change: add-github-project-automation
- Phase: design
- Mode: compact
- Context hash: c3e622381d84e2c198be85dd24e11b772c86395ad2174be1f61e1f8adcdedda7

Generated-by: comet-handoff.sh

OpenSpec remains the canonical capability spec. This handoff is a deterministic, source-traceable context pack, not an agent-authored summary.

## openspec/changes/add-github-project-automation/proposal.md

- Source: openspec/changes/add-github-project-automation/proposal.md
- Lines: 1-39
- SHA256: 4be9c777a7016cd5834ef81613745165edd938f8235266036b0a1c53188ac67f

```md
## Why

This repository has no GitHub project automation even though it is a public Go framework with a broad package surface. Aligning with gin-gonic/gin's `.github` process gives contributors a clearer intake path and gives maintainers repeatable CI, dependency update, and security scan gates before changes land.

The target is not a byte-for-byte copy of gin. The selected direction is an adapted workflow: keep gin's governance and automation shape, but tune names, links, Go version behavior, coverage, and release defaults for `github.com/prismgo/framework`.

## What Changes

- Add GitHub issue templates for bug reports and feature requests modeled after gin, adapted to Prismgo terminology, documentation links, and security reporting guidance.
- Add a pull request template modeled after gin, including target branch, CI, tests, and documentation checklist items.
- Add Dependabot configuration for Go modules and GitHub Actions, matching gin's daily update cadence and grouped action updates.
- Add GitHub Actions CI equivalent in coverage to gin's workflow:
  - lint with `golangci-lint`;
  - test on Ubuntu and macOS;
  - test against the supported Go matrix for this repository;
  - include `-race` coverage comparable to gin's matrix;
  - upload coverage to Codecov while preserving the existing `codecov.yml`.
- Add CodeQL and Trivy security workflows modeled after gin:
  - CodeQL runs on push, pull request, and weekly schedule;
  - Trivy runs on push, pull request, daily schedule, and manual dispatch, uploading SARIF and failing on relevant findings.
- Design the GoReleaser path but do not enable tag-triggered automatic release by default in this change. Release automation must remain opt-in until `.goreleaser.yaml`, tag policy, and maintainer expectations are confirmed.

## Capabilities

### New Capabilities

- `github-project-automation`: GitHub repository governance, CI, dependency update, security scan, and release-readiness automation for this project.

### Modified Capabilities

- None.

## Impact

- Adds `.github/` project automation files.
- May add repository-local CI support files such as `Makefile` and `.golangci.yml` if needed to make the workflows deterministic.
- Does not change Go package APIs or runtime behavior.
- Must not overwrite the existing uncommitted `.gitignore` change or existing `codecov.yml`.
- Requires GitHub Actions availability and Codecov token/settings consistent with `codecov/codecov-action`.
```

## openspec/changes/add-github-project-automation/design.md

- Source: openspec/changes/add-github-project-automation/design.md
- Lines: 1-123
- SHA256: a55dbfa144c550301abaa053c42dc2b0e7cc3f020099da87ccc264b8ef653d6a

[TRUNCATED]

```md
## Context

gin's `.github` directory defines a compact open-source maintenance process:

- `ISSUE_TEMPLATE/bug-report.yaml`, `feature-request.yaml`, and `config.yml` govern issue intake.
- `PULL_REQUEST_TEMPLATE.md` gives contributors a short PR readiness checklist.
- `dependabot.yml` updates `gomod` and GitHub Actions daily, grouping action updates.
- `workflows/gin.yml` runs lint, then a CI matrix across Ubuntu/macOS, Go versions, test tags, race, and Codecov upload.
- `workflows/codeql.yml` runs Go CodeQL analysis on push, pull request, and weekly schedule.
- `workflows/trivy-scan.yml` runs filesystem vulnerability, secret, and misconfiguration scans and uploads SARIF.
- `workflows/goreleaser.yml` publishes on tag push and reindexes pkg.go.dev.

The selected project direction is:

- copy the process shape;
- adapt language and links to Prismgo;
- keep gin-equivalent CI breadth;
- keep CodeQL, Trivy, and Dependabot;
- design but do not auto-enable GoReleaser release.

## Decisions

### Branch Triggers

Use the repository's default branch once confirmed from git metadata. If the local branch cannot be confirmed, use `master` to match gin and because the user asked to align with gin. All workflow branch filters must use one branch consistently.

### CI Shape

Create a `Run Tests` workflow equivalent to gin's coverage:

1. `lint` job:
   - checkout with full history;
   - setup Go;
   - run `golangci/golangci-lint-action`.

2. `test` job:
   - depends on lint;
   - matrix over `ubuntu-latest` and `macos-latest`;
   - matrix over supported Go versions for this repository;
   - matrix over normal test mode and race mode;
   - cache Go build and module caches per OS;
   - run a project-owned command instead of embedding a long shell script in YAML;
   - upload coverage to Codecov.

This project does not currently define gin's build tags such as `nomsgpack`, `sonic`, or `go_json`, so the adapted matrix should not invent those tags unless a code search proves they exist. The gin-equivalent part for this project is OS x Go version x race coverage, not unrelated gin-specific tag variants.

### Project Commands

If no `Makefile` exists, add one with stable targets:

- `make test`: run `go test ./...` and write a coverage profile suitable for Codecov;
- `make test-race`: run `go test -race ./...`;
- `make lint`: run `golangci-lint run`;
- `make fmt-check`: verify `gofmt` output;
- `make ci`: run format check, vet, and test where appropriate.

The workflow should call these targets so local and CI behavior stay aligned.

### Security Automation

CodeQL should mirror gin's Go-only analysis and run on push, pull request, and weekly schedule.

Trivy should mirror gin's filesystem scan:

- scanners: `vuln,secret,misconfig`;
- SARIF upload to GitHub Security;
- table output for logs;
- fail on configured severities after SARIF upload.

Because this repository may contain local development logs under `storage/`, Trivy configuration should avoid turning known local runtime artifacts into noisy CI failures if those files are already intended to be untracked or ignored.

### Dependency Automation

Dependabot should match gin:

- `gomod` at `/`, daily;
- `github-actions` at `/`, daily;
- group all GitHub Action updates together.

Optional future grouping for Go dependencies can be added later if daily PR volume becomes noisy; it is not part of the initial gin alignment.
```

Full source: openspec/changes/add-github-project-automation/design.md

## openspec/changes/add-github-project-automation/tasks.md

- Source: openspec/changes/add-github-project-automation/tasks.md
- Lines: 1-32
- SHA256: 2ace1ad00bbaf98d5d8fd19a6ea1b81a8249d85156ae060bbe1d4ac0000571f0

```md
## 1. Governance Templates

- [ ] 1.1 Add `.github/ISSUE_TEMPLATE/bug-report.yaml` adapted from gin for Prismgo.
- [ ] 1.2 Add `.github/ISSUE_TEMPLATE/feature-request.yaml` adapted from gin for Prismgo.
- [ ] 1.3 Add `.github/ISSUE_TEMPLATE/config.yml` with blank issue policy and project contact links.
- [ ] 1.4 Add `.github/PULL_REQUEST_TEMPLATE.md` with default branch, CI, test, and docs checklist.

## 2. CI Support

- [ ] 2.1 Confirm the repository default branch and supported Go versions available in GitHub Actions.
- [ ] 2.2 Add project-owned local commands, such as `Makefile` targets, if missing.
- [ ] 2.3 Add lint configuration if required by `golangci-lint-action`.
- [ ] 2.4 Add `.github/workflows/gin.yml`-equivalent CI adapted for Prismgo with Ubuntu/macOS, selected Go versions, race testing, and Codecov upload.
- [ ] 2.5 Ensure the workflow preserves existing `codecov.yml` and does not overwrite unrelated user changes.

## 3. Dependency and Security Automation

- [ ] 3.1 Add `.github/dependabot.yml` for daily `gomod` and grouped GitHub Actions updates.
- [ ] 3.2 Add `.github/workflows/codeql.yml` for Go CodeQL on push, pull request, and weekly schedule.
- [ ] 3.3 Add `.github/workflows/trivy-scan.yml` for filesystem vulnerability, secret, and misconfiguration scans.
- [ ] 3.4 Tune Trivy scope or ignore behavior only for deliberate repository artifacts, not broad suppression.

## 4. Release Readiness

- [ ] 4.1 Document the gin GoReleaser behavior and why automatic tag publishing is deferred.
- [ ] 4.2 Leave tag-triggered release automation disabled unless a later maintainer decision enables it.

## 5. Verification

- [ ] 5.1 Run formatting or YAML validation for added files where available.
- [ ] 5.2 Run local Go test command used by CI, or document why it cannot run.
- [ ] 5.3 Review generated GitHub workflows for least-privilege permissions and default-branch consistency.
```

## openspec/changes/add-github-project-automation/specs/github-project-automation/spec.md

- Source: openspec/changes/add-github-project-automation/specs/github-project-automation/spec.md
- Lines: 1-93
- SHA256: b2253915ab46875031551640082c964438cd80adfe29100cbe907a1ba44e27ea

[TRUNCATED]

```md
## ADDED Requirements

### Requirement: Contributor Intake Templates

The repository SHALL provide GitHub issue templates for bug reports and feature requests adapted from gin's `.github/ISSUE_TEMPLATE` structure.

#### Scenario: Bug report created

- **WHEN** a contributor opens a bug report
- **THEN** the template SHALL request Prismgo version or commit, Go version, operating system, reproduction status, problem description, and minimal reproduction details.
- **AND** the template SHALL warn that security issues must not be disclosed publicly.

#### Scenario: Feature request created

- **WHEN** a contributor opens a feature request
- **THEN** the template SHALL request the proposed feature, use case, and relevant alternatives or API expectations.

#### Scenario: Blank issue attempted

- **WHEN** a contributor opens the issue chooser
- **THEN** blank issues SHOULD be disabled unless maintainers explicitly choose otherwise.
- **AND** contact links SHOULD route questions to documentation or discussions.

### Requirement: Pull Request Readiness Template

The repository SHALL provide a pull request template modeled after gin's concise checklist.

#### Scenario: Pull request opened

- **WHEN** a contributor opens a pull request
- **THEN** the template SHALL remind them to target the default branch, ensure CI passes, add or update tests, and update `README.md` or `docs/` for user-facing changes.

### Requirement: Dependency Update Automation

The repository SHALL configure Dependabot for Go modules and GitHub Actions.

#### Scenario: Go module dependency updates

- **WHEN** Dependabot runs for `gomod`
- **THEN** it SHALL inspect the repository root daily.

#### Scenario: GitHub Actions dependency updates

- **WHEN** Dependabot runs for `github-actions`
- **THEN** it SHALL inspect the repository root daily.
- **AND** action updates SHALL be grouped together.

### Requirement: Continuous Integration Workflow

The repository SHALL provide a GitHub Actions CI workflow equivalent in intent to gin's `Run Tests` workflow and adapted to this project.

#### Scenario: Pull request or push to default branch

- **WHEN** code is pushed to the default branch or a pull request targets it
- **THEN** CI SHALL run lint before tests.
- **AND** tests SHALL run on Ubuntu and macOS.
- **AND** tests SHALL cover the selected supported Go version matrix.
- **AND** CI SHALL include race testing.
- **AND** coverage SHALL be uploaded to Codecov without replacing the existing `codecov.yml`.

#### Scenario: Unsupported gin-specific test tags

- **WHEN** implementing the CI test matrix
- **THEN** gin-specific tags such as `nomsgpack`, `sonic`, and `go_json` SHALL NOT be added unless this repository contains matching build constraints or test paths.

### Requirement: Security Scanning Workflows

The repository SHALL provide CodeQL and Trivy workflows modeled after gin's security automation.

#### Scenario: CodeQL analysis

- **WHEN** code is pushed, a pull request targets the default branch, or the weekly schedule fires
- **THEN** CodeQL SHALL analyze Go code and upload security results.

#### Scenario: Trivy scan

- **WHEN** code is pushed, a pull request targets the default branch, the daily schedule fires, or the workflow is manually dispatched
- **THEN** Trivy SHALL scan the filesystem for vulnerabilities, secrets, and misconfiguration.
- **AND** Trivy SHALL upload SARIF results before failing the workflow on configured severities.

```

Full source: openspec/changes/add-github-project-automation/specs/github-project-automation/spec.md

