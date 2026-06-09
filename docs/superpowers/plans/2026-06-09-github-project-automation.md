---
change: add-github-project-automation
design-doc: docs/superpowers/specs/2026-06-09-github-project-automation-design.md
base-ref: 5226a82e9fe736048f0ac58522287de30baa1335
---

# GitHub Project Automation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a gin-inspired, Prismgo-adapted GitHub governance, CI, dependency update, and security automation flow without enabling automatic releases.

**Architecture:** Repository automation lives under `.github/`, while local reproducibility lives in root-level `Makefile` and `.golangci.yml`. GitHub Actions call Make targets instead of embedding long shell scripts, so local and CI behavior stay aligned.

**Tech Stack:** GitHub Actions, GitHub Issue Forms, Dependabot, CodeQL, Trivy, Codecov, Go tooling, GNU Make, golangci-lint.

---

## File Structure

- Create: `.github/ISSUE_TEMPLATE/bug-report.yaml`
  - Responsibility: collect actionable Prismgo bug reports and route security issues to GitHub Security Advisory.
- Create: `.github/ISSUE_TEMPLATE/feature-request.yaml`
  - Responsibility: collect feature proposals with use case, desired behavior, and alternatives.
- Create: `.github/ISSUE_TEMPLATE/config.yml`
  - Responsibility: disable blank issues and provide documentation contact links.
- Create: `.github/PULL_REQUEST_TEMPLATE.md`
  - Responsibility: provide a concise PR readiness checklist.
- Create: `.github/dependabot.yml`
  - Responsibility: update Go modules and GitHub Actions daily.
- Create: `.github/workflows/ci.yml`
  - Responsibility: run lint, test matrix, race tests, caching, and Codecov upload.
- Create: `.github/workflows/codeql.yml`
  - Responsibility: run Go CodeQL analysis.
- Create: `.github/workflows/trivy-scan.yml`
  - Responsibility: run Trivy filesystem scans and upload SARIF.
- Create: `Makefile`
  - Responsibility: provide local commands used by CI.
- Create: `.golangci.yml`
  - Responsibility: pin a conservative lint configuration for repeatable CI.
- Modify: `openspec/changes/add-github-project-automation/tasks.md`
  - Responsibility: mark completed OpenSpec tasks as implementation progresses.

## Task 1: Contributor Templates

**Files:**
- Create: `.github/ISSUE_TEMPLATE/bug-report.yaml`
- Create: `.github/ISSUE_TEMPLATE/feature-request.yaml`
- Create: `.github/ISSUE_TEMPLATE/config.yml`
- Create: `.github/PULL_REQUEST_TEMPLATE.md`
- Modify: `openspec/changes/add-github-project-automation/tasks.md`

- [ ] **Step 1: Create issue template directories**

Run:

```bash
mkdir -p .github/ISSUE_TEMPLATE
```

Expected: command exits with status 0.

- [ ] **Step 2: Create the bug report template**

Create `.github/ISSUE_TEMPLATE/bug-report.yaml` with:

```yaml
name: Bug Report
description: Found unexpected behavior in Prismgo? Report it here.
labels: ["type/bug"]
body:
  - type: markdown
    attributes:
      value: |
        If this is a security issue, do not open a public issue.
        Please report it privately through GitHub Security Advisory.
  - type: markdown
    attributes:
      value: |
        Before opening a bug report, please check the existing issues and make sure you are using a current Prismgo version or commit.
  - type: textarea
    id: description
    attributes:
      label: Description
      description: Describe the problem and include links or context that help explain it.
    validations:
      required: true
  - type: input
    id: prismgo-version
    attributes:
      label: Prismgo Version
      description: Prismgo version or commit reference.
      placeholder: v0.0.0 or 5226a82
    validations:
      required: true
  - type: dropdown
    id: can-reproduce
    attributes:
      label: Can you reproduce the bug?
      description: If yes, include the steps or code in the minimal reproduction field.
      options:
        - "Yes"
        - "No"
    validations:
      required: true
  - type: textarea
    id: reproduction
    attributes:
      label: Minimal Reproduction
      description: Provide the smallest code sample, command, or configuration that reproduces the issue.
      render: go
  - type: textarea
    id: expected-behavior
    attributes:
      label: Expected Behavior
      description: Describe what you expected to happen.
  - type: textarea
    id: actual-behavior
    attributes:
      label: Actual Behavior
      description: Describe what actually happened.
  - type: textarea
    id: logs
    attributes:
      label: Logs or Error Output
      description: Paste relevant logs, panic output, or command output.
      render: shell
  - type: input
    id: go-version
    attributes:
      label: Go Version
      description: Output of `go version`.
      placeholder: go version go1.26.2 linux/amd64
  - type: input
    id: os-version
    attributes:
      label: Operating System
      description: OS and architecture used to run Prismgo.
      placeholder: Ubuntu 24.04 amd64
```

- [ ] **Step 3: Create the feature request template**

Create `.github/ISSUE_TEMPLATE/feature-request.yaml` with:

```yaml
name: Feature Request
description: Suggest an improvement for Prismgo.
labels: ["type/proposal"]
body:
  - type: markdown
    attributes:
      value: |
        Please check existing issues before opening a new feature request.
  - type: textarea
    id: description
    attributes:
      label: Feature Description
      description: Describe the feature or behavior you want Prismgo to support.
      placeholder: I think Prismgo should support...
    validations:
      required: true
  - type: textarea
    id: use-case
    attributes:
      label: Use Case
      description: Explain the problem this feature solves and who needs it.
    validations:
      required: true
  - type: textarea
    id: proposed-api
    attributes:
      label: Proposed API or Behavior
      description: Describe the API, command, configuration, or behavior you expect.
      render: go
  - type: textarea
    id: alternatives
    attributes:
      label: Alternatives Considered
      description: Describe workarounds or alternative designs you considered.
```

- [ ] **Step 4: Create issue template config**

Create `.github/ISSUE_TEMPLATE/config.yml` with:

```yaml
blank_issues_enabled: false
contact_links:
  - name: Go.dev API Documentation
    url: https://pkg.go.dev/github.com/prismgo/framework
    about: API documentation for Prismgo packages.
  - name: Prismgo Documentation
    url: https://github.com/prismgo/framework/tree/main/docs
    about: Project documentation and guides.
```

- [ ] **Step 5: Create the pull request template**

Create `.github/PULL_REQUEST_TEMPLATE.md` with:

```markdown
# Pull Request Checklist

Please ensure your pull request meets the following requirements:

- [ ] Open your pull request against the `main` branch.
- [ ] All tests pass in available continuous integration systems.
- [ ] Tests are added or modified as needed to cover code changes.
- [ ] User-facing changes are documented in `README.md` or `docs/`.
- [ ] Public API changes are intentional and described in this pull request.

Thank you for contributing!
```

- [ ] **Step 6: Verify template files exist**

Run:

```bash
test -s .github/ISSUE_TEMPLATE/bug-report.yaml
test -s .github/ISSUE_TEMPLATE/feature-request.yaml
test -s .github/ISSUE_TEMPLATE/config.yml
test -s .github/PULL_REQUEST_TEMPLATE.md
```

Expected: all commands exit with status 0.

- [ ] **Step 7: Mark governance template tasks complete**

Modify `openspec/changes/add-github-project-automation/tasks.md` by changing these lines:

```markdown
- [ ] 1.1 Add `.github/ISSUE_TEMPLATE/bug-report.yaml` adapted from gin for Prismgo.
- [ ] 1.2 Add `.github/ISSUE_TEMPLATE/feature-request.yaml` adapted from gin for Prismgo.
- [ ] 1.3 Add `.github/ISSUE_TEMPLATE/config.yml` with blank issue policy and project contact links.
- [ ] 1.4 Add `.github/PULL_REQUEST_TEMPLATE.md` with default branch, CI, test, and docs checklist.
```

to:

```markdown
- [x] 1.1 Add `.github/ISSUE_TEMPLATE/bug-report.yaml` adapted from gin for Prismgo.
- [x] 1.2 Add `.github/ISSUE_TEMPLATE/feature-request.yaml` adapted from gin for Prismgo.
- [x] 1.3 Add `.github/ISSUE_TEMPLATE/config.yml` with blank issue policy and project contact links.
- [x] 1.4 Add `.github/PULL_REQUEST_TEMPLATE.md` with default branch, CI, test, and docs checklist.
```

- [ ] **Step 8: Commit contributor templates**

Run:

```bash
git add .github/ISSUE_TEMPLATE .github/PULL_REQUEST_TEMPLATE.md openspec/changes/add-github-project-automation/tasks.md
git commit -m "chore: add GitHub contributor templates"
```

Expected: commit succeeds.

## Task 2: Local CI Commands

**Files:**
- Create: `Makefile`
- Create: `.golangci.yml`
- Modify: `openspec/changes/add-github-project-automation/tasks.md`

- [ ] **Step 1: Create Makefile**

Create `Makefile` with:

```makefile
GO ?= go
GOFMT ?= gofmt
PACKAGES ?= ./...
COVERAGE_OUT ?= coverage.out

.PHONY: test
test:
	$(GO) test -v -covermode=count -coverprofile=$(COVERAGE_OUT) $(PACKAGES)

.PHONY: test-race
test-race:
	$(GO) test -v -race $(PACKAGES)

.PHONY: vet
vet:
	$(GO) vet $(PACKAGES)

.PHONY: fmt
fmt:
	$(GOFMT) -w $$(find . -name '*.go' -not -path './tmp/*')

.PHONY: fmt-check
fmt-check:
	@diff="$$( $(GOFMT) -d $$(find . -name '*.go' -not -path './tmp/*') )"; \
	if [ -n "$$diff" ]; then \
		echo "Please run 'make fmt' and commit the result:"; \
		echo "$$diff"; \
		exit 1; \
	fi

.PHONY: lint
lint:
	golangci-lint run

.PHONY: ci
ci: fmt-check vet test
```

- [ ] **Step 2: Create golangci-lint config**

Create `.golangci.yml` with:

```yaml
version: "2"

run:
  timeout: 5m
  tests: true

linters:
  enable:
    - govet
    - ineffassign
    - staticcheck
    - unused

issues:
  max-issues-per-linter: 0
  max-same-issues: 0
```

- [ ] **Step 3: Verify Make targets are discoverable**

Run:

```bash
make -n test
make -n test-race
make -n vet
make -n fmt-check
make -n lint
```

Expected: each command prints the command it would run and exits with status 0.

- [ ] **Step 4: Mark local CI support tasks complete**

Modify `openspec/changes/add-github-project-automation/tasks.md` by changing:

```markdown
- [ ] 2.1 Confirm the repository default branch and supported Go versions available in GitHub Actions.
- [ ] 2.2 Add project-owned local commands, such as `Makefile` targets, if missing.
- [ ] 2.3 Add lint configuration if required by `golangci-lint-action`.
```

to:

```markdown
- [x] 2.1 Confirm the repository default branch and supported Go versions available in GitHub Actions.
- [x] 2.2 Add project-owned local commands, such as `Makefile` targets, if missing.
- [x] 2.3 Add lint configuration if required by `golangci-lint-action`.
```

- [ ] **Step 5: Commit local CI commands**

Run:

```bash
git add Makefile .golangci.yml openspec/changes/add-github-project-automation/tasks.md
git commit -m "chore: add local CI commands"
```

Expected: commit succeeds.

## Task 3: CI Workflow

**Files:**
- Create: `.github/workflows/ci.yml`
- Modify: `openspec/changes/add-github-project-automation/tasks.md`

- [ ] **Step 1: Create workflows directory**

Run:

```bash
mkdir -p .github/workflows
```

Expected: command exits with status 0.

- [ ] **Step 2: Create CI workflow**

Create `.github/workflows/ci.yml` with:

```yaml
name: Run Tests

on:
  push:
    branches:
      - main
  pull_request:
    branches:
      - main

permissions:
  contents: read

jobs:
  lint:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout
        uses: actions/checkout@v6
        with:
          fetch-depth: 0

      - name: Set up Go
        uses: actions/setup-go@v6
        with:
          go-version: "1.26"

      - name: Run golangci-lint
        uses: golangci/golangci-lint-action@v9
        with:
          version: v2.11
          args: --verbose

  test:
    needs: lint
    strategy:
      fail-fast: false
      matrix:
        os: [ubuntu-latest, macos-latest]
        go: ["1.25", "1.26"]
        test-mode: ["normal", "race"]
        include:
          - os: ubuntu-latest
            go-build: ~/.cache/go-build
          - os: macos-latest
            go-build: ~/Library/Caches/go-build
    name: ${{ matrix.os }} @ Go ${{ matrix.go }} ${{ matrix.test-mode }}
    runs-on: ${{ matrix.os }}
    env:
      GO111MODULE: on
      GOPROXY: https://proxy.golang.org
    steps:
      - name: Set up Go ${{ matrix.go }}
        uses: actions/setup-go@v6
        with:
          go-version: ${{ matrix.go }}
          cache: false

      - name: Checkout
        uses: actions/checkout@v6
        with:
          ref: ${{ github.ref }}

      - name: Cache Go build and modules
        uses: actions/cache@v5
        with:
          path: |
            ${{ matrix.go-build }}
            ~/go/pkg/mod
          key: ${{ runner.os }}-go-${{ matrix.go }}-${{ hashFiles('**/go.sum') }}
          restore-keys: |
            ${{ runner.os }}-go-${{ matrix.go }}-
            ${{ runner.os }}-go-

      - name: Run Tests
        run: |
          if [ "${{ matrix.test-mode }}" = "race" ]; then
            make test-race
          else
            make test
          fi

      - name: Upload coverage to Codecov
        if: matrix.test-mode == 'normal'
        uses: codecov/codecov-action@v6
        with:
          files: coverage.out
          flags: ${{ matrix.os }},go-${{ matrix.go }},${{ matrix.test-mode }}
```

- [ ] **Step 3: Verify CI workflow branch filters**

Run:

```bash
rg -n "master" .github/workflows/ci.yml
```

Expected: no matches and exit status 1.

- [ ] **Step 4: Verify CI workflow has no gin-specific tags**

Run:

```bash
rg -n "nomsgpack|sonic|go_json" .github/workflows/ci.yml
```

Expected: no matches and exit status 1.

- [ ] **Step 5: Mark CI workflow tasks complete**

Modify `openspec/changes/add-github-project-automation/tasks.md` by changing:

```markdown
- [ ] 2.4 Add `.github/workflows/gin.yml`-equivalent CI adapted for Prismgo with Ubuntu/macOS, selected Go versions, race testing, and Codecov upload.
- [ ] 2.5 Ensure the workflow preserves existing `codecov.yml` and does not overwrite unrelated user changes.
```

to:

```markdown
- [x] 2.4 Add `.github/workflows/gin.yml`-equivalent CI adapted for Prismgo with Ubuntu/macOS, selected Go versions, race testing, and Codecov upload.
- [x] 2.5 Ensure the workflow preserves existing `codecov.yml` and does not overwrite unrelated user changes.
```

- [ ] **Step 6: Commit CI workflow**

Run:

```bash
git add .github/workflows/ci.yml openspec/changes/add-github-project-automation/tasks.md
git commit -m "ci: add GitHub Actions test workflow"
```

Expected: commit succeeds.

## Task 4: Dependency and Security Workflows

**Files:**
- Create: `.github/dependabot.yml`
- Create: `.github/workflows/codeql.yml`
- Create: `.github/workflows/trivy-scan.yml`
- Modify: `openspec/changes/add-github-project-automation/tasks.md`

- [ ] **Step 1: Create Dependabot config**

Create `.github/dependabot.yml` with:

```yaml
version: 2
updates:
  - package-ecosystem: gomod
    directory: /
    schedule:
      interval: daily
  - package-ecosystem: github-actions
    directory: /
    groups:
      actions:
        patterns:
          - "*"
    schedule:
      interval: daily
```

- [ ] **Step 2: Create CodeQL workflow**

Create `.github/workflows/codeql.yml` with:

```yaml
name: CodeQL

on:
  push:
    branches: [main]
  pull_request:
    branches: [main]
  schedule:
    - cron: "0 17 * * 5"

jobs:
  analyze:
    name: Analyze
    runs-on: ubuntu-latest

    permissions:
      contents: read
      security-events: write

    strategy:
      fail-fast: false
      matrix:
        language: ["go"]

    steps:
      - name: Checkout repository
        uses: actions/checkout@v6

      - name: Initialize CodeQL
        uses: github/codeql-action/init@v4
        with:
          languages: ${{ matrix.language }}

      - name: Perform CodeQL Analysis
        uses: github/codeql-action/analyze@v4
```

- [ ] **Step 3: Create Trivy workflow**

Create `.github/workflows/trivy-scan.yml` with:

```yaml
name: Trivy Security Scan

on:
  push:
    branches:
      - main
  pull_request:
    branches:
      - main
  schedule:
    - cron: "0 0 * * *"
  workflow_dispatch:

permissions:
  contents: read
  security-events: write

jobs:
  trivy-scan:
    name: Trivy Security Scan
    runs-on: ubuntu-latest
    steps:
      - name: Checkout code
        uses: actions/checkout@v6
        with:
          fetch-depth: 0

      - name: Run Trivy vulnerability scanner
        uses: aquasecurity/trivy-action@v0.36.0
        with:
          scan-type: "fs"
          scan-ref: "."
          scanners: "vuln,secret,misconfig"
          format: "sarif"
          output: "trivy-results.sarif"
          severity: "CRITICAL,HIGH,MEDIUM"
          ignore-unfixed: true

      - name: Upload Trivy results to GitHub Security
        uses: github/codeql-action/upload-sarif@v4
        if: always()
        with:
          sarif_file: "trivy-results.sarif"

      - name: Run Trivy scanner table output
        uses: aquasecurity/trivy-action@v0.36.0
        if: always()
        with:
          scan-type: "fs"
          scan-ref: "."
          scanners: "vuln,secret,misconfig"
          format: "table"
          severity: "CRITICAL,HIGH,MEDIUM"
          ignore-unfixed: true
          exit-code: "1"
```

- [ ] **Step 4: Verify no release workflow was added**

Run:

```bash
find .github/workflows -maxdepth 1 -type f -name '*release*' -o -name '*goreleaser*'
```

Expected: no output.

- [ ] **Step 5: Mark dependency and security tasks complete**

Modify `openspec/changes/add-github-project-automation/tasks.md` by changing:

```markdown
- [ ] 3.1 Add `.github/dependabot.yml` for daily `gomod` and grouped GitHub Actions updates.
- [ ] 3.2 Add `.github/workflows/codeql.yml` for Go CodeQL on push, pull request, and weekly schedule.
- [ ] 3.3 Add `.github/workflows/trivy-scan.yml` for filesystem vulnerability, secret, and misconfiguration scans.
- [ ] 3.4 Tune Trivy scope or ignore behavior only for deliberate repository artifacts, not broad suppression.
- [ ] 4.1 Document the gin GoReleaser behavior and why automatic tag publishing is deferred.
- [ ] 4.2 Leave tag-triggered release automation disabled unless a later maintainer decision enables it.
```

to:

```markdown
- [x] 3.1 Add `.github/dependabot.yml` for daily `gomod` and grouped GitHub Actions updates.
- [x] 3.2 Add `.github/workflows/codeql.yml` for Go CodeQL on push, pull request, and weekly schedule.
- [x] 3.3 Add `.github/workflows/trivy-scan.yml` for filesystem vulnerability, secret, and misconfiguration scans.
- [x] 3.4 Tune Trivy scope or ignore behavior only for deliberate repository artifacts, not broad suppression.
- [x] 4.1 Document the gin GoReleaser behavior and why automatic tag publishing is deferred.
- [x] 4.2 Leave tag-triggered release automation disabled unless a later maintainer decision enables it.
```

- [ ] **Step 6: Commit dependency and security workflows**

Run:

```bash
git add .github/dependabot.yml .github/workflows/codeql.yml .github/workflows/trivy-scan.yml openspec/changes/add-github-project-automation/tasks.md
git commit -m "ci: add dependency and security automation"
```

Expected: commit succeeds.

## Task 5: Verification

**Files:**
- Modify: `openspec/changes/add-github-project-automation/tasks.md`

- [ ] **Step 1: Verify YAML parses structurally**

Run:

```bash
go run github.com/goccy/go-yaml/cmd/ycat@latest .github/ISSUE_TEMPLATE/bug-report.yaml >/dev/null
go run github.com/goccy/go-yaml/cmd/ycat@latest .github/ISSUE_TEMPLATE/feature-request.yaml >/dev/null
go run github.com/goccy/go-yaml/cmd/ycat@latest .github/ISSUE_TEMPLATE/config.yml >/dev/null
go run github.com/goccy/go-yaml/cmd/ycat@latest .github/dependabot.yml >/dev/null
go run github.com/goccy/go-yaml/cmd/ycat@latest .github/workflows/ci.yml >/dev/null
go run github.com/goccy/go-yaml/cmd/ycat@latest .github/workflows/codeql.yml >/dev/null
go run github.com/goccy/go-yaml/cmd/ycat@latest .github/workflows/trivy-scan.yml >/dev/null
```

Expected: all commands exit with status 0. If `go run` cannot download the command because network is unavailable, record that verification gap in the final response and continue with local text checks.

- [ ] **Step 2: Run local format check**

Run:

```bash
make fmt-check
```

Expected: exits with status 0, or prints gofmt diff that must be fixed before continuing.

- [ ] **Step 3: Run local tests**

Run:

```bash
make test
```

Expected: exits with status 0 and writes `coverage.out`.

- [ ] **Step 4: Verify branch consistency**

Run:

```bash
rg -n "master" .github
```

Expected: no matches and exit status 1.

- [ ] **Step 5: Verify no automatic release workflow exists**

Run:

```bash
find .github/workflows -maxdepth 1 -type f -print
rg -n "goreleaser|release --clean|tags:" .github/workflows
```

Expected: first command lists `ci.yml`, `codeql.yml`, and `trivy-scan.yml`; second command has no matches and exit status 1.

- [ ] **Step 6: Verify existing Codecov config was not changed by this implementation**

Run:

```bash
git diff -- codecov.yml
```

Expected: no diff caused by this implementation.

- [ ] **Step 7: Mark verification tasks complete**

Modify `openspec/changes/add-github-project-automation/tasks.md` by changing:

```markdown
- [ ] 5.1 Run formatting or YAML validation for added files where available.
- [ ] 5.2 Run local Go test command used by CI, or document why it cannot run.
- [ ] 5.3 Review generated GitHub workflows for least-privilege permissions and default-branch consistency.
```

to:

```markdown
- [x] 5.1 Run formatting or YAML validation for added files where available.
- [x] 5.2 Run local Go test command used by CI, or document why it cannot run.
- [x] 5.3 Review generated GitHub workflows for least-privilege permissions and default-branch consistency.
```

- [ ] **Step 8: Commit verification task completion**

Run:

```bash
git add openspec/changes/add-github-project-automation/tasks.md
git commit -m "chore: verify GitHub automation setup"
```

Expected: commit succeeds.

## Self-Review

- Spec coverage: contributor intake, PR template, Dependabot, CI, CodeQL, Trivy, and opt-in release policy are each mapped to implementation tasks.
- Placeholder scan: the plan has no placeholder sections or deferred implementation steps.
- Scope check: all implementation files are project automation or local CI support; no Go runtime API changes are included.
- Boundary check: `codecov.yml` and `.gitignore` are not modified by the plan.
- Branch consistency: all GitHub workflows use `main`.
- Release safety: no GoReleaser workflow is created.
