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

### Requirement: Release Automation Is Opt-In

The repository SHALL NOT enable automatic tag-triggered GoReleaser publishing as part of the initial gin alignment.

#### Scenario: Tag is pushed after this change

- **WHEN** a tag is pushed
- **THEN** no newly added workflow from this change SHALL automatically publish a GitHub Release unless maintainers have explicitly enabled release automation in a later change.

#### Scenario: Release path reviewed later

- **WHEN** maintainers decide to enable GoReleaser
- **THEN** the release workflow SHALL require a reviewed `.goreleaser.yaml`, an agreed tag policy, and pkg.go.dev reindex behavior consistent with gin's release workflow.
