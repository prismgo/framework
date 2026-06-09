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

### Issue and PR Templates

Bug report template should request:

- Prismgo version or commit;
- Go version;
- operating system;
- reproducibility;
- minimal reproduction or code snippet;
- expected and actual behavior;
- logs when relevant;
- security-report warning to avoid public disclosure.

Feature request template should request:

- feature description;
- problem/use case;
- proposed API or behavior where known;
- alternatives considered.

PR template should keep gin's brevity but replace `docs/doc.md` with this repository's documentation locations, primarily `README.md` and `docs/`.

### Release Readiness

gin auto-releases on any tag push through GoReleaser and then reindexes pkg.go.dev. For this project, the first change should document or scaffold release readiness only if it can be made inert by default. A tag-triggered publishing workflow should not be enabled until:

- `.goreleaser.yaml` exists and is reviewed;
- tag naming policy is defined;
- maintainers confirm that GitHub Releases should be created automatically.

## Risks

- A full Ubuntu/macOS matrix can be slow and may surface OS-specific failures unrelated to the GitHub automation itself.
- `go 1.26.2` in `go.mod` may be ahead of broadly available GitHub-hosted setup-go versions; the workflow must pick available versions deliberately or use a stable fallback.
- Codecov may require repository-side configuration or token policy depending on visibility.
- Trivy secret scanning may flag historical/local artifacts; ignore policy must be deliberate, not a broad suppression.

## Open Questions

- Default branch should be confirmed before implementation.
- Supported Go version matrix should be pinned to versions available in GitHub Actions at implementation time.
- Security contact destination must be supplied by the maintainer before final issue templates are considered production-ready.
