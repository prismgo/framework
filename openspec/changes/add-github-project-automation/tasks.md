## 1. Governance Templates

- [x] 1.1 Add `.github/ISSUE_TEMPLATE/bug-report.yaml` adapted from gin for Prismgo.
- [x] 1.2 Add `.github/ISSUE_TEMPLATE/feature-request.yaml` adapted from gin for Prismgo.
- [x] 1.3 Add `.github/ISSUE_TEMPLATE/config.yml` with blank issue policy and project contact links.
- [x] 1.4 Add `.github/PULL_REQUEST_TEMPLATE.md` with default branch, CI, test, and docs checklist.

## 2. CI Support

- [x] 2.1 Confirm the repository default branch and supported Go versions available in GitHub Actions.
- [x] 2.2 Add project-owned local commands, such as `Makefile` targets, if missing.
- [x] 2.3 Add lint configuration if required by `golangci-lint-action`.
- [x] 2.4 Add `.github/workflows/gin.yml`-equivalent CI adapted for Prismgo with Ubuntu/macOS, selected Go versions, race testing, and Codecov upload.
- [x] 2.5 Ensure the workflow preserves existing `codecov.yml` and does not overwrite unrelated user changes.

## 3. Dependency and Security Automation

- [x] 3.1 Add `.github/dependabot.yml` for daily `gomod` and grouped GitHub Actions updates.
- [x] 3.2 Add `.github/workflows/codeql.yml` for Go CodeQL on push, pull request, and weekly schedule.
- [x] 3.3 Add `.github/workflows/trivy-scan.yml` for filesystem vulnerability, secret, and misconfiguration scans.
- [x] 3.4 Tune Trivy scope or ignore behavior only for deliberate repository artifacts, not broad suppression.

## 4. Release Readiness

- [x] 4.1 Document the gin GoReleaser behavior and why automatic tag publishing is deferred.
- [x] 4.2 Leave tag-triggered release automation disabled unless a later maintainer decision enables it.

## 5. Verification

- [ ] 5.1 Run formatting or YAML validation for added files where available.
- [ ] 5.2 Run local Go test command used by CI, or document why it cannot run.
- [ ] 5.3 Review generated GitHub workflows for least-privilege permissions and default-branch consistency.
