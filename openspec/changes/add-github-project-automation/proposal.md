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
