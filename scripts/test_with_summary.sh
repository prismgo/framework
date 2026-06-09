#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -eq 0 ]; then
  echo "usage: $0 <test command> [args...]" >&2
  exit 2
fi

REPO_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
LOG_ROOT="${TEST_SUMMARY_LOG_DIR:-$REPO_ROOT/.coverage}"
mkdir -p "$LOG_ROOT"

TEST_LOG="$LOG_ROOT/test-summary-$(date +"%Y%m%d%H%M%S")-$$.log"

echo "==> Command: $*"
set +e
"$@" 2>&1 | tee "$TEST_LOG"
status="${PIPESTATUS[0]}"
set -e

if [ "$status" -eq 0 ]; then
  exit 0
fi

SUMMARY_FILE="$(mktemp)"
trap 'rm -f "$SUMMARY_FILE"' EXIT
awk '
  /^[[:space:]]*--- FAIL:/ {
    line = $0
    sub(/^[[:space:]]*--- FAIL:[[:space:]]*/, "", line)
    sub(/[[:space:]]*\([^)]*\).*$/, "", line)
    if (!seen_test[line]++) {
      tests[++test_count] = line
    }
  }

  /^FAIL[[:space:]]+/ {
    line = $0
    sub(/[[:space:]]+$/, "", line)
    if (!seen_package[line]++) {
      packages[++package_count] = line
    }
  }

  END {
    print ""
    print "==> Failed test summary"

    if (test_count > 0) {
      print "Failed test cases:"
      for (i = 1; i <= test_count; i++) {
        print "  - " tests[i]
      }
    } else {
      print "Failed test cases: (none parsed from go test output)"
    }

    if (package_count > 0) {
      print "Failed packages:"
      for (i = 1; i <= package_count; i++) {
        print "  - " packages[i]
      }
    }

    print "Full log: " log_path
  }
' log_path="$TEST_LOG" "$TEST_LOG" >"$SUMMARY_FILE"

cat "$SUMMARY_FILE" >&2

if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
  {
    echo "## Failed test summary"
    echo
    sed '1,2d; s/^  - /- /' "$SUMMARY_FILE"
  } >>"$GITHUB_STEP_SUMMARY"
fi

exit "$status"
