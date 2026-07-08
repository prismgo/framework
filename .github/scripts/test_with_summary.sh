#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -eq 0 ]; then
  echo "usage: $0 <test command> [args...]" >&2
  exit 2
fi

# Scripts live under .github/scripts, so walk two levels up to place logs at the repository root.
REPO_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd -P)"
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
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

"$SCRIPT_DIR/summarize_test_failures.sh" "$TEST_LOG" >&2

exit "$status"
