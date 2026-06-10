#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
# Scripts live under .github/scripts, so walk two levels up to reach the repository root.
REPO_ROOT="$(cd -- "$SCRIPT_DIR/../.." && pwd -P)"

cd "$REPO_ROOT"

native_path() {
  local path="$1"

  if command -v cygpath >/dev/null 2>&1; then
    cygpath -m "$path"
    return
  fi

  (
    cd "$path"
    pwd -P
  )
}

timestamp() {
  date +"%Y%m%d%H%M%S"
}

split_words() {
  local value="$1"

  # shellcheck disable=SC2206
  SPLIT_WORDS_RESULT=($value)
}

COV_ROOT="${COV_ROOT:-.coverage}"
COV_ROOT_ABS="$REPO_ROOT/$COV_ROOT"
COV_DIR="$COV_ROOT_ABS/tmp/covdata-$(timestamp)"
GOCACHE_DIR="${GOCACHE_DIR:-$REPO_ROOT/tmp/gocache}"
COUNT="${COUNT:-1}"
COVERMODE="${COVERMODE:-atomic}"
TEST_LOG="$COV_ROOT_ABS/test.log"

EXTRA_TEST_FLAGS=()
if [ -n "${TEST_FLAGS:-}" ]; then
  split_words "$TEST_FLAGS"
  EXTRA_TEST_FLAGS=("${SPLIT_WORDS_RESULT[@]}")
fi

mkdir -p "$COV_DIR" "$GOCACHE_DIR"

export GOCACHE="$(native_path "$GOCACHE_DIR")"
COV_ROOT_NATIVE="$(native_path "$COV_ROOT_ABS")"
COV_DIR_NATIVE="$(native_path "$COV_DIR")"

DEFAULT_TEST_PATTERNS=(
  "./..."
)

DEFAULT_COVERPKG_PATTERNS=(
  "./..."
)

if [ "$#" -gt 0 ]; then
  TEST_PATTERNS=("$@")
elif [ -n "${TEST_PATTERNS:-}" ]; then
  split_words "$TEST_PATTERNS"
  TEST_PATTERNS=("${SPLIT_WORDS_RESULT[@]}")
else
  TEST_PATTERNS=("${DEFAULT_TEST_PATTERNS[@]}")
fi

if [ -n "${COVERPKG_PATTERNS:-}" ]; then
  split_words "$COVERPKG_PATTERNS"
  COVERPKG_PATTERNS=("${SPLIT_WORDS_RESULT[@]}")
elif [ "$#" -gt 0 ]; then
  COVERPKG_PATTERNS=("$@")
else
  COVERPKG_PATTERNS=("${DEFAULT_COVERPKG_PATTERNS[@]}")
fi

echo "==> Repository: $REPO_ROOT"
echo "==> Go: $(go env GOVERSION) $(go env GOOS)/$(go env GOARCH)"
echo "==> GOCACHE: $GOCACHE"
echo "==> Coverage data: $COV_DIR"
echo "==> Test packages: ${TEST_PATTERNS[*]}"
echo "==> Cover packages: ${COVERPKG_PATTERNS[*]}"
echo "==> Cover mode: $COVERMODE"
if [ "${#EXTRA_TEST_FLAGS[@]}" -gt 0 ]; then
  echo "==> Extra test flags: ${EXTRA_TEST_FLAGS[*]}"
fi

echo "==> Resolving coverpkg list"
COVERPKG="$(
  go list -f '{{if or .GoFiles .CgoFiles}}{{.ImportPath}}{{end}}' "${COVERPKG_PATTERNS[@]}" \
    | sed '/^$/d' \
    | sort -u \
    | paste -sd, -
)"

if [ -z "$COVERPKG" ]; then
  echo "coverpkg list is empty" >&2
  exit 1
fi

IFS=, read -r -a COVERPKG_LIST <<< "$COVERPKG"
echo "==> Cover package count: ${#COVERPKG_LIST[@]}"

echo "==> Running go test with covdata"
GO_TEST_CMD=(go test)
if [ "${#EXTRA_TEST_FLAGS[@]}" -gt 0 ]; then
  GO_TEST_CMD+=("${EXTRA_TEST_FLAGS[@]}")
fi
GO_TEST_CMD+=("${TEST_PATTERNS[@]}" \
  "-count=$COUNT" \
  -cover \
  "-covermode=$COVERMODE" \
  "-coverpkg=$COVERPKG" \
  -args "-test.gocoverdir=$COV_DIR_NATIVE")

if ! "${GO_TEST_CMD[@]}" >"$TEST_LOG" 2>&1; then
  cat "$TEST_LOG" >&2
  exit 1
fi

awk '
  /^ok[[:space:]]/ { ok++ }
  /^\?[[:space:]]/ { no_tests++ }
  END {
    printf("==> Test result: %d packages passed, %d packages without tests\n", ok, no_tests)
  }
' "$TEST_LOG"

echo "==> Calculating package coverage"
go tool covdata percent "-i=$COV_DIR_NATIVE" \
  >"$COV_ROOT_ABS/coverage.percent.txt"

echo "==> Exporting text coverage profile"
go tool covdata textfmt "-i=$COV_DIR_NATIVE" \
  "-o=$COV_ROOT_NATIVE/coverage.out"

echo "==> Calculating function coverage"
go tool cover "-func=$COV_ROOT_NATIVE/coverage.out" \
  >"$COV_ROOT_ABS/coverage.func.txt"

TOTAL_COVERAGE="$(awk '/^total:/ {print $NF}' "$COV_ROOT_ABS/coverage.func.txt")"

echo "==> Building HTML coverage report"
go tool cover "-html=$COV_ROOT_NATIVE/coverage.out" \
  "-o=$COV_ROOT_NATIVE/coverage.html"

echo "==> Summary"
echo "total=$TOTAL_COVERAGE"
echo "covdata=$COV_DIR"
echo "profile=$COV_ROOT_ABS/coverage.out"
echo "functions=$COV_ROOT_ABS/coverage.func.txt"
echo "html=$COV_ROOT_ABS/coverage.html"
echo "test_log=$TEST_LOG"
