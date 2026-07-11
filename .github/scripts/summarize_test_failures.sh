#!/usr/bin/env bash
# Emits a structured summary of failed `go test` cases parsed from a log file.
# Usage: summarize_test_failures.sh <log-file>
#
# The script is a shared helper invoked by both `test_with_summary.sh` and
# `coverage.sh` so that CI steps surface the same "Failed test summary" block
# when tests fail. It writes the summary to stdout and, when running inside
# GitHub Actions, appends a Markdown version to $GITHUB_STEP_SUMMARY.

set -euo pipefail

if [ "$#" -ne 1 ]; then
  echo "usage: $0 <log-file>" >&2
  exit 2
fi

LOG_FILE="$1"
if [ ! -f "$LOG_FILE" ]; then
  echo "log file not found: $LOG_FILE" >&2
  exit 1
fi

SUMMARY_FILE="$(mktemp)"
trap 'rm -f "$SUMMARY_FILE"' EXIT

awk '
  function trim(value) {
    sub(/^[[:space:]]+/, "", value)
    sub(/[[:space:]]+$/, "", value)
    return value
  }

  function remember_detail(test_name, value) {
    value = trim(value)
    if (test_name == "" || value == "") {
      return
    }
    if (value ~ /^(=== RUN|--- PASS:|--- FAIL:|PASS|FAIL)$/) {
      return
    }
    if (detail_count[test_name] >= 8) {
      detail_truncated[test_name] = 1
      return
    }
    details[test_name, ++detail_count[test_name]] = value
  }

  function add_build_error(package_name, error_line) {
    error_line = trim(error_line)
    if (package_name == "" || error_line == "") {
      return
    }
    if (!seen_package[package_name]++) {
      packages[++package_count] = package_name
    }
    if (build_error_count[package_name] >= 10) {
      build_error_truncated[package_name] = 1
      return
    }
    build_errors[package_name, ++build_error_count[package_name]] = error_line
  }

  /^=== RUN[[:space:]]+/ {
    current_test = $0
    sub(/^=== RUN[[:space:]]+/, "", current_test)
    next
  }

  # 捕获编译错误：# package [package.test]
  /^# [^[:space:]]+/ {
    current_build_package = $0
    sub(/^# /, "", current_build_package)
    sub(/[[:space:]]+\[.*$/, "", current_build_package)
    # 过滤掉非包名的行（如 FAIL）
    if (current_build_package !~ /^(FAIL|PASS|ok|---)/) {
      collecting_build_errors = 1
    }
    next
  }

  # 捕获编译错误行：file.go:line:col: error message
  collecting_build_errors && /^[^[:space:]]+\.[a-z]+:[0-9]+:[0-9]+:/ {
    add_build_error(current_build_package, $0)
    next
  }

  # 非编译错误行结束编译错误收集
  collecting_build_errors {
    collecting_build_errors = 0
  }

  # 捕获 panic 行，将其记录为失败测试用例
  /^panic:/ {
    if (current_test != "") {
      test_name = current_test
    } else {
      test_name = "panic"
    }
    if (!seen_test[test_name]++) {
      tests[++test_count] = test_name
    }
    collecting_detail = test_name
    remember_detail(test_name, $0)
    next
  }

  /^[[:space:]]*--- FAIL:/ {
    line = $0
    sub(/^[[:space:]]*--- FAIL:[[:space:]]*/, "", line)
    sub(/[[:space:]]*\([^)]*\).*$/, "", line)
    if (!seen_test[line]++) {
      tests[++test_count] = line
    }
    current_test = line
    next
  }

  /^[[:space:]]+[^[:space:]]+:[0-9]+:/ {
    detail_target = current_test
    remember_detail(detail_target, $0)
    collecting_detail = detail_target
    next
  }

  /^[[:space:]]+/ {
    if (collecting_detail != "") {
      remember_detail(collecting_detail, $0)
    }
    next
  }

  {
    collecting_detail = ""
  }

  /^FAIL[[:space:]]+/ {
    line = $0
    sub(/^FAIL[[:space:]]+/, "", line)
    sub(/[[:space:]]+$/, "", line)
    # 提取包名（去掉 [setup failed] 等后缀）
    pkg_name = line
    sub(/[[:space:]]+\[.*$/, "", pkg_name)
    if (!seen_package[pkg_name]++) {
      packages[++package_count] = pkg_name
    }
  }

  END {
    print ""
    print "==> Failed test summary"

    if (test_count > 0) {
      print "Failed test cases:"
      for (i = 1; i <= test_count; i++) {
        print "  - " tests[i]
        if (detail_count[tests[i]] > 0) {
          print "    Error details:"
          for (j = 1; j <= detail_count[tests[i]]; j++) {
            print "      " details[tests[i], j]
          }
          if (detail_truncated[tests[i]]) {
            print "      ... (truncated; see full log)"
          }
        }
      }
    } else {
      print "Failed test cases: (none parsed from go test output)"
    }

    if (package_count > 0) {
      print ""
      print "Failed packages:"
      for (i = 1; i <= package_count; i++) {
        pkg = packages[i]
        print "  - " pkg
        if (build_error_count[pkg] > 0) {
          print "    Build errors:"
          for (j = 1; j <= build_error_count[pkg]; j++) {
            print "      " build_errors[pkg, j]
          }
          if (build_error_truncated[pkg]) {
            print "      ... (truncated; see full log)"
          }
        }
      }
    }

    print ""
    print "Full log: " log_path
  }
' log_path="$LOG_FILE" "$LOG_FILE" >"$SUMMARY_FILE"

cat "$SUMMARY_FILE"

if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
  {
    echo "## Failed test summary"
    echo
    sed '1,2d; s/^  - /- /' "$SUMMARY_FILE"
  } >>"$GITHUB_STEP_SUMMARY"
fi
