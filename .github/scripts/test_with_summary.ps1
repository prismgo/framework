# Runs a test command and, on failure, prints a structured summary of failed tests.
# Usage: test_with_summary.ps1 [-Command <string[]>]
#
# PowerShell port of test_with_summary.sh for Windows CI runners.

param(
    [Parameter(Mandatory = $true, ValueFromRemainingArguments = $true)]
    [string[]]$Command
)

$ErrorActionPreference = 'Stop'

if ($Command.Count -eq 0) {
    Write-Error "usage: $PSCommandPath <test command> [args...]"
    exit 2
}

# Scripts live under .github/scripts, so walk two levels up to place logs at the repository root.
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = (Resolve-Path (Join-Path $ScriptDir '..\..')).Path
$LogRoot = if ($env:TEST_SUMMARY_LOG_DIR) { $env:TEST_SUMMARY_LOG_DIR } else { Join-Path $RepoRoot '.coverage' }

if (-not (Test-Path $LogRoot)) {
    New-Item -ItemType Directory -Path $LogRoot -Force | Out-Null
}

$timestamp = Get-Date -Format 'yyyyMMddHHmmss'
$pid = $PID
$TestLog = Join-Path $LogRoot "test-summary-$timestamp-$pid.log"

Write-Host "==> Command: $($Command -join ' ')"

try {
    & $Command[0] $Command[1..($Command.Count - 1)] 2>&1 | Tee-Object -FilePath $TestLog
    $exitCode = $LASTEXITCODE
    if ($null -eq $exitCode) {
        $exitCode = 0
    }
} catch {
    Write-Error $_
    $exitCode = 1
}

if ($exitCode -eq 0) {
    exit 0
}

$summarizeScript = Join-Path $ScriptDir 'summarize_test_failures.ps1'
& $summarizeScript -LogFile $TestLog

exit $exitCode
