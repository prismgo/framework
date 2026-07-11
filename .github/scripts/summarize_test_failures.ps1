# Emits a structured summary of failed `go test` cases parsed from a log file.
# Usage: summarize_test_failures.ps1 <log-file>
#
# PowerShell port of summarize_test_failures.sh for Windows CI runners.

param(
    [Parameter(Mandatory = $true)]
    [string]$LogFile
)

$ErrorActionPreference = 'Stop'

if (-not (Test-Path $LogFile)) {
    Write-Error "log file not found: $LogFile"
    exit 1
}

$tests = [System.Collections.ArrayList]::new()
$testSet = @{}
$details = @{}
$detailCount = @{}
$detailTruncated = @{}
$packages = [System.Collections.ArrayList]::new()
$packageSet = @{}
$currentTest = ''
$collectingDetail = ''

function Trim-Value([string]$value) {
    return $value.Trim()
}

function Remember-Detail([string]$testName, [string]$value) {
    $value = Trim-Value $value
    if ([string]::IsNullOrEmpty($testName) -or [string]::IsNullOrEmpty($value)) {
        return
    }
    if ($value -match '^(=== RUN|--- PASS:|--- FAIL:|PASS|FAIL)$') {
        return
    }
    if (-not $detailCount.ContainsKey($testName)) {
        $detailCount[$testName] = 0
        $details[$testName] = [System.Collections.ArrayList]::new()
    }
    if ($detailCount[$testName] -ge 8) {
        $detailTruncated[$testName] = $true
        return
    }
    [void]$details[$testName].Add($value)
    $detailCount[$testName]++
}

foreach ($line in (Get-Content -LiteralPath $LogFile)) {
    # === RUN lines
    if ($line -match '^=== RUN\s+(.+)$') {
        $currentTest = $Matches[1]
        $collectingDetail = ''
        continue
    }

    # --- FAIL: lines
    if ($line -match '^\s*--- FAIL:\s*(\S+)') {
        $failTest = $Matches[1]
        if (-not $testSet.ContainsKey($failTest)) {
            $testSet[$failTest] = $true
            [void]$tests.Add($failTest)
        }
        $currentTest = $failTest
        $collectingDetail = ''
        continue
    }

    # Indented detail lines with file:line: pattern
    if ($line -match '^\s+\S+\.\w+:\d+:') {
        $target = if (-not [string]::IsNullOrEmpty($currentTest)) { $currentTest } else { $collectingDetail }
        if (-not [string]::IsNullOrEmpty($target)) {
            Remember-Detail $target $line
            $collectingDetail = $target
        }
        continue
    }

    # General indented lines (continuation of detail)
    if ($line -match '^\s+' -and -not [string]::IsNullOrEmpty($collectingDetail)) {
        Remember-Detail $collectingDetail $line
        continue
    }

    # FAIL <package> lines
    if ($line -match '^FAIL\s+(.+)') {
        $pkgLine = $Matches[1].TrimEnd()
        if (-not $packageSet.ContainsKey($pkgLine)) {
            $packageSet[$pkgLine] = $true
            [void]$packages.Add($pkgLine)
        }
        $collectingDetail = ''
        continue
    }

    # Non-indented, non-matching line resets detail collection
    $collectingDetail = ''
}

# Build summary text
$sb = [System.Text.StringBuilder]::new()
[void]$sb.AppendLine('')
[void]$sb.AppendLine('==> Failed test summary')

if ($tests.Count -gt 0) {
    [void]$sb.AppendLine('Failed test cases:')
    foreach ($t in $tests) {
        [void]$sb.AppendLine("  - $t")
        if ($detailCount.ContainsKey($t) -and $detailCount[$t] -gt 0) {
            [void]$sb.AppendLine('    Error details:')
            foreach ($d in $details[$t]) {
                [void]$sb.AppendLine("      $d")
            }
            if ($detailTruncated.ContainsKey($t) -and $detailTruncated[$t]) {
                [void]$sb.AppendLine('      ... (truncated; see full log)')
            }
        }
    }
} else {
    [void]$sb.AppendLine('Failed test cases: (none parsed from go test output)')
}

if ($packages.Count -gt 0) {
    [void]$sb.AppendLine('Failed packages:')
    foreach ($p in $packages) {
        [void]$sb.AppendLine("  - $p")
    }
}

[void]$sb.AppendLine("Full log: $LogFile")

$summaryText = $sb.ToString()
Write-Host $summaryText

# Write to GitHub Step Summary if available
if ($env:GITHUB_STEP_SUMMARY) {
    $markdownLines = [System.Text.StringBuilder]::new()
    [void]$markdownLines.AppendLine('## Failed test summary')
    [void]$markdownLines.AppendLine('')

    if ($tests.Count -gt 0) {
        [void]$markdownLines.AppendLine('Failed test cases:')
        foreach ($t in $tests) {
            [void]$markdownLines.AppendLine("- $t")
            if ($detailCount.ContainsKey($t) -and $detailCount[$t] -gt 0) {
                [void]$markdownLines.AppendLine('  Error details:')
                foreach ($d in $details[$t]) {
                    [void]$markdownLines.AppendLine("  $d")
                }
                if ($detailTruncated.ContainsKey($t) -and $detailTruncated[$t]) {
                    [void]$markdownLines.AppendLine('  ... (truncated; see full log)')
                }
            }
        }
    } else {
        [void]$markdownLines.AppendLine('Failed test cases: (none parsed from go test output)')
    }

    if ($packages.Count -gt 0) {
        [void]$markdownLines.AppendLine('')
        [void]$markdownLines.AppendLine('Failed packages:')
        foreach ($p in $packages) {
            [void]$markdownLines.AppendLine("- $p")
        }
    }

    [void]$markdownLines.AppendLine('')
    [void]$markdownLines.AppendLine("Full log: $LogFile")

    Add-Content -LiteralPath $env:GITHUB_STEP_SUMMARY -Value $markdownLines.ToString()
}
