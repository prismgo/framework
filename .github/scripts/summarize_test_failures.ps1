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
$buildErrors = @{}
$buildErrorCount = @{}
$buildErrorTruncated = @{}
$currentTest = ''
$collectingDetail = ''
$currentBuildPackage = ''
$collectingBuildErrors = $false

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

function Add-BuildError([string]$packageName, [string]$errorLine) {
    $errorLine = Trim-Value $errorLine
    if ([string]::IsNullOrEmpty($packageName) -or [string]::IsNullOrEmpty($errorLine)) {
        return
    }
    if (-not $packageSet.ContainsKey($packageName)) {
        $packageSet[$packageName] = $true
        [void]$packages.Add($packageName)
    }
    if (-not $buildErrorCount.ContainsKey($packageName)) {
        $buildErrorCount[$packageName] = 0
        $buildErrors[$packageName] = [System.Collections.ArrayList]::new()
    }
    if ($buildErrorCount[$packageName] -ge 10) {
        $buildErrorTruncated[$packageName] = $true
        return
    }
    [void]$buildErrors[$packageName].Add($errorLine)
    $buildErrorCount[$packageName]++
}

foreach ($line in (Get-Content -LiteralPath $LogFile)) {
    # === RUN lines
    if ($line -match '^=== RUN\s+(.+)$') {
        $currentTest = $Matches[1]
        $collectingDetail = ''
        $collectingBuildErrors = $false
        continue
    }

    # 捕获编译错误：# package [package.test]
    if ($line -match '^#\s+([^\s]+)') {
        $currentBuildPackage = $Matches[1]
        $currentBuildPackage = $currentBuildPackage -replace '\s+\[.*$', ''
        # 过滤掉非包名的行（如 FAIL）
        if ($currentBuildPackage -notmatch '^(FAIL|PASS|ok|---)') {
            $collectingBuildErrors = $true
        }
        $collectingDetail = ''
        continue
    }

    # 捕获编译错误行：file.go:line:col: error message
    if ($collectingBuildErrors -and $line -match '^[^\s]+\.[a-z]+:\d+:\d+:') {
        Add-BuildError $currentBuildPackage $line
        continue
    }

    # 非编译错误行结束编译错误收集
    if ($collectingBuildErrors) {
        $collectingBuildErrors = $false
    }

    # panic: lines - treat as failure when no --- FAIL: is emitted
    if ($line -match '^panic:') {
        $target = if (-not [string]::IsNullOrEmpty($currentTest)) { $currentTest } else { 'panic' }
        if (-not $testSet.ContainsKey($target)) {
            $testSet[$target] = $true
            [void]$tests.Add($target)
        }
        Remember-Detail $target $line
        $collectingDetail = $target
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
        # 提取包名（去掉 [setup failed] 等后缀）
        $pkgName = $pkgLine -replace '\s+\[.*$', ''
        if (-not $packageSet.ContainsKey($pkgName)) {
            $packageSet[$pkgName] = $true
            [void]$packages.Add($pkgName)
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
        if ($buildErrorCount.ContainsKey($p) -and $buildErrorCount[$p] -gt 0) {
            [void]$sb.AppendLine('    Build errors:')
            foreach ($e in $buildErrors[$p]) {
                [void]$sb.AppendLine("      $e")
            }
            if ($buildErrorTruncated.ContainsKey($p) -and $buildErrorTruncated[$p]) {
                [void]$sb.AppendLine('      ... (truncated; see full log)')
            }
        }
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
            if ($buildErrorCount.ContainsKey($p) -and $buildErrorCount[$p] -gt 0) {
                [void]$markdownLines.AppendLine('  Build errors:')
                foreach ($e in $buildErrors[$p]) {
                    [void]$markdownLines.AppendLine("  $e")
                }
                if ($buildErrorTruncated.ContainsKey($p) -and $buildErrorTruncated[$p]) {
                    [void]$markdownLines.AppendLine('  ... (truncated; see full log)')
                }
            }
        }
    }

    [void]$markdownLines.AppendLine('')
    [void]$markdownLines.AppendLine("Full log: $LogFile")

    Add-Content -LiteralPath $env:GITHUB_STEP_SUMMARY -Value $markdownLines.ToString()
}
