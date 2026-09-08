#!/usr/bin/env pwsh
<#
.SYNOPSIS
  Runs the full InferTest suite (all entailment + inconsistency constructs,
  including blank-node) against this repo's Windows Konclude build, and
  reports whether anything actually REGRESSED -- as opposed to the small,
  already-diagnosed set of Konclude kernel limitations documented in
  README.md, which are expected to fail every run and are not a problem.

.DESCRIPTION
  Unlike materialize.bat -selftest (one hardcoded, fast check), this runs
  every construct InferTest defines: merges InferTest's Turtle test data,
  converts to OWL2-XML, runs it through materialize.bat, converts the
  result back, and checks every expected entailment -- then does the same
  for the inconsistency-style constructs via Konclude's own `consistency`
  command. Requires the InferTest repo checked out as a sibling of this
  repo (or pass -InferTestPath), and Python + rdflib.

.PARAMETER InferTestPath
  Path to a checkout of https://github.com/ISE-FIZKarlsruhe/InferTest.
  Defaults to ..\..\InferTest relative to this script (i.e. a sibling of
  this repo).

.PARAMETER SkipInconsistency
  Only run the entailment-construct batch.
#>
param(
    [string]$InferTestPath = (Join-Path $PSScriptRoot "..\..\InferTest"),
    [switch]$SkipInconsistency
)

# Known, already-diagnosed gaps in Konclude's own reasoning/parsing (see
# README.md) -- expected to fail every run. Anything that fails but is NOT
# in this list is a real regression.
$KnownFailures = @(
    "has-key",
    "[inconsistency] asymmetric-property",
    "[inconsistency] disjoint-properties",
    "[inconsistency] has-key",
    "[inconsistency] irreflexive-property"
)

$ToolsDir = $PSScriptRoot
$KoncludeDir = Split-Path $ToolsDir -Parent
$MaterializeBat = Join-Path $KoncludeDir "materialize.bat"
$KoncludeExe = Join-Path $KoncludeDir "Binaries\Konclude.exe"
$OpenlletJar = Join-Path $ToolsDir "openllet.jar"
$EntailConfig = Join-Path $ToolsDir "infertest-all-enabled.txt"
$InconsistConfig = Join-Path $ToolsDir "infertest-inconsistency-enabled.txt"

if (-not (Test-Path $InferTestPath)) {
    Write-Host "ERROR: InferTest not found at '$InferTestPath'." -ForegroundColor Red
    Write-Host "Clone it as a sibling of this repo, or pass -InferTestPath:" -ForegroundColor Red
    Write-Host "  git clone https://github.com/ISE-FIZKarlsruhe/InferTest.git"
    exit 2
}
$InferTestPath = (Resolve-Path $InferTestPath).Path

python -c "import rdflib"
if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: python + rdflib required. Run: pip install rdflib" -ForegroundColor Red
    exit 2
}

if (-not (Test-Path (Join-Path $ToolsDir "ConvertToOWLXML.class"))) {
    Write-Host "Compiling converters (one-time)..."
    & "$ToolsDir\setup.bat"
    if ($LASTEXITCODE -ne 0) { exit 2 }
}

$CP = "$OpenlletJar;$ToolsDir"
$Work = Join-Path $env:TEMP "koncludix-infertest-run"
Remove-Item $Work -Recurse -Force -ErrorAction SilentlyContinue
New-Item -ItemType Directory -Path $Work | Out-Null

$allFailed = @()

Write-Host ""
Write-Host "=== Entailment constructs (test-cases/) ===" -ForegroundColor Cyan
python "$InferTestPath\scripts\prepare_test_ontology.py" --config $EntailConfig --format xml --output-dir $Work | Out-Null
java -cp $CP ConvertToOWLXML "$Work\infertest-merged.owl" "$Work\infertest-merged.owl.xml" | Out-Null
& $MaterializeBat "$Work\infertest-merged.owl.xml" "$Work\infertest-materialized.owl.xml" | Out-Null
java -cp $CP ConvertToRDFXML "$Work\infertest-materialized.owl.xml" "$Work\infertest-materialized.rdf.xml" | Out-Null
$entailOutput = python "$InferTestPath\scripts\validate_entailments.py" --inferred "$Work\infertest-materialized.rdf.xml" --config $EntailConfig --skip-inconsistency
$entailOutput | ForEach-Object { Write-Host $_ }
$entailOutput | Select-String "^\[FAIL\] (.+)$" | ForEach-Object { $allFailed += $_.Matches[0].Groups[1].Value.Trim() }

if (-not $SkipInconsistency) {
    Write-Host ""
    Write-Host "=== Inconsistency constructs (inconsistency-test-cases/) ===" -ForegroundColor Cyan
    python "$InferTestPath\scripts\prepare_test_ontology.py" --config $EntailConfig --inconsistency-config $InconsistConfig --format xml --output-dir $Work | Out-Null

    $reportedConsistent = @()
    Get-ChildItem "$Work\inconsistency\*.owl" | ForEach-Object {
        $name = $_.BaseName
        $owlxml = "$Work\inconsistency\$name.owl.xml"
        java -cp $CP ConvertToOWLXML $_.FullName $owlxml | Out-Null
        $resultLine = & $KoncludeExe consistency -i $owlxml | Select-String "is consistent\.|is inconsistent\."
        if ($resultLine -match "is consistent\.") {
            $reportedConsistent += $name
        }
    }

    # validate_entailments.py always reports both batches together (there's
    # no "inconsistency only" flag) -- the entailment batch it prints here
    # duplicates the block above; only its inconsistency lines are new.
    if ($reportedConsistent.Count -gt 0) {
        $incOutput = python "$InferTestPath\scripts\validate_entailments.py" --inferred "$Work\infertest-materialized.rdf.xml" --config $EntailConfig --inconsistency-config $InconsistConfig --reported-consistent @($reportedConsistent)
    } else {
        $incOutput = python "$InferTestPath\scripts\validate_entailments.py" --inferred "$Work\infertest-materialized.rdf.xml" --config $EntailConfig --inconsistency-config $InconsistConfig
    }
    $incLines = $incOutput | Where-Object { $_ -match "\[inconsistency\]" -or $_ -match "^InferTest .* inconsistency" }
    $incLines | ForEach-Object { Write-Host $_ }
    $incOutput | Select-String "^\[FAIL\] (\[inconsistency\] .+)$" | ForEach-Object { $allFailed += $_.Matches[0].Groups[1].Value.Trim() }
}

Remove-Item $Work -Recurse -Force -ErrorAction SilentlyContinue

$newFailures = $allFailed | Where-Object { $KnownFailures -notcontains $_ }
$knownSeen = $allFailed | Where-Object { $KnownFailures -contains $_ }

Write-Host ""
if ($newFailures.Count -gt 0) {
    Write-Host "REGRESSION: $($newFailures.Count) unexpected failure(s) -- something broke:" -ForegroundColor Red
    $newFailures | ForEach-Object { Write-Host "  - $_" -ForegroundColor Red }
    if ($knownSeen.Count -gt 0) {
        Write-Host "(plus $($knownSeen.Count) already-known Konclude limitation(s), not a new problem: $($knownSeen -join ', '))"
    }
    exit 1
} else {
    Write-Host "OK: no regressions. ($($knownSeen.Count) already-known Konclude limitation(s) seen, as expected: $($knownSeen -join ', '))" -ForegroundColor Green
    exit 0
}
