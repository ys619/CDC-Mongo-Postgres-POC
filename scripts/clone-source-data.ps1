<#
.SYNOPSIS
    Safely clones source data from the existing MongoDB instance into the isolated POC MongoDB container.
.DESCRIPTION
    - Connects to source on localhost:27017 (read-only, never modifies source).
    - Checks disk space and reports data volume size before cloning.
    - Preserves all collection names and BSON data types (ObjectId, dates, binary).
    - Restores into isolated container on localhost:27018.
.PARAMETER SourceDb
    Source database name (default: ISRA_new).
.PARAMETER TargetDb
    Target database name (default: ISRA_new).
.PARAMETER Collections
    Optional comma-separated collection names to clone. If omitted, all collections are considered.
.PARAMETER SampleLimit
    Limit the number of documents per collection (useful for rapid POC testing on huge collections).
    0 means full collection clone.
.PARAMETER Force
    Skip confirmation prompts.
.PARAMETER SeedSampleIfEmpty
    If source DB is empty, seeds sample business collections for CDC verification.
#>
[CmdletBinding()]
param(
    [string]$SourceDb = "ISRA_new",
    [string]$TargetDb = "ISRA_new",
    [string]$Collections = "",
    [int]$SampleLimit = 0,
    [switch]$Force,
    [switch]$SeedSampleIfEmpty
)

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "  Safe Logical Source Data Clone (MongoDB -> POC MongoDB)" -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

# 1. Check drive space on C:
$drive = Get-PSDrive -Name C -ErrorAction SilentlyContinue
$freeGb = [math]::Round($drive.Free / 1GB, 2)
Write-Host "[1/5] Host Storage Verification:" -ForegroundColor Cyan
Write-Host "      Host Drive C: Free Space: $freeGb GB" -ForegroundColor Green

# 2. Execute inspection via Python
$helperScript = Join-Path $PSScriptRoot "clone_helper.py"
if (-not (Test-Path $helperScript)) {
    Write-Host "[ERROR] Helper script not found: $helperScript" -ForegroundColor Red
    exit 1
}

$sampleArg = if ($SampleLimit -gt 0) { "--sample-limit $SampleLimit" } else { "" }
$seedArg = if ($SeedSampleIfEmpty) { "--seed-if-empty" } else { "" }
$collArg = if ($Collections) { "--collections `"$Collections`"" } else { "" }

Write-Host "[2/5] Inspecting source database '$SourceDb' on mongodb://localhost:27017..." -ForegroundColor Cyan

# Run helper in inspect mode first
$inspectCmd = "python `"$helperScript`" --inspect --source-db `"$SourceDb`" --target-db `"$TargetDb`" $collArg"
Invoke-Expression $inspectCmd
if ($LASTEXITCODE -ne 0) {
    Write-Host "[ERROR] Inspection failed. Verify existing MongoDB is running on localhost:27017." -ForegroundColor Red
    exit 1
}

if (-not $Force) {
    $confirm = Read-Host "`nDo you want to proceed with the clone to isolated MongoDB (localhost:27018)? (Y/N)"
    if ($confirm -ne 'Y' -and $confirm -ne 'y') {
        Write-Host "Cloning aborted by user." -ForegroundColor Yellow
        exit 0
    }
}

Write-Host "`n[3/5] Executing logical clone to mongodb://localhost:27018/?directConnection=true..." -ForegroundColor Cyan
$cloneCmd = "python `"$helperScript`" --clone --source-db `"$SourceDb`" --target-db `"$TargetDb`" $collArg $sampleArg $seedArg"
Invoke-Expression $cloneCmd
if ($LASTEXITCODE -ne 0) {
    Write-Host "[ERROR] Data clone failed. Check the error log above." -ForegroundColor Red
    exit 1
}

Write-Host "`n[4/5] Verifying target data in isolated container..." -ForegroundColor Cyan
$verifyCmd = "python `"$helperScript`" --verify --target-db `"$TargetDb`""
Invoke-Expression $verifyCmd

Write-Host "`n[5/5] Clone operation completed successfully!" -ForegroundColor Green
Write-Host "Isolated database '$TargetDb' is populated and ready for CDC capture." -ForegroundColor Cyan
