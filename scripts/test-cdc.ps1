<#
.SYNOPSIS
    End-to-End Automated CDC Pipeline Verification Test.
.DESCRIPTION
    Validates the complete CDC flow:
    1. Inserts a unique document into isolated MongoDB (localhost:27018).
    2. Verifies Debezium captures the change and emits it to Kafka.
    3. Verifies Python consumer writes the record into PostgreSQL (localhost:5432).
    4. Updates the document in MongoDB and verifies PostgreSQL synchronization.
    5. Deletes the document in MongoDB and verifies PostgreSQL row removal.
    6. Demonstrates snapshot / existing data synchronization.
#>
[CmdletBinding()]
param(
    [string]$Database = "ISRA_new",
    [string]$Collection = "cdc_test_pipeline",
    [int]$TimeoutSeconds = 30
)

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "  End-to-End Event-Driven CDC Pipeline Test" -ForegroundColor Cyan
Write-Host "  MongoDB (27018) -> Kafka -> Consumer -> Postgres (5432)" -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

$testId = "cdc_test_" + [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
$timestamp = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")

Write-Host "`nTest Parameters:" -ForegroundColor Cyan
Write-Host "  Database:           $Database"
Write-Host "  Test Collection:    $Collection"
Write-Host "  Test Document ID:   $testId"
Write-Host "  Kafka Expected Topic: mongo_cdc.$Database.$Collection"

# Python test executor for high-fidelity BSON & Postgres verification
$pythonTestScript = Join-Path $PSScriptRoot "cdc_test_runner.py"

python $pythonTestScript --db $Database --collection $Collection --test-id $testId --timeout $TimeoutSeconds
if ($LASTEXITCODE -eq 0) {
    Write-Host "`n==========================================================" -ForegroundColor Green
    Write-Host "  [SUCCESS] All End-to-End CDC Tests Passed!" -ForegroundColor Green
    Write-Host "  - Snapshot / Insert Captured & Upserted" -ForegroundColor Green
    Write-Host "  - Live Update Synchronized" -ForegroundColor Green
    Write-Host "  - Live Delete Safely Removed Target Record" -ForegroundColor Green
    Write-Host "==========================================================" -ForegroundColor Green
} else {
    Write-Host "`n==========================================================" -ForegroundColor Red
    Write-Host "  [FAILED] One or more CDC pipeline tests failed." -ForegroundColor Red
    Write-Host "  Check container logs:" -ForegroundColor Yellow
    Write-Host "    docker compose logs cdc-consumer" -ForegroundColor Yellow
    Write-Host "    docker compose logs kafka-connect" -ForegroundColor Yellow
    Write-Host "==========================================================" -ForegroundColor Red
    exit 1
}
