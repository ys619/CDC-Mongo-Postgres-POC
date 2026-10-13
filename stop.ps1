<#
.SYNOPSIS
    Stops the CDC POC containers safely without deleting persistent volumes or data.
#>
Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "  Stopping CDC POC Services (Preserving Volumes & Data)" -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

Set-Location $PSScriptRoot
docker compose down

Write-Host "`n[SUCCESS] POC containers stopped." -ForegroundColor Green
Write-Host "Named volumes ('isra_mongo_data', 'isra_kafka_data', 'isra_postgres_data') are preserved." -ForegroundColor Yellow
Write-Host "To resume anytime, run: .\start.ps1" -ForegroundColor Cyan
