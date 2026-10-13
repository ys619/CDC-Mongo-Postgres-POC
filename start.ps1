<#
.SYNOPSIS
    Starts the complete Event-Driven CDC POC infrastructure and validates all components.
#>
[CmdletBinding()]
param(
    [switch]$SkipBuild
)

$ErrorActionPreference = "Stop"

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "  Starting CDC POC: MongoDB -> Debezium -> Kafka -> Postgres" -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

# 1. Verify Docker Desktop
Write-Host "[1/7] Verifying Docker Desktop availability..." -ForegroundColor Cyan
try {
    $dockerInfo = docker info --format '{{.OSType}}'
    if ($dockerInfo -ne "linux") {
        Write-Host "[ERROR] Docker is not in Linux container mode." -ForegroundColor Red
        exit 1
    }
    Write-Host "      Docker Desktop is active and running." -ForegroundColor Green
} catch {
    Write-Host "[ERROR] Docker Desktop is not running. Please start Docker Desktop first." -ForegroundColor Red
    exit 1
}

# 2. Verify / Create .env
Write-Host "[2/7] Checking environment configuration..." -ForegroundColor Cyan
$envPath = Join-Path $PSScriptRoot ".env"
$envExamplePath = Join-Path $PSScriptRoot ".env.example"
if (-not (Test-Path $envPath)) {
    Write-Host "      .env not found. Creating from .env.example..." -ForegroundColor Yellow
    Copy-Item $envExamplePath $envPath
}
Write-Host "      Environment file .env verified." -ForegroundColor Green

# 3. Start Infrastructure via Docker Compose
Write-Host "[3/7] Starting Docker Compose services..." -ForegroundColor Cyan
Set-Location $PSScriptRoot

if ($SkipBuild) {
    docker compose up -d
} else {
    docker compose up -d --build
}

if ($LASTEXITCODE -ne 0) {
    Write-Host "[ERROR] Failed to start Docker Compose services." -ForegroundColor Red
    exit 1
}

# 4. Wait for services healthy state
Write-Host "[4/7] Waiting for services to become healthy..." -ForegroundColor Cyan
$services = @("mongo-source", "kafka", "postgres", "kafka-connect")
foreach ($svc in $services) {
    Write-Host "      Waiting for service '$svc'..." -NoNewline
    $healthy = $false
    for ($i = 1; $i -le 40; $i++) {
        $status = docker inspect --format='{{json .State.Health.Status}}' "isra-$svc" 2>$null
        if ($status -eq '"healthy"') {
            $healthy = $true
            Write-Host " Healthy!" -ForegroundColor Green
            break
        }
        Write-Host "." -NoNewline
        Start-Sleep -Seconds 2
    }
    if (-not $healthy) {
        Write-Host "`n[WARNING] Service '$svc' did not report healthy within timeout." -ForegroundColor Yellow
    }
}

# 5. Initialize MongoDB replica set rs0
Write-Host "[5/7] Ensuring MongoDB replica set rs0 is initialized..." -ForegroundColor Cyan
try {
    $initOutput = docker exec isra-mongo-source mongosh --port 27017 /scripts/init-replica-set.js
    Write-Host "      $initOutput" -ForegroundColor Gray
} catch {
    Write-Host "      [WARNING] Replica set init script executed with warning: $($_.Exception.Message)" -ForegroundColor Yellow
}

# 6. Register Debezium MongoDB Connector
Write-Host "[6/7] Registering Debezium MongoDB Connector..." -ForegroundColor Cyan
$regScript = Join-Path $PSScriptRoot "scripts\register-connector.ps1"
& $regScript

# 7. Print Endpoints Summary
$uiPort = "8082"
if (Test-Path $envPath) {
    Get-Content $envPath | ForEach-Object {
        if ($_ -match '^\s*KAFKA_UI_HOST_PORT\s*=\s*(.+)$') {
            $uiPort = $matches[1].Trim()
        }
    }
}

Write-Host "`n[7/7] POC Services Active! Access Endpoints:" -ForegroundColor Cyan
Write-Host "----------------------------------------------------------" -ForegroundColor Gray
Write-Host "  Kafka UI:           http://localhost:$uiPort" -ForegroundColor Yellow
Write-Host "  Kafka Connect REST: http://localhost:8083" -ForegroundColor Yellow
Write-Host "  Kafka Broker:       localhost:9092" -ForegroundColor Yellow
Write-Host "  Isolated MongoDB:   localhost:27018 (Replica Set: rs0)" -ForegroundColor Yellow
Write-Host "  Target PostgreSQL:  localhost:5432 (Database: isra_cdc)" -ForegroundColor Yellow
Write-Host "----------------------------------------------------------" -ForegroundColor Gray
Write-Host "`nNext Steps:" -ForegroundColor Cyan
Write-Host "  1. Run End-to-End Test:       .\scripts\test-cdc.ps1" -ForegroundColor Green
Write-Host "  2. Clone Data from Source:   .\scripts\clone-source-data.ps1" -ForegroundColor Green
Write-Host "  3. Verify System Health:     .\verify.ps1" -ForegroundColor Green
Write-Host "  4. View Consumer Logs:       docker compose logs -f cdc-consumer" -ForegroundColor Green
Write-Host "  5. Stop POC (keep data):     .\stop.ps1" -ForegroundColor Green
Write-Host "==========================================================" -ForegroundColor Cyan
