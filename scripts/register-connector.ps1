<#
.SYNOPSIS
    Registers or updates the Debezium MongoDB CDC connector in Kafka Connect.
.PARAMETER ConfigFile
    Path to the connector JSON configuration file.
.PARAMETER KafkaConnectUrl
    Base URL for Kafka Connect REST API.
.PARAMETER Force
    If specified, deletes existing connector and registers fresh.
#>
[CmdletBinding()]
param(
    [string]$ConfigFile = "$PSScriptRoot\..\debezium\register-connector.json",
    [string]$KafkaConnectUrl = "http://localhost:8083",
    [switch]$Force
)

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "  Debezium MongoDB Connector Registration" -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

# 1. Check if Kafka Connect is available
Write-Host "[1/4] Waiting for Kafka Connect at $KafkaConnectUrl..." -NoNewline
$maxAttempts = 30
$connected = $false
for ($i = 1; $i -le $maxAttempts; $i++) {
    try {
        $response = Invoke-RestMethod -Uri "$KafkaConnectUrl/connectors" -Method Get -TimeoutSec 3 -ErrorAction Stop
        $connected = $true
        Write-Host " Ready!" -ForegroundColor Green
        break
    } catch {
        Write-Host "." -NoNewline
        Start-Sleep -Seconds 2
    }
}

if (-not $connected) {
    Write-Host "`n[ERROR] Kafka Connect REST API ($KafkaConnectUrl) is unreachable after $maxAttempts attempts." -ForegroundColor Red
    Write-Host "Ensure the 'kafka-connect' container is running: docker compose logs kafka-connect" -ForegroundColor Yellow
    exit 1
}

# 2. Verify Debezium MongoDB Connector Plugin is installed
Write-Host "[2/4] Verifying Debezium MongoDB Connector plugin..."
try {
    $plugins = Invoke-RestMethod -Uri "$KafkaConnectUrl/connector-plugins" -Method Get -ErrorAction Stop
    $mongoPlugin = $plugins | Where-Object { $_.class -eq "io.debezium.connector.mongodb.MongoDbConnector" }
    if ($mongoPlugin) {
        Write-Host "      Found plugin: $($mongoPlugin.class) (v$($mongoPlugin.version))" -ForegroundColor Green
    } else {
        Write-Host "      [ERROR] io.debezium.connector.mongodb.MongoDbConnector not found in plugin list!" -ForegroundColor Red
        Write-Host "      Available plugins:" -ForegroundColor Yellow
        $plugins | ForEach-Object { Write-Host "        - $($_.class)" -ForegroundColor Yellow }
        exit 1
    }
} catch {
    Write-Host "      [WARNING] Could not query connector plugins: $($_.Exception.Message)" -ForegroundColor Yellow
}

# 3. Read configuration file
if (-not (Test-Path $ConfigFile)) {
    Write-Host "[ERROR] Configuration file not found: $ConfigFile" -ForegroundColor Red
    exit 1
}

$rawJson = Get-Content -Path $ConfigFile -Raw -Encoding utf8
$connDef = $rawJson | ConvertFrom-Json
$connectorName = $connDef.name

Write-Host "[3/4] Checking status of connector '$connectorName'..."
$existing = $null
try {
    $existing = Invoke-RestMethod -Uri "$KafkaConnectUrl/connectors/$connectorName" -Method Get -ErrorAction Stop
} catch {
    # 404 is expected if not registered yet
}

if ($existing) {
    if ($Force) {
        Write-Host "      Connector exists. -Force specified, deleting connector..." -ForegroundColor Yellow
        Invoke-RestMethod -Uri "$KafkaConnectUrl/connectors/$connectorName" -Method Delete -ErrorAction Stop | Out-Null
        Start-Sleep -Seconds 2
        Write-Host "      Registering fresh connector..."
        Invoke-RestMethod -Uri "$KafkaConnectUrl/connectors" -Method Post -Body $rawJson -ContentType "application/json" -ErrorAction Stop | Out-Null
    } else {
        Write-Host "      Connector already exists. Updating configuration..." -ForegroundColor Yellow
        $configJson = $connDef.config | ConvertTo-Json -Compress
        Invoke-RestMethod -Uri "$KafkaConnectUrl/connectors/$connectorName/config" -Method Put -Body $configJson -ContentType "application/json" -ErrorAction Stop | Out-Null
    }
} else {
    Write-Host "      Registering new connector..." -ForegroundColor Green
    Invoke-RestMethod -Uri "$KafkaConnectUrl/connectors" -Method Post -Body $rawJson -ContentType "application/json" -ErrorAction Stop | Out-Null
}

# 4. Wait for connector and tasks to be RUNNING
Write-Host "[4/4] Waiting for connector '$connectorName' tasks to become RUNNING..."
$maxWait = 30
$isRunning = $false
for ($i = 1; $i -le $maxWait; $i++) {
    try {
        $status = Invoke-RestMethod -Uri "$KafkaConnectUrl/connectors/$connectorName/status" -Method Get -ErrorAction Stop
        $cState = $status.connector.state
        $tasks = $status.tasks
        $tasksRunning = ($tasks | Where-Object { $_.state -eq "RUNNING" }).Count
        
        Write-Host "      Status attempt $($i): Connector=$cState, Tasks Running=$tasksRunning/$($totalTasks)" -ForegroundColor Gray
        
        if ($cState -eq "RUNNING" -and ($totalTasks -eq 0 -or $tasksRunning -eq $totalTasks)) {
            $isRunning = $true
            Write-Host "`n[SUCCESS] Debezium connector '$connectorName' is RUNNING!" -ForegroundColor Green
            Write-Host "Connector details:" -ForegroundColor Cyan
            $status | ConvertTo-Json -Depth 4 | Write-Host -ForegroundColor Gray
            break
        }
    } catch {
        Write-Host "      Waiting for status endpoint..." -ForegroundColor Gray
    }
    Start-Sleep -Seconds 2
}

if (-not $isRunning) {
    Write-Host "`n[WARNING] Connector did not reach RUNNING state within $maxWait seconds." -ForegroundColor Yellow
    Write-Host "Check status manually: curl http://localhost:8083/connectors/$connectorName/status" -ForegroundColor Yellow
    Write-Host "Check Kafka Connect logs: docker compose logs kafka-connect" -ForegroundColor Yellow
}
