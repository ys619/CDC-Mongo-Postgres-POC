<#
.SYNOPSIS
    Verifies container health, replica set status, connector status, Kafka topics, and PostgreSQL synchronization.
#>
Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "  CDC POC Health & Pipeline Status Verification" -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

Set-Location $PSScriptRoot

# 1. Container Status
Write-Host "`n[1/5] Container Status:" -ForegroundColor Cyan
docker compose ps

# 2. MongoDB Replica Set Status
Write-Host "`n[2/5] MongoDB Replica Set Status (rs0):" -ForegroundColor Cyan
try {
    $rsStatus = docker exec isra-mongo-source mongosh --port 27017 --eval 'JSON.stringify({ok: rs.status().ok, set: rs.status().set, members: rs.status().members.map(m => ({name: m.name, stateStr: m.stateStr}))})' --quiet
    Write-Host "      $rsStatus" -ForegroundColor Green
} catch {
    Write-Host "      [ERROR] Could not check replica set: $($_.Exception.Message)" -ForegroundColor Red
}

# 3. Debezium Connector Status
Write-Host "`n[3/5] Debezium Connector Status:" -ForegroundColor Cyan
try {
    $cStatus = Invoke-RestMethod -Uri "http://localhost:8083/connectors/isra-mongodb-connector/status" -Method Get -TimeoutSec 3 -ErrorAction Stop
    Write-Host "      Connector State: $($cStatus.connector.state)" -ForegroundColor Green
    $cStatus.tasks | ForEach-Object {
        Write-Host "      Task $($_.id) State: $($_.state) (Worker: $($_.worker_id))" -ForegroundColor Green
    }
} catch {
    Write-Host "      [WARNING] Could not retrieve connector status from Kafka Connect: $($_.Exception.Message)" -ForegroundColor Yellow
}

# 4. Kafka Topics List
Write-Host "`n[4/5] Active Kafka Topics:" -ForegroundColor Cyan
try {
    $topics = docker exec isra-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list 2>$null
    $topics | Where-Object { $_ -match "^mongo_cdc" } | ForEach-Object {
        Write-Host "      - $_" -ForegroundColor Green
    }
} catch {
    Write-Host "      [WARNING] Could not list Kafka topics: $($_.Exception.Message)" -ForegroundColor Yellow
}

# 5. PostgreSQL Target Row Count
Write-Host "`n[5/5] PostgreSQL Target Database ('isra_cdc'):" -ForegroundColor Cyan
try {
    $pgCounts = docker exec isra-postgres psql -U postgres -d isra_cdc -t -c "SELECT source_database, collection_name, COUNT(*), MAX(updated_at) FROM mongo_documents GROUP BY source_database, collection_name;" 2>$null
    if ($pgCounts.Trim()) {
        Write-Host "      Current synchronized collections & counts:" -ForegroundColor Green
        $pgCounts.Trim() | ForEach-Object { Write-Host "        $_" -ForegroundColor Green }
    } else {
        Write-Host "      Table 'mongo_documents' is currently empty (run .\scripts\test-cdc.ps1 or .\scripts\clone-source-data.ps1 to populate)." -ForegroundColor Yellow
    }
} catch {
    Write-Host "      [WARNING] Could not query PostgreSQL: $($_.Exception.Message)" -ForegroundColor Yellow
}

Write-Host "`nVerification check completed." -ForegroundColor Cyan
