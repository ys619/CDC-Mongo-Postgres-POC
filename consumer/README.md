# Event-Driven CDC Consumer (MongoDB to PostgreSQL)

## Overview
This standalone Python consumer ingests Change Data Capture (CDC) events published by Debezium from MongoDB into Apache Kafka, processes BSON Extended JSON payloads, and synchronizes state into PostgreSQL with idempotent upserts and tombstone-safe deletes.

## Key Features
- **Dynamic Topic Subscription**: Uses regex pattern `^mongo_cdc\.ISRA_new\..*` to automatically discover and consume all current and future collections in `ISRA_new`.
- **Snapshot & Streaming Support**: Seamlessly processes both initial snapshot events (`op: r`) and live change events (`op: c`, `op: u`, `op: d`).
- **Idempotency & Replay Protection**:
  - PostgreSQL target table `mongo_documents` uses `PRIMARY KEY (source_database, collection_name, document_id)`.
  - Upsert query guards against out-of-order replays:
    `WHERE mongo_documents.source_timestamp IS NULL OR EXCLUDED.source_timestamp >= mongo_documents.source_timestamp`
- **Robust Delete Handling**:
  - Extracts deleted document ID from Debezium's `filter`, `before`, or message `key`.
  - Safely ignores Kafka tombstone records (null payload) without crashing.
- **Transactional Offset Management**: Offsets are committed to Kafka **only after** PostgreSQL transaction commits (`enable.auto.commit = false`), guaranteeing at-least-once delivery with zero data loss.
- **Resilient Reconnection**: Automatically detects PostgreSQL disconnects, backs off, and reconnects without losing message state.

## Configuration (Environment Variables)
| Variable | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:29092` | Kafka broker address |
| `KAFKA_GROUP_ID` | `isra-cdc-consumer-group` | Consumer group ID |
| `TOPIC_PATTERN` | `^mongo_cdc\.ISRA_new\..*` | Topic subscription regex |
| `POSTGRES_HOST` | `postgres` | Target PostgreSQL host |
| `POSTGRES_PORT` | `5432` | Target PostgreSQL port |
| `POSTGRES_DB` | `isra_cdc` | Target database name |
| `POSTGRES_USER` | `postgres` | PostgreSQL username |
| `POSTGRES_PASSWORD` | `postgres` | PostgreSQL password |
| `LOG_LEVEL` | `INFO` | Logging verbosity (`DEBUG`, `INFO`, `WARNING`, `ERROR`) |
| `LOG_FILE` | `/app/logs/consumer.log` | Persistent log file destination |

## Local Execution (Outside Docker)
```powershell
cd C:\CDC-Mongo-Postgres-POC\consumer
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
$env:KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
$env:POSTGRES_HOST = "localhost"
python main.py
```
