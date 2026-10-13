# CDC Pipeline — Complete Monitoring Guide
## How to Run a Test & Watch Everything Happening in Real-Time

> **All 7 containers are running.** Here's exactly where to look when you run any test.

---

## The Big Picture — What Runs Where

```
[YOU run a script]
      |
      v
 MongoDB (27018)          <-- You INSERT/UPDATE/DELETE data here
      |  Oplog / Change Stream
      v
 Debezium (8083)          <-- Watches the oplog, packages events
      |  Kafka Message
      v
 Kafka Broker (9092)      <-- Stores events in topics
      |  Consumer reads
      v
 Python CDC Receiver      <-- Cleans, transforms, writes
      |  SQL UPSERT/DELETE
      v
 PostgreSQL (5432)        <-- Final destination
      |
      v
 pgAdmin (5050)           <-- You can VIEW the data here
```

---

## STEP 1 — Run the Test Script

Open a **new terminal** in `c:\CDC-Mongo-Postgres-POC` and run:

```powershell
# Full live demo (Insert -> Update -> Watch everything)
python scripts/live_cdc_demo.py

# OR the automated end-to-end test
powershell -File scripts/test-cdc.ps1
```

**Keep this terminal visible** — it prints every stage as it happens.

---

## STEP 2 — Watch the Python Consumer Log (Real-Time)

This log shows **every single event** the Python Receiver processes from Kafka.

```powershell
# Stream live logs from the CDC consumer container
docker compose logs -f cdc-consumer
```

**What you'll see:**
```
[INFO] UPSERT (c) processed: ISRA_new.Radio_broadcast_live [ID: broadcast_123]   <- INSERT
[INFO] UPSERT (u) processed: ISRA_new.Radio_broadcast_live [ID: broadcast_123]   <- UPDATE
[INFO] DELETE processed: ISRA_new.Radio_broadcast_live [ID: broadcast_123]        <- DELETE
```

> **Op codes:** `c` = create/insert, `u` = update, `r` = snapshot read, `d` = delete

Or read the log **file directly** (updated in real-time):
```powershell
Get-Content logs\consumer.log -Wait -Tail 50
```

---

## STEP 3 — Kafka UI (Brokers, Topics, Messages, Consumers)

**URL:** http://localhost:8082

### A. Brokers Tab
```
http://localhost:8082/ui/clusters/local-cluster/brokers
```
- Shows **Broker 1** health, partitions it leads, bytes in/out
- Should show: 1 broker, healthy

### B. Topics Tab — Watch Messages Arrive
```
http://localhost:8082/ui/clusters/local-cluster/all-topics
```
When you run a test, look for your topic e.g.:
- `mongo_cdc.ISRA_new.Radio_broadcast_live`
- Click it → **Messages tab**
- Every INSERT, UPDATE, DELETE appears here as a new message

**Message breakdown you'll see:**
| Field | Meaning |
|---|---|
| `"op": "c"` | Create (INSERT) |
| `"op": "u"` | Update |
| `"op": "d"` | Delete |
| `"after": {...}` | Full document state |
| `"source.ts_ms"` | Exact MongoDB oplog timestamp |

### C. Consumers Tab — Check Lag
```
http://localhost:8082/ui/clusters/local-cluster/consumer-groups/isra-cdc-consumer-group
```
- **Lag = 0** → Python receiver consumed ALL messages (healthy)
- **Lag > 0** → Consumer is behind, still processing

### D. Kafka Connect Tab — Debezium Status
```
http://localhost:8082/ui/clusters/local-cluster/kafka-connect
```
- Shows connector `isra-mongodb-connector`
- Status should be `RUNNING` / Task `RUNNING`

---

## STEP 4 — Debezium REST API (Direct)

```
http://localhost:8083/connectors/isra-mongodb-connector/status
```

You'll see JSON like:
```json
{
  "name": "isra-mongodb-connector",
  "connector": { "state": "RUNNING" },
  "tasks": [{ "id": 0, "state": "RUNNING" }]
}
```

**Other useful Debezium endpoints:**
```
http://localhost:8083/connectors                              <- list all connectors
http://localhost:8083/connectors/isra-mongodb-connector/config   <- full config
```

---

## STEP 5 — PostgreSQL via pgAdmin (Visual Table View)

**URL:** http://localhost:5050  
**Login:** `admin@isra.com` / `admin`

### Navigate to the data:
```
Servers
  └── ISRA CDC - PostgreSQL          (click to connect, password: postgres)
        └── Databases
              └── isra_cdc
                    └── Schemas
                          └── public
                                └── Tables
                                      └── mongo_documents
                                            └── RIGHT-CLICK -> View/Edit Data -> All Rows
```

### Or use Query Tool (Tools menu → Query Tool):

```sql
-- 1. Overview: all collections synced and their row counts
SELECT collection_name,
       COUNT(*) AS total_docs,
       MAX(updated_at) AS last_synced
FROM mongo_documents
GROUP BY collection_name
ORDER BY last_synced DESC;

-- 2. Latest 20 records (live view of what just came in)
SELECT
    source_database,
    collection_name,
    document_id,
    document->>'status'     AS status,
    last_operation,
    source_timestamp,
    updated_at
FROM mongo_documents
ORDER BY updated_at DESC
LIMIT 20;

-- 3. Watch a specific test document
SELECT *
FROM mongo_documents
WHERE collection_name = 'Radio_broadcast_live'
ORDER BY updated_at DESC;

-- 4. Count by operation type
SELECT last_operation, COUNT(*) AS total
FROM mongo_documents
GROUP BY last_operation;
-- c = created, u = updated, r = snapshot read
```

---

## STEP 6 — MongoDB Direct (Check Source Data)

```powershell
# See what's in MongoDB right now
docker exec -it isra-mongo-source mongosh --port 27017 --eval "
  use ISRA_new;
  printjson(db.Radio_broadcast_live.find().toArray());
"

# See replica set oplog (last 5 entries)
docker exec -it isra-mongo-source mongosh --port 27017 --eval "
  use local;
  db.oplog.rs.find().sort({ts:-1}).limit(5).pretty();
"
```

---

## STEP 7 — All Logs at Once (Multi-tail)

To watch **ALL** containers simultaneously:

```powershell
# Watch all service logs at once
docker compose logs -f

# Watch only specific services
docker compose logs -f cdc-consumer kafka-connect

# Watch Debezium (Kafka Connect) connector logs
docker compose logs -f kafka-connect | Select-String "isra-mongodb"
```

---

## Quick Cheat Sheet — "What do I check if something goes wrong?"

| Problem | Where to Look |
|---|---|
| Data not appearing in PostgreSQL | `docker compose logs -f cdc-consumer` |
| No Kafka messages | Kafka UI Topics → check message count |
| Debezium not capturing | `http://localhost:8083/connectors/isra-mongodb-connector/status` |
| Consumer lag growing | Kafka UI Consumers → lag column |
| PostgreSQL data wrong | pgAdmin → Query Tool → run SQL above |
| Container crashed | `docker ps` → check STATUS column |

---

## Current System Health (as of now)

| Collection | Records Synced | Last Sync |
|---|---|---|
| Radio_broadcast_live | 1 | 2026-10-13 |
| Radio_foreigndistribution | 16,640 | 2026-10-12 |
| Radio_songregister | 1,000 | 2026-10-12 |
| Radio_priority | 3 | 2026-10-12 |
| Radio_masterradioproject | 5 | 2026-10-12 |
| cdc_test_pipeline | 1 | 2026-10-12 |

**Consumer Lag: 0** — All messages consumed and written to PostgreSQL.
