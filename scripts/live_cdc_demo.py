"""
live_cdc_demo.py
================
LIVE End-to-End CDC Pipeline Demonstration
 
Architecture:
  MongoDB (Change Stream) --> Debezium (Oplog Watcher) --> Kafka (Event Bus) --> Python CDC Receiver --> PostgreSQL

Covers:
  1. Insert a rich Radio Broadcast record into MongoDB
  2. Watch MongoDB Change Stream (API) capture the event in real-time
  3. Monitor Debezium connector observing the Oplog via Kafka Connect REST API
  4. Show Kafka Broker / Topic / Consumer stats before and after
  5. Confirm the Python CDC Receiver cleaned, transformed, and wrote to PostgreSQL
  6. Print all live portal links (Kafka UI, Debezium, PostgreSQL, MongoDB)

Run:
  python scripts/live_cdc_demo.py

Dependencies: pymongo psycopg2-binary requests
"""

import sys
import time
import json
import requests
import threading
from datetime import datetime, timezone

# ── colour helpers (Windows-safe ANSI) ──────────────────────────────────────
CYAN    = "\033[96m"
GREEN   = "\033[92m"
YELLOW  = "\033[93m"
RED     = "\033[91m"
MAGENTA = "\033[95m"
BOLD    = "\033[1m"
RESET   = "\033[0m"

def banner(title, colour=CYAN):
    line = "=" * 72
    print(f"\n{colour}{BOLD}{line}{RESET}")
    print(f"{colour}{BOLD}  {title}{RESET}")
    print(f"{colour}{BOLD}{line}{RESET}")

def step(n, total, msg, colour=CYAN):
    print(f"\n{colour}{BOLD}[STEP {n}/{total}]{RESET} {msg}")

def ok(msg):   print(f"  {GREEN}[OK]{RESET}    {msg}")
def info(msg): print(f"  {CYAN}[--]{RESET}    {msg}")
def warn(msg): print(f"  {YELLOW}[!!]{RESET}    {msg}")
def err(msg):  print(f"  {RED}[ERR]{RESET}   {msg}")

# ── config ───────────────────────────────────────────────────────────────────
MONGO_URI       = "mongodb://localhost:27018/?directConnection=true"
PG_DSN          = dict(host="localhost", port=5432, dbname="isra_cdc",
                       user="postgres", password="postgres", connect_timeout=4)
KAFKA_UI        = "http://localhost:8082"
KAFKA_CONNECT   = "http://localhost:8083"
CONNECTOR_NAME  = "isra-mongodb-connector"
DB_NAME         = "ISRA_new"
COLL_NAME       = "Radio_broadcast_live"
TOPIC           = f"mongo_cdc.{DB_NAME}.{COLL_NAME}"
CONSUMER_GROUP  = "isra-cdc-consumer-group"
TIMEOUT         = 30   # seconds to wait for each CDC stage

# ── helper: Kafka UI API ──────────────────────────────────────────────────────
def kafka_ui_get(path):
    try:
        r = requests.get(f"{KAFKA_UI}{path}", timeout=6)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        return {"error": str(e)}

def kafka_connect_get(path):
    try:
        r = requests.get(f"{KAFKA_CONNECT}{path}", timeout=6)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        return {"error": str(e)}

# ── change stream watcher (runs in background thread) ─────────────────────────
CHANGE_EVENTS = []

def watch_change_stream(mongo_coll, stop_event, max_events=3):
    """Opens a MongoDB Change Stream and records events."""
    try:
        pipeline = [{"$match": {"operationType": {"$in": ["insert", "update", "delete"]}}}]
        with mongo_coll.watch(pipeline, full_document="updateLookup") as stream:
            while not stop_event.is_set() and len(CHANGE_EVENTS) < max_events:
                change = stream.try_next()
                if change:
                    CHANGE_EVENTS.append(change)
                else:
                    time.sleep(0.1)
    except Exception as e:
        # Stream closed after stop_event set is normal
        pass

# ── main demo ─────────────────────────────────────────────────────────────────
def main():
    try:
        import pymongo
        import psycopg2
    except ImportError as e:
        err(f"Missing package: {e}")
        err("pip install pymongo psycopg2-binary requests")
        sys.exit(1)

    # Enable ANSI colours on Windows
    try:
        import os; os.system("")
    except Exception:
        pass

    # ── 0. Header & Links ────────────────────────────────────────────────────
    banner("LIVE CDC PIPELINE DEMO  |  ISRA Radio Broadcast System", MAGENTA)
    print(f"""
  {BOLD}Architecture:{RESET}
    MongoDB Change Stream
      --> Debezium Oplog Watcher (Kafka Connect)
        --> Apache Kafka Broker  (Event Bus)
          --> Python CDC Receiver  (Clean, Transform, Write)
            --> PostgreSQL  (Destination DB)

  {BOLD}{YELLOW}Portal Links{RESET} {YELLOW}(open these in your browser now):{RESET}
  ---------------------------------------------------------------
  {GREEN}Kafka UI (Brokers, Topics, Consumers, Connect):{RESET}
    http://localhost:8082

  {GREEN}Kafka UI - Brokers tab:{RESET}
    http://localhost:8082/ui/clusters/local-cluster/brokers

  {GREEN}Kafka UI - Topics tab:{RESET}
    http://localhost:8082/ui/clusters/local-cluster/all-topics

  {GREEN}Kafka UI - Consumers tab:{RESET}
    http://localhost:8082/ui/clusters/local-cluster/consumer-groups/{CONSUMER_GROUP}

  {GREEN}Kafka Connect (Debezium) REST API:{RESET}
    http://localhost:8083/connectors/{CONNECTOR_NAME}/status

  {GREEN}Debezium Connector Config:{RESET}
    http://localhost:8083/connectors/{CONNECTOR_NAME}/config

  {GREEN}MongoDB Shell (replica set status):{RESET}
    docker exec -it isra-mongo-source mongosh --port 27017 --eval "rs.status()"

  {GREEN}PostgreSQL (psql client):{RESET}
    docker exec -it isra-postgres psql -U postgres -d isra_cdc -c "SELECT * FROM mongo_documents WHERE collection_name='{COLL_NAME}' ORDER BY updated_at DESC LIMIT 5;"
  ---------------------------------------------------------------
""")
    input(f"  {CYAN}Press ENTER to start the live demo...{RESET}")

    # ── STEP 1: Connect ────────────────────────────────────────────────────────
    step(1, 7, "Connecting to MongoDB & PostgreSQL", CYAN)
    try:
        mongo_client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=4000)
        mongo_db     = mongo_client[DB_NAME]
        mongo_coll   = mongo_db[COLL_NAME]
        mongo_client.admin.command("ping")
        ok(f"MongoDB connected  (localhost:27018 -> replica set rs0)")
    except Exception as e:
        err(f"MongoDB connection failed: {e}")
        sys.exit(1)

    try:
        pg_conn = psycopg2.connect(**PG_DSN)
        pg_conn.autocommit = True
        ok(f"PostgreSQL connected (localhost:5432 -> isra_cdc)")
    except Exception as e:
        err(f"PostgreSQL connection failed: {e}")
        sys.exit(1)

    # ── STEP 2: Kafka Infra Status Before ─────────────────────────────────────
    step(2, 7, "Kafka Infrastructure Status  (BEFORE insert)", CYAN)

    brokers = kafka_ui_get("/api/clusters/local-cluster/brokers")
    if isinstance(brokers, list):
        for b in brokers:
            ok(f"Broker ID: {b.get('id')} | Host: {b.get('host')}:{b.get('port')} | Partitions Leading: {b.get('partitionsLeader')}")
    else:
        warn(f"Broker info: {brokers}")

    connector = kafka_connect_get(f"/connectors/{CONNECTOR_NAME}/status")
    c_state   = connector.get("connector", {}).get("state", "?")
    t_state   = connector.get("tasks", [{}])[0].get("state", "?")
    ok(f"Debezium Connector: {CONNECTOR_NAME}")
    ok(f"  Connector State: {c_state}  |  Task[0] State: {t_state}")
    ok(f"  Monitoring Oplog on: {DB_NAME} collections via Change Stream")

    cg = kafka_ui_get(f"/api/clusters/local-cluster/consumer-groups/{CONSUMER_GROUP}")
    lag_before = cg.get("consumerLag", "?")
    ok(f"Consumer Group: {CONSUMER_GROUP}")
    ok(f"  State: {cg.get('state','?')} | Topics: {cg.get('topics','?')} | Lag (Before): {lag_before}")

    # ── STEP 3: Open MongoDB Change Stream ─────────────────────────────────────
    step(3, 7, f"Opening MongoDB Change Stream on [{DB_NAME}.{COLL_NAME}]", CYAN)
    info("MongoDB Change Stream API watches for real-time document changes in the collection.")
    info("The stream uses the oplog (operation log) under the hood.")
    info("Debezium ALSO watches this same oplog concurrently via Kafka Connect.")

    # Clean up any old demo docs
    mongo_coll.delete_many({"demo_tag": "live_cdc_demo"})
    with pg_conn.cursor() as cur:
        cur.execute("""
            DELETE FROM mongo_documents
            WHERE source_database = %s AND collection_name = %s AND document::text LIKE '%%live_cdc_demo%%'
        """, (DB_NAME, COLL_NAME))

    stop_event  = threading.Event()
    watcher     = threading.Thread(target=watch_change_stream, args=(mongo_coll, stop_event, 3), daemon=True)
    watcher.start()
    ok("Change Stream OPENED - now listening for events...")
    print(f"\n  {YELLOW}>> Open Kafka UI now: http://localhost:8082/ui/clusters/local-cluster/all-topics <<{RESET}")

    # ── STEP 4: Insert Document ───────────────────────────────────────────────
    step(4, 7, "INSERT - Radio DJ adds a new broadcast record to MongoDB", CYAN)
    doc_id  = f"broadcast_{int(time.time())}"
    now_utc = datetime.now(timezone.utc).isoformat()
    broadcast_doc = {
        "_id": doc_id,
        "demo_tag":     "live_cdc_demo",
        "type":         "LIVE_BROADCAST",
        "station":      "ISRA FM 104.5",
        "programme":    "Morning Masala",
        "host":         "Ravi Kumar",
        "language":     "Hindi",
        "genre":        "Bollywood",
        "air_time":     "06:00 - 09:00",
        "region":       "North India",
        "status":       "SCHEDULED",
        "listeners":    0,
        "created_at":   now_utc,
    }
    print(f"\n  {BOLD}Inserting document into MongoDB [{DB_NAME}.{COLL_NAME}]:{RESET}")
    print("  " + json.dumps(broadcast_doc, indent=4).replace("\n", "\n  "))

    t_insert = time.time()
    mongo_coll.insert_one(broadcast_doc)
    ok(f"MongoDB INSERT complete. Document ID: {doc_id}")
    info(f"  Oplog entry written by MongoDB at {now_utc}")

    # Wait briefly to let Change Stream event arrive
    time.sleep(3)
    if CHANGE_EVENTS:
        ev = CHANGE_EVENTS[0]
        print(f"\n  {GREEN}{BOLD}MongoDB Change Stream captured INSERT event:{RESET}")
        print(f"  operationType : {ev.get('operationType')}")
        print(f"  ns            : {ev.get('ns')}")
        print(f"  documentKey   : {ev.get('documentKey')}")
        fd = ev.get("fullDocument", {})
        print(f"  fullDocument  : {json.dumps({k: str(v) for k, v in fd.items() if k != '_id'}, indent=4).replace(chr(10), chr(10) + '  ')}")
        ok("Change Stream event received INSTANTLY from MongoDB oplog!")
    else:
        info("Change Stream event not yet captured (will arrive soon via Debezium path)")

    info(f"\n  Debezium is also watching this same oplog entry right now...")
    info(f"  It will package this as a Kafka message and publish to topic:")
    info(f"    {TOPIC}")
    print(f"\n  {YELLOW}>> Watch Kafka UI topic now: http://localhost:8082/ui/clusters/local-cluster/all-topics/{TOPIC}/messages <<{RESET}")

    # ── STEP 5: Wait for PostgreSQL Sync (CDC full trip) ──────────────────────
    step(5, 7, "Waiting: Debezium -> Kafka -> Python Receiver -> PostgreSQL", CYAN)
    info(f"Polling PostgreSQL table [mongo_documents] for doc_id = '{doc_id}' ...")
    info(f"Timeout: {TIMEOUT}s")

    synced = False
    row    = None
    while time.time() - t_insert < TIMEOUT:
        with pg_conn.cursor() as cur:
            cur.execute("""
                SELECT source_database, collection_name, document_id,
                       document, last_operation, source_timestamp, updated_at
                FROM mongo_documents
                WHERE source_database = %s AND collection_name = %s AND document_id = %s
            """, (DB_NAME, COLL_NAME, doc_id))
            row = cur.fetchone()
        if row:
            synced = True
            break
        sys.stdout.write(".")
        sys.stdout.flush()
        time.sleep(0.5)
    print()

    if not synced:
        err(f"Document NOT synced to PostgreSQL within {TIMEOUT}s.")
        err("  Check: docker compose logs cdc-consumer")
        sys.exit(1)

    latency_insert = time.time() - t_insert
    ok(f"Document materialized in PostgreSQL in {latency_insert:.3f}s!")
    print(f"\n  {GREEN}{BOLD}PostgreSQL Record (mongo_documents table):{RESET}")
    print(f"    source_database  : {row[0]}")
    print(f"    collection_name  : {row[1]}")
    print(f"    document_id      : {row[2]}")
    print(f"    last_operation   : {row[4]}  ('c' = create/insert)")
    print(f"    source_timestamp : {row[5]}")
    print(f"    pg_sync_time     : {row[6]}")
    print(f"    JSONB document   : {json.dumps(row[3], indent=4).replace(chr(10), chr(10) + '    ')}")

    # ── UPDATE ────────────────────────────────────────────────────────────────
    step(6, 7, "UPDATE - Broadcast goes LIVE and listeners spike", CYAN)
    update_payload = {"status": "ON_AIR", "listeners": 42800, "updated_at": datetime.now(timezone.utc).isoformat()}
    print(f"\n  Updating MongoDB document with: {update_payload}")
    t_update = time.time()
    mongo_coll.update_one(
        {"_id": doc_id},
        {"$set": update_payload}
    )
    ok(f"MongoDB UPDATE complete. Oplog entry emitted.")

    # Wait for PostgreSQL sync
    update_synced = False
    while time.time() - t_update < TIMEOUT:
        with pg_conn.cursor() as cur:
            cur.execute("""
                SELECT document, last_operation, updated_at FROM mongo_documents
                WHERE source_database = %s AND collection_name = %s AND document_id = %s
            """, (DB_NAME, COLL_NAME, doc_id))
            r = cur.fetchone()
        if r and r[0].get("status") == "ON_AIR":
            update_synced = True
            break
        sys.stdout.write(".")
        sys.stdout.flush()
        time.sleep(0.5)
    print()

    if update_synced:
        latency_update = time.time() - t_update
        ok(f"UPDATE synced to PostgreSQL in {latency_update:.3f}s!")
        ok(f"  status    = {r[0].get('status')}")
        ok(f"  listeners = {r[0].get('listeners')}")
        ok(f"  op code   = '{r[1]}'  ('u' = update)")
    else:
        warn("UPDATE not synced within timeout.")

    # ── FINAL KAFKA / CONSUMER STATS ─────────────────────────────────────────
    step(7, 7, "Final Kafka UI Stats (AFTER all events)", GREEN)

    cg_after = kafka_ui_get(f"/api/clusters/local-cluster/consumer-groups/{CONSUMER_GROUP}")
    lag_after = cg_after.get("consumerLag", "?")
    ok(f"Consumer Group: {CONSUMER_GROUP}")
    ok(f"  State: {cg_after.get('state','?')} | Lag (After): {lag_after}")

    for p in cg_after.get("partitions", []):
        ok(f"  Topic: {p.get('topic')}")
        ok(f"    currentOffset={p.get('currentOffset')} | endOffset={p.get('endOffset')} | lag={p.get('consumerLag')}")

    # Show Kafka message count for our topic
    topic_info = kafka_ui_get(f"/api/clusters/local-cluster/topics/{TOPIC}")
    if "error" not in topic_info:
        partitions = topic_info.get("partitions", [{}])
        offset_max = partitions[0].get("offsetMax", "?") if partitions else "?"
        ok(f"\n  Kafka Topic: {TOPIC}")
        ok(f"    Total messages in topic (offset max): {offset_max}")
        ok(f"    Direct link: http://localhost:8082/ui/clusters/local-cluster/all-topics/{TOPIC}/messages")
    else:
        info(f"Topic will appear after first message: {TOPIC}")

    # ── Summary ───────────────────────────────────────────────────────────────
    stop_event.set()

    banner("LIVE CDC PIPELINE COMPLETED SUCCESSFULLY", GREEN)
    print(f"""
  What just happened (step by step):
  -----------------------------------
  1. MongoDB INSERT   -> Oplog entry written by replica set (rs0)
  2. Change Stream    -> Your app (this script) received the event INSTANTLY
  3. Debezium         -> Read the same oplog, packaged as Kafka message
  4. Kafka Broker     -> Stored the event in topic [{TOPIC}]
  5. Python Receiver  -> Consumed message, cleaned/transformed BSON -> JSONB
  6. PostgreSQL       -> Upserted into [mongo_documents] table
  7. UPDATE event     -> Same full pipeline repeated for the update

  Latency:
    INSERT end-to-end : {latency_insert:.3f}s
    UPDATE end-to-end : {latency_update:.3f}s

  {BOLD}{YELLOW}Open these in your browser to inspect everything:{RESET}

  Kafka UI Dashboard  : http://localhost:8082
  Brokers             : http://localhost:8082/ui/clusters/local-cluster/brokers
  Topics              : http://localhost:8082/ui/clusters/local-cluster/all-topics
  THIS Topic Messages : http://localhost:8082/ui/clusters/local-cluster/all-topics/{TOPIC}/messages
  Consumers (Lag=0)   : http://localhost:8082/ui/clusters/local-cluster/consumer-groups/{CONSUMER_GROUP}
  Kafka Connect       : http://localhost:8082/ui/clusters/local-cluster/kafka-connect
  Debezium REST API   : http://localhost:8083/connectors/{CONNECTOR_NAME}/status

  PostgreSQL Query:
    docker exec -it isra-postgres psql -U postgres -d isra_cdc -c \\
      "SELECT document_id, document->>'status', document->>'listeners', last_operation, updated_at FROM mongo_documents WHERE collection_name='{COLL_NAME}' ORDER BY updated_at DESC;"

  MongoDB Query:
    docker exec -it isra-mongo-source mongosh --port 27017 --eval \\
      "use {DB_NAME}; printjson(db.{COLL_NAME}.find().toArray())"

{"=" * 72}
""")

    pg_conn.close()
    mongo_client.close()


if __name__ == "__main__":
    main()
