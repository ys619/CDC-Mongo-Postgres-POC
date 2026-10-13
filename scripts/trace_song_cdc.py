#!/usr/bin/env python3
"""
trace_song_cdc.py
Demonstration script following the Change Data Capture (CDC) Fresher's Guide:
MongoDB (ISRA_new.Radio_songregister) -> Debezium -> Kafka -> Python Consumer -> PostgreSQL
"""

import sys
import time
import json
import argparse
from datetime import datetime, timezone

try:
    import pymongo
    import psycopg2
except ImportError as e:
    print(f"[ERROR] Missing required python package: {e}")
    print("Please install requirements: pip install pymongo psycopg2-binary")
    sys.exit(1)

def print_separator(title=""):
    print("\n" + "=" * 70)
    if title:
        print(f"  {title}")
        print("=" * 70)

def main():
    parser = argparse.ArgumentParser(description="Trace a single song through the CDC pipeline.")
    parser.add_argument("--song-id", default="song_999", help="Song ID to insert, update, and delete")
    parser.add_argument("--title", default="Midnight City", help="Song title")
    parser.add_argument("--artist", default="M83", help="Artist name")
    parser.add_argument("--timeout", type=int, default=30, help="Timeout in seconds for each CDC sync stage")
    parser.add_argument("--skip-delete", action="store_true", help="Keep the record without deleting")
    args = parser.parse_args()

    db_name = "ISRA_new"
    coll_name = "Radio_songregister"
    song_id = args.song_id
    song_title = args.title
    artist_name = args.artist
    timeout = args.timeout

    print_separator("EVENT-DRIVEN CDC DEMO: TRACING A SONG FROM START TO FINISH")
    print(f"  Source:      MongoDB replica set (localhost:27018)")
    print(f"  Database:    {db_name}")
    print(f"  Collection:  {coll_name}")
    print(f"  Kafka Topic: mongo_cdc.{db_name}.{coll_name}")
    print(f"  Target:      PostgreSQL (localhost:5432, table: mongo_documents)")
    print(f"  Kafka UI:    http://localhost:8082")

    # Connect to MongoDB
    print("\n[INIT] Connecting to MongoDB (localhost:27018)...")
    try:
        mongo_client = pymongo.MongoClient("mongodb://localhost:27018/?directConnection=true", serverSelectionTimeoutMS=4000)
        mongo_db = mongo_client[db_name]
        mongo_coll = mongo_db[coll_name]
        mongo_client.admin.command('ping')
        print("  -> Connected to MongoDB!")
    except Exception as e:
        print(f"[ERROR] Failed to connect to MongoDB: {e}")
        sys.exit(1)

    # Connect to PostgreSQL
    print("[INIT] Connecting to PostgreSQL (localhost:5432)...")
    try:
        pg_conn = psycopg2.connect(
            host="localhost",
            port=5432,
            dbname="isra_cdc",
            user="postgres",
            password="postgres",
            connect_timeout=4
        )
        pg_conn.autocommit = True
        print("  -> Connected to PostgreSQL (database: isra_cdc)!")
    except Exception as e:
        print(f"[ERROR] Failed to connect to PostgreSQL: {e}")
        sys.exit(1)

    # --------------------------------------------------------------------------
    # SCENE A: INSERTING A NEW SONG
    # --------------------------------------------------------------------------
    print_separator("SCENE A: INSERTING A NEW SONG (op = 'c')")
    # Clean up any leftover document with this ID first
    mongo_coll.delete_one({"_id": song_id})
    with pg_conn.cursor() as cur:
        cur.execute("DELETE FROM mongo_documents WHERE source_database = %s AND collection_name = %s AND document_id = %s",
                    (db_name, coll_name, song_id))

    new_song = {
        "_id": song_id,
        "song_title": song_title,
        "artist": artist_name,
        "status": "DRAFT",
        "created_at": datetime.now(timezone.utc).isoformat()
    }
    print(f"Step 1: Radio DJ registers song in MongoDB collection '{coll_name}':")
    print(json.dumps(new_song, indent=2))
    t0 = time.time()
    mongo_coll.insert_one(new_song)
    print("  -> Inserted into MongoDB. Oplog entry created!")

    print("\nStep 2-4: Debezium captures Oplog -> Publishes to Kafka -> Python Consumer Upserts to PG...")
    synced = False
    row = None
    while time.time() - t0 < timeout:
        with pg_conn.cursor() as cur:
            cur.execute("""
                SELECT source_database, collection_name, document_id, document, last_operation, source_timestamp, updated_at
                FROM mongo_documents
                WHERE source_database = %s AND collection_name = %s AND document_id = %s
            """, (db_name, coll_name, song_id))
            row = cur.fetchone()
            if row:
                synced = True
                break
        time.sleep(0.5)

    if not synced:
        print(f"[FAILED] Document [{song_id}] was NOT synced to PostgreSQL within {timeout}s.")
        sys.exit(1)

    latency_insert = time.time() - t0
    print(f"\nStep 5: Document successfully materialized in PostgreSQL in {latency_insert:.3f}s!")
    print(f"  PostgreSQL Record:")
    print(f"    - Database:        {row[0]}")
    print(f"    - Collection:      {row[1]}")
    print(f"    - Document ID:     {row[2]}")
    print(f"    - CDC Operation:   {row[4]} (c = create)")
    print(f"    - JSONB Data:      {json.dumps(row[3])}")
    print(f"    - Oplog Timestamp: {row[5]}")
    print(f"    - PG Sync Time:    {row[6]}")

    # Brief delay for demonstration observation
    time.sleep(2)

    # --------------------------------------------------------------------------
    # SCENE B: UPDATING THE SONG
    # --------------------------------------------------------------------------
    print_separator("SCENE B: UPDATING THE SONG (op = 'u')")
    print(f"Step 1: Program Director approves the song in MongoDB:")
    update_data = {
        "status": "APPROVED",
        "approved_by": "Program Director",
        "approved_at": datetime.now(timezone.utc).isoformat()
    }
    print(f"  Updating: {update_data}")
    t1 = time.time()
    mongo_coll.update_one(
        {"_id": song_id},
        {"$set": update_data}
    )
    print("  -> MongoDB document updated. New update event emitted to Oplog!")

    print("\nStep 2-4: Waiting for update event in Kafka & PostgreSQL...")
    update_synced = False
    updated_row = None
    while time.time() - t1 < timeout:
        with pg_conn.cursor() as cur:
            cur.execute("""
                SELECT document, last_operation, updated_at
                FROM mongo_documents
                WHERE source_database = %s AND collection_name = %s AND document_id = %s
            """, (db_name, coll_name, song_id))
            updated_row = cur.fetchone()
            if updated_row and updated_row[0].get("status") == "APPROVED":
                update_synced = True
                break
        time.sleep(0.5)

    if not update_synced:
        print(f"[FAILED] Update event for [{song_id}] was NOT synced to PostgreSQL within {timeout}s.")
        sys.exit(1)

    latency_update = time.time() - t1
    print(f"\nStep 5: Updated state synchronized in PostgreSQL in {latency_update:.3f}s!")
    print(f"  PostgreSQL Record:")
    print(f"    - CDC Operation: {updated_row[1]} (u = update)")
    print(f"    - Status:        {updated_row[0].get('status')}")
    print(f"    - Approved By:   {updated_row[0].get('approved_by')}")
    print(f"    - Full JSONB:    {json.dumps(updated_row[0])}")

    time.sleep(2)

    # --------------------------------------------------------------------------
    # SCENE C: DELETING THE SONG
    # --------------------------------------------------------------------------
    if args.skip_delete:
        print_separator("SCENE C SKIPPED (--skip-delete specified)")
        print(f"Record [{song_id}] remains in MongoDB and PostgreSQL for UI inspection.")
    else:
        print_separator("SCENE C: DELETING THE SONG (op = 'd')")
        print(f"Step 1: Radio station purges song [{song_id}] from MongoDB library...")
        t2 = time.time()
        mongo_coll.delete_one({"_id": song_id})
        print("  -> Deleted from MongoDB. Delete event emitted to Oplog!")

        print("\nStep 2-4: Waiting for delete event propagation to PostgreSQL...")
        delete_synced = False
        while time.time() - t2 < timeout:
            with pg_conn.cursor() as cur:
                cur.execute("""
                    SELECT COUNT(*) FROM mongo_documents
                    WHERE source_database = %s AND collection_name = %s AND document_id = %s
                """, (db_name, coll_name, song_id))
                cnt = cur.fetchone()[0]
                if cnt == 0:
                    delete_synced = True
                    break
            time.sleep(0.5)

        if not delete_synced:
            print(f"[FAILED] Delete event for [{song_id}] did NOT purge row in PostgreSQL within {timeout}s.")
            sys.exit(1)

        latency_delete = time.time() - t2
        print(f"\nStep 5: Record [{song_id}] successfully purged from PostgreSQL in {latency_delete:.3f}s!")

    print_separator("SUMMARY OF CDC PIPELINE EXECUTION")
    print(f"  [OK] Insert Latency: {latency_insert:.3f}s (op: 'c')")
    print(f"  [OK] Update Latency: {latency_update:.3f}s (op: 'u')")
    if not args.skip_delete:
        print(f"  [OK] Delete Latency: {latency_delete:.3f}s (op: 'd')")
    print(f"  [OK] Kafka Topic:   mongo_cdc.{db_name}.{coll_name}")
    print(f"  [OK] Kafka UI URL:  http://localhost:8082/ui/clusters/local-cluster/all-topics/mongo_cdc.{db_name}.{coll_name}/messages")
    print("=" * 70)

    pg_conn.close()
    mongo_client.close()

if __name__ == "__main__":
    main()
