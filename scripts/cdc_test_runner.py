import sys
import time
import json
import argparse
from datetime import datetime, timezone

try:
    import pymongo
    import psycopg2
    from bson import ObjectId
except ImportError as e:
    print(f"[ERROR] Missing required python package: {e}")
    print("Please install requirements: pip install pymongo psycopg2-binary")
    sys.exit(1)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default="ISRA_new")
    parser.add_argument("--collection", default="cdc_test_pipeline")
    parser.add_argument("--test-id", required=True)
    parser.add_argument("--timeout", type=int, default=30)
    args = parser.parse_args()

    db_name = args.db
    coll_name = args.collection
    test_id = args.test_id
    timeout = args.timeout

    print(f"[STAGE 1/4] Connecting to Isolated MongoDB (localhost:27018) & PostgreSQL (localhost:5432)...")
    try:
        mongo_client = pymongo.MongoClient("mongodb://localhost:27018/?directConnection=true", serverSelectionTimeoutMS=4000)
        mongo_db = mongo_client[db_name]
        mongo_coll = mongo_db[coll_name]
        mongo_client.admin.command('ping')
        print("  -> Connected to MongoDB (isolated port 27018)")
    except Exception as e:
        print(f"[ERROR] Cannot connect to isolated MongoDB: {e}")
        sys.exit(1)

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
        print("  -> Connected to PostgreSQL (target database: isra_cdc)")
    except Exception as e:
        print(f"[ERROR] Cannot connect to target PostgreSQL: {e}")
        sys.exit(1)

    # --------------------------------------------------------------------------
    # Step 1: Insert Test Document into MongoDB
    # --------------------------------------------------------------------------
    print(f"\n[STAGE 2/4] Step 1: Inserting test document [ID: {test_id}] into MongoDB...")
    now_utc = datetime.now(timezone.utc)
    test_doc = {
        "_id": test_id,
        "title": "ISRA CDC Automated Verification Document",
        "system_status": "INITIAL_INSERT",
        "version": 1,
        "metadata": {
            "environment": "POC",
            "author": "Antigravity CDC Pipeline"
        },
        "created_at": now_utc
    }
    mongo_coll.insert_one(test_doc)
    print(f"  -> Inserted document into MongoDB collection '{db_name}.{coll_name}'.")

    # Step 2: Poll PostgreSQL for Document Synchronization
    print(f"  -> Waiting for Debezium + Kafka + Consumer to sync document to PostgreSQL (timeout: {timeout}s)...")
    start_time = time.time()
    synced = False
    row = None
    while time.time() - start_time < timeout:
        with pg_conn.cursor() as cur:
            cur.execute("""
                SELECT source_database, collection_name, document_id, document, last_operation, source_timestamp
                FROM mongo_documents
                WHERE source_database = %s AND collection_name = %s AND document_id = %s
            """, (db_name, coll_name, test_id))
            row = cur.fetchone()
            if row:
                synced = True
                break
        time.sleep(1)

    if not synced:
        print(f"[FAILED] Document [ID: {test_id}] was NOT found in PostgreSQL table 'mongo_documents' within {timeout}s.")
        sys.exit(1)

    elapsed_insert = time.time() - start_time
    print(f"  -> SUCCESS! Document found in PostgreSQL in {elapsed_insert:.2f}s!")
    print(f"     Last Operation: {row[4]} (c=create/r=read)")
    print(f"     Document in JSONB: title='{row[3].get('title')}', status='{row[3].get('system_status')}'")

    # --------------------------------------------------------------------------
    # Step 3: Update Document in MongoDB
    # --------------------------------------------------------------------------
    print(f"\n[STAGE 3/4] Step 2: Updating test document in MongoDB...")
    mongo_coll.update_one(
        {"_id": test_id},
        {"$set": {"system_status": "UPDATED_STATE_VERIFIED", "version": 2, "updated_at": datetime.now(timezone.utc)}}
    )
    print("  -> Updated document in MongoDB.")

    print(f"  -> Waiting for update event in PostgreSQL...")
    start_time = time.time()
    updated_synced = False
    while time.time() - start_time < timeout:
        with pg_conn.cursor() as cur:
            cur.execute("""
                SELECT document, last_operation, updated_at
                FROM mongo_documents
                WHERE source_database = %s AND collection_name = %s AND document_id = %s
            """, (db_name, coll_name, test_id))
            row = cur.fetchone()
            if row and row[0].get("system_status") == "UPDATED_STATE_VERIFIED":
                updated_synced = True
                break
        time.sleep(1)

    if not updated_synced:
        print(f"[FAILED] Update event for document [ID: {test_id}] was NOT synced to PostgreSQL within {timeout}s.")
        sys.exit(1)

    elapsed_update = time.time() - start_time
    print(f"  -> SUCCESS! Updated state synced to PostgreSQL in {elapsed_update:.2f}s!")
    print(f"     Updated JSONB status: '{row[0].get('system_status')}', version: {row[0].get('version')}")

    # --------------------------------------------------------------------------
    # Step 4: Delete Document in MongoDB
    # --------------------------------------------------------------------------
    print(f"\n[STAGE 4/4] Step 3: Deleting test document from MongoDB...")
    mongo_coll.delete_one({"_id": test_id})
    print("  -> Deleted document from MongoDB.")

    print(f"  -> Waiting for delete event to propagate to PostgreSQL...")
    start_time = time.time()
    delete_synced = False
    while time.time() - start_time < timeout:
        with pg_conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM mongo_documents
                WHERE source_database = %s AND collection_name = %s AND document_id = %s
            """, (db_name, coll_name, test_id))
            cnt = cur.fetchone()[0]
            if cnt == 0:
                delete_synced = True
                break
        time.sleep(1)

    if not delete_synced:
        print(f"[FAILED] Document [ID: {test_id}] was NOT removed from PostgreSQL within {timeout}s.")
        sys.exit(1)

    elapsed_delete = time.time() - start_time
    print(f"  -> SUCCESS! Document deleted from PostgreSQL in {elapsed_delete:.2f}s!")

    print(f"\nAll 3 operations (INSERT -> UPDATE -> DELETE) passed with full fidelity!")
    pg_conn.close()
    mongo_client.close()
    sys.exit(0)

if __name__ == "__main__":
    main()
