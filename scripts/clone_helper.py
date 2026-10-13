import sys
import argparse
from datetime import datetime, timezone

try:
    import pymongo
    from bson import ObjectId
except ImportError:
    print("[ERROR] pymongo is required. Install via: pip install pymongo")
    sys.exit(1)

def get_clients(source_uri="mongodb://localhost:27017", target_uri="mongodb://localhost:27018/?directConnection=true"):
    src_client = pymongo.MongoClient(source_uri, serverSelectionTimeoutMS=3000)
    tgt_client = pymongo.MongoClient(target_uri, serverSelectionTimeoutMS=3000)
    return src_client, tgt_client

def inspect_source(src_client, source_db_name, filter_colls=None):
    try:
        dbs = src_client.list_database_names()
    except Exception as e:
        print(f"[ERROR] Cannot connect to source MongoDB on port 27017: {e}")
        sys.exit(1)

    print(f"      Available databases on source: {dbs}")
    if source_db_name not in dbs:
        print(f"      [NOTICE] Database '{source_db_name}' not found on source.")
        if "ISRA" in dbs:
            print(f"      [HINT] Found 'ISRA' database with existing data. You can clone from 'ISRA' into 'ISRA_new' using: --source-db ISRA")
        return []

    src_db = src_client[source_db_name]
    try:
        stats = src_db.command("dbStats")
        data_gb = stats.get("dataSize", 0) / (1024**3)
        storage_gb = stats.get("storageSize", 0) / (1024**3)
        print(f"      Source DB '{source_db_name}' Data Size: {data_gb:.3f} GB (Compressed Storage: {storage_gb:.3f} GB)")
    except Exception as e:
        print(f"      [WARNING] Could not retrieve dbStats: {e}")

    colls = src_db.list_collection_names()
    if filter_colls:
        colls = [c for c in colls if c in filter_colls]

    print(f"      Collections to process ({len(colls)}):")
    total_docs = 0
    for c in colls:
        cnt = src_db[c].estimated_document_count()
        total_docs += cnt
        if cnt > 100000:
            print(f"        - {c}: {cnt:,} documents (LARGE)")
        else:
            print(f"        - {c}: {cnt:,} documents")
    print(f"      Total estimated source documents: {total_docs:,}")
    return colls

def seed_sample_data(tgt_db):
    print("      Seeding sample collections into target database for CDC testing...")
    now = datetime.now(timezone.utc)
    sample_records = [
        {
            "_id": ObjectId(),
            "project_name": "ISRA Mumbai Radio 2026",
            "project_code": "RAD-MUM-001",
            "station": "Radio Mirchi 98.3",
            "city": "Mumbai",
            "frequency": 98.3,
            "status": "ACTIVE",
            "created_at": now
        },
        {
            "_id": ObjectId(),
            "project_name": "ISRA Delhi Digital 2026",
            "project_code": "DIG-DEL-002",
            "platform": "Spotify",
            "status": "IN_PROGRESS",
            "created_at": now
        }
    ]
    res = tgt_db["Radio_projectcreation"].insert_many(sample_records)
    print(f"      Inserted {len(res.inserted_ids)} records into 'Radio_projectcreation'")

def clone_data(src_client, tgt_client, source_db_name, target_db_name, filter_colls=None, sample_limit=0, seed_if_empty=False):
    src_db = src_client[source_db_name]
    tgt_db = tgt_client[target_db_name]

    colls = src_db.list_collection_names()
    if filter_colls:
        colls = [c for c in colls if c in filter_colls]

    if not colls:
        print(f"      [WARNING] No collections found in source database '{source_db_name}'.")
        if seed_if_empty:
            seed_sample_data(tgt_db)
            return
        else:
            print("      Use --seed-if-empty to automatically seed initial sample collections for testing.")
            return

    for coll_name in colls:
        src_coll = src_db[coll_name]
        tgt_coll = tgt_db[coll_name]
        total = src_coll.estimated_document_count()
        limit_desc = f" (limited to {sample_limit:,})" if sample_limit > 0 and total > sample_limit else ""
        print(f"      Cloning collection '{coll_name}' ({total:,} docs){limit_desc}...", end="", flush=True)

        cursor = src_coll.find({})
        if sample_limit > 0:
            cursor = cursor.limit(sample_limit)

        batch = []
        batch_size = 500
        copied = 0
        for doc in cursor:
            batch.append(doc)
            if len(batch) >= batch_size:
                tgt_coll.insert_many(batch, ordered=False)
                copied += len(batch)
                batch = []
        if batch:
            tgt_coll.insert_many(batch, ordered=False)
            copied += len(batch)

        print(f" Done ({copied:,} docs).")

def verify_target(tgt_client, target_db_name):
    tgt_db = tgt_client[target_db_name]
    colls = tgt_db.list_collection_names()
    print(f"      Target DB '{target_db_name}' collections: {len(colls)}")
    for c in colls:
        cnt = tgt_db[c].count_documents({})
        print(f"        - {c}: {cnt:,} documents")

def main():
    parser = argparse.ArgumentParser(description="MongoDB Logical Clone Helper")
    parser.add_argument("--inspect", action="store_true")
    parser.add_argument("--clone", action="store_true")
    parser.add_argument("--verify", action="store_true")
    parser.add_argument("--source-db", default="ISRA_new")
    parser.add_argument("--target-db", default="ISRA_new")
    parser.add_argument("--collections", default="")
    parser.add_argument("--sample-limit", type=int, default=0)
    parser.add_argument("--seed-if-empty", action="store_true")
    args = parser.parse_args()

    filter_colls = [c.strip() for c in args.collections.split(",") if c.strip()] if args.collections else None
    src_client, tgt_client = get_clients()

    if args.inspect:
        inspect_source(src_client, args.source_db, filter_colls)
    elif args.clone:
        clone_data(src_client, tgt_client, args.source_db, args.target_db, filter_colls, args.sample_limit, args.seed_if_empty)
    elif args.verify:
        verify_target(tgt_client, args.target_db)

if __name__ == "__main__":
    main()
