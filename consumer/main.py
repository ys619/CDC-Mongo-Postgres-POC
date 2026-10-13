import os
import sys
import json
import time
import signal
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

import psycopg2
from psycopg2.extras import Json
from confluent_kafka import Consumer, KafkaError, KafkaException

from config import config

# Setup structured logging
log_format = "%(asctime)s [%(levelname)s] [%(name)s] %(message)s"
logging.basicConfig(level=getattr(logging, config.log_level.upper(), logging.INFO), format=log_format)
logger = logging.getLogger("cdc-consumer")

# Add file handler if log directory is available
try:
    log_dir = os.path.dirname(config.log_file)
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)
    file_handler = logging.FileHandler(config.log_file, encoding="utf-8")
    file_handler.setFormatter(logging.Formatter(log_format))
    logger.addHandler(file_handler)
except Exception as e:
    logger.warning("Could not initialize file logger: %s", e)

running = True

def signal_handler(sig, frame):
    global running
    logger.info("Received termination signal %s. Initiating graceful shutdown...", sig)
    running = False

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def get_postgres_connection():
    for attempt in range(1, config.max_retries + 1):
        try:
            conn = psycopg2.connect(
                host=config.postgres_host,
                port=config.postgres_port,
                dbname=config.postgres_db,
                user=config.postgres_user,
                password=config.postgres_password,
                connect_timeout=5
            )
            conn.autocommit = False
            return conn
        except Exception as e:
            logger.warning("PostgreSQL connection attempt %d/%d failed: %s", attempt, config.max_retries, e)
            if attempt == config.max_retries:
                raise
            time.sleep(config.retry_backoff * attempt)

def extract_id_string(raw_id: Any) -> str:
    """Converts any MongoDB _id representation into a deterministic, stable string."""
    if raw_id is None:
        return ""
    if isinstance(raw_id, str):
        raw_str = raw_id.strip()
        # Handle JSON-encoded values or quoted string literals (e.g. '"doc_123"' or '{"$oid": "..."}')
        if (raw_str.startswith("{") and raw_str.endswith("}")) or (raw_str.startswith('"') and raw_str.endswith('"')):
            try:
                parsed = json.loads(raw_str)
                return extract_id_string(parsed)
            except Exception:
                pass
        return raw_str
    if isinstance(raw_id, (int, float)):
        return str(raw_id)
    if isinstance(raw_id, dict):
        if "$oid" in raw_id:
            return str(raw_id["$oid"])
        if "_id" in raw_id:
            return extract_id_string(raw_id["_id"])
        return json.dumps(raw_id, sort_keys=True)
    return str(raw_id)

def parse_document_payload(payload: Any) -> Optional[Dict[str, Any]]:
    """Parses document payload which can be either a JSON string or dict."""
    if payload is None:
        return None
    if isinstance(payload, dict):
        return payload
    if isinstance(payload, str):
        try:
            return json.loads(payload)
        except Exception as e:
            logger.error("Failed to parse document payload as JSON: %s (payload: %s)", e, payload[:200] if len(payload) > 200 else payload)
            return None
    return None

def extract_delete_document_id(key_obj: Any, val_obj: Dict[str, Any]) -> str:
    """
    Extracts the deleted document ID from:
    1. val_obj['filter']
    2. val_obj['before']
    3. key_obj
    """
    # 1. Filter in value
    if val_obj and "filter" in val_obj and val_obj["filter"]:
        filter_doc = parse_document_payload(val_obj["filter"])
        if filter_doc and "_id" in filter_doc:
            return extract_id_string(filter_doc["_id"])
    
    # 2. Before-image in value
    if val_obj and "before" in val_obj and val_obj["before"]:
        before_doc = parse_document_payload(val_obj["before"])
        if before_doc and "_id" in before_doc:
            return extract_id_string(before_doc["_id"])
            
    # 3. Key object
    if key_obj:
        if isinstance(key_obj, dict):
            if "id" in key_obj:
                return extract_id_string(key_obj["id"])
            if "_id" in key_obj:
                return extract_id_string(key_obj["_id"])
        elif isinstance(key_obj, str):
            try:
                parsed = json.loads(key_obj)
                if isinstance(parsed, dict):
                    return extract_delete_document_id(parsed, val_obj)
            except Exception:
                pass
            return extract_id_string(key_obj)
                
    return ""

def process_cdc_event(cursor, topic: str, key_bytes: Optional[bytes], value_bytes: Optional[bytes]) -> bool:
    """
    Processes a single Kafka CDC event and performs the appropriate PostgreSQL operation.
    Returns True if an operation was performed or safely skipped.
    """
    # Handle Kafka tombstone record (null value)
    if value_bytes is None:
        logger.debug("Received tombstone record on topic %s. Skipping.", topic)
        return True

    # Parse value JSON
    try:
        val_obj = json.loads(value_bytes.decode("utf-8"))
    except Exception as e:
        logger.error("Failed to decode value JSON on topic %s: %s", topic, e)
        return False

    # Extract Debezium envelope
    op = val_obj.get("op")
    source = val_obj.get("source", {})
    source_db = source.get("db") or "ISRA_new"
    collection = source.get("collection")
    
    # Fallback collection extraction from topic if missing in source
    if not collection and topic.startswith("mongo_cdc."):
        parts = topic.split(".", 2)
        if len(parts) == 3:
            collection = parts[2]

    if not collection:
        logger.warning("Could not identify collection for event on topic %s: %s", topic, val_obj)
        return True

    # Parse source timestamp
    ts_ms = source.get("ts_ms") or val_obj.get("ts_ms")
    source_ts = None
    if ts_ms:
        try:
            source_ts = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
        except Exception:
            pass

    # Parse key JSON if present
    key_obj = None
    if key_bytes:
        try:
            key_obj = json.loads(key_bytes.decode("utf-8"))
        except Exception:
            key_obj = key_bytes.decode("utf-8", errors="ignore")

    # Handle Deletes (op == 'd')
    if op == "d":
        doc_id = extract_delete_document_id(key_obj, val_obj)
        if not doc_id:
            logger.error("Cannot resolve document_id for delete event on %s.%s", source_db, collection)
            return False

        query = """
        DELETE FROM mongo_documents
        WHERE source_database = %s
          AND collection_name = %s
          AND document_id = %s
          AND (source_timestamp IS NULL OR %s IS NULL OR %s >= source_timestamp);
        """
        cursor.execute(query, (source_db, collection, doc_id, source_ts, source_ts))
        logger.info("DELETE processed: %s.%s [ID: %s] (rows affected: %d)", source_db, collection, doc_id, cursor.rowcount)
        return True

    # Handle Read (snapshot), Create (insert), Update (replace/update)
    elif op in ("r", "c", "u"):
        raw_after = val_obj.get("after")
        after_doc = parse_document_payload(raw_after)
        
        if not after_doc:
            logger.warning("Event op '%s' on %s.%s has empty 'after' document. Skipping.", op, source_db, collection)
            return True

        raw_id = after_doc.get("_id")
        doc_id = extract_id_string(raw_id)
        if not doc_id:
            # Fallback to key
            doc_id = extract_delete_document_id(key_obj, val_obj)

        if not doc_id:
            logger.error("Cannot determine document_id for op '%s' on %s.%s", op, source_db, collection)
            return False

        # Idempotent Upsert with newer-state timestamp protection
        query = """
        INSERT INTO mongo_documents (
            source_database, collection_name, document_id, document,
            last_operation, source_timestamp, updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (source_database, collection_name, document_id)
        DO UPDATE SET
            document = EXCLUDED.document,
            last_operation = EXCLUDED.last_operation,
            source_timestamp = EXCLUDED.source_timestamp,
            updated_at = NOW()
        WHERE mongo_documents.source_timestamp IS NULL 
           OR EXCLUDED.source_timestamp IS NULL
           OR EXCLUDED.source_timestamp >= mongo_documents.source_timestamp;
        """
        cursor.execute(query, (
            source_db,
            collection,
            doc_id,
            Json(after_doc),
            op,
            source_ts
        ))
        logger.info("UPSERT (%s) processed: %s.%s [ID: %s]", op, source_db, collection, doc_id)
        return True

    else:
        logger.info("Ignored unhandled Debezium op '%s' on %s.%s", op, source_db, collection)
        return True

def main():
    global running
    logger.info("Starting Event-Driven CDC Consumer...")
    logger.info("Kafka Broker: %s", config.kafka_bootstrap_servers)
    logger.info("Consumer Group: %s", config.kafka_group_id)
    logger.info("Topic Pattern: %s", config.topic_pattern)
    logger.info("Target Postgres: %s:%s/%s", config.postgres_host, config.postgres_port, config.postgres_db)

    # Initialize Postgres
    pg_conn = None
    while running and pg_conn is None:
        try:
            pg_conn = get_postgres_connection()
            logger.info("Connected to PostgreSQL target database successfully.")
        except Exception as e:
            logger.error("Initial PostgreSQL connection failed: %s. Retrying in 5s...", e)
            time.sleep(5)

    # Initialize Kafka Consumer
    consumer_conf = {
        "bootstrap.servers": config.kafka_bootstrap_servers,
        "group.id": config.kafka_group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "topic.metadata.refresh.interval.ms": 5000,  # Auto-discover new collections quickly
        "session.timeout.ms": 30000,
        "max.poll.interval.ms": 300000
    }

    consumer = Consumer(consumer_conf)
    
    # Regex subscription dynamically picks up all current and newly created collection topics
    consumer.subscribe([config.topic_pattern])
    logger.info("Subscribed to pattern: %s", config.topic_pattern)

    poll_count = 0
    while running:
        try:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                elif msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                    # Expected initially until CDC topics are generated
                    time.sleep(1)
                    continue
                else:
                    logger.error("Kafka error: %s", msg.error())
                    time.sleep(1)
                    continue

            topic = msg.topic()
            key = msg.key()
            value = msg.value()

            # Execute DB transaction with retries
            success = False
            while running and not success:
                try:
                    with pg_conn.cursor() as cur:
                        ok = process_cdc_event(cur, topic, key, value)
                        if ok:
                            pg_conn.commit()
                            # Commit Kafka offset ONLY after Postgres transaction succeeds
                            consumer.commit(message=msg, asynchronous=False)
                            success = True
                        else:
                            # Re-raise to trigger rollback and retry
                            pg_conn.rollback()
                            logger.error("Could not process message. Retrying in 2s...")
                            time.sleep(2)
                except (psycopg2.OperationalError, psycopg2.DatabaseError) as db_err:
                    logger.warning("PostgreSQL error during event processing: %s. Reconnecting...", db_err)
                    try:
                        if pg_conn:
                            pg_conn.close()
                    except Exception:
                        pass
                    pg_conn = None
                    time.sleep(2)
                    pg_conn = get_postgres_connection()

            poll_count += 1
            if poll_count % 100 == 0:
                logger.info("Processed %d CDC events so far.", poll_count)

        except Exception as e:
            logger.error("Unexpected error in consumer loop: %s", e, exc_info=True)
            time.sleep(1)

    logger.info("Closing Kafka consumer and PostgreSQL connection...")
    try:
        consumer.close()
    except Exception:
        pass
    try:
        if pg_conn:
            pg_conn.close()
    except Exception:
        pass
    logger.info("CDC Consumer shutdown complete.")

if __name__ == "__main__":
    main()
