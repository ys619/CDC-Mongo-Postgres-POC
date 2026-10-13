-- PostgreSQL Target Schema for MongoDB Change Data Capture (CDC)
-- Target Database: isra_cdc

CREATE TABLE IF NOT EXISTS mongo_documents (
    source_database TEXT NOT NULL,
    collection_name TEXT NOT NULL,
    document_id TEXT NOT NULL,
    document JSONB,
    last_operation TEXT,
    source_timestamp TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (source_database, collection_name, document_id)
);

-- Optimization & Inspection Indexes
CREATE INDEX IF NOT EXISTS idx_mongo_documents_lookup ON mongo_documents(source_database, collection_name);
CREATE INDEX IF NOT EXISTS idx_mongo_documents_updated ON mongo_documents(updated_at);
CREATE INDEX IF NOT EXISTS idx_mongo_documents_gin ON mongo_documents USING GIN (document);

COMMENT ON TABLE mongo_documents IS 'Synchronized MongoDB documents captured via Debezium CDC and Kafka';
COMMENT ON COLUMN mongo_documents.source_database IS 'Source MongoDB database name (e.g. ISRA_new)';
COMMENT ON COLUMN mongo_documents.collection_name IS 'Source MongoDB collection name';
COMMENT ON COLUMN mongo_documents.document_id IS 'Unique stable string identifier extracted from MongoDB _id';
COMMENT ON COLUMN mongo_documents.document IS 'Complete original document preserved as JSONB';
COMMENT ON COLUMN mongo_documents.last_operation IS 'Debezium operation code: r=snapshot read, c=create, u=update, d=delete';
COMMENT ON COLUMN mongo_documents.source_timestamp IS 'Event timestamp from MongoDB oplog / change stream in UTC';
COMMENT ON COLUMN mongo_documents.updated_at IS 'Local timestamp when record was upserted in PostgreSQL';
