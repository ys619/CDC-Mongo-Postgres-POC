import os
from dataclasses import dataclass

@dataclass
class Config:
    kafka_bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    kafka_group_id: str = os.getenv("KAFKA_GROUP_ID", "isra-cdc-consumer-group")
    topic_pattern: str = os.getenv("TOPIC_PATTERN", r"^mongo_cdc\.(ISRA|ISRA_new)\..*")
    
    postgres_host: str = os.getenv("POSTGRES_HOST", "postgres")
    postgres_port: int = int(os.getenv("POSTGRES_PORT", "5432"))
    postgres_db: str = os.getenv("POSTGRES_DB", "isra_cdc")
    postgres_user: str = os.getenv("POSTGRES_USER", "postgres")
    postgres_password: str = os.getenv("POSTGRES_PASSWORD", "postgres")
    
    log_level: str = os.getenv("LOG_LEVEL", "INFO")
    log_file: str = os.getenv("LOG_FILE", "/app/logs/consumer.log")
    
    max_retries: int = int(os.getenv("MAX_RETRIES", "5"))
    retry_backoff: float = float(os.getenv("RETRY_BACKOFF", "2.0"))

config = Config()
