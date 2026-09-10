import os

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
ORDERS_TOPIC = os.getenv("ORDERS_TOPIC", "orders")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "orders.DLQ")
CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "order-processor")
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
RETRY_BACKOFF_SECONDS = float(os.getenv("RETRY_BACKOFF_SECONDS", "1.0"))
