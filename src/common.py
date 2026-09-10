from pathlib import Path
import json
import time

from confluent_kafka import Producer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer

from .config import (
    KAFKA_BOOTSTRAP_SERVERS,
    SCHEMA_REGISTRY_URL,
    ORDERS_TOPIC,
    DLQ_TOPIC,
)

SCHEMA_PATH = Path(__file__).resolve().parent.parent / "schemas" / "order.avsc"


def load_schema() -> str:
    return SCHEMA_PATH.read_text(encoding="utf-8")


def schema_registry_client() -> SchemaRegistryClient:
    return SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})


def create_avro_serializers():
    schema = load_schema()
    serializer = AvroSerializer(schema_registry_client(), schema)
    deserializer = AvroDeserializer(schema_registry_client(), schema)
    return serializer, deserializer


def create_topics() -> None:
    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    topics = [
        NewTopic(ORDERS_TOPIC, num_partitions=1, replication_factor=1),
        NewTopic(DLQ_TOPIC, num_partitions=1, replication_factor=1),
    ]
    futures = admin.create_topics(topics)
    for topic, future in futures.items():
        try:
            future.result()
            print(f"[SETUP] Created topic: {topic}")
        except Exception as exc:
            if getattr(exc, "args", [None])[0] == KafkaError.TOPIC_ALREADY_EXISTS or "TopicAlreadyExists" in str(exc):
                print(f"[SETUP] Topic already exists: {topic}")
            else:
                raise


def wait_for_service(retries: int = 30, delay: float = 2.0) -> None:
    last_error = None
    for _ in range(retries):
        try:
            AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS}).list_topics(timeout=3)
            schema_registry_client().get_subjects()
            return
        except Exception as exc:
            last_error = exc
            time.sleep(delay)
    raise RuntimeError(f"Kafka/Schema Registry not ready: {last_error}")


def delivery_report(err, msg):
    if err is not None:
        print(f"[PRODUCER] Delivery failed: {err}")
    else:
        print(
            f"[PRODUCER] Delivered order to {msg.topic()} "
            f"partition={msg.partition()} offset={msg.offset()}"
        )
