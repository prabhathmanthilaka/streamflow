from confluent_kafka import Consumer
from .common import create_avro_serializers

from .config import KAFKA_BOOTSTRAP_SERVERS, DLQ_TOPIC


def run():
    _, deserializer = create_avro_serializers()
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": "dlq-inspector",
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([DLQ_TOPIC])
    print(f"[DLQ] Listening on {DLQ_TOPIC}. Press Ctrl+C to stop.")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"[DLQ] Kafka error: {msg.error()}")
                continue
            order = deserializer(msg.value(), None)
            headers = {k: (v.decode("utf-8") if v is not None else None) for k, v in (msg.headers() or [])}
            print("[DLQ] Avro order:", order)
            print("[DLQ] Failure metadata:", headers)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == "__main__":
    run()
