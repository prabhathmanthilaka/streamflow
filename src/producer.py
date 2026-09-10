import argparse
import random
import time

from confluent_kafka import Producer
from confluent_kafka.serialization import SerializationContext, MessageField

from .common import create_avro_serializers, delivery_report, create_topics
from .config import KAFKA_BOOTSTRAP_SERVERS, ORDERS_TOPIC


def build_order(order_id: str, product: str | None = None, price: float | None = None):
    return {
        "orderId": order_id,
        "product": product or f"Item{random.randint(1, 5)}",
        "price": float(price if price is not None else round(random.uniform(10, 500), 2)),
    }


def produce_orders(count: int, interval: float):
    serializer, _ = create_avro_serializers()
    producer = Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "acks": "all",
        "enable.idempotence": True,
    })

    # Normal messages demonstrate aggregation. The final two messages deliberately
    # exercise the retry and DLQ paths without changing the required Order schema.
    demo_orders = [build_order(str(1001 + i)) for i in range(count)]
    demo_orders.extend([
        build_order("retry-2001", "RetryItem", 150.0),
        build_order("dlq-3001", "DLQItem", 275.0),
    ])

    for order in demo_orders:
        producer.produce(
            topic=ORDERS_TOPIC,
            key=order["orderId"],
            value=serializer(
                order,
                SerializationContext("orders", MessageField.VALUE),
            ),
            on_delivery=delivery_report,
        )
        producer.poll(0)
        print(f"[PRODUCER] Sent {order}")
        time.sleep(interval)

    producer.flush()
    print("[PRODUCER] Finished sending orders.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Produce Avro order messages.")
    parser.add_argument("--count", type=int, default=10, help="Number of normal randomized orders")
    parser.add_argument("--interval", type=float, default=0.5, help="Seconds between messages")
    args = parser.parse_args()
    create_topics()
    produce_orders(args.count, args.interval)
