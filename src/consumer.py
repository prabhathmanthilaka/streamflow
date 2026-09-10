
import time
from collections import defaultdict

from confluent_kafka import Consumer, Producer
from confluent_kafka.error import KafkaException
from confluent_kafka.serialization import SerializationContext, MessageField

from .common import create_avro_serializers, create_topics
from .config import (
    KAFKA_BOOTSTRAP_SERVERS,
    ORDERS_TOPIC,
    DLQ_TOPIC,
    CONSUMER_GROUP,
    MAX_RETRIES,
    RETRY_BACKOFF_SECONDS,
)


class RunningAverage:
    """Maintains a running average of successfully processed order prices."""

    def __init__(self):
        self.count = 0
        self.total = 0.0

    def add(self, price: float) -> float:
        self.count += 1
        self.total += price
        return self.total / self.count


class ProcessingFailure(Exception):
    """A temporary processing failure that should be retried."""


class PermanentProcessingFailure(Exception):
    """A permanent processing failure that should be sent to the DLQ."""


class FailureSimulator:
    """
    Demo-only failure injector used to demonstrate
    retry and DLQ behaviour.
    """

    def __init__(self):
        self.attempts = defaultdict(int)

    def process(self, order: dict) -> None:
        order_id = order["orderId"]
        product = order["product"]

        self.attempts[order_id] += 1

        # Simulate a temporary failure.
        # RetryItem succeeds on the third attempt.
        if product == "RetryItem":
            if self.attempts[order_id] <= 2:
                raise ProcessingFailure(
                    "Temporary downstream failure (simulated)"
                )
            return

        # Simulate a permanent business/data failure.
        # DLQItem will always fail.
        if product == "DLQItem":
            raise PermanentProcessingFailure(
                "Permanent business/data failure (simulated)"
            )


def run():
    # Create Avro serializer and deserializer.
    serializer, deserializer = create_avro_serializers()

    # Kafka consumer configuration.
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": CONSUMER_GROUP,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )

    # Separate producer used to publish permanently failed
    # messages to the Dead Letter Queue.
    dlq_producer = Producer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "acks": "all",
            "enable.idempotence": True,
        }
    )

    consumer.subscribe([ORDERS_TOPIC])

    average = RunningAverage()
    failures = FailureSimulator()

    print(
        "[CONSUMER] Listening for Avro orders. "
        "Press Ctrl+C to stop."
    )

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                raise KafkaException(msg.error())

            # Deserialize the Avro message.
            #
            # SerializationContext is required by the current
            # confluent-kafka Avro serializer/deserializer API.
            order = deserializer(
                msg.value(),
                SerializationContext(
                    msg.topic(),
                    MessageField.VALUE,
                ),
            )

            print(f"\n[CONSUMER] Received: {order}")

            processed = False
            last_error = None

            # Attempt processing up to MAX_RETRIES times.
            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    failures.process(order)

                    # Only successfully processed orders are included
                    # in the running average.
                    new_average = average.add(
                        float(order["price"])
                    )

                    print(
                        f"[SUCCESS] {order['orderId']} "
                        f"processed on attempt {attempt}. "
                        f"Running average = {new_average:.2f} "
                        f"(n={average.count})"
                    )

                    # Commit only after successful processing.
                    consumer.commit(
                        message=msg,
                        asynchronous=False,
                    )

                    processed = True
                    break

                except PermanentProcessingFailure as exc:
                    last_error = exc

                    print(
                        f"[PERMANENT FAILURE] {exc}"
                    )

                    # Do not retry permanent failures.
                    break

                except ProcessingFailure as exc:
                    last_error = exc

                    if attempt < MAX_RETRIES:
                        print(
                            f"[RETRY] {order['orderId']} "
                            f"failed on attempt {attempt}: {exc}. "
                            f"Retrying in "
                            f"{RETRY_BACKOFF_SECONDS:.1f}s..."
                        )

                        time.sleep(
                            RETRY_BACKOFF_SECONDS
                        )

                    else:
                        print(
                            f"[RETRY] Exhausted "
                            f"{MAX_RETRIES} attempts for "
                            f"{order['orderId']}."
                        )

            # If processing failed permanently, or all retry attempts
            # were exhausted, send the message to the DLQ.
            if not processed and last_error is not None:
                attempts = failures.attempts[
                    order["orderId"]
                ]

                dlq_producer.produce(
                    topic=DLQ_TOPIC,
                    key=order["orderId"],
                    value=serializer(
                        order,
                        SerializationContext(
                            DLQ_TOPIC,
                            MessageField.VALUE,
                        ),
                    ),
                    headers=[
                        (
                            "errorType",
                            type(last_error)
                            .__name__
                            .encode("utf-8"),
                        ),
                        (
                            "errorMessage",
                            str(last_error)
                            .encode("utf-8"),
                        ),
                        (
                            "attempts",
                            str(attempts)
                            .encode("utf-8"),
                        ),
                        (
                            "failedAtEpoch",
                            str(time.time())
                            .encode("utf-8"),
                        ),
                    ],
                )

                # Wait until Kafka confirms delivery of the DLQ message.
                dlq_producer.flush()

                # Commit the original message only after the DLQ
                # message has been successfully produced.
                consumer.commit(
                    message=msg,
                    asynchronous=False,
                )

                print(
                    f"[DLQ] Sent {order['orderId']} "
                    f"to {DLQ_TOPIC}"
                )

    except KeyboardInterrupt:
        print("\n[CONSUMER] Stopping...")

    finally:
        dlq_producer.flush()
        consumer.close()


if __name__ == "__main__":
    create_topics()
    run()

