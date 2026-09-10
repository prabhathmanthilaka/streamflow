# Kafka-Based Order Processing System

## 1. Introduction

This project implements a Kafka-based order message processing system. The system produces purchase orders, serializes each order using Apache Avro, publishes the messages to Kafka, consumes them in real time, calculates a running average of successful order prices, retries temporary processing failures, and routes permanently failed messages to a Dead Letter Queue (DLQ).

## 2. Requirements Mapping

| Assignment requirement | Implementation |
|---|---|
| Kafka-based producer/consumer | `src/producer.py` and `src/consumer.py` |
| Avro serialization | `schemas/order.avsc` + Confluent Avro Serializer/Deserializer |
| Running average | `RunningAverage` class in `src/consumer.py` |
| Retry temporary failures | Retry loop controlled by `MAX_RETRIES` and backoff |
| DLQ permanent failures | `orders.DLQ` topic and DLQ producer |
| Live demonstration | Reproducible `RetryItem` and `DLQItem` demo messages |
| Git repository | Project is structured for Git submission |

## 3. Order Schema

The order is represented by an Avro record named `Order` with exactly the fields required by the assignment:

- `orderId`: string
- `product`: string
- `price`: float

Schema Registry provides the shared schema contract between the producer and consumer.

## 4. System Architecture

The producer creates order objects and uses an Avro serializer. The serialized message is published to the Kafka `orders` topic. The consumer uses an Avro deserializer to reconstruct the order object.

Successful messages update the running total and count and produce a new average immediately. Temporary failures enter a retry loop. Permanent failures, and temporary failures that exhaust all retry attempts, are published to `orders.DLQ`.

## 5. Running Average

For every successfully processed order with price `p`:

`new_total = old_total + p`

`new_count = old_count + 1`

`running_average = new_total / new_count`

For example, successful prices of 100, 200 and 50 produce averages of 100.00, 150.00 and 116.67 respectively.

The calculation is online: only the running total and count are retained.

## 6. Retry Mechanism

A temporary processing error is retried up to three times by default. A one-second backoff is applied between attempts. The message is considered successfully processed if an attempt succeeds.

For the live demonstration, `RetryItem` is configured to fail on its first two processing attempts and succeed on the third. This makes the retry mechanism visible and deterministic without changing the required Avro schema.

## 7. Dead Letter Queue

A permanent processing failure is not allowed to block the main consumer indefinitely. The consumer creates a DLQ record containing:

- original order
- error type
- error message
- attempt count
- failure timestamp

The original Avro order is published to `orders.DLQ`, while error type, error message, attempt count and failure timestamp are carried as Kafka headers. The original Kafka message is committed only after the DLQ publication succeeds, preventing the consumer from losing the failed record.

For demonstration, `DLQItem` always raises a permanent processing failure and is therefore routed to the DLQ.

## 8. Reliability Considerations

The producer uses `acks=all` and idempotence. The consumer disables automatic offset commits. Offsets are committed after successful processing or after successful DLQ publication. This ordering prevents acknowledging a message before the application has dealt with it.

## 9. Testing

Unit tests cover:

1. Running average calculation.
2. Temporary failure and successful third retry.
3. Permanent failure classification.

A live integration demonstration verifies Kafka, Schema Registry, Avro serialization/deserialization, aggregation, retry behavior and DLQ routing.

## 10. Demonstration Procedure

Start infrastructure with Docker Compose. Create the topics with `python -m src.setup`. Start the consumer and then the producer. Observe the running average after successful orders. Observe the retry output for `RetryItem` and the DLQ output for `DLQItem`. Finally run `pytest -q`.

## 11. Conclusion

The completed system satisfies the assignment's required Kafka message flow, Avro serialization, real-time aggregation, retry handling and DLQ behavior. The Docker Compose environment makes the demonstration reproducible and the repository structure keeps configuration, schema, application code, tests and documentation separated.
