# Kafka Produce

Publishes the incoming event to a Kafka topic and emits the delivery result (topic, partition, offset) downstream.

## Configuration

```yaml
- kafka_produce:
    name: publish_customer
    credentials_path: /etc/kafka/credentials.json
    brokers: "{{env.KAFKA_BROKERS}}"
    topic: customers
    message_key: "customer-{{event.data.name}}"
    create_or_update: true
```

### Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Task name. |
| `credentials_path` | string | | Path to Kafka credentials file. Omit to connect without authentication. See [Credentials](/docs/flowgen/kafka#credentials). |
| `brokers` | string | `localhost:9092` | Comma-separated bootstrap broker addresses. |
| `topic` | string | required | Topic to publish to. Supports templating. |
| `message_key` | string | | Message key template (e.g. `key-{{event.id}}`). See [Templating](/docs/flowgen/concepts/templating). |
| `create_or_update` | bool | `false` | When `true`, the topic is created (1 partition, replication factor 1) if it does not exist. When `false`, an error is returned if the topic is absent from the cluster. |
| `depends_on` | list | | Upstream task names. |
| `retry` | object | | [Retry configuration](/docs/flowgen/concepts/retry). |

## Output

Format: [JSON](https://docs.rs/serde_json/latest/serde_json/enum.Value.html)

| Field | Type | Description |
|---|---|---|
| `topic` | string | Topic the message was written to. |
| `partition` | int | Partition the message was written to. |
| `offset` | int | Offset of the written message. |

## Examples

**Publish the incoming event as the message payload:**

```yaml
- kafka_produce:
    name: publish_customer
    credentials_path: /etc/kafka/credentials.json
    topic: customers
    create_or_update: true
```

**Keyed messages with an id fallback:**

Incoming events do not always carry an `id` (e.g. NATS messages without a `Nats-Msg-Id` header, generate tasks). The producer patches a UUID v7 fallback into the render context, so `{{event.id}}` in `message_key` always resolves to a value:

```yaml
- kafka_produce:
    name: publish_orders
    credentials_path: /etc/kafka/credentials.json
    topic: orders
    message_key: "{{event.id}}"
```

## Behaviour

The message payload is the incoming event's data — JSON is serialized as-is, `bytes`/Avro payloads are sent raw, and Arrow record batches are serialized as an Arrow IPC stream. Delivery is acknowledged before the result event is emitted downstream; if the broker never acknowledges, the task fails and retries per the [retry configuration](/docs/flowgen/concepts/retry).
