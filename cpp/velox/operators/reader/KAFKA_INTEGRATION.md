# Kafka Reader Integration

Experimental native read of Spark Structured Streaming Kafka sources (`MicroBatchScanExec` over
`KafkaScan`) in the Velox backend, using librdkafka.

Kafka support is disabled by default. Build with `--enable_kafka=ON` (which sets the `ENABLE_KAFKA`
CMake option and, with `--enable_vcpkg=ON`, installs the `velox-kafka` vcpkg feature providing
librdkafka).

## Flow

```
MicroBatchScanExecTransformer (JVM)
  - ReadRel with stream_kafka = true
  - one split info per KafkaBatchInputPartition, serialized as ReadRel.StreamKafka
    ↓
VeloxRuntime::parseSplitInfo keeps the raw split bytes
    ↓
SubstraitToVeloxPlanConverter::constructKafkaStreamNode
  - TableScanNode on the Kafka connector, with KafkaTableHandle / KafkaColumnHandle
  - takes the next split info slot and decodes it into a KafkaSplitInfo
    ↓
WholeStageResultIterator turns the KafkaSplitInfo into a KafkaConnectorSplit
    ↓
KafkaConnector::createDataSource -> KafkaDataSource reads [startOffset, endOffset)
```

## Components

- `KafkaSplit.h` - `KafkaConnectorSplit`: topic, partition, offset range, poll timeout,
  failOnDataLoss and the executor Kafka params.
- `KafkaSplitInfo.h` - `SplitInfo` carrying the decoded `ReadRel.StreamKafka`.
- `KafkaConnector.h/cc` - connector, table handle and column handle.
- `KafkaDataSource.h/cc` - Velox `DataSource` reading one split at a time.

## Schema

Columns are matched by name, so pruned or reordered projections work:

| Column          | Type      |
|-----------------|-----------|
| `key`           | VARBINARY |
| `value`         | VARBINARY |
| `topic`         | VARCHAR   |
| `partition`     | INTEGER   |
| `offset`        | BIGINT    |
| `timestamp`     | TIMESTAMP |
| `timestampType` | INTEGER   |

`headers` (`includeHeaders=true`) is not supported; such scans fall back to vanilla Spark.

## Semantics

- The end offset is exclusive, as in Spark. The split ends once the record before the end offset is
  read, or once the log end offset reaches the end offset (covers trailing transaction markers and
  compacted records).
- Offsets are never committed to Kafka; Spark tracks progress in its checkpoint. The partition is
  assigned directly, no consumer group membership.
- If no record arrives within `pollTimeoutMs` (Spark's `kafkaConsumer.pollTimeoutMs`), the task fails.
- If the start offset is no longer available, the task fails when `failOnDataLoss` is true,
  otherwise the rest of the split is skipped with a warning.
- The executor Kafka params are passed to librdkafka as is; Java-only properties that librdkafka
  does not recognize are ignored.

## Testing

`backends-velox/src-kafka/test/.../VeloxGlutenKafkaScanSuite.scala` needs a broker at
`localhost:9092` or `KAFKA_BOOTSTRAP_SERVERS`:

```bash
docker run -d --name kafka -p 9092:9092 apache/kafka
```
