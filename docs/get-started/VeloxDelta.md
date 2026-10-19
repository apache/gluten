# Delta Lake Feature Support Status in Apache Gluten (Velox Backend)

This document summarizes the support status of **Delta Lake table features** when used with **Apache Gluten (Velox backend)**.

## Supported Spark / Delta combinations

| Spark profile | Spark version | Scala version | Delta Lake artifact | Status |
|---|---|---|---|---|
| `spark-3.5` | Spark 3.5.x | 2.12 | `delta-spark_2.12` 3.3.x | Supported |
| `spark-4.0` | Spark 4.0.x | 2.13 | `delta-spark_4.0_2.13` 4.4.x | Supported |
| `spark-4.1` | Spark 4.1.x | 2.13 | `delta-spark_4.1_2.13` 4.4.x | Supported |
| `spark-4.2` | Spark 4.2.x | 2.13 | `delta-spark_4.2_2.13` 4.4.x | Supported |

Gluten works with the Delta release it is built against, so deploy that Delta artifact. All Spark
4.x profiles use the same Delta release, which Delta publishes for each Spark 4.x line; on Spark
4.0 the artifact is `delta-spark_4.0_2.13`, not `delta-spark_2.13`.

Native Delta write writes what vanilla Delta would write: the same rows, schema, Parquet physical
types, field IDs and partition columns, honoring the compression codec, `maxRecordsPerFile`,
Parquet version and encryption. Row group and page sizes, encodings and other Parquet tuning are
Velox's own. Settings are read where vanilla's writer reads them: the session, the writer options,
and the job's Hadoop configuration, which includes the table properties. Writes the Velox writer
cannot match fall back to vanilla Delta. On Spark 4.x these are:

- partitioned tables whose data files store the partition columns, which is Delta 4.4's default
  (`delta.writePartitionColumnsToParquet`) and required by `materializePartitionColumns`;
- column mapping (`name` or `id`), and other columns with `parquet.field.id` metadata, since
  Delta writes Parquet field IDs;
- timestamps, unless `spark.sql.parquet.outputTimestampType` (and the writer option or table
  property of the same name, if set) is `TIMESTAMP_MICROS`; Spark's default is `INT96`;
- `TIMESTAMP_NTZ` and `VOID` columns;
- compression codecs other than `uncompressed`, `snappy`, `gzip`, `lz4` and `zstd`;
- `maxRecordsPerFile` (option or session conf) on unpartitioned tables;
- Parquet format version 2 (`delta.parquet.format.version` 2.12.0 or later, or
  `parquet.writer.version=v2`);
- Parquet encryption (`parquet.crypto.factory.class`);
- a `spark.sql.parquet.datetimeRebaseModeInWrite` other than `CORRECTED` with date or timestamp
  columns, and `spark.sql.parquet.writeLegacyFormat` with array or map columns, or set as a table
  property to a value other than the session's;
- concurrent output writers (`spark.sql.maxConcurrentOutputFileWriters`) on partitioned tables;
- Iceberg compatibility, variant shredding and any writer option the gate does not know.

## Build and runtime notes

Build Gluten with Delta support by enabling `-Pdelta` together with the Velox backend profile and a Spark profile.

- Spark 3.5 build example:
  - `mvn clean package -Pbackends-velox -Pdelta -Pspark-3.5 -DskipTests`
- Spark 4.x build example (also `-Pspark-4.0` or `-Pspark-4.1`):
  - `mvn clean package -Pbackends-velox -Pdelta -Pspark-4.2 -Pscala-2.13 -Pjava-17 -DskipTests`

Native Delta write is controlled by:

- `spark.gluten.sql.columnar.backend.velox.delta.enableNativeWrite`
  - Default: `false`
  - Type: experimental

Native change data feed scan offload is controlled by:

- `spark.gluten.sql.columnar.backend.velox.delta.enableChangeDataFeedScan`
  - Default: `true`
  - Type: experimental

The native DELETE/UPDATE/MERGE deletion-vector target scan is controlled by:

- `spark.gluten.sql.columnar.backend.velox.delta.enableNativeDmlRowIndexScan`
  - Default: `true`
  - Type: experimental
  - When disabled, the DML target scan that produces file paths and row indexes for
    deletion-vector writes stays on Spark; other scans are unaffected.

| Feature | Delta minWriterVersion | Delta minReaderVersion | Iceberg format-version | Feature type | Supported by Gluten (Velox) |
|---|---:|---:|---:|---|---|
| Basic functionality | 2 | 1 | 1 | Writer | Yes |
| CHECK constraints | 3 | 1 | N/A | Writer | No |
| Change data feed | 4 | 1 | N/A | Writer | Yes |
| Generated columns | 4 | 1 | N/A | Writer | Partial |
| Column mapping | 5 | 2 | N/A | Reader and writer | Yes |
| Identity columns | 6 | 1 | N/A | Writer | Yes |
| Row tracking | 7 | 1 | 3 | Writer | Partial |
| Deletion vectors | 7 | 3 | 3 | Reader and writer | Partial |
| TimestampNTZ | 7 | 3 | 1 | Reader and writer | No |
| Liquid clustering | 7 | 3 | 1 | Reader and writer | Yes |
| Iceberg readers (UniForm) | 7 | 2 | N/A | Writer | Not tested |
| Type widening | 7 | 3 | N/A | Reader and writer | Partial |
| Variant | 7 | 3 | 3 | Reader and writer | Not tested |
| Variant shredding | 7 | 3 | 3 | Reader and writer | Not tested |
| Collations | 7 | 3 | N/A | Reader and writer | Not tested |
| Protected checkpoints | 7 | 1 | N/A | Writer | Not tested |
