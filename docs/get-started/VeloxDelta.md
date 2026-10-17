# Delta Lake Feature Support Status in Apache Gluten (Velox Backend)

This document summarizes the support status of **Delta Lake table features** when used with **Apache Gluten (Velox backend)**.

## Supported Spark / Delta combinations

| Spark profile | Spark version | Scala version | Delta Lake version | Status |
|---|---|---|---|---|
| `spark-3.5` | Spark 3.5.x | 2.12 | 3.3.x | Supported |
| `spark-4.0` | Spark 4.0.x | 2.13 | 4.0.x | Supported |

Native Delta write is supported in both Spark 3.5 and Spark 4.0 profiles. The difference between
the two rows above is the Spark/Delta compatibility target (Spark 3.5 + Delta 3.3 vs Spark 4.0 +
Delta 4.0), not a native-write capability gap.

## Build and runtime notes

Build Gluten with Delta support by enabling `-Pdelta` together with the Velox backend profile and a Spark profile.

- Spark 3.5 build example:
  - `mvn clean package -Pbackends-velox -Pdelta -Pspark-3.5 -DskipTests`
- Spark 4.0 build example:
  - `mvn clean package -Pbackends-velox -Pdelta -Pspark-4.0 -Pscala-2.13 -Pjava-17 -DskipTests`

Native Delta write is controlled by:

- `spark.gluten.sql.columnar.backend.velox.delta.enableNativeWrite`
  - Default: `false`
  - Type: experimental

With native write enabled, MERGE writes its data files natively too. This is controlled by:

- `spark.gluten.sql.columnar.backend.velox.delta.enableNativeMergeWrite`
  - Default: `true`
  - Type: experimental
  - MERGE keeps Delta's writer when the target table uses column mapping, IcebergCompat
    (UniForm), the `materializePartitionColumns` feature, a FLOAT or DOUBLE partition column,
    a TIMESTAMP_NTZ column, or nested field names with upper-case letters while
    `spark.sql.caseSensitive` is off. It also keeps Delta's writer when stats collection is
    disabled or `spark.sql.maxConcurrentOutputFileWriters` is set, and when
    `spark.sql.files.maxRecordsPerFile` is set on an unpartitioned table.

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
