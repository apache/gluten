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

## Generated deletion-vector metadata

With Spark 3.5 / Delta 3.3 and Spark 4.x / Delta 4.0, native scans support the
top-level fields Delta generates when
`spark.databricks.delta.deletionVectors.useMetadataRowIndex=false`:

- `__delta_internal_row_index` is the absolute, zero-based position in the data file,
  not an ordinal among rows surviving a predicate or deletion vector.
- `__delta_internal_is_row_deleted` is a byte-valued row-index-filter result. For
  `IF_CONTAINED`, bitmap members get `1`; for `IF_NOT_CONTAINED`, non-members get `1`.
  Files without a DV get `0`, including DV-free files in mixed scans. A present
  empty DV with `IF_NOT_CONTAINED` instead marks every row.

Requesting the deleted-row field **does not remove rows**. The native reader marks
the rows and leaves Delta's consumer filter in place. Filters on generated fields
are not pushed into physical Parquet columns. Ordinary data predicates may still
prune rows; generated positions remain file-global across row groups and batches.
Join dynamic filters on generated fields are applied after materialization, including
when Velox replaces the join with its dynamic filter.
The default metadata-row-index path continues to apply the existing native DV mask.

DV-bearing scans without a deleted-row output, unsupported generated-field types or
duplicates, multiple row-index aliases without a flag, bucketed generated-metadata
scans, generated names colliding with partition/mapped columns, and required-only
generated fields missing from scan output retain JVM fallback. Spark 3.4 DV scans,
CDF scans touching DVs, and the DML configuration escape hatch also retain their
existing fallback. DV-free row-index-only scans are supported.
The fields are recognized by their exact Delta names, without requiring Spark
generated-metadata markers; non-Delta columns are not reclassified as Delta metadata.

DV descriptors still use the existing executor-side, checksum-validating payload
handoff, memoization and metrics. Generating metadata does not introduce driver-side
DV reads or a native filesystem range-read path. The generated-column contract changes
the JVM/native protocol, so rebuild and deploy the native library with the matching JARs.

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
