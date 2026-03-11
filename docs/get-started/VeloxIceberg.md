---
layout: page
title: Iceberg Support in Velox Backend
nav_order: 8
parent: Getting-Started
---

# Iceberg Support in Velox Backend

## Supported Spark version

All the spark version is supported, but for convenience, only Spark 3.4 is well tested.
Now only read is supported in Gluten.

## Support Status
Following value indicates the iceberg support progress:

| Value          | Description                                                                |
|----------------|----------------------------------------------------------------------------|
| Offload        | Offload to the Velox backend                                               |
| PartialOffload | Some operators offload and some fallback                                   |
| Fallback       | Fallback to spark to execute                                               |
| Exception      | Cannot fallback by some conditions, throw the exception                    |
| ResultMismatch | Some hidden bug may cause result mismatch, especially for some corner case |

## Adding catalogs
Fallback

## Creating a table
Fallback

## Writing
Fallback
````
INSERT INTO local.db.table VALUES (1, 'a'), (2, 'b'), (3, 'c');
````
PartialOffload

The write is fallback while read is offload.
````
INSERT INTO local.db.table SELECT id, data FROM source WHERE length(data) = 1;
````

## Reading
### Read data
Offload/Fallback

| Table Type  | No Delete       | Position Delete | Equality Delete |
|-------------|-----------------|-----------------|-----------------|
| unpartition | Offload         | Offload         | Fallback        |
| partition   | Fallback mostly | Fallback mostly | Fallback        |
| metadata    | Fallback        |                 |                 |

Offload the simple query.
````
SELECT count(1) as count, data
FROM local.db.table
GROUP BY data;
````

If delete by Spark and copy on read, will generate position delete file, the query may offload.

If delete by Flink, may generate the equality delete file, fallback in tht case.

Now we only offload the simple query, for partition table, many operators are fallback by Expression
StaticInvoke such as BucketFunction, wait to be supported.

DataFrame reads are supported and can now reference tables by name using spark.table:

````
val df = spark.table("local.db.table")
df.count()
````

### Read metadata
Fallback
````
SELECT data, _file FROM local.db.table;
````

## DataType
Timestamptz in orc format is not supported, throws exception.
UUID type and Fixed type is fallback.

## Format
PartialOffload

Supports parquet and orc format.
Not support avro format.

## SQL
Only support SELECT.

## Schema evolution
PartialOffload

Gluten uses column name to match the parquet file, so if the column is renamed or
the added column name is same to the deleted column, the scan will fall back.

## Configuration
### Catalogs
All the catalog configurations are transparent to Gluten

### SQL Extensions
Fallback

Supports the option `spark.sql.extensions`, fallback the SQL command `CALL`.

### Runtime configuration
The "Gluten Support" column is now ready to be populated with:

✅ Supported<br>
❌ Not Supported<br>
⚠️ Partial Support<br>
🔄 In Progress<br>
🚫 Not applied or transparent to Gluten<br>

### Spark SQL Options
| Spark option | Default | Description | Gluten Support |
| --- | --- | --- | --- |
| spark.sql.iceberg.vectorization.enabled | Table default | Enables vectorized reads of data files |❌|
| spark.sql.iceberg.parquet.reader-type | ICEBERG | Sets Parquet reader implementation (ICEBERG,COMET) |✅ |
| spark.sql.iceberg.check-nullability | true | Validate that the write schema's nullability matches the table's nullability |✅ |
| spark.sql.iceberg.check-ordering | true | Validates the write schema column order matches the table schema order |✅ |
| spark.sql.iceberg.planning.preserve-data-grouping | false | When true, co-locate scan tasks for the same partition in the same read split, used in Storage Partitioned Joins |✅ |
| spark.sql.iceberg.aggregate-push-down.enabled | true | Enables pushdown of aggregate functions (MAX, MIN, COUNT) | |
| spark.sql.iceberg.distribution-mode | See Spark Writes | Controls distribution strategy during writes | ✅ |
| spark.wap.id | null | Write-Audit-Publish snapshot staging ID | |
| spark.wap.branch | null | WAP branch name for snapshot commit | |
| spark.sql.iceberg.compression-codec | Table default | Write compression codec (e.g., zstd, snappy) | |
| spark.sql.iceberg.compression-level | Table default | Compression level for Parquet/Avro | |
| spark.sql.iceberg.compression-strategy | Table default | Compression strategy for ORC | |
| spark.sql.iceberg.data-planning-mode | AUTO | Scan planning mode for data files (AUTO, LOCAL, DISTRIBUTED) | |
| spark.sql.iceberg.delete-planning-mode | AUTO | Scan planning mode for delete files (AUTO, LOCAL, DISTRIBUTED) | |
| spark.sql.iceberg.advisory-partition-size | Table default | Advisory size (bytes) used for writing to the Table when Spark's Adaptive Query Execution is enabled. Used to size output files | |
| spark.sql.iceberg.locality.enabled | false | Report locality information for Spark task placement on executors |✅ |
| spark.sql.iceberg.executor-cache.enabled | true | Enables cache for executor-side (currently used to cache Delete Files) |❌|
| spark.sql.iceberg.executor-cache.timeout | 10 | Timeout in minutes for executor cache entries |❌|
| spark.sql.iceberg.executor-cache.max-entry-size | 67108864 (64MB) | Max size per cache entry (bytes) |❌|
| spark.sql.iceberg.executor-cache.max-total-size | 134217728 (128MB) | Max total executor cache size (bytes) |❌|
| spark.sql.iceberg.executor-cache.locality.enabled | false | Enables locality-aware executor cache usage |❌|
| spark.sql.iceberg.merge-schema | false | Enables modifying the table schema to match the write schema. Only adds columns missing columns |✅|
| spark.sql.iceberg.report-column-stats | true | Report Puffin Table Statistics if available to Spark's Cost Based Optimizer. CBO must be enabled for this to be effective |✅|

#### Read options
| Spark option | Default | Description | Gluten Support |
| --- | --- | --- | --- |
| snapshot-id | (latest) | Snapshot ID of the table snapshot to read |✅|
| as-of-timestamp | (latest) | A timestamp in milliseconds; the snapshot used will be the snapshot current at this time. |✅|
| split-size | As per table property | Overrides this table's read.split.target-size and read.split.metadata-target-size |✅|
| lookback | As per table property | Overrides this table's read.split.planning-lookback |✅|
| file-open-cost | As per table property | Overrides this table's read.split.open-file-cost |✅|
| vectorization-enabled | As per table property | Overrides this table's read.parquet.vectorization.enabled |❌|
| batch-size | As per table property | Overrides this table's read.parquet.vectorization.batch-size |❌|
| stream-from-timestamp | (none) | A timestamp in milliseconds to stream from; if before the oldest known ancestor snapshot, the oldest will be used | |
| streaming-max-files-per-micro-batch | INT_MAX | Maximum number of files per microbatch | |
| streaming-max-rows-per-micro-batch | INT_MAX | Maximum number of rows per microbatch | |

#### Write options

| Spark option | Default | Description | Gluten Support |
| --- | --- | --- | --- |
| write-format | Table write.format.default | File format to use for this write operation; parquet, avro, or orc |⚠️ Parquet only|
| target-file-size-bytes | As per table property | Overrides this table's write.target-file-size-bytes | |
| check-nullability | true | Sets the nullable check on fields | |
| snapshot-property.custom-key | null | Adds an entry with custom-key and corresponding value in the snapshot summary (the snapshot-property. prefix is only required for DSv2) | |
| fanout-enabled | false | Overrides this table's write.spark.fanout.enabled |✅|
| check-ordering | true | Checks if input schema and table schema are same | |
| isolation-level | null | Desired isolation level for Dataframe overwrite operations. null => no checks (for idempotent writes), serializable => check for concurrent inserts or deletes in destination partitions, snapshot => checks for concurrent deletes in destination partitions. | |
| validate-from-snapshot-id | null | If isolation level is set, id of base snapshot from which to check concurrent write conflicts into a table. Should be the snapshot before any reads from the table. Can be obtained via Table API or Snapshots table. If null, the table's oldest known snapshot is used. | |
| compression-codec | Table write.(fileformat).compression-codec | Overrides this table's compression codec for this write | |
| compression-level | Table write.(fileformat).compression-level | Overrides this table's compression level for Parquet and Avro tables for this write | |
| compression-strategy | Table write.orc.compression-strategy | Overrides this table's compression strategy for ORC tables for this write | |
| distribution-mode | See Spark Writes for defaults | Override this table's distribution mode for this write |🚫|
| delete-granularity | file | Override this table's delete granularity for this write | |

### Iceberg Table Properties
extracted from https://iceberg.apache.org/docs/latest/configuration/

#### READ Properties

| Property | Default | Description | Gluten Support |
| --- | --- | --- | --- |
| read.split.target-size | 134217728 (128 MB) | Target size when combining data input splits |  |
| read.split.metadata-target-size | 33554432 (32 MB) | Target size when combining metadata input splits |  |
| read.split.planning-lookback | 10 | Number of bins to consider when combining input splits |  |
| read.split.open-file-cost | 4194304 (4 MB) | The estimated cost to open a file, used as a minimum weight when combining splits. |  |
| read.parquet.vectorization.enabled | true | Controls whether Parquet vectorized reads are used |  |
| read.parquet.vectorization.batch-size | 5000 | The batch size for parquet vectorized reads |  |
| read.orc.vectorization.enabled | false | Controls whether orc vectorized reads are used |  |
| read.orc.vectorization.batch-size | 5000 | The batch size for orc vectorized reads |  |

#### WRITE Properties

| Property | Default | Description | Gluten Support |
| --- | --- | --- | --- |
| write.format.default | parquet | Default file format for the table; parquet, avro, or orc |  |
| write.delete.format.default | data file format | Default delete file format for the table; parquet, avro, or orc |  |
| write.parquet.row-group-size-bytes | 134217728 (128 MB) | Parquet row group size |  |
| write.parquet.page-size-bytes | 1048576 (1 MB) | Parquet page size |  |
| write.parquet.page-row-limit | 20000 | Parquet page row limit |  |
| write.parquet.dict-size-bytes | 2097152 (2 MB) | Parquet dictionary page size |  |
| write.parquet.compression-codec | zstd | Parquet compression codec: zstd, brotli, lz4, gzip, snappy, uncompressed |  |
| write.parquet.compression-level | null | Parquet compression level |  |
| write.parquet.bloom-filter-enabled.column.col1 | (not set) | Hint to parquet to write a bloom filter for the column: 'col1' |  |
| write.parquet.bloom-filter-max-bytes | 1048576 (1 MB) | The maximum number of bytes for a bloom filter bitset |  |
| write.parquet.bloom-filter-fpp.column.col1 | 0.01 | The false positive probability for a bloom filter applied to 'col1' (must > 0.0 and < 1.0) |  |
| write.parquet.stats-enabled.column.col1 | (not set) | Controls whether to collect parquet column statistics for column 'col1' |  |
| write.avro.compression-codec | gzip | Avro compression codec: gzip(deflate with 9 level), zstd, snappy, uncompressed |  |
| write.avro.compression-level | null | Avro compression level |  |
| write.orc.stripe-size-bytes | 67108864 (64 MB) | Define the default ORC stripe size, in bytes |  |
| write.orc.block-size-bytes | 268435456 (256 MB) | Define the default file system block size for ORC files |  |
| write.orc.compression-codec | zlib | ORC compression codec: zstd, lz4, lzo, zlib, snappy, none |  |
| write.orc.compression-strategy | speed | ORC compression strategy: speed, compression |  |
| write.orc.bloom.filter.columns | (not set) | Comma separated list of column names for which a Bloom filter must be created |  |
| write.orc.bloom.filter.fpp | 0.05 | False positive probability for Bloom filter (must > 0.0 and < 1.0) |  |
| write.location-provider.impl | null | Optional custom implementation for LocationProvider |  |
| write.metadata.compression-codec | none | Metadata compression codec; none or gzip |  |
| write.metadata.metrics.max-inferred-column-defaults | 100 | Defines the maximum number of columns for which metrics are collected. Columns are included with a pre-order traversal of the schema: top level fields first; then all elements of the first nested s... |  |
| write.metadata.metrics.default | truncate(16) | Default metrics mode for all columns in the table; none, counts, truncate(length), or full |  |
| write.metadata.metrics.column.col1 | (not set) | Metrics mode for column 'col1' to allow per-column tuning; none, counts, truncate(length), or full |  |
| write.target-file-size-bytes | 536870912 (512 MB) | Controls the size of files generated to target about this many bytes |  |
| write.delete.target-file-size-bytes | 67108864 (64 MB) | Controls the size of delete files generated to target about this many bytes |  |
| write.distribution-mode | not set, see engines for specific defaults, for example Spark Writes | Defines distribution of write data: none: don't shuffle rows; hash: hash distribute by partition key ; range: range distribute by partition key or sort key if table has an SortOrder |  |
| write.delete.distribution-mode | (not set) | Defines distribution of write delete data |  |
| write.update.distribution-mode | (not set) | Defines distribution of write update data |  |
| write.merge.distribution-mode | (not set) | Defines distribution of write merge data |  |
| write.wap.enabled | false | Enables write-audit-publish writes |  |
| write.summary.partition-limit | 0 | Includes partition-level summary stats in snapshot summaries if the changed partition count is less than this limit |  |
| write.metadata.delete-after-commit.enabled | false | Controls whether to delete the oldest tracked version metadata files after each table commit. See the Remove old metadata files section for additional details |  |
| write.metadata.previous-versions-max | 100 | The max number of previous version metadata files to track |  |
| write.spark.fanout.enabled | false | Enables the fanout writer in Spark that does not require data to be clustered; uses more memory |  |
| write.object-storage.enabled | false | Enables the object storage location provider that adds a hash component to file paths |  |
| write.object-storage.partitioned-paths | true | Includes the partition values in the file path |  |
| write.data.path | table location + /data | Base location for data files |  |
| write.metadata.path | table location + /metadata | Base location for metadata files |  |
| write.delete.mode | copy-on-write | Mode used for delete commands: copy-on-write or merge-on-read (v2 and above) |  |
| write.delete.isolation-level | serializable | Isolation level for delete commands: serializable or snapshot |  |
| write.update.mode | copy-on-write | Mode used for update commands: copy-on-write or merge-on-read (v2 and above) |  |
| write.update.isolation-level | serializable | Isolation level for update commands: serializable or snapshot |  |
| write.merge.mode | copy-on-write | Mode used for merge commands: copy-on-write or merge-on-read (v2 and above) |  |
| write.merge.isolation-level | serializable | Isolation level for merge commands: serializable or snapshot |  |
| write.delete.granularity | partition | Controls the granularity of generated delete files: partition or file |  |

#### COMMIT Properties

| Property | Default | Description | Gluten Support |
| --- | --- | --- | --- |
| commit.retry.num-retries | 4 | Number of times to retry a commit before failing |  |
| commit.retry.min-wait-ms | 100 | Minimum time in milliseconds to wait before retrying a commit |  |
| commit.retry.max-wait-ms | 60000 (1 min) | Maximum time in milliseconds to wait before retrying a commit |  |
| commit.retry.total-timeout-ms | 1800000 (30 min) | Total retry timeout period in milliseconds for a commit |  |
| commit.status-check.num-retries | 3 | Number of times to check whether a commit succeeded after a connection is lost before failing due to an unknown commit state |  |
| commit.status-check.min-wait-ms | 1000 (1s) | Minimum time in milliseconds to wait before retrying a status-check |  |
| commit.status-check.max-wait-ms | 60000 (1 min) | Maximum time in milliseconds to wait before retrying a status-check |  |
| commit.status-check.total-timeout-ms | 1800000 (30 min) | Total timeout period in which the commit status-check must succeed, in milliseconds |  |
| commit.manifest.target-size-bytes | 8388608 (8 MB) | Target size when merging manifest files |  |
| commit.manifest.min-count-to-merge | 100 | Minimum number of manifests to accumulate before merging |  |
| commit.manifest-merge.enabled | true | Controls whether to automatically merge manifests on writes |  |

#### HISTORY Properties

| Property | Default | Description | Gluten Support |
| --- | --- | --- | --- |
| history.expire.max-snapshot-age-ms | 432000000 (5 days) | Default max age of snapshots to keep on the table and all of its branches while expiring snapshots |  |
| history.expire.min-snapshots-to-keep | 1 | Default min number of snapshots to keep on the table and all of its branches while expiring snapshots |  |
| history.expire.max-ref-age-ms | Long.MAX_VALUE (forever) | For snapshot references except the main branch, default max age of snapshot references to keep while expiring snapshots. The main branch never expires. |  |
| format-version | 2 | Table's format version (can be 1 or 2) as defined in the Spec. Defaults to 2 since version 1.4.0. |  |

#### COMPATIBILITY Properties

| Property | Default | Description | Gluten Support |
| --- | --- | --- | --- |
| compatibility.snapshot-id-inheritance.enabled | false | Enables committing snapshots without explicit snapshot IDs (always true if the format version is > 1) |  |
| catalog-impl | null | a custom Catalog implementation to use by an engine |  |
| io-impl | null | a custom FileIO implementation to use in a catalog |  |
| warehouse | null | the root path of the data warehouse |  |
| uri | null | a URI string, such as Hive metastore URI |  |
| clients | 2 | client pool size |  |
| cache-enabled | true | Whether to cache catalog entries |  |

#### CACHE Properties

| Property | Default | Description | Gluten Support |
| --- | --- | --- | --- |
| cache.expiration-interval-ms | 30000 | How long catalog entries are locally cached, in milliseconds; 0 disables caching, negative values disable expiration |❌|
| metrics-reporter-impl | org.apache.iceberg.metrics.LoggingMetricsReporter | Custom MetricsReporter implementation to use in a catalog. See the Metrics reporting section for additional details |❌|
| lock-impl | null | a custom implementation of the lock manager, the actual interface depends on the catalog used |❌|

#### LOCK Properties

| Property | Default | Description | Gluten Support |
| --- | --- | --- | --- |
| lock.table | null | an auxiliary table for locking, such as in AWS DynamoDB lock manager |  |
| lock.acquire-interval-ms | 5000 (5 s) | the interval to wait between each attempt to acquire a lock |  |
| lock.acquire-timeout-ms | 180000 (3 min) | the maximum time to try acquiring a lock |  |
| lock.heartbeat-interval-ms | 3000 (3 s) | the interval to wait between each heartbeat after acquiring a lock |  |
| lock.heartbeat-timeout-ms | 15000 (15 s) | the maximum time without a heartbeat to consider a lock expired |  |

#### ICEBERG Properties

| Property | Default | Description | Gluten Support |
| --- | --- | --- | --- |
| iceberg.hive.client-pool-size | 5 | The size of the Hive client pool when tracking tables in HMS |  |
| iceberg.hive.lock-creation-timeout-ms | 180000 (3 min) | Maximum time in milliseconds to create a lock in the HMS |  |
| iceberg.hive.lock-creation-min-wait-ms | 50 | Minimum time in milliseconds between retries of creating the lock in the HMS |  |
| iceberg.hive.lock-creation-max-wait-ms | 5000 | Maximum time in milliseconds between retries of creating the lock in the HMS |  |
| iceberg.hive.lock-timeout-ms | 180000 (3 min) | Maximum time in milliseconds to acquire a lock |  |
| iceberg.hive.lock-check-min-wait-ms | 50 | Minimum time in milliseconds between checking the acquisition of the lock |  |
| iceberg.hive.lock-check-max-wait-ms | 5000 | Maximum time in milliseconds between checking the acquisition of the lock |  |
| iceberg.hive.lock-heartbeat-interval-ms | 240000 (4 min) | The heartbeat interval for the HMS locks. |  |
| iceberg.hive.metadata-refresh-max-retries | 2 | Maximum number of retries when the metadata file is missing |  |
| iceberg.hive.table-level-lock-evict-ms | 600000 (10 min) | The timeout for the JVM table lock is |  |
| iceberg.engine.hive.lock-enabled | true | Use HMS locks to ensure atomicity of commits |  |
