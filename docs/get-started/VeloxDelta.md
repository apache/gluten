---
layout: page
title: Delta Lake Support in Velox Backend
nav_order: 13
parent: Getting-Started
---

# Delta Lake Support in Velox Backend

## Supported Spark version

| Spark | Delta Lake | Delta Lake jar | Read | Native write | Tested by |
|---|---|---|---|---|---|
| 3.4.4 | 2.4.0 | `io.delta:delta-core_2.12:2.4.0` (`_2.13` for Scala 2.13) | Offload. Deletion vector scans fall back. | None | Gluten suites |
| 3.5.5 | 3.3.2 | `io.delta:delta-spark_2.12:3.3.2` (`_2.13` for Scala 2.13) | Offload | Experimental, off by default | Gluten suites, copied Delta Lake suites (Scala 2.12 only) |
| 4.0.2 | 4.0.1 | `io.delta:delta-spark_2.13:4.0.1` | Offload | Experimental, off by default | Gluten suites, copied Delta Lake suites tagged `ExtendedSQLTest` |
| 4.1.1 | 4.1.0 | `io.delta:delta-spark_4.1_2.13:4.1.0` | Offload | Experimental, off by default | Gluten suites, nightly Delta Lake 4.2.0 job |
| 4.2 | None on main | | None | None | None |

Each Spark profile builds against the one Delta Lake release in the table. The copied Delta Lake suites run with native write on.

Spark 4.1 is the most tested. A nightly job builds Delta Lake 4.2.0 from source and runs its Spark test suites on Spark 4.1. It runs them with native write on and a Gluten bundle compiled against Delta Lake 4.1.0. "Seen on 4.1" on this page refers to that job. Its known failures are listed in `.github/workflows/util/delta-spark-ut/known-failures.txt`. Many entries come from the job's setup, so read the README in that directory before treating an entry as a Gluten bug.

On Spark 4.1, Gluten's own suites check deletion vector reads. DML behavior there is checked only by the nightly job.

Spark 4.2 support is in progress in [#13271](https://github.com/apache/gluten/pull/13271).

### Build and run

Delta Lake support is compiled in only with the `-Pdelta` Maven profile. Build the native library first, as described in [Gluten with Velox Backend](Velox.md).

````
./build/mvn clean install -Pbackends-velox -Pspark-3.5 -Pdelta -DskipTests
./build/mvn clean install -Pbackends-velox -Pspark-4.1 -Pscala-2.13 -Pjava-17 -Pdelta \
  -DskipTests -Dmaven.compiler.release=17
````

`dev/buildbundle-veloxbe.sh` does not pass `-Pdelta`, so its bundle has no Delta Lake support. No Gluten bundle contains the Delta Lake jar, including one built with `-Pdelta`. Add the jar from the table above. When `io.delta.sql.DeltaSparkSessionExtension` is not on the classpath, Gluten logs `Excluding runtime-incompatible component` and leaves its Delta support off.

Native writes to HDFS, S3, GCS or ABFS paths need the native library built with `--enable_hdfs=ON`, `--enable_s3=ON`, `--enable_gcs=ON` or `--enable_abfs=ON`. Without that connector, scans of those paths fall back and native writes fail (see [Write problems](#write-problems)).

For example, on Spark 3.5:

````
spark-shell \
  --packages io.delta:delta-spark_2.12:3.3.2 \
  --jars gluten-velox-bundle-spark3.5_2.12-<os>_<arch>-<version>.jar \
  --conf spark.plugins=org.apache.gluten.GlutenPlugin \
  --conf spark.memory.offHeap.enabled=true \
  --conf spark.memory.offHeap.size=4g \
  --conf spark.shuffle.manager=org.apache.spark.shuffle.sort.ColumnarShuffleManager \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
````

On Spark 4.x, use the Delta Lake jar for that version and add `--conf spark.sql.ansi.enabled=false`.

### ANSI mode on Spark 4.x

Spark 4.0 and 4.1 turn ANSI mode on by default. With Gluten's default `spark.gluten.sql.ansiFallback.enabled=true`, every query then falls back, Delta scans and native writes included.

````
spark.sql.ansi.enabled=false
````

Gluten's Delta tests and the nightly Delta Lake job set this. They also set `spark.gluten.sql.ansiFallback.enabled=false`. Spark 3.x behaves the same when ANSI mode is turned on.

Keeping ANSI on and setting `spark.gluten.sql.ansiFallback.enabled=false` runs ANSI semantics in Velox. That combination is tested only by the Delta Lake 4.2.0 suites that turn ANSI on, in the nightly Delta Lake job. It has a known MERGE problem (see [MERGE, UPDATE and DELETE problems](#merge-update-and-delete-problems)).

## Support Status
The following values show the Delta Lake support status:

| Value          | Description                                                                |
|----------------|----------------------------------------------------------------------------|
| Offload        | Runs in the Velox backend. For writes, Velox writes the data files.       |
| PartialOffload | Some operators run in Velox and the rest fall back                         |
| Fallback       | Runs on Spark or Delta Lake                                                |
| Exception      | Fails instead of falling back                                              |
| ResultMismatch | Can return wrong results or write wrong files, mostly in corner cases      |

Not tested: no test covers the case.<br>
From code: the status comes from reading the source, with no test.<br>
Seen on 4.1: the nightly Delta Lake job on Spark 4.1 showed the result.<br>

## Adding catalogs
Fallback

Catalog operations run on the driver.

## Creating a table
Fallback

````
CREATE TABLE t (id INT, name STRING) USING delta;
````

A table created without `AS SELECT` is a metadata-only commit. There is nothing to offload.

Offload

````
SET spark.gluten.sql.columnar.backend.velox.delta.enableNativeWrite=true;
CREATE TABLE t2 USING delta AS SELECT * FROM t;
````

With `spark.gluten.sql.columnar.backend.velox.delta.enableNativeWrite=true` on Spark 3.5 to 4.1, Velox writes the data files of `CREATE TABLE ... AS SELECT` and `REPLACE TABLE ... AS SELECT` when the target catalog is a DeltaCatalog.

## Writing
PartialOffload

````
INSERT INTO t SELECT id, name FROM source WHERE length(name) = 1;
````

Delta's writer writes the files of `INSERT`, with or without native write. The `SELECT` can run in Velox.

Fallback

````
INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c');
````

Offload

````
SET spark.gluten.sql.columnar.backend.velox.delta.enableNativeWrite=true;
UPDATE t SET name = 'b' WHERE id = 2;
````

Set `spark.gluten.sql.columnar.backend.velox.delta.enableNativeWrite=true` to have Velox write the data files. On Spark 4.x, also apply [ANSI mode on Spark 4.x](#ansi-mode-on-spark-4x).

Native write does not check the codec, the path scheme or the partition column types before it runs. Unsupported values make the task fail or write wrong files. Read [Write problems](#write-problems) before turning it on.

### Checking the plan

A command that Gluten wraps for native write shows a `GlutenDelta` node, for example `Execute GlutenDelta UpdateCommand` or `GlutenDelta AtomicCreateTableAsSelect`. Without the prefix, Delta's writer wrote the files.

````
val df = spark.sql("UPDATE delta.`/data/t` SET name = 'bb' WHERE id = 2")
df.queryExecution.executedPlan.treeString.contains("GlutenDelta ")
````

The node also shows when Velox writes no data files. This happens for a DELETE that writes only deletion vectors or only metadata, and for a Spark 4.x write with a VARIANT column. For `DataFrameWriter` calls, read the plan from the Spark UI SQL tab or a `QueryExecutionListener`.

### Write commands

The Status column assumes Spark 3.5 to 4.1 with native write on. Otherwise every row is at best PartialOffload. In PartialOffload rows, Delta writes the files. The query that feeds the write can run in Velox.

| SQL | DataFrame API | Status | Notes |
|---|---|---|---|
| | `df.write.format("delta").save(path)` | Offload | Every save mode, with or without `partitionBy`. |
| `CREATE TABLE ... AS SELECT` | `writeTo(t).create()`, `saveAsTable(t)` when `t` does not exist | Offload | Needs a DeltaCatalog target. |
| `REPLACE TABLE ... AS SELECT`, `CREATE OR REPLACE TABLE ... AS SELECT` | `writeTo(t).replace()`, `writeTo(t).createOrReplace()`, `saveAsTable(t)` in overwrite mode | Offload | Needs a DeltaCatalog target. |
| `UPDATE` | `DeltaTable.update()` | Offload | On a table with deletion vectors, Velox writes only the updated rows. |
| `DELETE` | `DeltaTable.delete()` | Offload | Velox writes the rewritten files. A deletion-vector DELETE and a metadata-only DELETE write no data files. |
| `OPTIMIZE`, `OPTIMIZE ... WHERE` | | Offload | Bin-packing, by path or by name. |
| `OPTIMIZE ... ZORDER BY`, `OPTIMIZE ... FULL`, `OPTIMIZE` on a liquid-clustered table | | PartialOffload | |
| `REORG TABLE ... APPLY (PURGE)`, `REORG TABLE ... APPLY (UPGRADE UNIFORM ...)` | | PartialOffload | |
| `INSERT INTO ... SELECT`, `INSERT OVERWRITE`, `INSERT INTO ... REPLACE WHERE` | | PartialOffload | From code. To append natively, use `save(path)` in append mode. |
| | `insertInto(t)`, `saveAsTable(t)` in append mode on an existing table, `writeTo(t).append()`, `overwrite()`, `overwritePartitions()` | PartialOffload | From code. |
| `MERGE INTO` | | PartialOffload | Scans, joins and the DML target scan can run in Velox. Native MERGE write is in progress in [#13324](https://github.com/apache/gluten/pull/13324). |
| | `writeStream.format("delta")` | PartialOffload | Inside `foreachBatch`, batch writes follow the rows above. From code. |

With native write on, `DESCRIBE HISTORY` operation metrics of `save(path)`, UPDATE, DELETE and `CREATE TABLE ... AS SELECT` match Delta Lake. Seen on 4.1.

### Write features

| Feature | Status | Notes |
|---|---|---|
| Optimized write | Offload | In the commands marked Offload, the rebalancing shuffle runs in Velox. If it fails validation, Delta's shuffle is used. `OPTIMIZE` never uses it. |
| Auto compaction | Offload | Compacts natively written tables (seen on 4.1). After a native write, the compaction runs in a Gluten transaction (from code). After INSERT, MERGE or streaming writes, Delta's writer compacts. |

## Reading
### Read data
Offload/Fallback

| Table type | Spark 3.4 / Delta Lake 2.4 | Spark 3.5 to 4.1 |
|---|---|---|
| Unpartitioned | Offload | Offload |
| Partitioned | Offload | Offload |
| Column mapping, name mode | Offload | Offload |
| Column mapping, id mode | Offload, matched by physical name | Offload, matched by physical name |
| Files with deletion vectors | Fallback | Offload |
| Change data feed, no deletion vectors in range | PartialOffload | PartialOffload |
| Change data feed, deletion vectors in range | Fallback | Fallback |

````
SELECT count(1) AS count, name
FROM t
GROUP BY name;
````

Reads need no Gluten option. The native scan shows up as `FileDeltaScanTransformer` in the plan. Delta prunes partitions and skips files by statistics on the driver before the scan runs.

| Read | Status | Notes |
|---|---|---|
| By table name, ``delta.`/path` `` or `spark.read.format("delta").load(path)` | Offload | Same native scan. |
| `VERSION AS OF` | Offload | Tested on Spark 3.4 to 4.1. |
| `TIMESTAMP AS OF`, `versionAsOf`, `timestampAsOf` | Offload | Seen on 4.1. Plan not checked. |
| Streaming source | Offload | Each micro-batch is a Delta file scan. Seen on 4.1. Plan not checked. |
| Dynamic partition pruning | Not tested | |

### Deletion vectors
Offload

````
ALTER TABLE t SET TBLPROPERTIES ('delta.enableDeletionVectors' = true);
DELETE FROM t WHERE id IN (3, 4);
SELECT * FROM t;
````

On Spark 3.5 to 4.1, Velox drops the deleted rows in the scan. This needs Delta's `spark.databricks.delta.deletionVectors.useMetadataRowIndex=true`, the default. The DML target scan, which finds the rows for new deletion vectors, also runs in Velox. Delta writes the deletion-vector files.

Fallback

````
SET spark.databricks.delta.deletionVectors.useMetadataRowIndex=false;
SELECT * FROM t;
````

On Spark 3.4, or with `useMetadataRowIndex=false`, a scan that may read deletion vectors falls back. A normal read falls back only when a file it selected has a deletion vector. DML target scans, streaming micro-batches and reads with `spark.databricks.delta.stats.skipping=false` fall back whenever the table protocol allows deletion vectors. The fallback reason, shown in [Gluten UI](GlutenUI.md), is `fallback Spark 3.4 Delta DV scan` or `fallback Delta DV scan without metadata row index`.

### Change data feed
PartialOffload

````
CREATE TABLE c (id INT, name STRING) USING delta
  TBLPROPERTIES ('delta.enableChangeDataFeed' = true);
INSERT INTO c VALUES (1, 'a'), (2, 'b');
UPDATE c SET name = 'bb' WHERE id = 2;
SELECT id, name, _change_type, _commit_version
FROM table_changes('c', 1)
WHERE id = 2;
````

Gluten expands a batch change data feed read into Delta file scans that run in Velox with the filters pushed down. If the expansion fails, the read uses Delta's change data feed scan on Spark without an error.

Fallback

````
SET spark.gluten.sql.columnar.backend.velox.delta.enableChangeDataFeedScan=false;
SELECT * FROM table_changes('c', 1);
````

A read whose version range has any file with a deletion vector stays on Delta's change data feed scan on Spark. Insert-only ranges of a table with deletion vectors still run in Velox. With `spark.gluten.sql.columnar.backend.velox.delta.enableChangeDataFeedScan=false`, every batch change data feed read uses Delta's scan. Streaming change data feed reads ignore this option and run as Delta file scans, which fall back on files with deletion vectors.

### Read metadata
Offload

````
SELECT id, _metadata.file_path, _metadata.row_index
FROM t
WHERE _metadata.row_index = 2;
````

The native scan fills the `_metadata` file fields, `_metadata.row_index` and `input_file_name()`. Delta tests cover `_metadata.row_index` on Spark 3.5 to 4.1 only. On row-tracking tables, `_metadata.row_id` and `_metadata.row_commit_version` are read natively. Seen on 4.1.

Fallback

````
DESCRIBE HISTORY t;
````

Delta reads the transaction log on Spark. Every scan under `_delta_log`, checkpoints included, stays on Spark. The reason is `fallback Delta _delta_log scan`.

## Table features
Offload/Fallback

The Read column covers Spark 3.5 to 4.1. It covers Spark 3.4 only where the row names it. The Native write column applies to Spark 3.5 to 4.1 with native write on.

| Feature | Read | Native write | Notes |
|---|---|---|---|
| Deletion vectors | Offload, Fallback on Spark 3.4 | PartialOffload | See [Deletion vectors](#deletion-vectors). |
| Change data feed | PartialOffload, also on Spark 3.4 | Offload | UPDATE and DELETE write the `_change_data` files with Velox. Seen on 4.1. See [Change data feed](#change-data-feed). |
| Column mapping, name mode | Offload, also on Spark 3.4 | ResultMismatch | See [Write problems](#write-problems). |
| Column mapping, id mode | Offload, also on Spark 3.4. ResultMismatch when field IDs differ from physical names. | Exception | Gluten matches columns by physical name. See [Known problems](#known-problems). |
| Identity columns | Offload | Offload | High-water marks are correct. Seen on 4.1. The projection that generates identity values runs on Spark. |
| Generated columns | Offload | Offload | Seen on 4.1. No Gluten test covers generated columns. Velox computes the generated values. Partition filters derived from them reach the native scan. Years above 9999 differ (see [Write problems](#write-problems)). |
| CHECK constraints, NOT NULL | Offload | PartialOffload | Delta checks the rows on the JVM before Velox writes them. |
| Row tracking | Offload, Exception for `_metadata.base_row_id` filters | Offload | Seen on 4.1. |
| Liquid clustering | Offload | PartialOffload | Native commands write to clustered tables. From code. `OPTIMIZE`, which clusters the data, uses Delta's writer. |
| UniForm (IcebergCompatV1, V2) | | ResultMismatch, Exception when partitioned | The write status is from code. No test reads a UniForm table. |
| Type widening | Offload, Exception for DATE to TIMESTAMP_NTZ | Offload, Exception for FLOAT or DOUBLE partition columns | Seen on 4.1. Spark 3.5 and 4.0 are not covered. |
| TIMESTAMP_NTZ | PartialOffload, also on Spark 3.4 | Offload, Exception when a native operator produces the column | See [DataType](#datatype). |
| Variant, including shredding | Fallback | Fallback | Spark 4.x. See [DataType](#datatype). |
| Collations | Not tested | Not tested | Spark 4.x. See [DataType](#datatype). |
| materializePartitionColumns | | ResultMismatch | Spark 4.1. From code. |

## DataType

The native write columns apply to Spark 3.5 to 4.1 with native write on.

| Type | Read | Native write | Native write as partition column | Notes |
|---|---|---|---|---|
| BOOLEAN | Offload | Offload | Offload | |
| TINYINT, SMALLINT, INT, BIGINT | Offload | Offload | Offload | |
| FLOAT, DOUBLE | Offload | Offload | Exception | See [Write problems](#write-problems). |
| DECIMAL | Offload | Offload | Offload | Precision above 18 included. |
| STRING | Offload | Offload | Offload | |
| CHAR, VARCHAR | Not tested | PartialOffload | | Stored as STRING. Delta checks the length on the JVM before the native write. From code. |
| BINARY | Not tested | Not tested | Not tested | Only indirect coverage. |
| DATE | Offload | Offload | Offload | |
| TIMESTAMP | Offload, ResultMismatch for sub-millisecond partition values | Offload | Offload, Exception for sub-millisecond values | The partition column status is from code. See [Known problems](#known-problems). |
| TIMESTAMP_NTZ | PartialOffload | Offload, Exception when a native operator produces the column | Offload, Exception for sub-millisecond values | Filters, sorts, joins and other operators whose input or output includes a TIMESTAMP_NTZ column fall back, even when they do not use that column. Scans, aggregates, shuffles and some projections run in Velox. |
| STRUCT, ARRAY, MAP | Offload | Offload | | |
| VARIANT | Fallback | Fallback | | Spark 4.x. Shredded variant reads fall back too. The write status is from code. |
| Collated STRING | Not tested | Not tested | | Spark 4.x. Scans fall back and native writes use Delta's writer. From code. |

A plan node falls back when its output contains a type Velox does not support, such as VARIANT. A scan that does not select such a column can still offload. Delta Lake does not allow interval columns.

## Format
PartialOffload

Delta data files are Parquet, and Gluten reads them with Velox's Parquet reader. Velox writes them only for native write commands on Spark 3.5 to 4.1. Otherwise Delta's Parquet writer writes them.

### Natively written files

| Property | Behavior |
|---|---|
| Compression | snappy (default), gzip, zstd (always level 3), lz4 (Hadoop framing), none. Other codecs: see [Write problems](#write-problems). |
| Row groups | Sized by `spark.gluten.sql.columnar.parquet.write.blockSize` and `spark.gluten.sql.native.parquet.write.blockRows`. `parquet.*` settings are ignored. |
| TIMESTAMP | INT64 microseconds. `spark.sql.parquet.outputTimestampType` is ignored. Delta Lake's writer uses INT96 by default. Spark reads both. From code. |
| Parquet field IDs | Not written, for column-mapped and UniForm tables too. See [Write problems](#write-problems). |
| Spark schema footer entry | `org.apache.spark.sql.parquet.row.metadata` is not written ([#13288](https://github.com/apache/gluten/issues/13288)). |
| Nested field names | Lowercased below the top level when `spark.sql.caseSensitive=false`. Spark and Gluten match them case-insensitively. A case-sensitive reader can miss them. From code. |
| Page index | Always written. From code. |
| `spark.sql.parquet.writeLegacyFormat=true` | Applies to DECIMAL only. Arrays and maps keep the standard layout. From code. |
| File statistics | numRecords, min, max and nullCount are computed in Velox. They match Delta's in Gluten's tests. Most of Delta's data-skipping tests pass (seen on 4.1). About 22 tests that check exact per-file values still fail. |
| Data file paths | Come from Delta's committer, so names and random prefixes follow Delta. |

## SQL

| Statement | Status | Notes |
|---|---|---|
| `SELECT` | Offload | See [Reading](#reading). |
| `table_changes()` | PartialOffload | See [Change data feed](#change-data-feed). |
| `CREATE TABLE ... AS SELECT`, `REPLACE TABLE ... AS SELECT` | Offload | With native write on. See [Creating a table](#creating-a-table). |
| `INSERT`, `UPDATE`, `DELETE`, `MERGE`, `OPTIMIZE`, `REORG` | | See [Write commands](#write-commands). |
| `CREATE TABLE` without `AS SELECT`, `ALTER TABLE`, `DROP TABLE`, `DESCRIBE` | Fallback | Run on the driver. |
| `VACUUM`, `RESTORE`, `CONVERT TO DELTA`, `SHALLOW CLONE` | Fallback | Write no Parquet data files. |

## Schema evolution
PartialOffload

The `mergeSchema` and `overwriteSchema` rows need native write on Spark 3.5 to 4.1. Otherwise Delta writes the files.

| Change | Status | Notes |
|---|---|---|
| `mergeSchema` with `save(path)` | Offload | Velox writes the evolved schema. |
| `overwriteSchema` with `save(path)` | Offload | Delta applies the change before the write. |
| Schema evolution in SQL `INSERT` | PartialOffload | |
| Schema evolution in `MERGE` | PartialOffload | Results match Delta Lake except for struct map keys and DATE to TIMESTAMP_NTZ widening. Seen on 4.1. |
| `RENAME COLUMN` with column mapping | Offload | Reads after top-level and nested renames keep partition pruning and file skipping. |
| `DROP COLUMN` with column mapping | Offload | Seen on 4.1. |
| Type widening | Offload, Exception for DATE to TIMESTAMP_NTZ | Seen on 4.1. See [Table features](#table-features). |
| `spark.sql.parquet.mergeSchema=true` | Fallback | Every Parquet scan falls back, Delta scans included. |

## Known problems

### Read problems

| Problem | Status | Versions | Issue |
|---|---|---|---|
| A column default stored as `EXISTS_DEFAULT` metadata, for example after `CONVERT TO DELTA`, reads as NULL for rows in files without that column. | ResultMismatch | Seen on 4.1. #11494 shows the same bug for plain Parquet on 3.4 and later. | [#11494](https://github.com/apache/gluten/issues/11494) |
| In column mapping id mode, when field IDs no longer match the Parquet column names, Gluten returns the column with the matching physical name. Delta Lake returns NULL or the column the ID points to. | ResultMismatch | 3.4 to 4.1, seen on 4.1 | [#12241](https://github.com/apache/gluten/issues/12241) |
| On a TIMESTAMP-partitioned table with six partition values from .1 to .123456 seconds, `count(DISTINCT ts)` returns 3, as if the values were truncated to milliseconds. Equality filters on the column still prune correctly. | ResultMismatch | Seen on 4.1 | |
| Reading a column widened from DATE to TIMESTAMP_NTZ fails with `Converted type DATE is not allowed for requested type TIMESTAMP`. | Exception | Seen on 4.1 | |
| A query that selects only a `_metadata` field, such as `_metadata.file_name`, from a table with a data column of the same name reads the data column and fails. | Exception | Seen on 4.1 | |
| A filter on `_metadata.base_row_id` fails with `Synthesized column 'base_row_id' failed filter validation`. | Exception | Seen on 4.1 | |
| On a row-tracking table whose older files lack base row IDs, selecting `_metadata.row_id` returns rows. Delta raises an error. Only a manual metadata commit gets a table into this state. | ResultMismatch | Seen on 4.1 | |
| Reading or deleting from a 2-billion-row table with deletion vectors can grow native memory until the JVM is killed. Not checked on main since the report. | Exception | 3.5 to 4.1, seen on 4.1 | [#12387](https://github.com/apache/gluten/issues/12387) |
| Selecting `__delta_internal_is_row_deleted` or `__delta_internal_row_index` from a relation built by hand over `DeltaParquetFileFormat` returns wrong values. SQL and DataFrame reads never select these columns. | ResultMismatch | Seen on 4.1 | [#13042](https://github.com/apache/gluten/pull/13042) |
| Scans of a table with many small files can be slower than vanilla Spark on EBS storage. The report shows two of five TPC-DS SF1000 queries 54% to 71% slower, with 182,300 files in one table. A maintainer did not see the slowdown on a host with local NVMe storage. | | Reported on 3.4 | [#10778](https://github.com/apache/gluten/issues/10778) |

### Write problems

These need native write on, unless a row says otherwise.

| Problem | Status | Versions | Issue |
|---|---|---|---|
| A write partitioned by a FLOAT or DOUBLE column fails with `Unsupported partition type`. | Exception | 3.5 to 4.1, seen on 4.1 | [#13138](https://github.com/apache/gluten/issues/13138) |
| A write to a path scheme the native library cannot write fails at task time. This covers custom Hadoop file systems, and S3, GCS, HDFS or ABFS in a build without that connector. | Exception | 3.5 to 4.1, seen on 4.1 | [#4963](https://github.com/apache/gluten/issues/4963) |
| Files written for a column mapping table, in name or id mode, have no Parquet field IDs, which the Delta Lake protocol requires. Delta Lake and Gluten read name-mode files by physical name. Readers that match by field ID cannot. | ResultMismatch | 3.5 to 4.1, seen on 4.1 | |
| Spark's own reader fails on natively written id-mode files with `Spark read schema expects field Ids, but Parquet file schema doesn't contain any field Ids`. Spark uses this reader when a scan falls back or Gluten is off. Spark skips this check when `spark.sql.parquet.fieldId.read.ignoreMissing=true`. That setting is not tested with these files. | Exception | 3.5 to 4.1, seen on 4.1 | |
| Files written for a UniForm table have no Parquet field IDs, so the Iceberg metadata points at files without IDs. No error is raised. | ResultMismatch | 3.5 to 4.1, from code | [#12957](https://github.com/apache/gluten/pull/12957) |
| A write to a partitioned UniForm table fails with `The file schema type should be equal with the input rowvector type`. | Exception | 3.5 to 4.1, from code | [#12039](https://github.com/apache/gluten/issues/12039) |
| Writes to a table with the `materializePartitionColumns` feature leave the partition columns out of the data files. | ResultMismatch | 4.1, from code | |
| A write fails with `The file schema type should be equal with the input rowvector type` when a native operator produces a TIMESTAMP_NTZ column, for example `CAST(s AS TIMESTAMP_NTZ)`. On Spark 4.x, `spark.gluten.sql.columnar.backend.velox.enableTimestampNtzValidation=true` sends TIMESTAMP_NTZ writes to Delta's writer. It is not tested as a workaround. | Exception | 3.5 to 4.1, seen on 4.1 | |
| A partitioned write fails in Velox's `PartitionIdGenerator` when a TIMESTAMP or TIMESTAMP_NTZ partition value has sub-millisecond digits. | Exception | 3.5 to 4.1, seen on 4.1 (TIMESTAMP_NTZ), from code (TIMESTAMP) | |
| A generated partition column such as `DATE_FORMAT(ts, 'yyyy-MM')` stores `12345-07` for the year 12345. Spark stores `+12345-07`. Not checked with native write off. | ResultMismatch | Seen on 4.1 | |
| The `compression` option set to brotli or lzo fails the write. | Exception | 3.5 to 4.1, from code | |
| The `compression` option set to lz4_raw writes Snappy data into a `.lz4raw.parquet` file. | ResultMismatch | 3.5 to 4.1, from code | |
| `maxRecordsPerFile` on an unpartitioned write applies per batch. A file can exceed the limit and the other rotated files are empty. | ResultMismatch | 3.5 to 4.1, from code | |
| `spark.sql.maxConcurrentOutputFileWriters` above 0 makes a partitioned write with unsorted input fail with `Underlying columnar data is inaccessible from BatchCarrierRow`. | Exception | 3.5 to 4.1, from code | |
| An unpartitioned native write whose first task gets no rows, for example a write of an empty DataFrame, leaves a 0-byte file that is not valid Parquet. Reading the table directory as plain Parquet then fails with `is not a Parquet file (length is too low: 0)`. With statistics on, the file stays on disk until `VACUUM`. With `spark.databricks.delta.stats.collect=false`, Delta commits it. From code. | ResultMismatch | 3.5 to 4.1, seen on 4.1 | |
| A statistics aggregation that Velox cannot offload fails the write with `ClassCastException`. No type is known to trigger it on main. | Exception | 3.5 to 4.1, from code | [#12538](https://github.com/apache/gluten/issues/12538) |

### MERGE, UPDATE and DELETE problems

These also happen with native write off, unless a row says otherwise.

| Problem | Status | Versions | Issue |
|---|---|---|---|
| UPDATE or DELETE through a temp view that swaps column names, such as `SELECT value AS key, key AS value FROM t`, changes different rows than Delta Lake. In Delta Lake's test, a DELETE removes both rows where Delta Lake removes one. Not checked with native write off. | ResultMismatch | 3.5 to 4.1, seen on 4.1 | |
| MERGE operation metrics in `DESCRIBE HISTORY` are too high, and a MERGE that changes nothing records change data rows on a change data feed table. | ResultMismatch | 3.5 to 4.1, seen on 4.1 | [#9003](https://github.com/apache/gluten/issues/9003) |
| With ANSI mode on and `spark.gluten.sql.ansiFallback.enabled=false`, a MERGE that casts `ARRAY<BIGINT>` to `ARRAY<INT>` or `MAP<STRING,BIGINT>` to `MAP<STRING,INT>` with overflowing values completes without an error. | ResultMismatch | Seen on 4.1 | |
| MERGE schema evolution that adds or removes fields of a STRUCT map key fails with `map key cannot be indeterminate`. | Exception | Seen on 4.1 | |

### Error types

These operations fail as in Delta Lake, with a different exception type. A failed write commits nothing. Seen on 4.1. Spark 3.5 and 4.0 share the code.

| Failure | Delta Lake raises | Gluten raises |
|---|---|---|
| CHECK, NOT NULL or generated-column violation on a partitioned native write | `InvariantViolationException` | `SparkException` (`TASK_WRITE_FAILED`) wrapping `GlutenException` |
| `replaceWhere` violation on a partitioned native write | `AnalysisException` (`DELTA_REPLACE_WHERE_MISMATCH`) | `SparkException` (`TASK_WRITE_FAILED`) wrapping `GlutenException` |
| Store-assignment cast overflow in MERGE | `DeltaArithmeticException` (`DELTA_CAST_OVERFLOW_IN_TABLE_WRITE`) | `GlutenException` |
| ANSI cast overflow evaluated in Velox | `ArithmeticException` | `GlutenException` (`VeloxUserError`) |
| Missing data file | `FAILED_READ_FILE.FILE_NOT_EXIST` | `GlutenException` (`FILE_NOT_FOUND`) |

For errors raised on the JVM, the Delta Lake message stays in the cause. On unpartitioned `save(path)` writes, CHECK and NOT NULL violations keep Delta Lake's exception types. Tracked in [#11912](https://github.com/apache/gluten/issues/11912).

## Configuration
### Gluten Options

| Gluten option | Default | Description |
| --- | --- | --- |
| spark.gluten.sql.columnar.backend.velox.delta.enableNativeWrite | false | Experimental. Velox writes the data files of the commands marked Offload in [Write commands](#write-commands). Spark 3.5 to 4.1. No effect on Spark 3.4. |
| spark.gluten.sql.columnar.backend.velox.delta.enableNativeDmlRowIndexScan | true | Experimental. When false, the DML target scan that reads row indexes for deletion vectors stays on Spark. Other scans still offload. |
| spark.gluten.sql.columnar.backend.velox.delta.enableChangeDataFeedScan | true | Experimental. When false, batch change data feed reads use Delta's own scan on Spark. |
| spark.gluten.sql.columnar.filescan | true | When false, every file scan falls back, Delta scans included. Delta has no read switch of its own. |
| spark.gluten.sql.ansiFallback.enabled | true | With ANSI mode on, every query falls back. See [ANSI mode on Spark 4.x](#ansi-mode-on-spark-4x). |
| spark.gluten.sql.columnar.parquet.write.blockSize | 128MB | Row-group size for native Delta writes. |
| spark.gluten.sql.native.parquet.write.blockRows | 100000000 | Row-group row limit for native Delta writes. |
| spark.gluten.sql.native.writer.enabled | (unset) | Controls Spark's generic native Parquet write. No effect on Delta writes. |
| spark.gluten.sql.columnar.backend.velox.enableTimestampNtzValidation | false | When true, every plan with TIMESTAMP_NTZ falls back. On Spark 4.x, native Delta writes of TIMESTAMP_NTZ schemas then use Delta's writer. |
| spark.gluten.sql.columnar.backend.velox.scanMapKeyPruningEnabled | false | When true, Velox prunes unused map keys in the scan. Applies to Delta scans without column mapping, with or without deletion vectors. Off for column-mapped tables. |
| spark.gluten.sql.columnar.backend.velox.parquetUseColumnNames | true | Velox matches Parquet columns by name. Id-mode column mapping tables are read by physical name when true and by position when false. |
| spark.gluten.sql.fallbackUnexpectedMetadataParquet | false | When true, Delta scans fall back if sampled footers are encrypted, carry legacy datetime rebase metadata, use LZO or BROTLI, or have variant annotations. LZ4_RAW also counts on Spark 4.0 and 4.1. The variant check runs on Spark 4.1 only. The sample is the first 10 files (`.limit`) under the table root, `_delta_log` files included, so it may contain no data files. |
| spark.gluten.sql.scan.fileSchemeValidation.enabled | true | Scans on a file scheme Velox cannot read fall back. |

The three `spark.gluten.sql.columnar.backend.velox.delta.*` options are read per query, so `SET` changes them for the session.

### Catalogs

Gluten does not change DeltaCatalog. `CREATE TABLE ... AS SELECT` and `REPLACE TABLE ... AS SELECT` are written natively only through a DeltaCatalog. With native write on, `UPDATE`, `DELETE`, `OPTIMIZE` and `save(path)` are written natively with any catalog.

### SQL Extensions
Fallback

Supports `spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension`. Delta's own SQL commands, such as `VACUUM`, `RESTORE` and `DESCRIBE HISTORY`, fall back. `OPTIMIZE` follows [Write commands](#write-commands). GlutenPlugin appends its own extension.

### Runtime configuration
The "Gluten Support" column uses these values:

✅ Supported<br>
❌ Not Supported<br>
⚠️ Partial Support<br>
🚫 Not applied or transparent to Gluten<br>

A blank cell means unknown.

### Spark SQL Options

| Option | Default | Description | Gluten Support |
| --- | --- | --- | --- |
| spark.sql.extensions | (none) | Set to `io.delta.sql.DeltaSparkSessionExtension`. | ✅ |
| spark.sql.catalog.spark_catalog | Spark session catalog | Set to `org.apache.spark.sql.delta.catalog.DeltaCatalog`. | ✅ |
| spark.sql.ansi.enabled | true on Spark 4.x, false on 3.x | When true, every query falls back unless `spark.gluten.sql.ansiFallback.enabled=false`. | ⚠️ |
| spark.sql.parquet.compression.codec | snappy | Codec for native writes when the `compression` option is unset. | ⚠️ |
| spark.sql.files.maxRecordsPerFile | 0 | Exact on partitioned native writes. See [Write problems](#write-problems). | ⚠️ |
| spark.sql.maxConcurrentOutputFileWriters | 0 | Above 0, partitioned native writes with unsorted input fail. From code. | ❌ |
| spark.sql.files.ignoreMissingFiles | false | Passed to Velox. Native scans skip missing files when true. | ✅ |
| spark.sql.parquet.mergeSchema | false | When true, Delta scans fall back. | ❌ |
| spark.sql.parquet.outputTimestampType | INT96 | Ignored by native writes. See [Natively written files](#natively-written-files). | ❌ |
| spark.sql.parquet.writeLegacyFormat | false | Native writes apply it to DECIMAL only. See [Natively written files](#natively-written-files). | ⚠️ |
| spark.sql.caseSensitive | false | When false, native writes lowercase nested struct field names. | ⚠️ |
| spark.sql.sources.partitionOverwriteMode | STATIC | Velox writes the files of a dynamic overwrite through `save(path)`, and the results are correct (seen on 4.1). | ✅ |
| spark.databricks.delta.deletionVectors.useMetadataRowIndex | true | When false, scans that may read deletion vectors fall back. | ⚠️ |
| spark.databricks.delta.stats.collect | true | Native writes compute file statistics in Velox. | ✅ |
| spark.databricks.delta.stats.skipping | true | File skipping runs on the driver. The scan offloads either way, except in the deletion-vector case in [Deletion vectors](#deletion-vectors). | 🚫 |
| spark.databricks.delta.optimizeWrite.enabled | (unset) | Native writes run the optimized write shuffle in Velox. | ✅ |
| spark.databricks.delta.optimizeWrite.binSize | 512 (MiB) | Used by the native optimized write shuffle. | ✅ |
| spark.databricks.delta.autoCompact.enabled | (unset) | Compacts natively written tables. Seen on 4.1. | ✅ |
| spark.databricks.delta.schema.autoMerge.enabled | false | Delta applies schema changes before the write. | 🚫 |
| spark.databricks.delta.dynamicPartitionOverwrite.enabled | true | Delta decides which partitions to replace. | ✅ |
| spark.databricks.delta.merge.repartitionBeforeWrite.enabled | true | MERGE uses Delta's writer, so this only shapes Delta's write. | 🚫 |
| spark.databricks.delta.optimize.maxFileSize | 1073741824 (1 GiB) | Delta plans the bins. Velox writes the files of plain `OPTIMIZE`. | ✅ |
| spark.databricks.delta.optimize.minFileSize | 1073741824 (1 GiB) | Delta plans the bins. Velox writes the files of plain `OPTIMIZE`. | ✅ |
| spark.databricks.delta.write.txnAppId, spark.databricks.delta.write.txnVersion | (unset) | Idempotent writes. Commit only. | 🚫 |
| spark.databricks.delta.commitInfo.userMetadata | (unset) | Commit only. | 🚫 |
| spark.databricks.delta.properties.defaults.* | (unset) | Default for the matching `delta.*` table property on new tables. | 🚫 |

#### Read options

| Option | Default | Description | Gluten Support |
| --- | --- | --- | --- |
| versionAsOf | (latest) | Version of the table snapshot to read | ✅ |
| timestampAsOf | (latest) | Read the snapshot current at this timestamp | ✅ |
| readChangeFeed | false | Read the change data feed. Ranges with deletion vectors fall back. | ⚠️ |
| startingVersion, endingVersion | (none) | Change data feed version range. `startingVersion` also applies to streaming reads. | ✅ |
| startingTimestamp, endingTimestamp | (none) | Change data feed timestamp range. `startingTimestamp` also applies to streaming reads. | ✅ |
| maxFilesPerTrigger | 1000 | Streaming: files per micro-batch | 🚫 |
| maxBytesPerTrigger | (unset) | Streaming: bytes per micro-batch | 🚫 |
| ignoreDeletes, ignoreChanges, skipChangeCommits | false | Streaming: how to treat commits that remove data | 🚫 |
| excludeRegex | (none) | Streaming: skip files whose path matches | 🚫 |
| failOnDataLoss | true | Streaming: fail when data was deleted | 🚫 |
| allowSourceColumnDrop, allowSourceColumnRename, allowSourceColumnTypeChange | | Streaming schema tracking. Delta Lake 4.0.1 and 4.1.0. | 🚫 |

#### Write options

| Option | Default | Description | Gluten Support |
| --- | --- | --- | --- |
| compression | spark.sql.parquet.compression.codec | Native writes support the codecs in [Natively written files](#natively-written-files). | ⚠️ |
| maxRecordsPerFile | spark.sql.files.maxRecordsPerFile | Exact on partitioned native writes. See [Write problems](#write-problems). | ⚠️ |
| partitionOverwriteMode | spark.sql.sources.partitionOverwriteMode | `dynamic` works through `save(path)`. `INSERT OVERWRITE` and `insertInto` use Delta's writer. | ✅ |
| replaceWhere | (none) | A violating write fails and commits nothing, with a different error type (see [Error types](#error-types)). | ⚠️ |
| mergeSchema | spark.databricks.delta.schema.autoMerge.enabled | Delta applies the schema change before the write. | 🚫 |
| overwriteSchema | false | Delta applies the schema change before the write. | 🚫 |
| optimizeWrite | (unset) | Runs the rebalancing shuffle in Velox before the native write. | ✅ |
| txnAppId, txnVersion | (unset) | Idempotent writes. Commit only. | 🚫 |
| userMetadata | (unset) | Stored in `commitInfo.userMetadata`. Commit only. | 🚫 |
| dataChange | true | Sets `dataChange` on the file actions. Commit only. | 🚫 |
| parquet.* (writer option or Hadoop setting) | | Ignored by native writes. Use `spark.gluten.sql.columnar.parquet.write.blockSize` for row groups. | ❌ |
| fs.* Hadoop file system options | | A custom file system scheme fails on native writes. Per-write options for supported stores, such as `fs.s3a.access.key`, are not tested with native writes. | ❌ |

### Delta Table Properties

`delta.enableRowTracking`, `delta.enableTypeWidening`, `delta.universalFormat.enabledFormats`, `delta.enableIcebergCompatV2`, `delta.autoOptimize.*` and `delta.dataSkippingStatsColumns` do not exist in Delta Lake 2.4.0.

#### Read and write properties

| Property | Default | Description | Gluten Support |
| --- | --- | --- | --- |
| delta.enableDeletionVectors | false | Velox applies deletion vectors in the scan on Spark 3.5 to 4.1. Spark 3.4 falls back. | ✅ Spark 3.5 to 4.1 |
| delta.enableChangeDataFeed | false | Reads and native UPDATE and DELETE offload. For MERGE, see [MERGE, UPDATE and DELETE problems](#merge-update-and-delete-problems). | ⚠️ |
| delta.columnMapping.mode | none | Reads offload. For native writes, see [Write problems](#write-problems). | ⚠️ |
| delta.enableRowTracking | false | Reads and native writes offload. For `_metadata.base_row_id` filters, see [Read problems](#read-problems). | ⚠️ |
| delta.enableTypeWidening | false | Most widenings read natively. For DATE to TIMESTAMP_NTZ and for FLOAT or DOUBLE partition columns, see [Known problems](#known-problems). | ⚠️ |
| delta.universalFormat.enabledFormats | (empty) | See [Write problems](#write-problems). | ⚠️ |
| delta.enableIcebergCompatV2 | (unset) | Same as `delta.universalFormat.enabledFormats`. | ⚠️ |
| delta.autoOptimize.optimizeWrite | (unset) | Native writes run the optimized write shuffle in Velox. | ✅ |
| delta.autoOptimize.autoCompact | (unset) | Compacts natively written tables. Seen on 4.1. | ✅ |
| delta.dataSkippingNumIndexedCols | 32 | Native statistics cover the configured columns. | ✅ |
| delta.dataSkippingStatsColumns | (unset) | Native statistics cover the configured columns. | ✅ |
| delta.randomizeFilePrefixes | false | File paths come from Delta's committer. Delta Lake 3.3.2 and 4.0.1 apply it only to deletion-vector files. Delta Lake 4.1.0 also applies it to data files. | 🚫 |
| delta.randomPrefixLength | 2 | File paths come from Delta's committer. | 🚫 |

#### Commit and log properties

| Property | Default | Description | Gluten Support |
| --- | --- | --- | --- |
| delta.appendOnly | false | Commit check in Delta. | 🚫 |
| delta.checkpointInterval | 10 | Checkpoints are written by Delta. | 🚫 |
| delta.logRetentionDuration | interval 30 days | Log cleanup runs in Delta. | 🚫 |
| delta.deletedFileRetentionDuration | interval 1 week | Used by `VACUUM`, which runs in Delta. | 🚫 |
| delta.isolationLevel | Serializable | Commit check in Delta. | 🚫 |
