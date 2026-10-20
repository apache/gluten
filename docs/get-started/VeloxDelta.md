---
layout: page
title: Delta Lake Support in Velox Backend
nav_order: 13
parent: Getting-Started
---

# Delta Lake Support in Velox Backend

Gluten's Velox backend can accelerate eligible scans and query operators over Delta Lake tables. Read
acceleration does not require native writes. Native Delta writes are experimental and disabled by
default. This guide covers the supported Spark and Delta Lake versions, setup, feature support and
known limitations.

## How a Delta query runs

Take a Delta table partitioned by `sale_date`:

```sql
SELECT SUM(amount)
FROM sales
WHERE sale_date = DATE '2026-10-01'
  AND amount > 100;
```

![How Gluten plans and runs a Delta query: Delta snapshot and file pruning, then Gluten offload checks, then Velox and Spark operators on the executors](../image/delta-query-flow.svg)

Delta Lake loads the snapshot and prunes partitions and files while Spark plans the query. Gluten then
offloads the operators Velox supports, and the rest run on Spark, so one query can mix Velox and Spark
operators. Delta Lake keeps the table metadata, the transaction log and commits.

## Supported Spark and Delta Lake versions

| Spark | Delta Lake | Delta Lake jar | Read | Native write | Tested by |
|---|---|---|---|---|---|
| 3.4.4 | 2.4.0 | `io.delta:delta-core_2.12:2.4.0` | Offload. Deletion vector scans fall back. | None | Gluten suites |
| 3.5.5 | 3.3.2 | `io.delta:delta-spark_2.12:3.3.2` (`_2.13` for Scala 2.13) | Offload | Experimental, off by default | Gluten suites, copied Delta Lake suites (Scala 2.12 only) |
| 4.0.2 | 4.4.1 | `io.delta:delta-spark_4.0_2.13:4.4.1` | Offload | Experimental, off by default | Gluten suites, copied Delta Lake suites tagged `ExtendedSQLTest` |
| 4.1.1 | 4.4.1 | `io.delta:delta-spark_4.1_2.13:4.4.1` | Offload | Experimental, off by default | Gluten suites, copied Delta Lake suites tagged `ExtendedSQLTest`, nightly Delta Lake job |
| 4.2.0 | 4.4.1 | `io.delta:delta-spark_4.2_2.13:4.4.1` | Offload | Experimental, off by default | Gluten suites |

Each Spark profile builds against the one Delta Lake release in the table, so deploy that Delta Lake artifact. All Spark 4.x profiles use Delta Lake 4.4.1, which Delta publishes for each Spark 4.x line. On Spark 4.0 the artifact is `delta-spark_4.0_2.13`, not `delta-spark_2.13`. On Spark 4.x, the Read and Native write columns assume ANSI mode off (see [ANSI mode on Spark 4.x](#ansi-mode-on-spark-4x)). The copied Delta Lake suites run with native write on and `spark.gluten.sql.fallbackUnexpectedMetadataParquet=true`, which is off by default.

Spark 4.1 has the most tests, because the nightly job runs on it. That job builds Delta Lake 4.4.1, the release Gluten builds against, from source against Spark 4.1.0. The job puts a Gluten bundle compiled against Spark 4.1.1 on the test classpath. It replaces Delta Lake's `DeltaSQLCommandTest` with Gluten's copy, which loads Gluten with native write on and the non-default `spark.gluten.sql.fallbackUnexpectedMetadataParquet=true`. It runs `.github/workflows/util/delta-spark-ut/apply-delta-test-patches.sh`, which sets a row-group row limit in two test fixtures and force-fails the 2-billion-row deletion vector tests. Then the job runs the tests of Delta Lake's `spark` sbt module. The same job also runs on pull requests that change Gluten's Delta code. "Seen on 4.1" on this page refers to that job.

The job's known failures are listed in `.github/workflows/util/delta-spark-ut/known-failures.txt`. Four entries are two 2-billion-row deletion vector tests, run in two suites, that the job force-fails. Some entries check a query plan that changes once Gluten offloads it. Read the README in that directory before treating an entry as a Gluten bug.

On Spark 4.1, Gluten's own suites check deletion vector, change data feed, column mapping, time travel and partition filter reads, and a partitioned native `save(path)`. The copied Delta Lake suites tagged `ExtendedSQLTest` also run there with native write on.

Spark 4.2 uses the same Delta Lake release and the same Gluten code as Spark 4.0 and 4.1. Where a row does not name Spark 4.2, its Spark 4.x statuses apply to 4.2 from code.

### Build and run

Delta Lake support is compiled in only with the `-Pdelta` Maven profile. Build the native library first, as described in [Gluten with Velox Backend](Velox.md).

```bash
./build/mvn clean install -Pbackends-velox -Pspark-3.5 -Pdelta -DskipTests
./build/mvn clean install -Pbackends-velox -Pspark-4.1 -Pscala-2.13 -Pjava-17 -Pdelta \
  -DskipTests -Dmaven.compiler.release=17
```

`dev/buildbundle-veloxbe.sh` does not pass `-Pdelta`, so its bundle has no Delta Lake support. `dev/release/build-release.sh` passes `-Pdelta` only for Spark 3.4 and 3.5, so its Spark 4.0 and 4.1 bundles have no Delta Lake support either. No Gluten bundle contains the Delta Lake jar, including one built with `-Pdelta`. Add Delta Lake with `--packages` and the coordinate from the table above, which also adds its `delta-storage` dependency. When `io.delta.sql.DeltaSparkSessionExtension` is not on the classpath, Gluten logs `Excluding runtime-incompatible component` and leaves its Delta support off. From code.

Native writes to HDFS, S3, GCS or ABFS paths need the native library built with `--enable_hdfs=ON`, `--enable_s3=ON`, `--enable_gcs=ON` or `--enable_abfs=ON`. Without that connector, scans of those paths fall back and native writes fail (see [Write problems](#write-problems)). From code.

For example, on Spark 3.5:

```bash
spark-shell \
  --packages io.delta:delta-spark_2.12:3.3.2 \
  --jars gluten-velox-bundle-spark3.5_2.12-<os>_<arch>-<version>.jar \
  --conf spark.plugins=org.apache.gluten.GlutenPlugin \
  --conf spark.memory.offHeap.enabled=true \
  --conf spark.memory.offHeap.size=4g \
  --conf spark.shuffle.manager=org.apache.spark.shuffle.sort.ColumnarShuffleManager \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
```

On Spark 4.x, use the Gluten bundle and the Delta Lake jar for that version, and add `--conf spark.sql.ansi.enabled=false`.

### ANSI mode on Spark 4.x

Spark 4.x turns ANSI mode on by default. With Gluten's default `spark.gluten.sql.ansiFallback.enabled=true`, every query then falls back, Delta scans and native writes included. From code. Turning ANSI mode off also changes Spark's own behavior for invalid input. For example, `CAST('a' AS INT)` returns NULL instead of failing.

```properties
spark.sql.ansi.enabled=false
```

Gluten's Delta tests run with ANSI mode off in CI. The nightly Delta Lake job turns ANSI mode off in the test setup that loads Gluten. That setup and some Gluten Delta suites also set `spark.gluten.sql.ansiFallback.enabled=false`. On Spark 3.x, turning ANSI mode on makes every query fall back in the same way. From code.

Keeping ANSI on and setting `spark.gluten.sql.ansiFallback.enabled=false` removes that fallback. Gluten passes ANSI mode to Velox. Velox does not implement ANSI behavior for every expression, so some invalid input returns a result where Spark fails ([#10134](https://github.com/apache/gluten/issues/10134)). For Delta Lake, only the nightly Delta Lake job tests that combination, in the Delta Lake 4.2.0 suites that turn ANSI on. It has a known MERGE problem (see [MERGE, UPDATE and DELETE problems](#merge-update-and-delete-problems)). Cast overflow errors raised in Velox have a different exception type (see [Error types](#error-types)).

## Support Status
The following values show the Delta Lake support status:

| Value          | Description                                                                |
|----------------|----------------------------------------------------------------------------|
| Offload        | Runs in the Velox backend. For writes, Velox writes the data files.       |
| PartialOffload | Some operators run in Velox and the rest fall back.                        |
| Fallback       | Runs on Spark or Delta Lake.                                               |
| Exception      | Fails instead of falling back.                                             |
| ResultMismatch | Can return wrong results or write wrong files.                             |

Not tested: no test covers the case.<br>
From code: the status comes from reading the source. No test that runs in CI checks it.<br>
Seen on 4.1: the nightly Delta Lake job on Spark 4.1 showed the result. These results come from runs of Delta Lake 4.2.0's tests, before Gluten moved its Spark 4.x profiles to Delta Lake 4.4.1.<br>
Reported on 3.4, Reported on 4.1: the linked issue reports the result on that Spark version. No CI test checks it.<br>
No label: a test in the Gluten repository that runs in CI checks the status on the stated versions.<br>

## Adding catalogs
Fallback

Catalog operations run on the driver. From code.

## Creating a table
Fallback

```sql
CREATE TABLE t (id INT, name STRING) USING delta;
```

A table created without `AS SELECT` is a metadata-only commit. There is nothing to offload. From code.

Offload

```sql
SET spark.gluten.sql.columnar.backend.velox.delta.enableNativeWrite=true;
CREATE TABLE t2 USING delta AS SELECT id, CAST(id AS STRING) AS name FROM range(0, 3, 1, 1);
```

With `spark.gluten.sql.columnar.backend.velox.delta.enableNativeWrite=true` on Spark 3.5 or 4.x, Velox writes the data files of `CREATE TABLE ... AS SELECT` and `REPLACE TABLE ... AS SELECT` when the target catalog is a DeltaCatalog. Seen on 4.1. On 3.5 and 4.0 this is from code. On Spark 4.x, also apply [ANSI mode on Spark 4.x](#ansi-mode-on-spark-4x).

## Writing
PartialOffload

```sql
INSERT INTO t SELECT id, name FROM source WHERE length(name) = 1;
```

SQL `INSERT` on a Delta table plans as a write node Gluten does not wrap for native write: `AppendDataExecV1` for `INSERT INTO`, `OverwriteByExpressionExecV1` for `INSERT OVERWRITE`, or Delta Lake's `DeltaDynamicPartitionOverwriteCommand` for a dynamic partition overwrite. Gluten wraps only the commands marked Offload in [Write commands](#write-commands). So Delta's writer writes the files of `INSERT`, even with native write on. The `SELECT` can still run in Velox. To append with Velox's writer, use `df.write.format("delta").mode("append").save(path)`. From code.

Fallback

```sql
INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c');
```

With the default `spark.gluten.sql.columnar.localTableScan=false`, the `VALUES` scan runs on Spark. Delta's writer writes the files. From code.

Offload

```sql
SET spark.gluten.sql.columnar.backend.velox.delta.enableNativeWrite=true;
UPDATE t SET name = 'b' WHERE id = 2;
```

On Spark 3.5 or 4.x, set `spark.gluten.sql.columnar.backend.velox.delta.enableNativeWrite=true` to have Velox write the data files of the commands marked Offload in [Write commands](#write-commands). Velox writes the files of this UPDATE. Seen on 4.1. On 3.5 and 4.0 this is from code. On Spark 4.x, also apply [ANSI mode on Spark 4.x](#ansi-mode-on-spark-4x).

On Spark 3.5, native write does not check the codec, the path scheme or the partition column types before it runs. Unsupported values make the task fail or write wrong files. On Spark 4.x, the checks in [Native write on Spark 4.x](#native-write-on-spark-4x) send most of those writes to Delta's writer. Read [Write problems](#write-problems) before turning it on.

### Native write on Spark 4.x

On Spark 4.x, native Delta write writes what Delta Lake's own writer would write: the same rows, schema, Parquet physical types, field IDs and partition columns, honoring the compression codec, `maxRecordsPerFile`, Parquet version and encryption. Row group and page sizes, encodings and other Parquet tuning are Velox's own. Settings are read where Delta Lake's writer reads them: the session, the writer options, and the job's Hadoop configuration, which includes the table properties. Writes the Velox writer cannot match fall back to Delta Lake's writer. These are:

- partitioned tables whose data files store the partition columns, which is Delta Lake 4.4's default
  (`delta.writePartitionColumnsToParquet`) and required by `materializePartitionColumns`;
- column mapping (`name` or `id`), and other columns with `parquet.field.id` metadata, since
  Delta Lake writes Parquet field IDs;
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

These checks are in `GlutenDeltaNativeWriteGate` (from code).

### Checking the plan

A command that Gluten wraps for native write shows a `GlutenDelta` node, for example `Execute GlutenDelta UpdateCommand` or `GlutenDelta AtomicCreateTableAsSelect`. Without the prefix, Delta's writer wrote the files.

```scala
val df = spark.sql("UPDATE delta.`/data/t` SET name = 'bb' WHERE id = 2")
df.queryExecution.executedPlan.treeString.contains("GlutenDelta ")
```

The node also shows when Velox writes no data files. This happens for a DELETE that writes only deletion vectors or only metadata, and for a Spark 4.x write whose schema Velox cannot write, such as one with a VARIANT column. From code. For `DataFrameWriter` calls, read the command's plan from the Spark UI SQL tab or a `QueryExecutionListener`. Each write also reports a nested `deltaTransactionalWrite` execution for the query that feeds it. That plan never has the prefix, whichever writer ran. From code.

### Write commands

The Status column assumes Spark 3.5 or 4.x with native write on and ANSI mode off. On Spark 4.x, an Offload write still falls back in the cases in [Native write on Spark 4.x](#native-write-on-spark-4x). Otherwise every row is at best PartialOffload. In PartialOffload rows, Delta writes the files and the query that feeds the write can run in Velox.

Gluten's CI checks that a partitioned `save(path)` on 3.5 to 4.1 and OPTIMIZE on 3.5 run as Gluten native write commands. It also checks on 3.5 and 4.0 that a DELETE that rewrites a file runs in Gluten's transaction and that its scan runs in Velox. On 3.5 and 4.0, the copied Delta Lake suites also run UPDATE, DELETE, `save(path)` and `CREATE TABLE ... AS SELECT` with native write on. They check results only. On 4.1, the nightly job shows every Offload row except DELETE going through Velox's writer. The other Offload cases are from code.

| SQL | DataFrame API | Status | Notes |
|---|---|---|---|
| | `df.write.format("delta").save(path)` | Offload | Every save mode, with or without `partitionBy`. |
| `CREATE TABLE ... AS SELECT` | `writeTo(t).create()`, `saveAsTable(t)` when `t` does not exist and the mode is not overwrite | Offload | Needs a DeltaCatalog target. |
| `REPLACE TABLE ... AS SELECT`, `CREATE OR REPLACE TABLE ... AS SELECT` | `writeTo(t).replace()`, `writeTo(t).createOrReplace()`, `saveAsTable(t)` in overwrite mode | Offload | Needs a DeltaCatalog target. |
| `UPDATE` | `DeltaTable.update()` | Offload | On a table with deletion vectors, Velox writes only the updated rows. |
| `DELETE` | `DeltaTable.delete()` | Offload (from code on 4.1), PartialOffload when it writes deletion vectors (from code on 4.1), Fallback when it is metadata-only (from code) | Velox writes the rewritten files. A deletion-vector DELETE and a metadata-only DELETE write no data files. |
| `OPTIMIZE`, `OPTIMIZE ... WHERE` | `DeltaTable.optimize().executeCompaction()`, with or without `where(...)` | Offload | Bin-packing, by path or by name. |
| `OPTIMIZE ... ZORDER BY`, `OPTIMIZE ... FULL`, `OPTIMIZE` on a liquid-clustered table | | PartialOffload | From code. |
| `REORG TABLE ... APPLY (PURGE)`, `REORG TABLE ... APPLY (UPGRADE UNIFORM ...)` | | PartialOffload | From code. |
| `INSERT INTO ... SELECT`, `INSERT OVERWRITE`, `INSERT INTO ... REPLACE WHERE` | | PartialOffload | From code. To append natively, use `save(path)` in append mode. |
| | `insertInto(t)`, `saveAsTable(t)` in append mode on an existing table, `writeTo(t).append()`, `overwrite(condition)`, `overwritePartitions()` | PartialOffload | From code. |
| `MERGE INTO` | | PartialOffload | Scans, the DML target scan and joins can run in Velox. On Spark 3.5 and 4.0, Gluten's tests check that a deletion-vector MERGE runs a scan of the target table in Velox. The other cases are from code. Native MERGE write is in progress in [#13324](https://github.com/apache/gluten/pull/13324). |
| | `writeStream.format("delta")` | PartialOffload | Inside `foreachBatch`, batch writes follow the rows above. From code. |

With native write on, `DESCRIBE HISTORY` operation metrics of `save(path)`, UPDATE, DELETE and `CREATE TABLE ... AS SELECT` match Delta Lake in `DescribeDeltaHistorySuite`. Seen on 4.1. Three unpartitioned tests in `DeleteMetricsSuite` and `UpdateMetricsSuite` expect two added files and get one. The job sets `spark.default.parallelism=1`, which plans the five small input files as one scan partition (from code).

### Write features

| Feature | Status | Notes |
|---|---|---|
| Optimized write | Offload | In the commands marked Offload, the rebalancing shuffle runs in Velox. If it fails validation, Delta's shuffle is used. Both are from code. `OPTIMIZE` never uses it. |
| Auto compaction | Offload after native writes, PartialOffload after other writes | Compacts natively written tables (seen on 4.1). The auto compaction tests that fail in the nightly job fail because its `spark.default.parallelism=1` makes their first write produce one file (from code). After a native write, the compaction runs in a Gluten transaction (from code). After INSERT, MERGE or streaming writes, Delta's writer compacts (from code). Delta Lake 2.4.0 has no auto compaction. |

## Reading
### Read data
Offload/Fallback

| Table type | Spark 3.4 / Delta Lake 2.4 | Spark 3.5 to 4.2 |
|---|---|---|
| Unpartitioned | Offload | Offload |
| Partitioned | Offload | Offload |
| Column mapping, name mode | Offload | Offload |
| Column mapping, id mode | Offload, matched by physical name. Known ResultMismatch cases: see [Read problems](#read-problems). | Offload, matched by physical name. Known ResultMismatch cases: see [Read problems](#read-problems). |
| Files with deletion vectors | Fallback | Offload |
| Change data feed, no deletion vectors in range | Offload. PartialOffload when the table allows deletion vectors and the range has `_change_data` files, because those scans then fall back (from code). | Offload |
| Change data feed, deletion vectors in range | Fallback, PartialOffload when a file scan inside Delta's change data feed scan reads no files with deletion vectors and no `_change_data` files (from code) | Fallback, PartialOffload when a file scan inside Delta's change data feed scan reads no files with deletion vectors (from code) |

```sql
SELECT count(1) AS count, name
FROM t
GROUP BY name;
```

Reads need no Gluten option. With `spark.sql.adaptive.enabled=false`, `EXPLAIN` shows the native scan as `FileDeltaScanTransformer` and `EXPLAIN FORMATTED` shows `DeltaScanTransformer`. With AQE on, the default, `EXPLAIN` and `EXPLAIN FORMATTED` of a query with a shuffle, such as this one, print the plan from before Gluten offloads it. The Spark UI plan graph of a finished query shows `DeltaScanTransformer`. The last three sentences are from code. Delta prunes partitions and skips files by statistics while it plans the query, before the scan runs.

| Read | Status | Notes |
|---|---|---|
| By table name, ``delta.`/path` `` or `spark.read.format("delta").load(path)` | Offload | Same native scan. |
| `VERSION AS OF` | Offload | Tested on Spark 3.4 to 4.1. |
| `TIMESTAMP AS OF`, `versionAsOf`, `timestampAsOf` | Offload | From code. Delta Lake's `DeltaTimeTravelSuite` passes (seen on 4.1). |
| Streaming source | Offload | Each micro-batch is a Delta file scan. From code. Delta Lake's `DeltaSourceSuite` passes (seen on 4.1). |
| Dynamic partition pruning | Not tested | |

### Deletion vectors
Offload

```sql
OPTIMIZE t;
ALTER TABLE t SET TBLPROPERTIES ('delta.enableDeletionVectors' = true);
DELETE FROM t WHERE id IN (3, 4);
SELECT * FROM t;
```

On Spark 3.5 to 4.2, Velox drops the deleted rows in the scan. This needs Delta's `spark.databricks.delta.deletionVectors.useMetadataRowIndex=true`, the default. The DML target scan, which finds the rows for new deletion vectors, also runs in Velox. Gluten tests check the scan on Spark 3.5 to 4.1 and the DML target scan on Spark 3.5 and 4.0. Delta writes the deletion-vector files.

Fallback

```sql
SET spark.databricks.delta.deletionVectors.useMetadataRowIndex=false;
SELECT * FROM t;
RESET spark.databricks.delta.deletionVectors.useMetadataRowIndex;
```

| Case | Behavior |
|---|---|
| Spark 3.5 and 4.x with `useMetadataRowIndex=false` | A scan that reads Delta's generated `__delta_internal_is_row_deleted` or `__delta_internal_row_index` column falls back with `fallback Delta scan requiring generated DV metadata`. Delta adds the first column when any file in the scanned table version, micro-batch, DML file set or `OPTIMIZE` bin has a deletion vector, so `OPTIMIZE` and `REORG TABLE ... APPLY (PURGE)` scans of those files also fall back (from code). It adds the second to DML target scans that write deletion vectors. Other scans offload. Tested on Spark 3.5 and 4.0. |
| Spark 3.4, normal reads | A read falls back with `fallback Spark 3.4 Delta DV scan` when a file it selected has a deletion vector. Tested on Spark 3.4. A read whose selected files have none offloads (from code). |
| Spark 3.4, other scans, such as DML target scans, `OPTIMIZE` scans, streaming micro-batches and change data feed scans of `_change_data` files | Fall back whenever the table protocol allows deletion vectors. Change data feed scans of added or removed files fall back only when a file they read has a deletion vector. From code. |
| `spark.databricks.delta.stats.skipping=false` | Delta Lake itself rejects reads of a table with deletion vectors with `Cannot work with a non-pinned table snapshot of the TahoeFileIndex` (seen on 4.1). Before Delta Lake 4.1.0, the table feature alone triggers it (from code). |

The fallback reasons show in [Gluten UI](GlutenUI.md).

### Change data feed
Offload

```sql
CREATE TABLE c (id INT, name STRING) USING delta
  TBLPROPERTIES ('delta.enableChangeDataFeed' = true);
INSERT INTO c VALUES (1, 'a'), (2, 'b');
UPDATE c SET name = 'bb' WHERE id = 2;
SELECT id, name, _change_type, _commit_version
FROM table_changes('c', 1)
WHERE id = 2;
```

Gluten expands a batch change data feed read into Delta file scans that run in Velox with the filters pushed down. Delta also adds an empty placeholder scan to the plan. That scan falls back when the table schema has a MAP at any nesting level (from code). If the expansion fails, Gluten logs a warning and the read uses Delta's change data feed scan. From code.

PartialOffload

```sql
SET spark.gluten.sql.columnar.backend.velox.delta.enableChangeDataFeedScan=false;
SELECT * FROM table_changes('c', 1);
```

A read whose version range adds or removes a file with a deletion vector uses Delta's change data feed scan. Insert-only ranges of a table with deletion vectors enabled still run in Velox. With `spark.gluten.sql.columnar.backend.velox.delta.enableChangeDataFeedScan=false`, every batch change data feed read uses Delta's scan. Delta's scan reads the data files in a query of its own that Gluten also plans, and `EXPLAIN` does not show it (from code). That query's file scans can run in Velox, except scans of added or removed files that have deletion vectors and, on Spark 3.4, scans of `_change_data` files when the table allows deletion vectors (from code). Streaming change data feed reads ignore this option. They run as Delta file scans. A scan whose files have deletion vectors falls back, and on Spark 3.4 so does a scan of `_change_data` files when the table allows deletion vectors (from code).

### Read metadata
Offload

```sql
SELECT id, _metadata.file_path, _metadata.row_index
FROM t
WHERE _metadata.row_index = 2;
```

The native scan fills the `_metadata` file fields and `input_file_name()` (from code), and `_metadata.row_index`. On Spark 3.5 and 4.x, Delta Lake exposes `_metadata.row_index` only on tables that allow deletion vectors or have row tracking, so the example needs `t` from [Deletion vectors](#deletion-vectors) (from code). A query that selects only `_metadata.file_name` can fail when the table has a BIGINT data column named `file_name` (seen on 4.1, see [Read problems](#read-problems)). Gluten tests check `_metadata.row_index` in the native scan on Spark 3.5 and 4.0, and Delta Lake tests that read it pass (seen on 4.1). Spark 3.4 is not tested. On row-tracking tables, the native scan reads `_metadata.row_id` (seen on 4.1, from code on 3.5 and 4.0) and `_metadata.row_commit_version` (from code). In the nightly job, they return correct values except in the cases in [Read problems](#read-problems) (seen on 4.1).

Fallback

```sql
DESCRIBE HISTORY t;
```

`DESCRIBE HISTORY` reads each commit file on Spark, through Delta's LogStore in a `mapPartitions` job. When Delta replays the log for a snapshot, every scan under `_delta_log`, checkpoints included, stays on Spark with the reason `fallback Delta _delta_log scan`. From code.

## Table features
Offload/Fallback

The Read column covers Spark 3.5 to 4.2. It covers Spark 3.4 only where the row names it. The Native write column applies to Spark 3.5 and 4.x with native write on. Where Spark 4.x differs, the row says so.

| Feature | Read | Native write | Notes |
|---|---|---|---|
| Deletion vectors | Offload, Fallback on Spark 3.4 | PartialOffload | Gluten's tests check the results of deletion vector DELETE and MERGE on Spark 3.5 and 4.0 with native write turned on. Delta writes the deletion vector files, and MERGE writes its data files with Delta's writer (see [Write commands](#write-commands)). Both are from code. See [Deletion vectors](#deletion-vectors). |
| Change data feed | Offload, also on Spark 3.4. Ranges with deletion vectors fall back. On Spark 3.4, scans of `_change_data` files also fall back when the table allows deletion vectors (from code). | Offload | UPDATE and DELETE write the `_change_data` files with Velox (from code). Change data feed reads of their changes are correct (seen on 4.1). See [Change data feed](#change-data-feed). |
| Column mapping, name mode | Offload, also on Spark 3.4 | ResultMismatch on Spark 3.5, Fallback on Spark 4.x | On Spark 3.5, natively written files have no Parquet field IDs. Spark 4.x sends these writes to Delta's writer. Both are from code. See [Write problems](#write-problems). |
| Column mapping, id mode | Offload, also on Spark 3.4. ResultMismatch when matching by field ID would select no column (seen on 4.1) or a different column (from code). | ResultMismatch on Spark 3.5, Exception when Spark's reader reads the files. Fallback on Spark 4.x. Both are from code. | Gluten matches columns by physical name. See [Known problems](#known-problems). |
| Identity columns | Offload | Offload | Seen on 4.1. High-water marks are correct. The projection that generates identity values runs on Spark (from code). |
| Generated columns | Offload | Offload | Seen on 4.1. No Gluten test covers generated columns. Velox computes the generated values when it supports the expression (from code). Partition filters derived from them reach the native scan. `DATE_FORMAT` results for years above 9999 differ (see [Write problems](#write-problems)). |
| CHECK constraints, NOT NULL | Offload | Offload | Delta checks the rows on the JVM before Velox writes them. Gluten's tests cover NOT NULL on Spark 3.5 and 4.0. Seen on 4.1. |
| Row tracking | Offload, Exception for `_metadata.base_row_id` filters, ResultMismatch for `_metadata.row_id` when older files lack base row IDs | Offload | Seen on 4.1. See [Read problems](#read-problems). |
| Liquid clustering | Offload | PartialOffload | Reads use the same native scan as other tables. Native commands write to clustered tables. `OPTIMIZE`, which clusters the data, uses Delta's writer. All three are from code. |
| UniForm (IcebergCompatV1, V2) | | ResultMismatch on Spark 3.5, Exception when partitioned. Fallback on Spark 4.x. | The write statuses are from code. No test reads a UniForm table. |
| Type widening | Offload, Exception for DATE to TIMESTAMP_NTZ data columns | Offload. Writes to FLOAT or DOUBLE partition columns and some TIMESTAMP_NTZ writes failed (seen on 4.1). Spark 4.x now sends those writes to Delta's writer (from code). | Seen on 4.1. Spark 3.5 and 4.0 are not covered. Delta Lake 3.3.2 on Spark 3.5 allows only TINYINT to SMALLINT to INT. |
| TIMESTAMP_NTZ | PartialOffload, also on Spark 3.4 | Offload on Spark 3.5, Exception for some writes. Fallback on Spark 4.x. Both are from code. | See [DataType](#datatype). |
| Variant, including shredding | Fallback | Fallback | Spark 4.x. From code. See [DataType](#datatype). |
| Collations | Fallback | | Spark 4.x. From code. See [DataType](#datatype). |
| materializePartitionColumns | | Fallback | Spark 4.x. The feature makes the data files store partition columns, and those partitioned writes fall back (from code). |

## DataType

The Read column applies to Spark 3.4 to 4.1. On all of them, Gluten's tests read INT, BIGINT, DECIMAL(12,2), STRING, DATE, TIMESTAMP, TIMESTAMP_NTZ, STRUCT, ARRAY and MAP columns natively. The other read statuses are from code, unless a row says otherwise. The native write columns apply to Spark 3.5 and 4.x with native write on. Where Spark 4.x differs, the row says so. On Spark 3.5 and 4.0, Gluten's tests write BOOLEAN, INT, BIGINT, STRING, STRUCT and ARRAY columns natively. The other native write statuses are seen on 4.1, unless a row says they are from code.

| Type | Read | Native write | Native write as partition column | Notes |
|---|---|---|---|---|
| BOOLEAN | Offload | Offload | Offload | |
| TINYINT, SMALLINT, INT, BIGINT | Offload | Offload | Offload | |
| FLOAT, DOUBLE | Offload | Offload | Exception on Spark 3.5. On Spark 4.x only with `delta.writePartitionColumnsToParquet=false`; otherwise the write falls back (from code). | See [Write problems](#write-problems). |
| DECIMAL | Offload | Offload | Offload | Precision above 18 included. |
| STRING | Offload | Offload | Offload | |
| CHAR, VARCHAR | Offload | Offload | | Stored as STRING. Delta checks the length on the JVM before the native write. Both statuses are from code. Reads return correct results (seen on 4.1). |
| BINARY | Offload | Offload | Not tested | The read status is from code. One Delta Lake test writes a table with only a BINARY column, checks its file statistics and runs a DELETE that filters on that column (seen on 4.1). |
| DATE | Offload | Offload | Offload | |
| TIMESTAMP | Offload, ResultMismatch for sub-millisecond partition values (seen on 4.1) | Offload on Spark 3.5. On Spark 4.x only with `spark.sql.parquet.outputTimestampType=TIMESTAMP_MICROS`; otherwise the write falls back (from code). | Offload, Exception for sub-millisecond values | The partition column status is from code. On Spark 4.x, partitioned writes also fall back unless `delta.writePartitionColumnsToParquet=false` (from code). See [Known problems](#known-problems). |
| TIMESTAMP_NTZ | PartialOffload | Offload on Spark 3.5, Exception for some writes. Fallback on Spark 4.x. Both are from code. | Offload on Spark 3.5, Exception for sub-millisecond values. Fallback on Spark 4.x. Both are from code. | Filters, sorts, joins and other operators whose input or output includes a TIMESTAMP_NTZ column fall back, even when they do not use that column. Scans, aggregates, shuffles and some projections can run in Velox. A scan whose pushed filter has a TIMESTAMP_NTZ literal falls back (from code). Gluten's tests check on Spark 3.4 to 4.1 that a scan of the column runs in Velox, and that a filter on the column and a sort on another column cause a fallback. The other operator rules are from code. |
| STRUCT, ARRAY, MAP | Offload | Offload | | |
| VARIANT | Fallback | Fallback | | Spark 4.x. Shredded variant reads fall back too. Both statuses are from code. |
| Collated STRING | Fallback | | | Spark 4.x. Scans that read the column fall back (from code). |

A plan node falls back when its output contains a type Velox does not support, such as VARIANT. With the default `spark.gluten.sql.columnar.backend.velox.parquetUseColumnNames=true`, a scan that does not select such a column can still offload. From code. Delta Lake does not allow interval columns.

## Format
PartialOffload

Delta data files are Parquet, and Gluten reads them with Velox's Parquet reader. Velox writes them only for native write commands on Spark 3.5 and 4.x. Otherwise Delta's Parquet writer writes them.

### Natively written files

| Property | Behavior |
|---|---|
| Compression | snappy (default), gzip, zstd (always level 3), lz4 (Hadoop framing), none. From code. Other codecs fall back on Spark 4.x; for Spark 3.5, see [Write problems](#write-problems). |
| Row groups | Sized by `spark.gluten.sql.columnar.parquet.write.blockSize` and `spark.gluten.sql.native.parquet.write.blockRows`. `parquet.*` settings are ignored. From code. |
| TIMESTAMP | INT64 microseconds. On Spark 3.5, `spark.sql.parquet.outputTimestampType` is ignored, and Delta Lake's writer uses INT96 by default. On Spark 4.x, writes with TIMESTAMP columns fall back unless that setting is `TIMESTAMP_MICROS`. Spark reads both. From code. |
| Parquet field IDs | Not written. On Spark 3.5 this includes column-mapped and UniForm tables. On Spark 4.x, writes that need field IDs fall back. Both are from code. See [Write problems](#write-problems). |
| Spark schema footer entry | `org.apache.spark.sql.parquet.row.metadata` is not written ([#13288](https://github.com/apache/gluten/issues/13288)). Seen on 4.1. |
| Nested field names | Lowercased below the top level when `spark.sql.caseSensitive=false`. Spark and Gluten match them case-insensitively. A case-sensitive reader can miss them. From code. |
| Page index | Always written. From code. |
| `spark.sql.parquet.writeLegacyFormat=true` | Applies to DECIMAL only. Arrays and maps keep the standard layout. On Spark 4.x, a write with array or map columns falls back when it is set. From code. |
| File statistics | numRecords, min, max and nullCount are computed in Velox (checked on Spark 3.5). Gluten's tests check the values against the rows in each file on Spark 3.5 to 4.1. Most tests in Delta's data-skipping suites pass (seen on 4.1). Of the 34 that fail, 22 check statistics and expect specific rows in each file after `repartition(2)`. The other 12 hit the `_metadata.file_name` failure in [Read problems](#read-problems). |
| Data file paths | Come from Delta's committer, so names and random prefixes follow Delta. File names seen on 4.1, random prefixes from code. |

## SQL

| Statement | Status | Notes |
|---|---|---|
| `SELECT` | Offload | See [Reading](#reading). |
| `table_changes()` | Offload | Ranges with deletion vectors fall back. See [Change data feed](#change-data-feed). |
| `CREATE TABLE ... AS SELECT`, `REPLACE TABLE ... AS SELECT` | Offload | With native write on, Spark 3.5 or 4.x, and a DeltaCatalog target. Seen on 4.1, from code on 3.5, 4.0 and 4.2. See [Creating a table](#creating-a-table). |
| `INSERT`, `UPDATE`, `DELETE`, `MERGE`, `OPTIMIZE`, `REORG` | | See [Write commands](#write-commands). |
| `CREATE TABLE` without `AS SELECT`, `DROP TABLE`, `DESCRIBE` | Fallback | They read and write no data files. From code. |
| `ALTER TABLE` | Fallback, PartialOffload when it reads data | Most forms read and write no data files. `ADD CONSTRAINT` scans the table, and that scan can run in Velox. On Spark 3.5 to 4.1, `ALTER COLUMN ... SYNC IDENTITY` also scans it, and `DROP FEATURE` and `SET TBLPROPERTIES ('delta.columnMapping.mode' = 'none')` can rewrite data files with Delta's writer. From code. |
| `VACUUM`, `RESTORE`, `SHALLOW CLONE` | | They write no Parquet data files. From code. |
| `CONVERT TO DELTA` | PartialOffload | Writes no Parquet data files. Unless the statement has `NO STATISTICS` or `spark.databricks.delta.stats.collect=false` is set, it scans the Parquet files for statistics. That scan can run in Velox. From code. |

## Schema evolution
PartialOffload

The `mergeSchema` and `overwriteSchema` rows need native write on Spark 3.5 or 4.x. Otherwise Delta writes the files.

| Change | Status | Notes |
|---|---|---|
| `mergeSchema` with `save(path)` | Offload | Velox writes the evolved schema (from code). On Spark 3.5 and 4.0, the copied `DeltaSuite` checks the results of a `mergeSchema` overwrite. Delta Lake's `SchemaEnforcementWithPathSuite` passes (seen on 4.1). |
| `overwriteSchema` with `save(path)` | Offload | Delta applies the change before the write. From code. Delta Lake's `SchemaEnforcementWithPathSuite` passes (seen on 4.1). |
| Schema evolution in SQL `INSERT` | PartialOffload | From code. |
| Schema evolution in `MERGE` | PartialOffload, Exception for some STRUCT map key changes | The merged table contents match Delta Lake. Some STRUCT map key changes fail in Velox (see [MERGE, UPDATE and DELETE problems](#merge-update-and-delete-problems)). The two TIMESTAMP_NTZ widening tests fail before the MERGE runs, in a setup write of TIMESTAMP_NTZ values. Seen on 4.1. |
| `RENAME COLUMN` with column mapping | Offload | Reads after top-level and nested renames return correct results. After a top-level rename, partition pruning and file skipping still work. |
| `DROP COLUMN` with column mapping | Offload | The drop is a metadata-only commit. Reads after the drop run in Velox. Both are from code. Delta Lake's `DeltaDropColumnSuite` passes (seen on 4.1). |
| Type widening | Offload, Exception for DATE to TIMESTAMP_NTZ and for native writes to FLOAT or DOUBLE partition columns | Seen on 4.1. See [Table features](#table-features). |
| Parquet schema merging on read (`spark.sql.parquet.mergeSchema=true`) | Fallback | This is Spark's Parquet read option. Delta Lake's `mergeSchema` write option is a different setting, and it is Offload (see the first row). With the read option on, every Parquet scan falls back, Delta scans included. From code. |

## Known problems

### Read problems

| Problem | Status | Versions | Issue |
|---|---|---|---|
| A column default stored as `EXISTS_DEFAULT` metadata, for example after `CONVERT TO DELTA`, reads as NULL for rows in files without that column. | ResultMismatch | Seen on 4.1. #11494 reports the same bug for plain Parquet on 3.4 to 4.1. | [#11494](https://github.com/apache/gluten/issues/11494) |
| In column mapping id mode, when field IDs no longer match the Parquet column names, Gluten returns the column with the matching physical name. Delta Lake returns NULL when no file column has that ID (seen on 4.1). It returns the column the ID points to when another column has it (from code). | ResultMismatch | 3.4 to 4.1. Seen on 4.1 for the NULL case, from code otherwise. | [#12241](https://github.com/apache/gluten/issues/12241) |
| On a TIMESTAMP-partitioned table with six partition values from .1 to .123456 seconds, `count(DISTINCT ts)` returns 3, as if the values were truncated to milliseconds. Equality filters on the column still return the right rows. | ResultMismatch | Seen on 4.1 | |
| Reading a data column widened from DATE to TIMESTAMP_NTZ fails on files written before the change with `Converted type DATE is not allowed for requested type TIMESTAMP UTC`. A partition column widened the same way reads correctly. | Exception | Seen on 4.1 | |
| A query that selects only `_metadata.file_name` from a table with a BIGINT data column named `file_name` reads the data column and fails with `Converted type BIGINT is not allowed for requested type VARCHAR`. | Exception | Seen on 4.1 | |
| An equality filter on `_metadata.base_row_id` fails with `Synthesized column 'base_row_id' failed filter validation` when a scanned file has a different base row ID. | Exception | Seen on 4.1 | |
| On a row-tracking table whose older files lack base row IDs, selecting `_metadata.row_id` returns rows. Delta raises an error. Delta Lake's test creates this state with a manual metadata commit. `ALTER TABLE ... SET TBLPROPERTIES` backfills base row IDs when it enables row tracking. | ResultMismatch | Seen on 4.1 | |
| Reading or deleting from a 2-billion-row table with deletion vectors can grow native memory until the JVM is killed. Delta Lake's two 2-billion-row tests reproduce it. The nightly job force-fails both tests. | Exception | Reported on 4.1 | [#12387](https://github.com/apache/gluten/issues/12387) |
| With `spark.databricks.delta.deletionVectors.useMetadataRowIndex=true`, the default, selecting `__delta_internal_is_row_deleted` from a relation built by hand over `DeltaParquetFileFormat` drops the deleted rows and returns 1 for the rows it keeps. The column is not in the table schema, so SQL and DataFrame queries over the table cannot select it. | ResultMismatch | Seen on 4.1 |  |
| Scans of a table with many small files can be slower than vanilla Spark on EBS storage. The reported run sets `spark.gluten.sql.columnar.scanOnly=true`, which offloads only scans and filters directly above them. In that run, two of five TPC-DS SF1000 queries take 2.2 and 3.4 times as long as on vanilla Spark, with 182,300 files in one table. A maintainer did not see the slowdown on a host with local NVMe storage. | | Reported on 3.4 | [#10778](https://github.com/apache/gluten/issues/10778) |

### Write problems

These need native write on, unless a row says otherwise. On Spark 4.x, many of these writes fall back to Delta's writer (see [Native write on Spark 4.x](#native-write-on-spark-4x)), and the Versions column says so.

| Problem | Status | Versions | Issue |
|---|---|---|---|
| A write partitioned by a FLOAT or DOUBLE column fails with `Unsupported partition type`. | Exception | 3.5, from code. Spark 4.x only with `delta.writePartitionColumnsToParquet=false` (from code) | [#13138](https://github.com/apache/gluten/issues/13138) |
| A write to a path scheme the native library cannot write fails at task time. This covers custom Hadoop file systems, and S3, GCS, HDFS or ABFS in a build without that connector. | Exception | 3.5 and 4.x, seen on 4.1 (custom file system), from code (missing connector) | [#4963](https://github.com/apache/gluten/issues/4963) |
| Files written for a column mapping table, in name or id mode, have no Parquet field IDs, which the Delta Lake protocol requires. Delta Lake and Gluten read name-mode files by physical name. Readers that match by field ID cannot. | ResultMismatch | 3.5, from code. Spark 4.x falls back | |
| Spark's own reader fails on natively written id-mode files with `Spark read schema expects field Ids, but Parquet file schema doesn't contain any field Ids`. Spark uses this reader when a scan falls back or Gluten is off. With `spark.sql.parquet.fieldId.read.ignoreMissing=true`, Spark skips this check and returns NULL for the data columns of these files (from code). That setting is not tested with these files. | Exception | 3.5, from code. Spark 4.x falls back | |
| Files written for a UniForm table have no Parquet field IDs, so the Iceberg metadata points at files without IDs. No error is raised. | ResultMismatch | 3.5, from code. Spark 4.x falls back | [#12957](https://github.com/apache/gluten/pull/12957) |
| A write to a partitioned UniForm table fails with `The file schema type should be equal with the input rowvector type`. | Exception | 3.5, from code. Spark 4.x falls back | |
| Some writes of a TIMESTAMP_NTZ data column fail with `The file schema type should be equal with the input rowvector type`. Delta Lake's type widening tests fail this way when they write `CAST(value AS TIMESTAMP_NTZ)`. Delta Lake's data skipping tests also write that cast and pass. | Exception | Seen on 4.1, from code on 3.5. Spark 4.x now falls back | |
| A partitioned write fails in Velox's `PartitionIdGenerator` when a TIMESTAMP or TIMESTAMP_NTZ partition value has sub-millisecond digits. | Exception | 3.5, from code. On Spark 4.x, TIMESTAMP_NTZ writes fall back, and TIMESTAMP only with `outputTimestampType=TIMESTAMP_MICROS` and `delta.writePartitionColumnsToParquet=false` (from code) | |
| A generated partition column such as `DATE_FORMAT(ts, 'yyyy-MM')` stores `12345-07` for the year 12345. Spark stores `+12345-07`. Not checked with native write off. | ResultMismatch | Seen on 4.1 | |
| The `compression` option set to brotli or lzo fails the write. So does `spark.sql.parquet.compression.codec` when the option is unset. The write fails even with the Hadoop codec class on the classpath. Without that class, Delta's writer fails too, and Spark does not ship it. | Exception | 3.5, from code. Spark 4.x falls back | |
| The `compression` option or `spark.sql.parquet.compression.codec` set to lz4_raw writes Snappy data into a `.lz4raw.parquet` file. | ResultMismatch | 3.5, from code. Spark 4.x falls back | |
| `maxRecordsPerFile` on an unpartitioned write counts rows but writes whole batches. When the limit is smaller than a batch, the whole batch goes into one file. The other files opened for that batch are 0-byte files that are not valid Parquet. | ResultMismatch | 3.5, from code. Spark 4.x falls back | |
| `spark.sql.maxConcurrentOutputFileWriters` above 0 makes a partitioned write with unsorted input fail with `Underlying columnar data is inaccessible from BatchCarrierRow`. | Exception | 3.5, from code. Spark 4.x falls back | |
| An unpartitioned native write whose first task gets no rows, for example a write of an empty DataFrame, leaves a 0-byte file that is not valid Parquet. Reading the table directory as plain Parquet can then fail with `is not a Parquet file (length is too low: 0)`. With statistics on, the file stays on disk until `VACUUM`. With `spark.databricks.delta.stats.collect=false`, Delta commits it. The last two sentences are from code. | ResultMismatch | 3.5 and 4.x, seen on 4.1 | |
| A statistics aggregation that Velox cannot offload fails the write with `ClassCastException`. With the default `spark.gluten.sql.columnar.backend.velox.enableTimestampNtzValidation=false`, the TIMESTAMP_NTZ case from the issue no longer fails on main. Tested on Spark 3.5, seen on 4.1, from code on 4.0. On Spark 3.5 with that option set to true, the TIMESTAMP_NTZ case still fails (from code). | Exception | 3.5 and 4.x, from code | [#12538](https://github.com/apache/gluten/issues/12538) |

### MERGE, UPDATE and DELETE problems

MERGE is a write command, but on main Delta's writer writes its files even with native write on (see `MERGE INTO` in [Write commands](#write-commands)). The MERGE problems below come from the parts of MERGE that run in Velox, so they also happen with native write off (from code). The UPDATE and DELETE row says when native write matters.

| Problem | Status | Versions | Issue |
|---|---|---|---|
| UPDATE or DELETE through a temp view that swaps column names, such as `SELECT value AS key, key AS value FROM t`, changes different rows than Delta Lake. Delta Lake's tests use two rows that both match the condition through the view's column names. Delta Lake changes one, the row that matches through the table's column names. With Gluten, the DELETE removes both rows and the UPDATE changes both rows. One updated row has its two new values in swapped columns. On tables with deletion vectors, the DELETE also removes both rows (seen on 4.1). That DELETE writes no data files, so the mismatch also happens with native write off (from code). The UPDATE and the DELETE without deletion vectors are not checked with native write off. | ResultMismatch | 3.5 and 4.x, seen on 4.1 | |
| Row counts of a MERGE with a `WHEN MATCHED` or `WHEN NOT MATCHED BY SOURCE` clause can be too high, both in the result of the MERGE statement (`num_affected_rows` and the other counts) and in the `DESCRIBE HISTORY` operation metrics. A MERGE with one `WHEN NOT MATCHED` clause and no other clause reports the same counts as Delta Lake (seen on 4.1). With several `WHEN NOT MATCHED` clauses and no other clause, the inserted and affected row counts can be too high (from code). A MERGE that changes nothing records change data rows on a change data feed table. | ResultMismatch | 3.5 and 4.x, seen on 4.1 | [#9003](https://github.com/apache/gluten/issues/9003) |
| With ANSI mode on, `spark.gluten.sql.ansiFallback.enabled=false` and `spark.databricks.delta.updateAndMergeCastingFollowsAnsiEnabledFlag=true`, a MERGE that casts `ARRAY<BIGINT>` to `ARRAY<INT>` or `MAP<STRING,BIGINT>` to `MAP<STRING,INT>` with overflowing values completes without an error. This was seen in a `WHEN MATCHED THEN UPDATE` clause and in a `WHEN NOT MATCHED THEN INSERT` clause inside `foreachBatch`. The Delta Lake flag is internal and false by default. | ResultMismatch | Seen on 4.1 | |
| MERGE schema evolution can fail with `map key cannot be indeterminate` when a STRUCT map key in the source has a field the target lacks, or lacks a field the target has. | Exception | Seen on 4.1 | |

### Error types

These operations fail as in Delta Lake, with a different exception type or error class. A failed write commits nothing (from code). Seen on 4.1. On Spark 4.0 this is from code. On Spark 3.5, Spark wraps the error of a failed task in a `SparkException` with no error class, so the outer exception type or error class differs in some rows (from code).

| Failure | Delta Lake raises | Gluten raises |
|---|---|---|
| CHECK, NOT NULL or generated-column violation on a partitioned native write. The CHECK and NOT NULL cases are from code. | `DeltaInvariantViolationException` | `SparkException` (`TASK_WRITE_FAILED`) wrapping `GlutenException` |
| `replaceWhere` violation on a partitioned native write | `DeltaAnalysisException` (`DELTA_REPLACE_WHERE_MISMATCH`) | `SparkException` (`TASK_WRITE_FAILED`) wrapping `GlutenException` |
| Store-assignment cast overflow in a MERGE `WHEN MATCHED THEN UPDATE` clause | `DeltaArithmeticException` (`DELTA_CAST_OVERFLOW_IN_TABLE_WRITE`) | `SparkException` wrapping `GlutenException` |
| ANSI cast overflow evaluated in Velox. A MERGE that casts overflowing ARRAY or MAP values can complete without an error (see [MERGE, UPDATE and DELETE problems](#merge-update-and-delete-problems)). | `SparkArithmeticException` | `SparkException` wrapping `GlutenException` (`VeloxUserError`) |
| Missing data file | `SparkException` (`FAILED_READ_FILE.FILE_NOT_EXIST`) on Spark 4.x | `SparkException` wrapping `GlutenException` (`FILE_NOT_FOUND`) |

For errors raised on the JVM, the message of the exception raised in the task stays in the `GlutenException` message. For a `replaceWhere` violation, that is the CHECK constraint message. The `DELTA_REPLACE_WHERE_MISMATCH` line that Delta Lake puts before it, `Written data does not conform to partial table overwrite condition or constraint`, is missing. The lost exception types are tracked in [#11912](https://github.com/apache/gluten/issues/11912). On unpartitioned `save(path)` writes, CHECK and NOT NULL violations keep Delta Lake's exception types. Gluten's tests check NOT NULL on Spark 3.5 and 4.0, and Delta Lake's NOT NULL tests pass (seen on 4.1). The CHECK case is from code.

## Configuration
### Gluten Options

| Gluten option | Default | Description |
| --- | --- | --- |
| spark.gluten.sql.columnar.backend.velox.delta.enableNativeWrite | false | Experimental. Velox writes the data files of the commands marked Offload in [Write commands](#write-commands). Spark 3.5 and 4.x. No effect on Spark 3.4. |
| spark.gluten.sql.columnar.backend.velox.delta.enableNativeDmlRowIndexScan | true | Experimental. When false, the DML target scan that reads row indexes for deletion vectors stays on Spark. Other scans still offload. Tested with DELETE on Spark 3.5 and 4.0. UPDATE, MERGE and Spark 4.1 are from code. Tested on Spark 3.5 and 4.0, from code on 4.1. On Spark 3.4, that scan falls back either way (from code). |
| spark.gluten.sql.columnar.backend.velox.delta.enableChangeDataFeedScan | true | Experimental. When false, batch change data feed reads use Delta Lake's change data feed scan. |
| spark.gluten.sql.columnar.filescan | true | When false, every V1 file source scan (`FileSourceScanExec`) falls back, Delta scans included. No Gluten option turns off Delta scans alone. From code. |
| spark.gluten.sql.ansiFallback.enabled | true | With ANSI mode on, every query falls back. From code. See [ANSI mode on Spark 4.x](#ansi-mode-on-spark-4x). |
| spark.gluten.sql.columnar.parquet.write.blockSize | 128MB | Row-group size for native Delta writes. Velox checks it after each input batch, so a row group can be larger. From code. |
| spark.gluten.sql.native.parquet.write.blockRows | 100000000 | Row-group row limit for native Delta writes. Seen on 4.1. |
| spark.gluten.sql.native.writer.enabled | (unset) | Controls native writes of non-Delta Parquet files. Unset means on. No effect on Delta writes (from code). |
| spark.gluten.sql.columnar.backend.velox.enableTimestampNtzValidation | false | When true, every operator whose input or output includes TIMESTAMP_NTZ falls back. On Spark 4.x, native Delta writes with TIMESTAMP_NTZ columns use Delta's writer either way. From code. |
| spark.gluten.sql.columnar.backend.velox.scanMapKeyPruningEnabled | false | When true, Velox skips map keys a query does not look up, if the lookups sit in filters and projections directly above the scan. Applies to Delta scans without column mapping. On Spark 3.5 to 4.1 it also applies to files with deletion vectors. Off for column-mapped tables. |
| spark.gluten.sql.columnar.backend.velox.parquetUseColumnNames | true | Velox matches Parquet columns by name. Id-mode column mapping tables are read by physical name. When false, Velox matches Parquet columns by position, including for tables without column mapping and id-mode tables. Name-mode tables are still read by physical name. From code. |
| spark.gluten.sql.fallbackUnexpectedMetadataParquet | false | When true, Delta scans fall back if a sampled file is encrypted, carries legacy datetime rebase metadata, uses LZO or BROTLI, or has variant annotations. LZ4_RAW also counts on Spark 4.0 and 4.1. The variant check runs on Spark 4.1 only. The sample is the first `spark.gluten.sql.fallbackUnexpectedMetadataParquet.limit` files (default 10) under the table root. `_delta_log` files count. HDFS and S3 list files in name order, and `_delta_log` sorts before `part-` files and lowercase partition directories. On those stores, a table laid out that way gets no data file in the sample once `_delta_log` holds as many files as the limit. The check is skipped when `spark.gluten.sql.scan.fileSchemeValidation.enabled=false`. From code. |
| spark.gluten.sql.scan.fileSchemeValidation.enabled | true | Scans on a file scheme Velox cannot read fall back. From code. |

The three `spark.gluten.sql.columnar.backend.velox.delta.*` options are read per query, so `SET` changes them for the session.

### Catalogs

Gluten does not change DeltaCatalog. `CREATE TABLE ... AS SELECT` and `REPLACE TABLE ... AS SELECT` are written natively only through a DeltaCatalog (from code). With native write on, `UPDATE`, `DELETE`, plain `OPTIMIZE` and `save(path)` are written natively with any catalog (from code).

### SQL Extensions
Fallback

Supports `spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension`. Gluten does not offload the command nodes of Delta's own SQL commands, such as `VACUUM`, `RESTORE` and `DESCRIBE HISTORY`. Their `_delta_log` scans stay on Spark. From code. `OPTIMIZE` follows [Write commands](#write-commands). GlutenPlugin appends its own extension.

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
| spark.sql.catalog.spark_catalog | builtin on Spark 4.x, (unset) on 3.x | Set to `org.apache.spark.sql.delta.catalog.DeltaCatalog`. | ✅ |
| spark.sql.ansi.enabled | true on Spark 4.x, false on 3.x | When true, every query falls back unless `spark.gluten.sql.ansiFallback.enabled=false`. From code. | ⚠️ |
| spark.sql.parquet.compression.codec | snappy | Codec for native writes when the `compression` option is unset. On Spark 4.x, a codec Velox cannot write makes the write fall back (from code). | ⚠️ |
| spark.sql.files.maxRecordsPerFile | 0 | Exact on partitioned native writes. On unpartitioned tables, Spark 4.x falls back (from code); for Spark 3.5, see [Write problems](#write-problems). | ⚠️ |
| spark.sql.maxConcurrentOutputFileWriters | 0 | Above 0, partitioned native writes with unsorted input fail on Spark 3.5 and fall back on Spark 4.x. From code. | ⚠️ |
| spark.sql.files.ignoreMissingFiles | false | Passed to Velox. Native scans skip missing files when true. Tested on Spark 3.5 and 4.0, seen on 4.1, from code on 3.4. | ✅ |
| spark.sql.parquet.mergeSchema | false | Spark's Parquet read option. When true, Delta scans fall back. Delta Lake's `mergeSchema` write option is a different setting (see [Write options](#write-options)). From code. | ❌ |
| spark.sql.parquet.outputTimestampType | INT96 | Ignored by native writes on Spark 3.5. On Spark 4.x, writes with TIMESTAMP columns fall back unless it is `TIMESTAMP_MICROS`. From code. See [Natively written files](#natively-written-files). | ⚠️ |
| spark.sql.parquet.writeLegacyFormat | false | Native writes apply it to DECIMAL only. On Spark 4.x, writes with array or map columns fall back when it is set. From code. See [Natively written files](#natively-written-files). | ⚠️ |
| spark.sql.caseSensitive | false | When false, native writes lowercase nested struct field names. From code. | ⚠️ |
| spark.sql.sources.partitionOverwriteMode | STATIC | With native write on, Velox writes the files of a dynamic overwrite through `save(path)` (from code). The results are correct (seen on 4.1). | ✅ |
| spark.databricks.delta.deletionVectors.useMetadataRowIndex | true | When false, scans that read Delta's generated deletion vector columns fall back. See [Deletion vectors](#deletion-vectors). | ⚠️ |
| spark.databricks.delta.stats.collect | true | Native writes compute file statistics in Velox (checked on Spark 3.5). See [Write problems](#write-problems). | ⚠️ |
| spark.databricks.delta.stats.skipping | true | Delta skips files by statistics before the scan runs. The scan offloads either way. When false, Delta Lake rejects reads of a table with deletion vectors (seen on 4.1, see [Deletion vectors](#deletion-vectors)). | 🚫 |
| spark.databricks.delta.optimizeWrite.enabled | (unset) | Native writes run the optimized write shuffle in Velox (from code). File counts match Delta Lake (seen on 4.1). | ✅ |
| spark.databricks.delta.optimizeWrite.binSize | 512 (MiB) | Used by the native optimized write shuffle. From code. | ✅ |
| spark.databricks.delta.autoCompact.enabled | (unset) | Compacts natively written tables. Seen on 4.1. | ✅ |
| spark.databricks.delta.schema.autoMerge.enabled | false | Delta applies schema changes before the write. For MERGE schema evolution, see [MERGE, UPDATE and DELETE problems](#merge-update-and-delete-problems). | ⚠️ |
| spark.databricks.delta.dynamicPartitionOverwrite.enabled | true | Delta decides which partitions to replace. | ✅ |
| spark.databricks.delta.merge.repartitionBeforeWrite.enabled | true | MERGE uses Delta's writer, so this only shapes Delta's write. | 🚫 |
| spark.databricks.delta.optimize.maxFileSize | 1073741824 (1 GiB) | Delta plans the bins. For the writer of plain `OPTIMIZE`, see [Write commands](#write-commands). | ✅ |
| spark.databricks.delta.optimize.minFileSize | 1073741824 (1 GiB) | Delta plans the bins. For the writer of plain `OPTIMIZE`, see [Write commands](#write-commands). | ✅ |
| spark.databricks.delta.write.txnAppId, spark.databricks.delta.write.txnVersion | (unset) | Idempotent writes. Commit only. | 🚫 |
| spark.databricks.delta.commitInfo.userMetadata | (unset) | Commit only. | 🚫 |
| spark.databricks.delta.properties.defaults.* | (unset) | Default for the matching `delta.*` table property on new tables. Gluten support follows that property (see [Delta Table Properties](#delta-table-properties)). | ⚠️ |

`spark.databricks.delta.deletionVectors.useMetadataRowIndex`, `spark.databricks.delta.optimizeWrite.enabled`, `spark.databricks.delta.optimizeWrite.binSize` and `spark.databricks.delta.autoCompact.enabled` do not exist in Delta Lake 2.4.0.

#### Read options

| Option | Default | Description | Gluten Support |
| --- | --- | --- | --- |
| versionAsOf | (latest) | Version of the table snapshot to read. Seen on 4.1. | ✅ |
| timestampAsOf | (latest) | Read the snapshot current at this timestamp. Seen on 4.1. | ✅ |
| readChangeFeed | false | Read the change data feed. Ranges with deletion vectors fall back. | ⚠️ |
| startingVersion, endingVersion | (none) | Change data feed version range. `startingVersion` also applies to streaming reads. | ✅ |
| startingTimestamp, endingTimestamp | (none) | Change data feed timestamp range. `startingTimestamp` also applies to streaming reads. Seen on 4.1. | ✅ |
| maxFilesPerTrigger | 1000 | Streaming: files per micro-batch. | 🚫 |
| maxBytesPerTrigger | (unset) | Streaming: bytes per micro-batch. | 🚫 |
| ignoreDeletes, ignoreChanges, skipChangeCommits | false | Streaming: how to treat commits that remove data. | 🚫 |
| excludeRegex | (none) | Streaming: skip files whose path matches. | 🚫 |
| failOnDataLoss | true | Streaming: fail when Delta log files the stream needs are missing. | 🚫 |
| allowSourceColumnDrop, allowSourceColumnRename, allowSourceColumnTypeChange | (unset) | Streaming schema tracking. Delta Lake 4.4.1. | 🚫 |

#### Write options

| Option | Default | Description | Gluten Support |
| --- | --- | --- | --- |
| compression | spark.sql.parquet.compression.codec | Native writes support the codecs in [Natively written files](#natively-written-files). On Spark 4.x, other codecs make the write fall back (from code). | ⚠️ |
| maxRecordsPerFile | spark.sql.files.maxRecordsPerFile | Exact on partitioned native writes. On unpartitioned tables, Spark 4.x falls back (from code); for Spark 3.5, see [Write problems](#write-problems). | ⚠️ |
| partitionOverwriteMode | spark.sql.sources.partitionOverwriteMode | `dynamic` works through `save(path)`. `INSERT OVERWRITE` and `insertInto` use Delta's writer (from code). | ✅ |
| replaceWhere | (none) | A violating partitioned native write fails with a different error type (seen on 4.1) and commits nothing (from code). See [Error types](#error-types). | ⚠️ |
| mergeSchema | spark.databricks.delta.schema.autoMerge.enabled | Delta applies the schema change before the write. With native write on, Velox writes the evolved schema through `save(path)` (see [Schema evolution](#schema-evolution)). | ✅ |
| overwriteSchema | false | Delta applies the schema change before the write. With native write on, Velox writes the files of `save(path)` (see [Schema evolution](#schema-evolution)). | ✅ |
| optimizeWrite | (unset) | Runs the rebalancing shuffle in Velox before the native write. From code. | ✅ |
| txnAppId, txnVersion | (unset) | Idempotent writes. Commit only. | 🚫 |
| userMetadata | (unset) | Stored in `commitInfo.userMetadata`. Commit only. | 🚫 |
| dataChange | true | Sets `dataChange` on the file actions. Commit only. | 🚫 |
| parquet.* Hadoop settings | | Native writes ignore `parquet.*` keys in the Hadoop configuration, such as `spark.hadoop.parquet.block.size`, which Delta's writer applies. Delta Lake drops `parquet.*` writer options before the write, so as writer options they have no effect with or without Gluten. Both are from code. Use `spark.gluten.sql.columnar.parquet.write.blockSize` for row groups. On Spark 4.x, `parquet.writer.version=v2` and `parquet.crypto.factory.class` make the write fall back (from code). | ❌ |
| File system schemes the native library cannot write | | Native writes fail at task time instead of falling back. A custom Hadoop file system scheme fails (seen on 4.1). See [Write problems](#write-problems). | ❌ |
| fs.* per-write options for supported stores | | Per-write options such as `fs.s3a.access.key` do not reach the native writer (from code). | ❌ |

### Delta Table Properties

`delta.enableRowTracking`, `delta.enableTypeWidening`, `delta.universalFormat.enabledFormats`, `delta.enableIcebergCompatV2`, `delta.autoOptimize.*` and `delta.dataSkippingStatsColumns` do not exist in Delta Lake 2.4.0.

#### Read and write properties

| Property | Default | Description | Gluten Support |
| --- | --- | --- | --- |
| delta.enableDeletionVectors | false | Velox applies deletion vectors in the scan on Spark 3.5 to 4.2. On Spark 3.4, a read falls back when a file it selects has a deletion vector. For other Spark 3.4 scans, see [Deletion vectors](#deletion-vectors). | ✅ Spark 3.5 to 4.1 |
| delta.enableChangeDataFeed | false | Change data feed reads offload, except ranges with deletion vectors. With native write on, Velox writes the `_change_data` files of UPDATE and DELETE (from code). For MERGE, see [MERGE, UPDATE and DELETE problems](#merge-update-and-delete-problems). | ⚠️ |
| delta.columnMapping.mode | none | Reads offload. On Spark 4.x, native writes fall back (from code). For id mode and for Spark 3.5 native writes, see [Known problems](#known-problems). | ⚠️ |
| delta.enableRowTracking | false | Reads and native writes offload. Seen on 4.1. For `_metadata.base_row_id` filters and for `_metadata.row_id` when older files lack base row IDs, see [Read problems](#read-problems). | ⚠️ |
| delta.enableTypeWidening | false | Most widenings read natively. Seen on 4.1. For DATE to TIMESTAMP_NTZ and for FLOAT or DOUBLE partition columns, see [Known problems](#known-problems). | ⚠️ |
| delta.universalFormat.enabledFormats | (empty) | Native writes fall back on Spark 4.x. For Spark 3.5, see [Write problems](#write-problems). From code. | ⚠️ |
| delta.enableIcebergCompatV2 | (unset) | Same as `delta.universalFormat.enabledFormats`. | ⚠️ |
| delta.autoOptimize.optimizeWrite | (unset) | Native writes run the optimized write shuffle in Velox. From code. | ✅ |
| delta.autoOptimize.autoCompact | (unset) | Compacts natively written tables. Seen on 4.1. | ✅ |
| delta.dataSkippingNumIndexedCols | 32 | Native statistics cover the configured columns. Seen on 4.1. | ✅ |
| delta.dataSkippingStatsColumns | (unset) | Native statistics cover the configured columns. Seen on 4.1. | ✅ |
| delta.randomizeFilePrefixes | false | File paths come from Delta's committer. Delta Lake 3.3.2 applies it only to deletion-vector files. Delta Lake 4.4.1 also applies it to data files. From code. | 🚫 |
| delta.randomPrefixLength | 2 | File paths come from Delta's committer. From code. | 🚫 |

#### Commit and log properties

| Property | Default | Description | Gluten Support |
| --- | --- | --- | --- |
| delta.appendOnly | false | Commit check in Delta. | 🚫 |
| delta.checkpointInterval | 10 | Checkpoints are written by Delta. | 🚫 |
| delta.logRetentionDuration | interval 30 days | Log cleanup runs in Delta. | 🚫 |
| delta.deletedFileRetentionDuration | interval 1 week | Used by `VACUUM`, which runs in Delta. | 🚫 |
| delta.isolationLevel | Serializable | Commit check in Delta. | 🚫 |
