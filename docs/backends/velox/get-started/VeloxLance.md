---
layout: page
title: Lance Support in Velox Backend
nav_order: 9
parent: Getting-Started
---

# Lance Support in Velox Backend

Gluten can offload reads of [Lance](https://lancedb.github.io/lance/) datasets (via
[lance-spark](https://github.com/lance-format/lance-spark)) to the Velox backend. The scan is
handed to Velox through the Arrow C Data Interface: lance-spark exports each fragment as an Arrow C
stream and Gluten imports it, so only the Arrow C-struct address crosses the boundary.

Writes to Lance tables can also be offloaded, as an experimental feature that is off by default.
See [Writing](#writing).

## Support summary

Lance support is partial. Anything not listed as offloaded runs on lance-spark and vanilla Spark as
it does without Gluten (fallback, not an error); the tables under [Reading](#reading) and
[Writing](#writing) give the details.

| Area | Status | Offloaded to Velox | Left on lance-spark |
|------|--------|--------------------|---------------------|
| Read | On whenever lance-spark is on the classpath | Scans with projection and filters, including vector columns | Pushed-down aggregation, full-text queries, `_rowid` / `_rowaddr` and blob columns |
| Write | Experimental, off by default | Append, overwrite, saving to a new path, `CREATE TABLE AS SELECT`; `REPLACE TABLE AS SELECT` on Spark 3.5 and later | `UPDATE`, `DELETE`, `MERGE`, column backfills, sharded tables, blob columns and the column types listed under [Writing](#writing) |

Gluten does not change lance-spark's commands, such as index creation and compaction, or its
commit protocol: an offloaded write is committed by lance-spark on the driver.

## Requirements

- **lance-spark** 0.8.0 or later on the classpath (the first release with the Arrow C stream
  export, `LanceArrowStreamScanner`), e.g. `org.lance:lance-spark-bundle-3.5_2.12:0.8.0`. The
  pinned version is the `lance.version` property in the root `pom.xml`. Writes need lance-spark
  0.9.0-beta.2 or later (the first release with `LanceBatchWrite.getFragmentWriteSpec()`); with an
  older release, writes stay on lance-spark's own writer.
- **Arrow:** Gluten and lance-spark share the Arrow Java classes on the classpath, and writes pass
  Arrow Java objects between them: the table's Arrow schema and the stream Lance reads. Use the
  lance-spark artifact for your Spark version. lance-spark 0.9.0-beta.2 builds it with the Arrow
  version Gluten uses for that Spark version (Arrow 15 for Spark 3.5, 18.1 for 4.0, 18.3 for 4.1 and
  19 for 4.2), except on Spark 3.4, where lance-spark uses Arrow 14 and Gluten Arrow 15.
- **Platform:** lance-core ships its native library for `linux-x86-64`, `linux-aarch64` and
  `darwin-aarch64`.
- **Spark:** Gluten's CI runs the Lance tests on Spark 3.5, 4.0, 4.1 and 4.2. lance-spark also
  publishes a Spark 3.4 artifact, which CI does not cover. On Spark 3.4 the two Arrow versions
  differ (see above), so the native write is not recommended there.

## Building

Enable the `lance` profile alongside the Velox backend:

```
./build/mvn clean package -Pbackends-velox -Pspark-3.5 -Plance -DskipTests
```

As for Iceberg, the code is split in two. The `gluten-lance` module holds the parts that do not
depend on Velox: the scan, the write checks that hold for any backend, and the Arrow conversions
the writer applies. `backends-velox/src-lance` holds the Velox parts: the writer that exports Velox
batches to Lance, the checks for what Velox exports, the write nodes and the rules that register
them. Add `-Plance-test` to also compile and run the Lance tests:

```
./build/mvn test -pl gluten-lance,backends-velox -am -Pbackends-velox -Pspark-3.5 -Plance \
  -Plance-test -Dtest=none -DwildcardSuites=org.apache.gluten.execution.VeloxLanceSuite
```

## Reading

A plain columnar Lance scan is offloaded:

```scala
spark.read.format("lance").option("path", "/data/table.lance").load().createOrReplaceTempView("t")
spark.sql("SELECT id, v FROM t WHERE id < 100")
```

Projection and filters are pushed into the Lance scan by lance-spark before export, so they stay
offloaded. A scan is left on vanilla Spark (fallback, not an error) when the export path cannot
serve it:

| Query shape                    | Behavior |
|--------------------------------|----------|
| Projection / filter            | Offload  |
| Vector (`FixedSizeList`) columns | Offload  |
| Pushed-down aggregation (e.g. `COUNT(*)`) | Fallback |
| Full-text query (`_score`)     | Fallback |

Vector columns are Arrow `FixedSizeList`s, which neither Spark's `ArrowColumnVector` nor Velox
reads, so the scan rewrites them as variable-size lists, the `ArrayType` lance-spark already
exposes. The vector values are not copied; each vector column only gets an offsets buffer. This
also applies to vectors nested in a struct or a list.

The offload decision is made at plan time (`LanceScanTransformer.isExportable`), so a
non-offloadable scan is detected before execution rather than failing inside Velox.

## Writing

Set `spark.gluten.sql.columnar.backend.velox.lance.enableNativeWrite=true` to offload writes to
Lance tables:

```scala
df.write.format("lance").option("path", "/data/table.lance").save()
df.write.format("lance").mode("append").option("path", "/data/table.lance").save()
df.write.format("lance").mode("overwrite").option("path", "/data/table.lance").save()
```

Each task exports its Velox batches as an Arrow C stream and writes them into uncommitted Lance
fragments with the write parameters lance-spark resolves for the table, such as its file format
version and file size limits. lance-spark's `LanceBatchWrite` then commits the fragments on the
driver, as it does for its own writer, so the commit, the table version and the metrics it reports
are unchanged.

A write is left on lance-spark's writer (fallback, not an error) when Velox cannot serve it:

| Write                                                      | Behavior |
|------------------------------------------------------------|----------|
| Append, overwrite (`INSERT INTO`, `INSERT OVERWRITE`)      | Offload  |
| Saving to a new path                                       | Offload  |
| `CREATE TABLE AS SELECT`                                   | Offload  |
| `REPLACE TABLE AS SELECT`                                  | Offload on Spark 3.5 and later |
| Table with a sharding key                                  | Fallback |
| Blob columns                                               | Fallback |
| `TimestampNTZ` columns                                     | Fallback, unless Gluten's NTZ validation is turned off |
| Timestamps, strings, binaries, arrays and maps inside a struct that can be null | Fallback, until Gluten's Velox exports a null struct's children as null |
| Large strings or binaries, half floats, unsigned integers, fixed-size lists inside other columns | Fallback |
| Row-level operations (`UPDATE`, `DELETE`, `MERGE`) and column backfills | Fallback |

Fixed-size list columns, such as vectors, are written natively when they are top-level columns of
floats or doubles: Velox exports them as plain lists, and each list must hold exactly the column's
number of elements. Float16 vectors, which lance-spark writes when the column's metadata asks for
them, fall back, since Velox has no half-float type.

The offload decision is made at plan time. Each batch a task exports is also checked against the
table's Arrow schema, so a type Velox exports differently fails the task, naming the column, rather
than writing wrong data.

When a task of an offloaded write fails, the table is left unchanged, and Lance deletes the data
files that task wrote. A task killed after handing Lance its last batch, such as a speculative
attempt that lost, leaves its files behind, as lance-spark's own writer does; they are never part
of the table.

## Configuration

Catalog, read and write options are transparent to Gluten; configure lance-spark as usual. The
read offload engages automatically whenever lance-spark is present on the classpath.

| Key | Default | Description |
|-----|---------|-------------|
| `spark.gluten.sql.columnar.backend.velox.lance.enableNativeWrite` | `false` | Experimental. Offload writes to Lance tables to Velox. |

## Limitations

- Writes are offloaded only when enabled, and only for the writes marked Offload in
  [Writing](#writing). Other writes run on lance-spark's writer.
- Only batch writes are offloaded. lance-spark has no streaming write.
- The native write is tested against the local filesystem only. Storage options and credentials
  reach Lance as lance-spark resolves them, but writes to object stores are not covered by tests
  yet.
- No native Velox scan pushdown for Lance — filters and projection are applied by lance-spark
  (in the Lance reader) before the Arrow stream is exported, not inside Velox.
- Rare schemas that require JVM post-processing (for example `_rowid` / `_rowaddr` / blob columns)
  are not offloaded.
- The native write takes batches as Velox produces them, sized by
  `spark.gluten.sql.columnar.maxBatchSize`. lance-spark's `batch_size`, `max_batch_bytes`,
  `queue_depth` and `use_queued_write_buffer` write options apply only to its own writer.
- Velox exports at most 2 GiB of string or binary data per column in one batch, so very large
  values need smaller batches.
- The memory Lance itself uses to encode and write fragments is not counted as Spark task memory,
  as with lance-spark's own writer, so leave room for it in `spark.executor.memoryOverhead`. By
  default Lance buffers up to 8 MiB per column for each file it writes, and on an object store
  also keeps up to 10 upload parts of 5 MiB in flight per file (`LANCE_UPLOAD_CONCURRENCY`). As a
  rough guide, allow concurrent write tasks per executor × (columns × 8 MiB, plus 55 MiB on an
  object store): 8 tasks writing 20 columns to S3 can use about 1.7 GiB. The Arrow batches the
  native write hands to Lance are counted.
- When a task stops while Lance is blocked, for example on an object-store request that keeps
  retrying, it waits up to 5 minutes for Lance to stop. If Lance is still running then, the task
  ends without releasing the Velox batches Lance holds, which may abort the executor. The task
  logs an error when this happens.
