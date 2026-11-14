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
- **Spark:** tested with Spark 3.5 and 4.0. lance-spark also publishes Spark 3.4, 4.1 and 4.2
  artifacts.

## Building

Enable the `lance` profile alongside the Velox backend:

```
mvn clean package -Pbackends-velox -Pspark-3.5 -Plance -DskipTests
```

The offload is entirely Velox-specific, so it lives under `backends-velox/src-lance`
(mirroring `backends-velox/src-iceberg`) rather than in a top-level module. Add `-Plance-test` to
also compile and run the Lance test suite:

```
mvn test -pl backends-velox -Pbackends-velox -Pspark-3.5 -Plance -Plance-test \
  -Dtest=none -DwildcardSuites=org.apache.gluten.execution.VeloxLanceSuite
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
| `CREATE TABLE AS SELECT`, `REPLACE TABLE AS SELECT`        | Offload on Spark 3.5 and later |
| Table with a sharding key                                  | Fallback |
| Blob columns                                               | Fallback |
| `TimestampNTZ` columns                                     | Fallback, unless Gluten's NTZ validation is turned off |
| Timestamps inside a struct that can be null                | Fallback |
| Large strings or binaries, half floats, unsigned integers, fixed-size lists inside other columns | Fallback |
| Row-level operations (`UPDATE`, `DELETE`, `MERGE`) and column backfills | Fallback |

Fixed-size list columns, such as vectors, are written natively when they are top-level columns of
signed integers or floats: Velox exports them as plain lists, and each list must hold exactly the
column's number of elements.

The offload decision is made at plan time. The first batch each task exports is also checked
against the table's Arrow schema, so a type Velox exports differently fails the task rather than
writing wrong data.

## Configuration

Catalog, read and write options are transparent to Gluten; configure lance-spark as usual. The
read offload engages automatically whenever lance-spark is present on the classpath.

| Key | Default | Description |
|-----|---------|-------------|
| `spark.gluten.sql.columnar.backend.velox.lance.enableNativeWrite` | `false` | Experimental. Offload writes to Lance tables to Velox. |

## Limitations

- Writes are offloaded only when enabled, and only for the writes marked Offload in
  [Writing](#writing). Other writes run on lance-spark's writer.
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
  as with lance-spark's own writer. The Arrow batches the native write hands to Lance are counted.
