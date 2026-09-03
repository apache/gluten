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
stream and Gluten imports it, so only the Arrow C-struct address crosses the boundary and Gluten's
Arrow version stays decoupled from lance-spark's.

Only reads are supported. Writes fall back to vanilla Spark.

## Requirements

- **lance-spark** on the classpath, at a version that includes the Arrow C stream forwarding
  (`LanceArrowStreamScanner`). See the `lance` Maven profile in `backends-velox/pom.xml` for the
  pinned version.
- **Platform:** lance-spark publishes its native library for `linux-x86-64` only, so the offload
  runs on `linux-x86-64` hosts. On other platforms the Lance scan is unavailable and queries fall
  back to vanilla Spark.
- **Spark:** tested with Spark 3.5. lance-spark publishes Spark 3.4 / 3.5 artifacts; there is no
  Lance runtime for Spark 4.0 yet.

## Building

Enable the `lance` profile alongside the Velox backend:

```
mvn clean package -Pbackends-velox -Pspark-3.5 -Plance -DskipTests
```

The read-only offload is entirely Velox-specific, so it lives under `backends-velox/src-lance`
(mirroring `backends-velox/src-iceberg`) rather than in a top-level module. Add `-Plance-test` to
also compile and run the Lance test suite.

## Reading

Offload / Fallback.

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
| Pushed-down aggregation (e.g. `COUNT(*)`) | Fallback |
| Full-text query (`_score`)     | Fallback |

The offload decision is made at plan time (`LanceScanTransformer.isExportable`), so a
non-offloadable scan is detected before execution rather than failing inside Velox.

## Configuration

Catalog and read options are transparent to Gluten; configure lance-spark as usual. The offload
engages automatically whenever lance-spark is present on the classpath.

## Limitations

- Read-only; writes and DDL fall back to vanilla Spark.
- No native Velox scan pushdown for Lance — filters and projection are applied by lance-spark
  (in the Lance reader) before the Arrow stream is exported, not inside Velox.
- Rare schemas that require JVM post-processing (for example `_rowid` / `_rowaddr` / blob columns)
  are not offloaded.
