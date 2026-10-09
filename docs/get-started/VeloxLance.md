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

- **lance-spark** 0.8.0 or later on the classpath (the first release with the Arrow C stream
  export, `LanceArrowStreamScanner`), e.g. `org.lance:lance-spark-bundle-3.5_2.12:0.8.0`. The
  pinned version is the `lance.version` property in the root `pom.xml`.
- **Platform:** lance-core ships its native library for `linux-x86-64`, `linux-aarch64` and
  `darwin-aarch64`.
- **Spark:** tested with Spark 3.5 and 4.0. lance-spark also publishes Spark 3.4, 4.1 and 4.2
  artifacts.

## Building

Enable the `lance` profile alongside the Velox backend:

```
mvn clean package -Pbackends-velox -Pspark-3.5 -Plance -DskipTests
```

The read-only offload is entirely Velox-specific, so it lives under `backends-velox/src-lance`
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
