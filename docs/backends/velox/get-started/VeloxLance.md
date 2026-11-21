---
layout: page
title: Lance Support in Velox Backend
nav_order: 9
parent: Getting-Started
---

# Lance Support in Velox Backend

Gluten can offload reads of [Lance](https://lancedb.github.io/lance/) datasets (via
[lance-spark](https://github.com/lance-format/lance-spark)) to the Velox backend. A Lance scan
emits Arrow batches (lance-spark's Arrow batch reads), and Gluten converts them into Velox batches
with its Java Arrow transitions. The scan stays a vanilla `BatchScanExec`, so it keeps lance-spark's
runtime filters, output partitioning and custom metrics.

Only reads are supported. Writes fall back to vanilla Spark.

## Requirements

- **lance-spark** 0.9.0 or later on the classpath (the first release with Arrow batch reads,
  `LanceScan.withArrowBatches`), e.g. `org.lance:lance-spark-bundle-3.5_2.12:0.9.0`. The pinned
  version is the `lance.version` property in the root `pom.xml`.
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

A Lance scan is offloaded when lance-spark can emit Arrow batches for it and Velox imports every
column type of them:

```scala
spark.read.format("lance").option("path", "/data/table.lance").load().createOrReplaceTempView("t")
spark.sql("SELECT id, v FROM t WHERE id < 100")
```

Projection and filters are pushed into the Lance scan by lance-spark. A scan is left as vanilla
Spark reads it (fallback, not an error) when it can't be offloaded:

| Query shape                    | Behavior |
|--------------------------------|----------|
| Projection / filter            | Offload  |
| Vector (`FixedSizeList`) columns | Offload  |
| Pushed-down aggregation (e.g. `COUNT(*)`) | Fallback |
| A column type Velox does not import | Fallback |

Velox imports nulls, booleans, signed integers, floats and doubles, strings and binaries,
decimals, dates, `TIMESTAMP` and `TIMESTAMP_NTZ`, and lists, structs and maps of them. Lance stores
vector columns as Arrow `FixedSizeList`s; lance-spark's Arrow batches carry them as variable-size
lists, the `ArrayType` it exposes for them, without copying the values. Other columns, such as
interval columns, leave the scan on vanilla Spark.

When a Velox operator consumes the scan, Gluten plans `BatchScanExec` → `OffloadArrowDataExec` →
`ArrowColumnarToVeloxColumnarExec`. A row-based consumer reads the Arrow batches through
`ColumnarToRowExec` directly, with no round trip through Velox. The offload decision is made at
plan time (`LanceArrowBatchScan`), so a scan Velox cannot read is detected before execution rather
than failing inside Velox.

## Configuration

Catalog and read options are transparent to Gluten; configure lance-spark as usual. The offload
engages automatically whenever lance-spark is present on the classpath, and
`spark.gluten.sql.columnar.batchscan=false` turns it off.

## Limitations

- Read-only; writes and DDL fall back to vanilla Spark.
- No native Velox scan pushdown for Lance: filters and projection are applied by lance-spark in the
  Lance reader, not inside Velox.
- lance-spark allocates the Arrow batches from Spark's Arrow allocator, a different root than
  Gluten's task allocator, so `OffloadArrowDataExec` copies each batch into Gluten's allocator
  before handing it to Velox.
