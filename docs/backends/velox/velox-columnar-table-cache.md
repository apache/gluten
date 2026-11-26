---
layout: page
title: Velox Columnar Table Cache
nav_order: 11
---

# Velox Columnar Table Cache

This document describes how the Velox backend implements Spark's in-memory table cache
(`df.cache()`, `df.persist()`, `CACHE TABLE`) and the `InMemoryTableScanExec` that reads it, the
formats the cached batches are stored in, and how the implementation compares with the Arrow-based
cache serializer in Apache Spark.

## 1. Overview

Gluten does not replace `InMemoryTableScanExec` with an operator of its own. It plugs a columnar
serializer into Spark's pluggable `CachedBatchSerializer` API, and the planner treats the vanilla
scan node as a Velox columnar operator.

### Enabling

`spark.gluten.sql.columnar.tableCache` is a static configuration and defaults to `true`. When it is
on, `VeloxListenerApi` sets on the driver:

```
spark.sql.cache.serializer = org.apache.spark.sql.execution.ColumnarCachedBatchSerializer
```

This overrides any value the user sets for `spark.sql.cache.serializer`. To use another cache
serializer, set `spark.gluten.sql.columnar.tableCache=false`.

Every `df.cache()` then takes Spark's normal path, `CacheManager` → `InMemoryRelation` →
`CachedRDDBuilder`, with `ColumnarCachedBatchSerializer` doing the conversion.

### Planner integration

| Code | Role |
|---|---|
| `VeloxBackend.batchTypeOf` | Tags an `InMemoryTableScanExec` whose `supportsColumnar` is true and whose serializer is `ColumnarCachedBatchSerializer` as `VeloxBatchType`, so Velox operators read its output without a transition |
| `PlanUtil.isGlutenTableCache` | Identifies such scans, including when AQE wraps them in a `TableCacheQueryStage` |
| `MiscColumnarRules.RemoveGlutenTableCacheColumnarToRow` | Removes a `ColumnarToRow` placed above the cache scan, because the scan converts to rows itself (see [Read path](#4-read-path)) |
| `ExpandFallbackPolicy` | Counts a Gluten table cache under a fallen-back operator as a transition cost, which biases the policy against falling back a stage that reads the cache |

### Write path

`ColumnarCachedBatchSerializer.convertColumnarBatchToCachedBatch`:

1. `supportsColumnarInput` / `supportsColumnarOutput` validate the schema with
   `ValidatorApi.doSchemaValidate`. If the schema is not supported, the whole relation is delegated
   to Spark's row-based `DefaultCachedBatchSerializer`.
2. If the cached plan produces rows, `convertInternalRowToCachedBatch` first converts them with
   `RowToVeloxColumnarExec.toColumnarBatchIterator`.
3. Each batch goes through `VeloxColumnarBatches.ensureVeloxBatch` and is serialized over JNI by
   `ColumnarBatchSerializerJniWrapper`, in one of the formats described in
   [section 3](#3-cached-batch-formats).
4. The result is a `CachedColumnarBatch(numRows, sizeInBytes, bytes, stats, schema)`, which extends
   `SimpleMetricsCachedBatch`. `sizeInBytes` is the serialized length, so Spark's memory accounting
   matches the real footprint.

Cached batches are not resized: each input Velox batch becomes one cached batch.

## 2. Where serialization is implemented

The serializer is a Gluten C++ class. It delegates the encoding of vectors to Velox's
`PrestoVectorSerde`; no Velox patch is involved.

| Layer | Location |
|---|---|
| Spark `CachedBatchSerializer` | `backends-velox/src/main/scala/org/apache/spark/sql/execution/ColumnarCachedBatchSerializer.scala` |
| JNI entry points | `ColumnarBatchSerializerJniWrapper` → `cpp/core/jni/JniWrapper.cc` (`serialize`, `serializeWithStats`, `serializeV3`, `serializeWithStatsV3`, `init`, `deserialize`, `deserializeWithProjection`) |
| Backend-neutral interface | `cpp/core/operators/serializer/ColumnarBatchSerializer.h`. Backends that do not implement the V2/V3 hooks return null, and the JVM falls back to an older format |
| Factory | `VeloxRuntime::createColumnarBatchSerializer` in `cpp/velox/compute/VeloxRuntime.cc` |
| Implementation | `cpp/velox/operators/serializer/VeloxColumnarBatchSerializer.{h,cc}` |
| Vector encoding | Velox `facebook::velox::serializer::presto::PrestoVectorSerde` |

Gluten owns the frame layouts, the statistics (`computeStats`, `buildStatsBlob`), column
projection on read (`deserializeV3`), and `CachedColumnLoader`, a Velox `VectorLoader` that makes
each projected column a `LazyVector`.

The serializer options are `useLosslessTimestamp=true`, `nullsFirst=false`, and the compression
kind from `spark.gluten.sql.columnar.backend.velox.columnarBatchSerializerCompression` (default
`none`). The same class also serializes batches for broadcast exchange.

## 3. Cached batch formats

There are three formats. The writer tries V3 first and falls back to V2, then to V1.

| | V1 (legacy) | V2 | V3 (default) |
|---|---|---|---|
| Velox API | `createIterativeSerializer` → `append` / `flush`: one Presto page for the whole batch | Same as V1 | `serializeSingleColumn` per column; each column is a self-contained blob |
| Frame | None; `bytes` is the Presto page | `[magic FE CA 53 02][statsLen][statsBlob][bytesLen][bytesBlob]` | `[magic FE CA 53 03][statsLen][statsBlob][numRows][numCols]` followed by `[colLen][colBytes]` per column |
| Stored in `CachedColumnarBatch.bytes` | The Presto page | Only `bytesBlob`; the JVM strips the frame and stores the stats in the `stats` field | The whole frame |
| Input vector | As produced, encodings kept | As produced, encodings kept | `getFlattenedRowVector()`: dictionary and constant encodings are flattened first |
| Compression | Configured `compressionKind` | Configured `compressionKind` | None. Velox requires `CompressionKind_NONE` for single-column serde |
| Partition stats | None (`stats = null`) | Always | Optional; `statsLen = 0` when stats are disabled |
| Read | `deserialize` decodes all columns, then `ColumnarBatches.select` projects | Same as V1 | `deserializeWithProjection` builds only the requested columns as `LazyVector`s, decoded on first access |

V1 and V2 store the same data bytes: V2's `framedSerializeWithStats` produces its data blob through
the same `append` / `serializeTo` path as V1 and only adds the statistics. V3 is a different
encoding. Its per-column layout is what lets a read decode only the projected columns; a single
Presto page cannot be partially decoded, so V1 and V2 always decode every cached column.

The trade-off of V3 is size: data is stored uncompressed and flattened, so a V3 cache can be larger
than a V1/V2 cache of the same data.

### Fallback between formats

Two JVM-wide flags, `statsExtV3Available` and `statsExtAvailable`, start as `true`. An
`UnsatisfiedLinkError`, for example a newer Gluten jar with an older `libgluten.so`, clears the flag
for the rest of the JVM's life and the writer uses the next format. A corrupt frame from a single
batch only falls back for that batch; its warning is logged at most 100 times per JVM.

## 4. Read path

`convertCachedBatchToColumnarBatch` creates one native deserializer per partition (`jni.init` with
the Arrow-exported cache schema). For each cached batch:

* V3 bytes go to `deserializeWithProjection(handle, bytes, requestedColumns)`. `requestedColumns` is
  `null` when all columns are selected, empty for `count(*)`, and the projected ordinals otherwise.
  The whole frame is passed to native code.
* V1/V2 bytes go to `deserialize`, followed by `ColumnarBatches.select` when a subset of columns is
  requested.

`convertCachedBatchToInternalRow` decodes to Velox batches the same way and then converts with
`VeloxColumnarToRowExec.toRowIterator`.

When a query that reads a Gluten cache falls back to vanilla Spark, `RemoveGlutenTableCacheColumnarToRow`
removes the explicit `ColumnarToRow`, and the vanilla operators call the scan's row path. The
columnar-to-row conversion of the whole cache then happens inside the `Scan In-memory table` node.
No separate node or metric shows it in the plan; its time is reported by the parent operator,
for example as `time in aggregation build` of a `HashAggregate` in the same whole-stage codegen.

## 5. Partition statistics and pruning

With `spark.gluten.sql.columnar.tableCache.partitionStats.enabled=true` (default `false`), the
native writer computes per-column lower bound, upper bound, null count, row count and size for each
batch. The JVM stores them in the 5-slots-per-column layout of `SimpleMetricsCachedBatch.stats`, and
Spark's `SimpleMetricsCachedBatchSerializer.buildFilter` prunes cached batches with them.

Statistics are kept for integral types, date, year-month and day-time intervals, timestamp and
timestamp without time zone, decimals up to precision 38, float and double (NaN values are
skipped), boolean, and binary-collation strings (bounds truncated to 256 bytes). Other columns are
marked unsupported.

`ColumnarCachedBatchSerializer.buildFilter` adds two safeguards to Spark's implementation:

* Batches without statistics, or with a null bound on a column a predicate references, are passed
  through unpruned. Spark's implementation would otherwise throw on null statistics or drop the
  batch.
* AND-conjuncts that reference non-binary-collation strings are removed before pruning, because
  byte-order bounds do not match collation-aware ordering.

## 6. Kryo

`CachedColumnarBatch` uses `CachedColumnarBatchKryoSerializer`. It writes `numRows`, `sizeInBytes`
and `bytes`, followed by the optional statistics and the schema as JSON. Every length field is
checked against `spark.kryoserializer.buffer.max`. A stream without the trailing statistics marker
is read as the V1 wire format. When `spark.kryo.registrationRequired=true`, register the class
explicitly:

```
spark.kryo.classesToRegister=org.apache.spark.sql.execution.CachedColumnarBatch
```

## 7. Comparison with Spark's Arrow cache serializer

Apache Spark added `org.apache.spark.sql.execution.columnar.ArrowCachedBatchSerializer`
([SPARK-57268](https://issues.apache.org/jira/browse/SPARK-57268),
[apache/spark#56334](https://github.com/apache/spark/pull/56334)). Both serializers plug in through
`spark.sql.cache.serializer` and extend `SimpleMetricsCachedBatchSerializer`; they differ in what is
stored and how it is read.

| | Spark `ArrowCachedBatchSerializer` | Gluten `ColumnarCachedBatchSerializer` |
|---|---|---|
| Enabled | Opt-in; Spark's default remains `DefaultCachedBatchSerializer` | On by default (`spark.gluten.sql.columnar.tableCache=true`) |
| Runs in | JVM, Arrow Java | Native C++ over JNI, Velox memory pool |
| Stored format | One Arrow IPC `RecordBatch` message per cached batch, without schema | V3 frame with one Presto blob per column (V1/V2: one Presto page per batch) |
| Compression | Arrow buffer compression `none` / `zstd` / `lz4` (`spark.sql.execution.arrow.compression.codec`) | V1/V2: Presto page compression. V3: none |
| Batch sizing | Row input split by `spark.sql.execution.arrow.maxRecordsPerBatch` and `maxBytesPerBatch`; columnar input kept as is | Input batches kept as is; row input converted with `RowToVeloxColumnarExec` |
| Columnar input | Zero-copy only when all columns are `ArrowColumnVector`s of a matching shape; other columnar input, including Spark's Parquet vectors, is converted through rows | Always native: `ensureVeloxBatch`, then serialize |
| Column pruning on read | Eager: `VectorLoader.load` decodes and decompresses all cached columns, then the selected ones are kept | Lazy (V3): only requested columns are built, each decoded on first access |
| Columnar output | `ColumnarBatch` of `ArrowColumnVector`, readable by vanilla Spark operators | Velox batch for Velox operators |
| Row output | Typed readers into `UnsafeRowWriter`; complex types go through `ColumnarBatch` and `UnsafeProjection` | Native decode, then `VeloxColumnarToRowExec` |
| Prefetch | Optional background decoding of the next batch (`spark.sql.execution.arrow.cache.prefetch.enabled`, default `false`) | None |
| Partition statistics | Always collected, following Spark's `ColumnStats` semantics | Optional, computed natively, with the `buildFilter` safeguards above |
| `sizeInBytes` | Sum of per-column uncompressed buffer sizes | Serialized byte length |
| Unsupported types | Query fails with `UNSUPPORTED_DATATYPE` | Relation falls back to `DefaultCachedBatchSerializer` |

The main design differences:

1. **Projection.** Spark's Arrow cache stores a batch as one IPC message and loads every buffer on
   read. Gluten's V3 layout decodes only the requested columns, so it does less work for wide
   cached tables read through narrow projections.
2. **Size.** Spark compresses each Arrow buffer. Gluten's V3 stores columns uncompressed and
   flattened, which can make the cache larger.
3. **Input conversion.** Spark's zero-copy path applies only when the cached plan already produces
   Arrow vectors. Gluten serializes Velox batches natively without a row conversion.
4. **Consumers.** Spark's cache output is read directly by vanilla Spark operators. Gluten's cache
   output is read directly by Velox operators; vanilla operators read it through a columnar-to-row
   conversion.

## 8. Configuration

| Configuration | Default | Description |
|---|---|---|
| `spark.gluten.sql.columnar.tableCache` | `true` | Static. Use the Velox columnar table cache serializer |
| `spark.gluten.sql.columnar.tableCache.partitionStats.enabled` | `false` | Compute and store per-batch column statistics for partition pruning |
| `spark.gluten.sql.columnar.backend.velox.columnarBatchSerializerCompression` | `none` | Internal. Compression for V1/V2 pages and broadcast batches. V3 is always uncompressed |
| `spark.sql.inMemoryColumnarStorage.enableVectorizedReader` | `true` | Spark configuration. Columnar output from the cache scan requires it |
