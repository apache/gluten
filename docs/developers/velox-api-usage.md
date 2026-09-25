# Velox APIs Used in Gluten

Inventory of the Velox API surface consumed by Gluten. All of it lives under `cpp/`
(mostly `cpp/velox/`), spanning **161 distinct Velox headers**.

Generated on 2026-08-14 from commit `6d1adefdf`.

Counts in parentheses are include/reference counts from a grep over
`cpp/**/*.{h,cc,cpp}` and indicate relative usage weight, not exhaustive call sites.

## 1. Type system — `velox/type/`

`Type.h` (30 includes, the single most-used header): `TypePtr`, `RowType`/`RowTypePtr`,
`TypeKind` (all kinds incl. `HUGEINT`, `UNKNOWN`), `ROW()`, `asRowType()`, `MAP`/`ARRAY`,
`DECIMAL`.

Also: `Timestamp.h`, `StringView.h`, `HugeInt.h` (`int128_t`), `Variant.h`,
`Filter.h` (`common::Filter`, `SubfieldFilters`), `TypeCoercer.h`, `SimpleFunctionApi.h`,
`fbhive/HiveTypeParser.h`.

## 2. Vectors & buffers — `velox/vector/`, `velox/buffer/`

`BaseVector` (`create`, `createEmpty`), `RowVector`/`RowVectorPtr`, `FlatVector`,
`ConstantVector`, `ArrayVector`, `MapVector`, `LazyVector`/`VectorLoader`,
`DecodedVector`, `SelectivityVector`, `VectorEncoding::Simple`, `vector_size_t`,
`column_index_t`, `IndexRange`, `RowSet`.

Buffers: `BufferPtr`, `AlignedBuffer::allocate`.

Arrow interop — `vector/arrow/Bridge.h` (11 includes): `exportToArrow`,
`importFromArrow`, `importFromArrowAsOwner`.

## 3. Plan construction — `velox/core/`

The Substrait -> Velox conversion builds these plan nodes:

| Node | Refs |
|---|---|
| `AggregationNode` | 91 |
| `WindowNode` | 30 |
| `ProjectNode` | 22 |
| `ValuesNode` | 10 |
| `TableScanNode` | 10 |
| `FilterNode` | 9 |
| `TopNRowNumberNode` | 8 |
| `TopNNode` | 8 |
| `OrderByNode` | 8 |
| `LimitNode` | 8 |
| `HashJoinNode` | 6 |
| `LocalPartitionNode` | 5 |
| `UnnestNode`, `TableWriteNode`, `RowNumberNode`, `NestedLoopJoinNode`, `MergeJoinNode`, `ExpandNode` | 2 each |

Plus `PlanNode`/`PlanNodeId`/`PlanNodePtr`, `JoinType` / `isCountingJoin`,
`ExecutionStrategy::kUngrouped`.

Expressions (`core/Expressions.h`): `FieldAccessTypedExpr` (28), `ConstantTypedExpr` (25),
`CallTypedExpr` (13), `CastTypedExpr` (5), `DereferenceTypedExpr` (3), `LambdaTypedExpr`,
`ITypedExpr`.

Config/context: `QueryCtx::create`, `QueryConfig` — keys used include `kSpillEnabled`,
`kSpillCompressionKind`, `kSessionTimezone`, `kThrowExceptionOnDuplicateMapKeys`.

## 4. Execution — `velox/exec/`

`Task`, `Driver`/`DriverCtx`/`DriverThreadContext`/`driverThreadContext`/`StopReason`,
`Operator` + `Operator::PlanNodeTranslator`, `SourceOperator`,
`OperatorUtils` (`exprToChannel`), `PlanNodeStats`/`printPlanWithStats`, `Cursor`,
`Aggregate`/`SimpleAggregateAdapter`, `RowContainer`, `VectorHasher`, `TableScan`,
`TableWriter`, `MemoryReclaimer`, `Split`.

Join / bloom path: `HashTable`/`BaseHashTable` (21 refs), `HashLookup`,
`HashTableCache::instance`, `HashJoinBridge`,
`BaseHashTable::kNoSpillInputStartPartitionBit`.

## 5. Expressions & function registration — `velox/expression/`, `velox/functions/`

`VectorFunction` / `registerVectorFunction` / `VectorFunctionMetadata`,
`registerFunction`, `Expr`/`ExprPtr`, `FunctionSignature`/`SignatureBinder`,
`FunctionRegistry`, `SimpleFunctionRegistry`,
`SpecialForm`/`SpecialFormRegistry`/`FunctionCallToSpecialForm`/`registerFunctionCallToSpecialForm`,
`ExprToSubfieldFilter` (`ExprToSubfieldFilterParser`).

Registration entry points:
- `sparksql::registerSparkFunctions`, `sparksql::aggregates::Register`,
  `sparksql::window::WindowFunctionsRegistration`
- `prestosql` equivalents: `registration/RegistrationFunctions.h`,
  `aggregates/RegisterAggregateFunctions.h`, `window/WindowFunctionsRegistration.h`
- `functions::iceberg::Register`
- `lib/RegistrationHelpers.h`, `lib/CheckedArithmetic.h`

Direct Spark function use: `sparksql::Hash`, `Rand`, `DecimalArithmetic`,
`SparkQueryConfig`.

## 6. Connectors — `velox/connectors/`

Base: `Connector`, `ConnectorFactory`, `ConnectorSplit`, `ConnectorTableHandle`/`Ptr`,
`ColumnHandle`/`ColumnHandleMap`, `DataSource`, `ConnectorQueryCtx`,
`registerConnector`/`unregisterConnector`/`hasConnector`, `Connector::getTracker`.

Hive: `HiveConnector`, `HiveConfig`, `HiveConnectorSplit`, `HiveDataSource`,
`HiveDataSink`, `TableHandle`, `SplitReader`/`HiveSplitReader`/`FileSplitReader`,
`FileHandle`, `FileProperties`, `BufferedInputBuilder`.

Iceberg: `IcebergConnector`, `IcebergSplit`, `IcebergDataSink`, `IcebergDeleteFile`,
`IcebergColumnHandle`, `IcebergPartitionSpec`.

Storage adapters:
- S3: `S3FileSystem`, `S3Config`, `S3Util`, `S3Counters`, `RegisterS3FileSystem`
- HDFS: `HdfsFileSystem`, `HdfsUtil`, `RegisterHdfsFileSystem`
- GCS: `GcsFileSystem`, `RegisterGcsFileSystem`
- ABFS: `AbfsFileSystem`, `RegisterAbfsFileSystem`, `registerAzureClientProvider`

## 7. File formats — `velox/dwio/`

`common/Options.h` (`ReaderOptions`, `WriterOptions`, `FileFormat::{PARQUET,ORC}`,
`formatConfigPrefix`), `BufferedInput`/`CachedBufferedInput`/`DirectBufferedInput`,
`ReadFileInputStream`, `ReaderFactory`, `FileSink`/`registerFileSinks`, `DataBuffer`,
`BufferUtil`.

- Parquet: `Writer`, `ParquetWriterOptions`, `ParquetReader`, `ParquetConfig`,
  `registerParquetReaderFactory`, `registerParquetWriterFactory`
- ORC: `OrcReader`, `registerOrcReaderFactory`
- DWRF: `dwrf/common/Config.h`

## 8. Memory — `velox/common/memory/`

`MemoryPool` (117 refs — the most-used symbol overall), `MemoryManager` /
`MemoryManager::Options` / `MemoryManager::initialize`, `MemoryArbitrator`
(Gluten subclasses it), `MemoryReclaimer`, `ScopedMemoryArbitrationContext`,
`kMaxMemory`, `MmapAllocator`, `MallocAllocator`, `HashStringAllocator`,
`ByteStream` (`StreamArena`, `OutputStream`, `OutputStreamListener`).

## 9. Serialization / row format

`serializers/PrestoSerializer.h`: `PrestoVectorSerde`,
`PrestoVectorSerde::PrestoOptions`, `PrestoOutputStreamListener`, `VectorStreamGroup`,
`IterativeVectorSerializer`, `registerVectorSerde`, `registerNamedVectorSerde`.

Row formats for shuffle: `row/UnsafeRowFast.h` (incl. `fixedRowSize`),
`row/CompactRow.h`.

## 10. Filesystem, caching, config, utils — `velox/common/`

- `file/FileSystems.h`: `FileSystem`, `getFileSystem`, `FileOptions`,
  `DirectoryOptions`, `registerLocalFileSystem`
- `file/File.h`: `ReadFile`, `WriteFile`
- `caching/AsyncDataCache.h`, `caching/SsdCache.h`, `StringIdLease`
- `config/Config.h`: `ConfigBase` (74 refs), `toDuration`, `toCapacity`, `CapacityUnit`
- `compression/Compression.h`: `CompressionKind`, `stringToCompressionKind`
- `base/Exceptions.h`: `VELOX_CHECK*` / `VELOX_FAIL`
- `base/BitUtil.h` (`bits::isBitNull`), `base/Nulls.h`, `base/SimdUtil.h`,
  `base/BloomFilter.h`, `base/Fs.h`, `base/StatsReporter.h`,
  `base/SuccinctPrinter.h` (`succinctBytes`)
- `time/CpuWallTimer.h` (`CpuWallTiming`), `io/IoStatistics.h` (`IoStats`),
  `process/StackTrace.h`, `external/xxhash/xxhash.h`
- `ContinueFuture`, `RuntimeMetric`

## 11. cuDF / GPU (experimental) — `velox/experimental/cudf/`

`exec/ToCudf.h` (`registerCudf`), `exec/CudfOperator.h`, `vector/CudfVector.h`,
`exec/VeloxCudfInterop.h` (`veloxToCudfDataType`), `exec/GpuResources.h`
(`cudfGlobalStreamPool`), `CudfConfig.h` (`CudfConfig::kCudfEnabled`),
`CudfNoDefaults.h`, `exec/NvtxHelper.h`, `exec/Utilities.h`,
`expression/SparkFunctions.h`, `exec/SparkAggregateFunctions.h`.

cuDF Hive connector: `CudfHiveConnector`, `CudfHiveDataSink`, `CudfHiveTableHandle`,
`CudfHiveConnectorSplit`.

## 12. Test-only APIs

`vector/tests/utils/VectorTestBase.h` (16), `vector/tests/utils/VectorMaker.h`,
`vector/tests/VectorTestUtils.h`, `test::assertEqualVectors` (16),
`exec/tests/utils/{PlanBuilder,AssertQueryBuilder,HiveConnectorTestBase,OperatorTestBase,TempDirectoryPath}.h`,
`dwio/common/tests/utils/DataFiles.h`,
`functions/{sparksql,prestosql}/tests/*BaseTest.h`,
`common/base/tests/GTestUtils.h`, `parse/TypeResolver.h`.

## Where Gluten *implements* Velox interfaces

These are the extension points — the API contracts Gluten is most tightly coupled to,
and the ones most likely to break on a Velox version bump.

| Gluten class | Velox base |
|---|---|
| `ListenableArbitrator` (`cpp/velox/memory/VeloxMemoryManager.cc`) | `memory::MemoryArbitrator` |
| `ValueStreamConnector`, `ValueStreamConnectorFactory`, `ValueStreamTableHandle`, `ValueStreamColumnHandle`, `ValueStreamDataSource` (`cpp/velox/operators/plannodes/RowVectorStream.h`) | `connector::Connector`, `ConnectorFactory`, `ConnectorTableHandle`, `ColumnHandle`, `DataSource` |
| `IteratorConnectorSplit` (`cpp/velox/operators/plannodes/IteratorSplit.h`) | `connector::ConnectorSplit` |
| `SparkExprToSubfieldFilterParser` (`cpp/velox/operators/functions/SparkExprToSubfieldFilterParser.h`) | `exec::ExprToSubfieldFilterParser` |
| `RowConstructorWithNullCallToSpecialForm` (`cpp/velox/operators/functions/RowConstructorWithNull.h`) | `exec::FunctionCallToSpecialForm` |
| `CudfValueStream`, `CudfVectorStreamOperatorTranslator` (`cpp/velox/operators/plannodes/CudfVectorStream.h`) | `exec::SourceOperator` + `cudf_velox::CudfOperator`; `Operator::PlanNodeTranslator` |
| `JniReadFile`, `JniWriteFile`, `JniFileSystem`, `FileSystemWrapper` (`cpp/velox/jni/JniFileSystem.cc`) | `ReadFile`, `WriteFile`, `filesystems::FileSystem` |
| `GlutenBufferedInputBuilder` (`cpp/velox/memory/GlutenBufferedInputBuilder.h`) | `connector::hive::BufferedInputBuilder` |
| `GlutenDirectBufferedInput` (`cpp/velox/memory/GlutenDirectBufferedInput.h`) | `dwio::common::DirectBufferedInput` |
| `CachedColumnLoader` (`cpp/velox/operators/serializer/VeloxColumnarBatchSerializer.cc`) | `VectorLoader` |
| `BufferOutputStream` (`cpp/velox/memory/BufferOutputStream.h`), `ArrowFixedSizeBufferOutputStream` (`cpp/velox/memory/ArrowMemory.h`) | `OutputStream` |
| `VeloxShuffleWriterTestBase` (`cpp/velox/tests/VeloxShuffleWriterTestBase.h`) | `test::VectorTestBase` |
| `MockMemoryReclaimer` (`cpp/velox/tests/MemoryManagerTest.cc`) | `memory::MemoryReclaimer` |

## Linked CMake targets

`velox`, `velox_cudf_exec`, `velox_cudf_expression`, `velox_cudf_expression_registry`,
`velox_cudf_hive_connector`, `velox_cudf_iceberg_connector`, `velox_cudf_vector`,
`velox_curl`, `veloxthrift`.

## Note: Java-level Velox binding

Separate from the C++ surface above, `gluten-flink/` binds to Velox through
**velox4j** (see `gluten-flink/patches/fix-velox4j.patch`). That path is not covered by
this inventory.

## How to regenerate

```bash
# Distinct Velox headers included
grep -rho '#include "velox/[^"]*"' cpp/ --include='*.cc' --include='*.h' \
  | sed 's/#include "//;s/"//' | sort | uniq -c | sort -rn

# Velox-qualified symbols
grep -rhoE '(facebook::)?velox::[A-Za-z_][A-Za-z0-9_:]*' cpp/ --include='*.cc' --include='*.h' \
  | sed 's/^facebook:://' | sort | uniq -c | sort -rn

# Classes extending Velox types
grep -rnE 'class +[A-Za-z_]+ *: *(public|private|protected) +[^{]*velox' cpp/ \
  --include='*.h' --include='*.cc'
```
