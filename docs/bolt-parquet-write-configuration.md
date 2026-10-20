---
layout: page
title: Bolt Parquet Write Configuration
nav_order: 24
---

# Bolt Parquet Write Configuration

This document outlines the Parquet write-time configuration options supported in Bolt. The Bolt Parquet writer is based on the Arrow C++ Parquet library and is primarily exposed through the Hive connector.

In this document, "Support" indicates that a configuration is available and effective when set through Bolt's public configuration pathways (e.g., Hive session/table properties, `QueryConfig`).

## Spark-Level Configurations (Reference)

These standard Spark properties are not directly used by Bolt's writer. Instead, Bolt provides equivalent functionality through its own configuration mechanisms.

| Property | Support | Bolt Default | Configuration Path | Notes |
| :--- | :--- | :--- | :--- | :--- |
| `spark.sql.parquet.outputTimestampType` | Partial | INT96 disabled; Coercion to `SECOND` | `HiveConfig` `arrowBridgeTimestampUnit`, `WriterOptions` `writeInt96AsTimestamp` | Bolt manages timestamp precision via `arrowBridgeTimestampUnit` (`SECOND`, `MILLI`, `MICRO`, `NANO`). The internal `writeInt96AsTimestamp` option provides an override for legacy INT96 format and takes precedence. See `bolt/connectors/hive/HiveDataSink.cpp` and `bolt/dwio/parquet/writer/Writer.h`. |
| `spark.sql.parquet.writeLegacyFormat` | No | N/A | Not applicable | Bolt does not use this flag. The format version is controlled by the `parquet.writer.version` property. |

## Arrow Writer Properties in Bolt

These properties correspond to settings within the underlying `parquet::arrow::WriterProperties` and `parquet::arrow::ArrowWriterProperties`. Bolt exposes some of these directly or maps them from other configurations.

| Property | Support | Bolt Default | Configuration Path | Notes |
| :--- | :--- | :--- | :--- | :--- |
| `write_batch_size` | Internal | `1024` | Not exposed | Default from Arrow writer properties. Bolt uses its own heuristics (`writeBatchBytes`, `minBatchSize`) for batching. See `bolt/dwio/parquet/arrow/Properties.h`. |
| `max_row_group_length` | Yes | `1,048,576` rows | Flush Policy | The default flush policy triggers at ~1M rows or ~128MiB. Configurable via a custom flush policy factory in `WriterOptions`. See `bolt/dwio/parquet/writer/Writer.h`. |
| `parquet_block_size` | Yes | `128 MiB` (when enabled) | `WriterOptions.parquet_block_size` | This is only effective when `enableFlushBasedOnBlockSize` is true, which overrides the default row/byte flush policy. See `bolt/dwio/parquet/writer/Writer.cpp`. |
| `data_page_version` | Yes | `V1` | `WriterOptions.dataPageVersion` | Can be set to `V1` or `V2`. See `bolt/dwio/parquet/writer/Writer.h`. |
| `writer.version` | Yes | `PARQUET_2_6` | `WriterOptions.parquetVersion` | Controls the Parquet format version. See `bolt/dwio/parquet/arrow/Properties.h`. |
| `compression` | Yes | `UNCOMPRESSED` | Hive `INSERT` `compressionKind` property | Supported codecs: `SNAPPY`, `GZIP`, `ZSTD`, `LZ4`, `UNCOMPRESSED`. Mapped in `bolt/connectors/hive/HiveDataSink.cpp`. |
| `compression_level` & `codec_options` | Internal | Varies by codec | `WriterOptions.codecOptions` | Supported internally by the Arrow writer but not exposed through Hive session/table properties. Can be configured per-column. See `bolt/dwio/parquet/arrow/Properties.h`. |
| `dictionary_enabled` | Yes | `true` | `WriterOptions.enableDictionary` (global), `columnEnableDictionaryMap` (per-column) | Dictionary encoding can be controlled globally or for specific columns. See `bolt/dwio/parquet/writer/Writer.h`. |
| `data_page_size` | Yes | `1 MiB` | `WriterOptions.dataPageSize` (global), `columnDataPageSizeMap` (per-column) | See `bolt/dwio/parquet/writer/Writer.h`. |
| `dictionary_page_size_limit` | Yes | `1 MiB` | `WriterOptions.dictionaryPageSizeLimit` (global), `columnDictionaryPageSizeLimitMap` (per-column) | See `bolt/dwio/parquet/writer/Writer.h`. |
| `page_index_enabled` | Internal | `false` | Not exposed | The Arrow writer supports page index generation, but it is not currently configurable in Bolt. See `bolt/dwio/parquet/arrow/Properties.h`. |
| `statistics_enabled` | Internal | `true` | Not exposed | Statistics are enabled by default with a max size of `4096` bytes per column. Not externally configurable. See `bolt/dwio/parquet/arrow/Properties.h`. |
| `store_decimal_as_integer` | Yes | `true` | `WriterOptions.storeDecimalAsInteger` | Bolt defaults to storing compatible decimals as `int32`/`int64` for efficiency. See `bolt/dwio/parquet/writer/Writer.h`. |
| `created_by` | Internal | `"parquet-cpp-bolt"` | Hardcoded | The created_by string in the file metadata is set internally. See `bolt/dwio/parquet/arrow/Properties.h`. |
| `sorting_columns` | Internal | Not set | Not exposed | The Arrow writer can store sorting metadata, but Bolt does not expose a pathway to set this. See `bolt/dwio/parquet/arrow/Properties.h`. |
| `page_checksum_enabled` | Yes | `false` | `parquet.page.write-checksum.enabled` table property | Mapped internally to the Arrow writer's `enable_page_checksum()` builder method. See `bolt/dwio/parquet/arrow/Properties.h`. |
| `encryption` | Internal | Disabled | `WriterOptions.encryptionOptions` | The writer supports AES_GCM_V1 and AES_GCM_CTR_V1 encryption if properties are provided, but this is not exposed through the Hive connector. See `bolt/dwio/parquet/writer/Writer.h`. |
| `threading (use_threads)` | Yes | `false` (0 threads) | `WriterOptions.threadPoolSize` | If `threadPoolSize > 0`, threading is enabled for parallel column writing using a global static thread pool. See `bolt/dwio/parquet/writer/Writer.cpp`. |
| `compliant_nested_types` | Internal | `true` | Not exposed | The writer follows the Parquet specification for nested list element naming ("element"). See `bolt/dwio/parquet/arrow/Properties.h`. |

## Parquet-MR Style Settings

This table indicates whether classic `parquet-mr` Hadoop configurations are effectively supported by Bolt's writer, typically by mapping to an equivalent `WriterOptions` or `WriterProperties` setting.

| Property | Effective in Bolt | Bolt Default | Configuration Path | Notes |
| :--- | :--- | :--- | :--- | :--- |
| `parquet.block.size` | Yes | `128 MiB` | `WriterOptions.parquet_block_size` | Used when `enableFlushBasedOnBlockSize` is true. |
| `parquet.page.size` | Yes | `1 MiB` | `WriterOptions.dataPageSize` | |
| `parquet.compression` | Yes | `UNCOMPRESSED` | Hive `compressionKind` table property | Maps to `WriterOptions.compression`. |
| `parquet.enable.dictionary` | Yes | `true` | `WriterOptions.enableDictionary` | |
| `parquet.dictionary.page.size` | Yes | `1 MiB` | `WriterOptions.dictionaryPageSizeLimit` | |
| `parquet.writer.version` | Yes | `PARQUET_2_6` | `WriterOptions.parquetVersion` | |
| `parquet.compression.codec.zstd.level` | Yes | 3 | `WriterOptions.codecOptions` | Exposed via session/table properties and mapped internally. Default is from zstd library. See `bolt/dwio/parquet/arrow/util/CompressionZstd.cpp`. |
| `parquet.page.write-checksum.enabled` | Yes | `false` | `WriterProperties::Builder` | Configurable via table properties. |
| `parquet.enable.summary-metadata` | No | N/A | Not implemented | Bolt does not create a separate summary file. |
| `parquet.bloom.filter.enabled` | No | N/A | Not implemented | Bloom filter writing is not supported. |
| `parquet.crypto.factory.class` | No | N/A | Not implemented | Encryption is handled internally via `WriterOptions`. |
| `parquet.compression.codec.zstd.workers` | No | N/A | Not implemented | Bolt's Parquet writer threading is controlled by `WriterOptions.threadPoolSize`. |
| `parquet.validation` | No | N/A | Not implemented | |

## Bolt-Specific Writer Options and Behaviors

These options and behaviors are specific to Bolt's implementation and provide more granular control over the writing process.

- **Default Flush Policy**: By default, the Parquet writer flushes a row group when it reaches approximately **1,048,576 rows** or its estimated size exceeds **128 MiB**. This is defined in `DefaultFlushPolicy` in `bolt/dwio/parquet/writer/Writer.h`.

- **`enableFlushBasedOnBlockSize`**: A `bool` in `WriterOptions` that changes the flush behavior from the default row/byte count policy to a purely block-size-based policy. When `true`, it uses `WriterOptions.parquet_block_size` (defaults to 128 MiB) to control row group size and sets `max_row_group_length` to a very large value to prevent it from triggering first. The Arrow writer's `NewBufferedRowGroup()` is used. Found in `bolt/dwio/parquet/writer/Writer.cpp`.

- **`enableRowGroupAlignedWrite`**: A `bool` in `WriterOptions` used for specialized data retention scenarios. It works with `expectedRowsInEachBlock` to create row groups with an exact number of rows. See `bolt/dwio/parquet/writer/Writer.h`.

- **`writeBatchBytes` / `minBatchSize`**: Internal heuristics within `WriterOptions` to manage memory and performance when converting large Bolt `VectorPtr` batches to Arrow `RecordBatch`. The writer may split large batches into smaller ones based on these thresholds (defaults: 40 MiB and 512 rows). See `bolt/dwio/parquet/writer/Writer.cpp`.

- **Filename Extension**: For Hive `INSERT` operations, `HiveDataSink` automatically appends the `.parquet` extension to output files when the storage format is `PARQUET`. This is handled in `bolt/connectors/hive/HiveDataSink.cpp`.

- **Timestamp Bridge Unit**: The precision of timestamps written to Parquet is controlled by the `arrow_bridge_timestamp_unit` session property (via `HiveConfig`), which can be set to `SECOND`, `MILLI`, `MICRO`, or `NANO`. For legacy compatibility, the internal `WriterOptions.writeInt96AsTimestamp` (`bool`) can be set to `true` to force the deprecated `INT96` format, and this setting takes precedence over the bridge unit. See `bolt/connectors/hive/HiveConfig.cpp` and `bolt/dwio/parquet/writer/Writer.h`.
