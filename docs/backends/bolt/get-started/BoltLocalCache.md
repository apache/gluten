---
layout: page
title: Bolt Local Caching
nav_order: 12
parent: Getting-Started
---

# Bolt Local Caching

Bolt supports a two-tiered local caching mechanism to accelerate queries that read data from remote storage systems like HDFS, S3, ABFS, or GCS. When enabled, data blocks read from a remote source are asynchronously persisted to local storage. Future requests for the same data can then be served from the local cache, avoiding repeated network I/O.

The cache consists of:
1.  **In-Memory Cache**: A primary, high-speed cache (`AsyncDataCache`) that resides in RAM.
2.  **SSD Cache**: An optional, larger-capacity secondary cache (`SsdCache`) that persists data blocks on a local solid-state drive (SSD) or NVMe device.

This document outlines how to configure and use Bolt's local caching feature.

## Configuration

Local caching in Bolt is configured through a combination of query-level settings and programmatic setup, rather than a single block of Spark-style properties.

### Enabling the Cache

Local caching is enabled by default within the `HiveDataSource` but can be explicitly controlled.

| Property | Description | Default |
|---|---|---|
| `native_cache_enabled` | A `QueryConfig` flag to enable or disable local caching for a query. Caching can also be disabled on a per-split basis via custom split info (`split_cache_enabled`). | `true` |

### In-Memory Cache Configuration

The in-memory cache capacity is managed by Bolt's `MemoryManager` and is influenced by general memory and I/O settings. There is no single knob to control its size directly.

- **Capacity**: The total size of the in-memory cache is determined by the `allocatorCapacity` set on the `MemoryManager` options when the application starts. It is not a dedicated `QueryConfig` setting.
- **I/O Tuning**: Read behavior that populates the cache can be tuned with `HiveConfig` properties, which control how data is fetched from remote storage:
  - `load-quantum`: The size of a single I/O request when reading data.
  - `max-coalesced-bytes`: The maximum size of a single coalesced I/O operation.
  - `max-coalesced-distance`: The maximum gap between two reads to be merged into a single I/O request.
  - `prefetch-rowgroups`: The number of subsequent row groups to prefetch in formats like Parquet.

### SSD Cache Configuration

The SSD cache is configured programmatically when an application initializes the `SsdCache` object, typically during startup. Benchmark harnesses offer flags for easy setup, but general applications must create the cache instance in code.

**Programmatic Options:**

The `SsdCache` constructor accepts the following options:

| Parameter | Description |
|---|---|
| `filePrefix` | The directory path where SSD cache files will be stored. |
| `maxBytes` | The total capacity of the SSD cache in bytes. |
| `numShards` | The number of shard files to partition the cache into. A higher number reduces lock contention. A typical value is 16. |
| `executor` | A `folly::Executor` to manage I/O threads for cache operations. |
| `checkpointIntervalBytes` | The interval in bytes at which the cache's state is durably checkpointed to disk. A non-zero value enables crash recovery. |
| `disableFileCow` | A boolean flag to disable Copy-on-Write (COW) for filesystems that support it (e.g., Btrfs) to prevent disk usage from exceeding the configured `maxBytes`. |
| `ssd_odirect` | (Linux-only) A boolean flag that enables or disables the use of `O_DIRECT` for cache writes to bypass the OS page cache. Default is `true`. |

**Benchmark Example Flags:**

The following flags are available in benchmark runners and serve as a good example of how to configure the SSD cache:

| Flag | Description | Example Value |
|---|---|---|
| `--ssd_path` | Directory for the SSD cache files. | `/mnt/ssd/cache` |
| `--ssd_cache_gb` | Total size of the SSD cache in gigabytes. | `64` |
| `--ssd_checkpoint_interval_gb` | Interval for checkpointing in gigabytes. | `8` |
| `--clear_ssd_cache` | If true, clears the SSD cache before each query run. | `false` |

## Recommended Setup

For optimal performance, follow these recommendations:

- **Hardware**: Mount a dedicated high-performance SSD or NVMe drive at the directory specified by `filePrefix` (or `--ssd_path`).
- **Sharding**: Set `numShards` to a value like 16 to support concurrent access.
- **Durability**: Enable checkpointing by setting `checkpointIntervalBytes` to a non-zero value (e.g., 8 GB). This allows the cache to be restored after an application restart, preserving its contents.
- **Direct I/O**: On Linux, keep `ssd_odirect` enabled (`true`) to reduce CPU overhead and avoid double-caching data in the OS page cache.

## Behavior

- **Asynchronous Writes**: When data is read from a remote source, it is admitted into the in-memory cache. Eligible blocks are then asynchronously written to the SSD cache without blocking the query thread.
- **I/O Coalescing**: The cache system automatically merges multiple small, contiguous read requests into a single, larger I/O operation to improve throughput.
- **Cache Hits**: If requested data is found in the in-memory cache, it is served immediately (a RAM hit). If not in memory, the SSD cache is checked. If present, it is read from the local SSD (an SSD hit) and populated back into memory. If the data is in neither cache, it is fetched from the remote source.
- **Admission and Eviction**: The cache uses a scoring mechanism based on access frequency and recency to determine which blocks to keep. When the cache is full, blocks with the lowest scores are evicted to make room for new data.
- **Cache Clearing and Shutdown**: The cache can be explicitly cleared via the `clear()` method. The `shutdown()` method ensures all pending writes are completed and a final checkpoint is created if durability is enabled.
