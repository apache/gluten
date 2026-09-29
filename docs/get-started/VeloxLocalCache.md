---
layout: page
title: Velox Local Caching
nav_order: 7
parent: Getting-Started
---

Velox supports local caching when reading data from HDFS/S3/ABFS. With this feature, Velox asynchronously caches data on
local disk when reading from remote storage, and future read requests for previously cached blocks are served from the
local cache files. To enable local caching, the following configurations are required:

```
spark.gluten.sql.columnar.backend.velox.cacheEnabled      // Enable Velox cache. Default: false.
spark.gluten.sql.columnar.backend.velox.memCacheSize      // In-memory cache size. Default: 128MB.
spark.gluten.sql.columnar.backend.velox.ssdCachePath      // Folder to store cache files, preferably on SSD. Default: "/tmp".
spark.gluten.sql.columnar.backend.velox.ssdCacheSize      // SSD cache size. Memory-only caching is used when set to 0. Default: 128MB.
spark.gluten.sql.columnar.backend.velox.ssdCacheShards    // Number of SSD cache shards. Default: 1.
spark.gluten.sql.columnar.backend.velox.ssdCacheIOThreads // Number of IO threads for SSD cache read/write. Enables read-ahead when > 1. Default: 4.
spark.gluten.sql.columnar.backend.velox.loadQuantum       // Load quantum size. Must be at most 8MB when Velox cache is enabled, otherwise Velox fails. Default: 256MB.
spark.gluten.sql.columnar.backend.velox.ssdODirect        // Enable O_DIRECT on cache write. Currently causes SSD cache writes to fail if enabled; see Velox issue #10597. Default: false.
spark.gluten.soft-affinity.enabled                        // Enable Soft Affinity scheduling. Should be enabled together with Velox cache. Default: false.
```

It's recommended to mount SSDs to the cache path to get the best performance of local caching. Cache files will be written
to "spark.gluten.sql.columnar.backend.velox.ssdCachePath", with UUID based suffix,
e.g. "/tmp/cache.13e8ab65-3af4-46ac-8d28-ff99b2a9ec9b0". Gluten cannot reuse older caches for now, and the old cache files
are left after Spark context shutdown.
