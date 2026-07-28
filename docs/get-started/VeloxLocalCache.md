---
layout: page
title: Velox Local Caching
nav_order: 7
parent: Getting-Started
---

Velox supports a local cache when reading data from HDFS/S3/ABFS. With this feature, Velox can asynchronously cache the data on local disk when reading from remote storage and future read requests on previously cached blocks will be serviced from local cache files. To enable the local caching feature, the following configurations are required:

```
spark.gluten.sql.columnar.backend.velox.cacheEnabled      // enable or disable velox cache, default false.
spark.gluten.sql.columnar.backend.velox.memCacheSize      // the total size of in-mem cache, default is 128MB.
spark.gluten.sql.columnar.backend.velox.ssdCachePath      // the folder to store the cache files, default is "/tmp".
spark.gluten.sql.columnar.backend.velox.ssdCacheSize      // the total size of the SSD cache, default is 128MB. Velox will do in-mem cache only if this value is 0.
spark.gluten.sql.columnar.backend.velox.ssdCacheShards    // the shards of the SSD cache, default is 1.
spark.gluten.sql.columnar.backend.velox.ssdCacheIOThreads // the number of IO threads for SSD cache read/write operations, default is 4. Velox will try to do "read-ahead" if this value is bigger than 1
spark.gluten.sql.columnar.backend.velox.ssdODirect        // enable or disable O_DIRECT on cache write, default false.
```

It's recommended to mount SSDs to the cache path to get the best performance of local caching. Cache files will be written to "spark.gluten.sql.columnar.backend.velox.cachePath", with UUID based suffix, e.g. "/tmp/cache.13e8ab65-3af4-46ac-8d28-ff99b2a9ec9b0". Gluten cannot reuse older caches for now, and the old cache files are left after Spark context shutdown.
