---
layout: page
title: VeloxDynamicSizingOffheap
nav_order: 16
parent: Developer Overview
---

## Dynamic Off-heap Sizing
Gluten requires setting both on-heap and off-heap memory sizes, which initializes different memory layouts. Improper configuration of these settings can lead to lower performance. 

To fix this issue, dynamic off-heap sizing is an experimental feature designed to simplify this process. Please note when enabled, user defined spark off-heap settings(`spark.memory.offHeap.enabled`, `spark.memory.offHeap.size`) will not be effective, and Velox uses the on-heap size as the memory size.
To enable this feature, users need to add below entry in Spark conf:
```
--conf spark.gluten.memory.dynamic.offHeap.sizing.enabled=true 
```

## Detail implementations
To understand the details, it's essential to learn the basics of JVM memory management. There are many resources discussing JVM internals:
- https://exia.dev/blog/2019-12-10/JVM-Memory-Model/
- https://docs.oracle.com/cd/E13150_01/jrockit_jvm/jrockit/geninfo/diagnos/garbage_collect.html
- https://www.scaler.com/topics/memory-management-in-java/
- https://developers.redhat.com/articles/2021/09/09/how-jvm-uses-and-allocates-memory#
- https://docs.oracle.com/en/java/javase/11/gctuning/factors-affecting-garbage-collection-performance.html

In general, the feature works as follows:

- Spark first attempts to allocate memory based on the on-heap size. Note that the maximum memory size is controlled by `spark.executor.memory`.
- When Velox tries to allocate memory, Gluten attempts to allocate from system memory and records this in the memory allocator.
- If there is sufficient memory, allocations proceed normally.
- If memory is insufficient, Spark performs garbage collection (GC) to free on-heap memory, allowing Velox to allocate memory.
- If memory remains insufficient after GC, Spark reports an out-of-memory (OOM) issue.
- The `MaxHeapFreeRatio` and `MinHeapFreeRatio` parameters are used to configure the max/min heap size for Spark JVM process. Note these two parameters are avaiable starts from JDK-11.

We then enforce a total memory quota, calculated as the sum of committed and in-use memory in the Java heap (using `Runtime.getRuntime().totalMemory()`) plus tracked off-heap memory in `TreeMemoryConsumer`. If an allocation exceeds this total committed memory, the allocation fails and triggers an OOM.

With this change, the "quota check" is performed when Gluten receives a memory allocation request. In practice, this means the Java codebase can oversubscribe memory within the on-heap quota, even if off-heap usage is sufficient to fail the allocation.

## Limitations

This feature is in the preliminary stages of development and will be improved in future updates.