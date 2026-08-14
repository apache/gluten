---
layout: page
title: Bolt Backend Window Function Support
nav_order: 22
---

# Window Functions Support Status in Bolt

| Spark Functions | Spark Expressions | Status | Restrictions |
|-----------------|-------------------|--------|--------------|
| cume_dist       | CumeDist          | S      | Returns a `DOUBLE` cumulative distribution value within the partition. |
| dense_rank      | DenseRank         | S      | Returns the dense rank within each partition. |
| lag             | Lag               | S      | Offset must be ≥ 0; offset = 0 returns the current row. Optional default value is supported. `IGNORE NULLS` is supported and `lag` operates over the whole partition, not limited by frame. |
| lead            | Lead              | S      | Same rules as `lag`: offset ≥ 0, offset = 0 returns the current row, optional default value and `IGNORE NULLS` are supported, operates over the whole partition. |
| nth_value       | NthValue          | S      | Return type matches the value expression. Offset must be ≥ 1 for both constant and column offsets; smaller values are rejected. `IGNORE NULLS` is supported in the engine for the PrestoSQL mapping. |
| ntile           | NTile             | S      | Number of buckets must be > 0; 0 or negative values are rejected. When buckets exceed the partition size, the bucket index effectively follows the row number. If the bucket expression is `NULL`, the result is `NULL`. |
| percent_rank    | PercentRank       | S      | Returns a `DOUBLE` in \\[0, 1], computed as `(rank - 1) / (partition_row_count - 1)` (0 when there is only one row in the partition). |
| rank            | Rank              | S      | Returns the rank within each partition using standard SQL rank semantics. |
| row_number      | RowNumber         | S      | Assigns a sequential row number within each partition. |
