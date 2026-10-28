# window

| case | inputs | expression | description |
| --- | --- | --- | --- |
| row-number | `id = standard.long()` | `row_number() OVER (PARTITION BY pmod(id, 7) ORDER BY id)` | Partitioned ranking across batches. |
| global-row-number | `id = standard.long()` | `row_number() OVER (ORDER BY id)` | Window results across input batch and partition boundaries. |
| rank | `id = standard.long()` | `rank() OVER (PARTITION BY pmod(id, 7) ORDER BY pmod(id, 5))` | Window results across input batch and partition boundaries. |
| dense-rank | `id = standard.long()` | `dense_rank() OVER (PARTITION BY pmod(id, 7) ORDER BY pmod(id, 5))` | Window results across input batch and partition boundaries. |
| sum-rows | `id = standard.long()` | `sum(id) OVER (PARTITION BY pmod(id, 7) ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)` | Window results across input batch and partition boundaries. |
| sum-sliding | `id = standard.long()` | `sum(id) OVER (PARTITION BY pmod(id, 7) ORDER BY id ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING)` | Window results across input batch and partition boundaries. |
| sum-range | `id = standard.long()` | `sum(id) OVER (PARTITION BY pmod(id, 7) ORDER BY pmod(id, 5) RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)` | Window results across input batch and partition boundaries. |
| lag | `id = standard.long()` | `lag(id, 2, -1) OVER (PARTITION BY pmod(id, 7) ORDER BY id)` | Window results across input batch and partition boundaries. |
| lead | `id = standard.long()` | `lead(id, 2, -1) OVER (PARTITION BY pmod(id, 7) ORDER BY id)` | Window results across input batch and partition boundaries. |
| lag-null | `id = standard.long(); v = nullableBoundedInt(nullPercent=50)` | `lag(v, 1) OVER (PARTITION BY pmod(id, 7) ORDER BY id)` | Preserve nulls and null boundary defaults. |
