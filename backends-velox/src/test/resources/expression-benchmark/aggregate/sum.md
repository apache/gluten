# sum

| case | inputs | expression | description | grouping | mode | maps | reducers | key-cardinality |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| global | `v = standard.long()` | `sum(v)` | Partial, local shuffle and Final. | - | partial-final | 3 | 1 | - |
| grouped | `k = standard.long(); v = standard.long()` | `sum(v)` | Configurable input partitions, local shuffle and Final. | `k` | - | - | - | - |
| grouped-low | `k = standard.long(); v = standard.long()` | `sum(v)` | Grouped aggregation with input key cardinality 100. | `k` | partial-final | 3 | 1 | 100 |
| grouped-high | `k = standard.long(); v = standard.long()` | `sum(v)` | Grouped aggregation with input key cardinality 4000000. | `k` | partial-final | 3 | 1 | 4000000 |
| random-key | `v = standard.long()` | `sum(v)` | Reproduce uninitialized grouping projection. | `rand(7)` | - | - | - | - |
| partition-id-key | `v = standard.long()` | `sum(v)` | Reproduce partition-local ID state across input partitions. | `monotonically_increasing_id()` | - | - | - | - |
| random-result | `k = standard.long(); v = standard.long()` | `sum(v) + rand(7)` | Deterministic keys with a nondeterministic result. | `k` | - | - | - | - |
| complete-global | `v = standard.long()` | `sum(v)` | Single-stage global aggregation. | - | complete | 3 | 1 | - |
| complete-pruned-input | `k = standard.long(); v = hashedLong(column=1)` | `sum(v)` | Preserve input column pruning below coalesce. | - | complete | 3 | 1 | - |
| one-map | `v = standard.long()` | `sum(v)` | One map keeps the shuffle boundary; requested reducers are recorded. | - | partial-final | 1 | 7 | - |
| complete-grouped | `k = standard.long(); v = standard.long()` | `sum(v)` | Single-stage grouped aggregation. | `k` | complete | 3 | 1 | 100 |
| complete-cast | `v = standard.long()` | `cast(sum(v) as string)` | Preserve aggregate output projection. | - | complete | 3 | 1 | - |
| complete-json | `v = standard.long()` | `from_json(concat('{"s":', nullif(sum(v), 7L), '}'), 's BIGINT')` | Preserve Spark codegen boundaries. | - | complete | 3 | 1 | - |
| partial-final-cast | `v = standard.long()` | `cast(sum(v) as string)` | Preserve aggregate output projection. | - | partial-final | 3 | 1 | - |
| partial-final-json | `v = standard.long()` | `from_json(concat('{"s":', nullif(sum(v), 7L), '}'), 's BIGINT')` | Preserve Spark codegen boundaries. | - | partial-final | 3 | 1 | - |
| constant-key-0 | `v = standard.long()` | `sum(v)` | Reproduce a constant grouping key interpreted as a SELECT ordinal. | `0` | partial-final | 3 | 1 | - |
| constant-key-2 | `v = standard.long()` | `sum(v)` | Reproduce a constant grouping key interpreted as a SELECT ordinal. | `2` | partial-final | 3 | 1 | - |
| projected-plus-one | `v = standard.long()` | `sum(v) + 1` | Aggregate result projection. | - | partial-final | 3 | 1 | - |
| projected-null | `v = standard.long()` | `nullif(sum(v), sum(v))` | Nullable aggregate result projection. | - | partial-final | 3 | 1 | - |
