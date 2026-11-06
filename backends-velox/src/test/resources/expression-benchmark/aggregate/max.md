# max

| case | inputs | expression | description | grouping | mode | maps | reducers | key-cardinality |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| global | `v = standard.long()` | `max(v)` | Partial, local shuffle and Final. | - | partial-final | 3 | 1 | - |
| grouped | `k = standard.long(); v = standard.long()` | `max(v)` | Configurable input partitions, local shuffle and Final. | `k` | - | - | - | - |
| grouped-low | `k = standard.long(); v = standard.long()` | `max(v)` | Grouped aggregation with input key cardinality 100. | `k` | partial-final | 3 | 1 | 100 |
| grouped-high | `k = standard.long(); v = standard.long()` | `max(v)` | Grouped aggregation with input key cardinality 4000000. | `k` | partial-final | 3 | 1 | 4000000 |
