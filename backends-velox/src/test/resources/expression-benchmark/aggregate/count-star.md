# count-star

| case | inputs | expression | description | grouping | mode | maps | reducers | key-cardinality |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| global | `v = standard.long()` | `count(*)` | Partial, local shuffle and Final. | - | partial-final | 3 | 1 | - |
| grouped | `k = standard.long(); v = standard.long()` | `count(*)` | Configurable input partitions, local shuffle and Final. | `k` | - | - | - | - |
| grouped-low | `k = standard.long(); v = standard.long()` | `count(*)` | Grouped aggregation with input key cardinality 100. | `k` | partial-final | 3 | 1 | 100 |
| grouped-high | `k = standard.long(); v = standard.long()` | `count(*)` | Grouped aggregation with input key cardinality 4000000. | `k` | partial-final | 3 | 1 | 4000000 |
