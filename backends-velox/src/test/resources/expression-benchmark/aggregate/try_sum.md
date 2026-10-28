# try_sum

| case | inputs | expression | description | grouping |
| --- | --- | --- | --- | --- |
| global | `v = standard.long()` | `try_sum(v)` | Partial, local shuffle and Final. | - |
| grouped | `k = standard.long(); v = standard.long()` | `try_sum(v)` | Configurable input partitions, local shuffle and Final. | `k` |
