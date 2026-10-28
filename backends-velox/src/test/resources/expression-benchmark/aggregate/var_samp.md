# var_samp

| case | inputs | expression | description | grouping | mode | maps | reducers | key-cardinality |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| global | `v = standard.long()` | `var_samp(v)` | Capability candidate; validate before timing. | - | partial-final | 3 | 1 | 16 |
| grouped | `v = standard.long(); k = standard.long()` | `var_samp(v)` | Capability candidate; validate before timing. | `k` | partial-final | 3 | 1 | 16 |
