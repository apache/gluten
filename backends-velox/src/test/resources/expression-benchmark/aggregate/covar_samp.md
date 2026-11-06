# covar_samp

| case | inputs | expression | description | grouping | mode | maps | reducers | key-cardinality |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| global | `v = standard.long()` | `covar_samp(v, v * 2)` | Capability candidate; validate before timing. | - | partial-final | 3 | 1 | 16 |
| grouped | `v = standard.long(); k = standard.long()` | `covar_samp(v, v * 2)` | Capability candidate; validate before timing. | `k` | partial-final | 3 | 1 | 16 |
