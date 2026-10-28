# collect_set

| case | inputs | expression | description | grouping | mode | maps | reducers | key-cardinality |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| global | `v = standard.long()` | `collect_set(v)` | Capability candidate; validate before timing. | - | partial-final | 3 | 1 | 16 |
| grouped | `v = standard.long(); k = standard.long()` | `collect_set(v)` | Capability candidate; validate before timing. | `k` | partial-final | 3 | 1 | 16 |
