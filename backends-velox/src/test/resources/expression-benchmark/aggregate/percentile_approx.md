# percentile_approx

Capability probe on A rejects approx_percentile in AggregateRel; retain these cases to expose Native fallback, not as validated Native performance.

| case | inputs | expression | description | grouping | mode | maps | reducers | key-cardinality |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| global | `v = standard.long()` | `percentile_approx(v, 0.5, 10000)` | Capability candidate; validate before timing. | - | partial-final | 3 | 1 | 16 |
| grouped | `v = standard.long(); k = standard.long()` | `percentile_approx(v, 0.5, 10000)` | Capability candidate; validate before timing. | `k` | partial-final | 3 | 1 | 16 |
