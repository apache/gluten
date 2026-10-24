# hll_sketch_agg

Compares estimated cardinality, not binary sketch identity.

| case | inputs | expression | description | grouping | mode | maps | reducers | key-cardinality |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| global | `v = standard.long()` | `hll_sketch_estimate(hll_sketch_agg(v))` | Capability candidate; validate before timing. | - | partial-final | 3 | 1 | 16 |
| grouped | `v = standard.long(); k = standard.long()` | `hll_sketch_estimate(hll_sketch_agg(v))` | Capability candidate; validate before timing. | `k` | partial-final | 3 | 1 | 16 |
