# hll_union_agg

Uses prepared valid HLL sketches and compares estimated cardinality, not binary sketch identity.

| case | inputs | expression | description | grouping | mode | maps | reducers | key-cardinality |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| global | `sketch = standard.hllSketch()` | `hll_sketch_estimate(hll_union_agg(sketch))` | Capability candidate; validate before timing. | - | partial-final | 3 | 1 | 16 |
| grouped | `k = standard.long(); sketch = standard.hllSketch()` | `hll_sketch_estimate(hll_union_agg(sketch))` | Capability candidate; validate before timing. | `k` | partial-final | 3 | 1 | 16 |
