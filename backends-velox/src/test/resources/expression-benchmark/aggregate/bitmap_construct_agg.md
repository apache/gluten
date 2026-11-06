# bitmap_construct_agg

| case | inputs | expression | description | grouping | mode | maps | reducers | key-cardinality |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| long-g256 | `id = standard.long()` | `bitmap_construct_agg(pmod(id, 32768))` | Bitmap construction with 256 groups and independent value cardinality. | `pmod(id, 256)` | partial-final | 3 | 1 | - |
