# map_filter

| case | inputs | expression | description |
| --- | --- | --- | --- |
| int-map | `input = standard.intMap()` | `map_filter(input, (k, v) -> v > 0)` | Retain positive-valued entries from prepared maps, including nullable values. |
