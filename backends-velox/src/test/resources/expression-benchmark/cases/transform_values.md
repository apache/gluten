# transform_values

| case | inputs | expression | description |
| --- | --- | --- | --- |
| int-map | `input = standard.intMap()` | `transform_values(input, (k, v) -> v + 1)` | Increment nullable integer map values while retaining their string keys. |
