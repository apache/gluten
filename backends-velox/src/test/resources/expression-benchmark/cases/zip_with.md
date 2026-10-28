# zip_with

| case | inputs | expression | description |
| --- | --- | --- | --- |
| int-array | `input = standard.intArray()` | `zip_with(input, input, (x, y) -> x + y)` | Combine corresponding elements of prepared integer arrays using a scalar lambda. |
