# aggregate

This is a higher-order scalar over each input array, not a SQL aggregate operator.

| case | inputs | expression | description |
| --- | --- | --- | --- |
| int-array | `input = standard.intArray()` | `aggregate(input, 0, (acc, x) -> acc + x)` | Sum the three elements within each prepared integer array. |
