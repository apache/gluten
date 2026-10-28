# ifnull

| case | inputs | expression | description |
| --- | --- | --- | --- |
| nullable-int | `input = nullableBoundedInt(nullPercent=50)` | `ifnull(input, 7)` | Replace deterministic approximately 50-percent NULL integers with seven. |
