# explode_outer

Changing long inputs are prepared outside timing. Array/map/struct construction and generator execution are timed. Mixed cases alternate NULL, empty and nonempty collections; outer variants preserve NULL and empty input rows.

| case | inputs | expression | description |
| --- | --- | --- | --- |
| array-mixed | `input = standard.long()` | `explode_outer(IF(input % 3 = 0, CAST(NULL AS ARRAY<BIGINT>), IF(input % 3 = 1, array(), array(input, input + 1))))` | Exercise generator output arity and row expansion. |
| map-mixed | `input = standard.long()` | `explode_outer(IF(input % 3 = 0, CAST(NULL AS MAP<STRING,BIGINT>), IF(input % 3 = 1, map(), map('a', input, 'b', input + 1))))` | Exercise generator output arity and row expansion. |
| stored-mixed | `a = standard.mixedLongArray()` | `explode_outer(a)` | Stored nullable and empty array input is prepared before timing. |
