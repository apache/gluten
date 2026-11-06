# explode

Changing long inputs are prepared outside timing. Array/map/struct construction and generator execution are timed. Mixed cases alternate NULL, empty and nonempty collections; outer variants preserve NULL and empty input rows.

| case | inputs | expression | description |
| --- | --- | --- | --- |
| array-mixed | `input = standard.long()` | `explode(IF(input % 3 = 0, CAST(NULL AS ARRAY<BIGINT>), IF(input % 3 = 1, array(), array(input, input + 1))))` | Exercise generator output arity and row expansion. |
| map-mixed | `input = standard.long()` | `explode(IF(input % 3 = 0, CAST(NULL AS MAP<STRING,BIGINT>), IF(input % 3 = 1, map(), map('a', input, 'b', input + 1))))` | Exercise generator output arity and row expansion. |
| stored-array-int | `a = standard.intArray()` | `explode(a)` | Stored typed array input is prepared before timing. |
| stored-array-string64 | `a = standard.stringArray(length=64)` | `explode(a)` | Stored string array input is prepared before timing. |
| stored-map | `a = standard.intMap()` | `explode(a) AS (map_key, map_value)` | Stored map input is prepared before timing. |
| construct-pair-int | `v = standard.int()` | `explode(array(v, v))` | Scalar input prepared before timing; array construction and generation are timed. |
| construct-pair-string64 | `v = standard.string(length=64)` | `explode(array(v, v))` | Scalar string prepared before timing; array construction and generation are timed. |
