# inline

Changing long inputs are prepared outside timing. Array/map/struct construction and generator execution are timed. Mixed cases alternate NULL, empty and nonempty collections; outer variants preserve NULL and empty input rows.

| case | inputs | expression | description |
| --- | --- | --- | --- |
| struct-array-mixed | `input = standard.long()` | `inline(IF(input % 3 = 0, CAST(NULL AS ARRAY<STRUCT<v:BIGINT,w:BIGINT>>), IF(input % 3 = 1, array(), array(named_struct('v', input, 'w', input + 1)))))` | Exercise generator output arity and row expansion. |
| stored-struct | `a = standard.mapEntries()` | `inline(a)` | Stored struct array input is prepared before timing. |
