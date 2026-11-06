# stack

Changing long inputs are prepared outside timing. Array/map/struct construction and generator execution are timed. Mixed cases alternate NULL, empty and nonempty collections; outer variants preserve NULL and empty input rows.

| case | inputs | expression | description |
| --- | --- | --- | --- |
| two-rows-two-columns | `input = standard.long()` | `stack(2, input, input + 1, input + 2, input + 3)` | Exercise generator output arity and row expansion. |
