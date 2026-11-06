# typeof

The input schema determines a constant result before timed evaluation.

| case | inputs | expression | description |
| --- | --- | --- | --- |
| constant | `input = standard.long()` | `typeof(input)` | BIGINT type name folded to a constant; not per-row type discovery. |
