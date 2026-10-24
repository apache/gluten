# current_timestamp

This measures constant timestamp output, not per-row clock access.

| case | inputs | expression | description |
| --- | --- | --- | --- |
| constant | `none` | `current_timestamp()` | Query timestamp resolved once to a shared constant and compared exactly across engines. |
