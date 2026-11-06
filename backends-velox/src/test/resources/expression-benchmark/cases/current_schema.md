# current_schema

This measures a prepared session constant, not per-row schema lookup.

| case | inputs | expression | description |
| --- | --- | --- | --- |
| constant | `none` | `current_schema()` | Current session namespace resolved once to a shared constant before timing. |
