# current_database

This measures a prepared session constant, not per-row database lookup.

| case | inputs | expression | description |
| --- | --- | --- | --- |
| constant | `none` | `current_database()` | Current session namespace resolved once to a shared constant before timing. |
