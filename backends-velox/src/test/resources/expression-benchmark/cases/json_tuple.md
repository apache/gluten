# json_tuple

Prepared changing JSON objects contain a string field v, a numeric field n and a boolean field b. Each input produces one output row, with one string column per requested field. Input generation is outside timing; native execution includes the JSON extraction and generator rewrite.

| case | inputs | expression | description |
| --- | --- | --- | --- |
| single-field | `input = standard.jsonTupleString()` | `json_tuple(input, 'v')` | Extract one changing string field. |
| three-fields | `input = standard.jsonTupleString()` | `json_tuple(input, 'v', 'n', 'b')` | Extract string, numeric and boolean fields into three columns. |
| duplicate-and-missing | `input = standard.jsonTupleString()` | `json_tuple(input, 'v', 'v', 'missing')` | Preserve duplicate output fields and NULL for an absent field. |
