# array_join

| case | inputs | expression | description | rows |
| --- | --- | --- | --- | --- |
| string-array | `input = standard.stringArray(length=10)` | `array_join(input, ',')` | Join two prepared string elements with a comma. | - |
| empty-array | `input = arrayJoinInput(mode="empty-array")` | `array_join(input, ',')` | Prepared empty-array inputs; generation is outside timing. | - |
| single-element | `input = arrayJoinInput(mode="single-element")` | `array_join(input, ',')` | Prepared single-element inputs; generation is outside timing. | - |
| many-short | `input = arrayJoinInput(mode="many-short")` | `array_join(input, ',')` | Prepared many-short inputs; generation is outside timing. | - |
| long-elements | `input = arrayJoinInput(mode="long-elements")` | `array_join(input, ',')` | Prepared long-elements inputs; generation is outside timing. | 2000000 |
| empty-elements | `input = arrayJoinInput(mode="empty-elements")` | `array_join(input, ',')` | Prepared empty-elements inputs; generation is outside timing. | - |
| skip-nulls | `input = arrayJoinInput(mode="mixed-nulls")` | `array_join(input, ',')` | Prepared skip-nulls inputs; generation is outside timing. | - |
| replace-nulls | `input = arrayJoinInput(mode="mixed-nulls")` | `array_join(input, ',', 'NULL')` | Prepared replace-nulls inputs; generation is outside timing. | - |
| all-null-skip | `input = arrayJoinInput(mode="all-null")` | `array_join(input, ',')` | Prepared all-null-skip inputs; generation is outside timing. | - |
| all-null-replace | `input = arrayJoinInput(mode="all-null")` | `array_join(input, ',', 'NULL')` | Prepared all-null-replace inputs; generation is outside timing. | - |
| utf8 | `input = arrayJoinInput(mode="utf8"); delim = arrayJoinDelimiter(mode="utf8")` | `array_join(input, delim)` | Prepared utf8 inputs; generation is outside timing. | - |
| embedded-nul | `input = arrayJoinInput(mode="embedded-nul"); delim = arrayJoinDelimiter(mode="embedded-nul")` | `array_join(input, delim)` | Prepared embedded-nul inputs; generation is outside timing. | - |
