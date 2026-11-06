# least

Dynamic columns use a deterministic hash of row identity and seed plus column index. Inputs are generated before timing and repeat at the configured key cardinality. NULLs are absent unless nullPercent is explicitly set.

hashedString uses ASCII hexadecimal hash bytes. hashedBinary uses raw big-endian hash bytes. A prefix of 56 means 56 shared x bytes before column-specific data. Each column's NULL mask uses a separate salted hash.

| case | inputs | expression | description |
| --- | --- | --- | --- |
| standard-long | `input = standard.long()` | `least(input, CAST(17 AS BIGINT))` | BIGINT input followed by constant 17; no NULLs. |
| constant-first-long | `input = standard.long()` | `least(CAST(17 AS BIGINT), input)` | Same BIGINT data with the constant first. |
| dynamic-two-long | `a = hashedLong(column=0); b = hashedLong(column=1)` | `least(a, b)` | Two changing BIGINT columns. |
| dynamic-four-long | `a = hashedLong(column=0); b = hashedLong(column=1); c = hashedLong(column=2); d = hashedLong(column=3)` | `least(a, b, c, d)` | Four changing BIGINT columns. |
| dynamic-string8 | `a = hashedString(column=0, length=8); b = hashedString(column=1, length=8)` | `least(a, b)` | Two changing 8-byte VARCHAR columns; no NULLs. |
| dynamic-four-string64 | `a = hashedString(column=0, length=64, prefix=56); b = hashedString(column=1, length=64, prefix=56); c = hashedString(column=2, length=64, prefix=56); d = hashedString(column=3, length=64, prefix=56)` | `least(a, b, c, d)` | Four 64-byte VARCHAR columns; no NULLs. |
| dynamic-four-string64-null50 | `a = hashedString(column=0, length=64, prefix=56, nullPercent=50); b = hashedString(column=1, length=64, prefix=56, nullPercent=50); c = hashedString(column=2, length=64, prefix=56, nullPercent=50); d = hashedString(column=3, length=64, prefix=56, nullPercent=50)` | `least(a, b, c, d)` | Four 64-byte VARCHAR columns, each independently 50-percent NULL. |
| dynamic-four-string64-null100 | `a = hashedString(column=0, length=64, prefix=56, nullPercent=100); b = hashedString(column=1, length=64, prefix=56, nullPercent=100); c = hashedString(column=2, length=64, prefix=56, nullPercent=100); d = hashedString(column=3, length=64, prefix=56, nullPercent=100)` | `least(a, b, c, d)` | Four all-NULL VARCHAR columns. |
| dynamic-four-binary64 | `a = hashedBinary(column=0, length=64, prefix=56); b = hashedBinary(column=1, length=64, prefix=56); c = hashedBinary(column=2, length=64, prefix=56); d = hashedBinary(column=3, length=64, prefix=56)` | `least(a, b, c, d)` | Four changing 64-byte VARBINARY columns; no NULLs. |
