# abs

| case | inputs | expression | description |
| --- | --- | --- | --- |
| signed-long | `input = hashedLong(column=0)` | `abs(input)` | Absolute values of deterministic signed BIGINT inputs. |
| aggregate-without-grouping | `input = standard.long()` | `sum(abs(input))` | Reproduce aggregate detection without a grouping declaration. |
