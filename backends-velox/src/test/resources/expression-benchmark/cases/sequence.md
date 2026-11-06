# sequence

Inputs are materialized before timing. Integer cases use changing values rather than fully constant expressions. nullableBoundedInt uses the same bounded values as standard.boundedInt, with a deterministic NULL mask controlled by nullPercent.

The DATE case is a capability candidate. Check actual Native support before timing; never report Spark fallback as Native execution.

| case | inputs | expression | description |
| --- | --- | --- | --- |
| bounded-int | `input = standard.boundedInt()` | `sequence(input, input + 3)` | Existing INTEGER baseline: four ascending elements. |
| single-int | `input = standard.boundedInt()` | `sequence(input, input)` | One INTEGER element per row. |
| wide-int | `input = standard.boundedInt()` | `sequence(input, input + 63)` | Sixty-four ascending INTEGER elements. |
| descending-int | `input = standard.boundedInt()` | `sequence(input + 3, input)` | Four INTEGER elements with an implicit negative step. |
| step-int | `input = standard.boundedInt()` | `sequence(input, input + 6, 2)` | Four INTEGER elements with explicit step two. |
| bounded-long | `input = standard.long()` | `sequence(input, input + CAST(3 AS BIGINT))` | Four ascending BIGINT elements. |
| nullable-int | `start = nullableBoundedInt(nullPercent=50); stop = standard.boundedInt()` | `sequence(start, stop + 3)` | NULL start on about half the rows; other rows produce four INTEGER elements. |
| date-days | `input = standard.date()` | `sequence(input, date_add(input, 3))` | Four DATE values with an implicit one-day step; Native support must be verified. |
