# to_date

| case | inputs | expression | description |
| --- | --- | --- | --- |
| date-string | `input = standard.dateString()` | `to_date(input)` | Parse prepared ISO date strings through Spark's DATE cast replacement. |
| formatted-date-string | `input = standard.dateString()` | `to_date(input, 'yyyy-MM-dd')` | Parse an explicit date format through Spark's timestamp-and-cast replacement. |
