# right

| case | inputs | expression | description |
| --- | --- | --- | --- |
| text | `input = standard.text()` | `right(input, 3)` | Take the last three characters of prepared UTF-8 text using Spark's replacement. |
| nullable-utf8 | `input = rtrimPattern(length=13, pattern="none", nullPercent=50, utf8=true)` | `right(input, 3)` | Character-based suffix extraction with deterministic 50-percent NULL inputs. |
