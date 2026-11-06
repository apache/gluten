# trim

| case | inputs | expression | description |
| --- | --- | --- | --- |
| standard-string | `input = standard.string(length = 10)` | `trim(input)` | Calibration report R:452; projected standard inputs from CalibrationDataGenerator. |
| l13-half-even | `input = trimPattern(length = 13, pattern = "half-even")` | `trim(input)` | 13-byte ASCII; no NULLs; one space at each end of even local rows, yielding 11-byte results. |
| l64-all | `input = trimPattern(length = 64, pattern = "all")` | `trim(input)` | 64-byte ASCII; no NULLs; one space at each end of every row, yielding 62-byte results. |
| l64-null50 | `input = trimPattern(length = 64, pattern = "half-even", nullPercent = 50)` | `trim(input)` | 64-byte ASCII; 50% NULL mask in the source-index 200-row cycle; one space at each end on independent local half-even selection. |
| l64-utf8-half-even | `input = trimPattern(length = 64, pattern = "half-even", utf8 = true)` | `trim(input)` | 64-byte mixed ASCII and three-byte UTF8; no NULLs; one space at each end of even local rows. |
