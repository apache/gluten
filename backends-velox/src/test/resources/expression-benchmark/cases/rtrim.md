# rtrim

| case | inputs | expression | description |
| --- | --- | --- | --- |
| standard-string | `input = standard.string(length = 10)` | `rtrim(input)` | Calibration report R:648; projected standard inputs from CalibrationDataGenerator. |
| l10-none | `input = rtrimPattern(length = 10, pattern = "none")` | `rtrim(input)` | Legacy rtrim; 10-byte ASCII; no NULLs. No spaces to trim. |
| l10-first | `input = rtrimPattern(length = 10, pattern = "first")` | `rtrim(input)` | Legacy rtrim; 10-byte ASCII; no NULLs. Two spaces on the first row of each batch. |
| l10-cluster | `input = rtrimPattern(length = 10, pattern = "cluster")` | `rtrim(input)` | Legacy rtrim; 10-byte ASCII; no NULLs. Two spaces on the first min(16, count) rows of each batch. |
| l10-half-even | `input = rtrimPattern(length = 10, pattern = "half-even")` | `rtrim(input)` | Legacy rtrim; 10-byte ASCII; no NULLs. Two spaces on even local row indexes in each batch. |
| l10-last | `input = rtrimPattern(length = 10, pattern = "last")` | `rtrim(input)` | Legacy rtrim; 10-byte ASCII; no NULLs. Two spaces on the last row of each actual batch, including partial batches. |
| l10-penultimate | `input = rtrimPattern(length = 10, pattern = "penultimate")` | `rtrim(input)` | Legacy rtrim; 10-byte ASCII; no NULLs. Swap the last two whole rows of the last pattern; singleton stays unchanged. |
| l10-all | `input = rtrimPattern(length = 10, pattern = "all")` | `rtrim(input)` | Legacy rtrim; 10-byte ASCII; no NULLs. Two spaces on every row. |
| l12-none | `input = rtrimPattern(length = 12, pattern = "none")` | `rtrim(input)` | Legacy rtrim; 12-byte ASCII; no NULLs. No spaces to trim. |
| l12-first | `input = rtrimPattern(length = 12, pattern = "first")` | `rtrim(input)` | Legacy rtrim; 12-byte ASCII; no NULLs. Two spaces on the first row of each batch. |
| l12-cluster | `input = rtrimPattern(length = 12, pattern = "cluster")` | `rtrim(input)` | Legacy rtrim; 12-byte ASCII; no NULLs. Two spaces on the first min(16, count) rows of each batch. |
| l12-half-even | `input = rtrimPattern(length = 12, pattern = "half-even")` | `rtrim(input)` | Legacy rtrim; 12-byte ASCII; no NULLs. Two spaces on even local row indexes in each batch. |
| l12-last | `input = rtrimPattern(length = 12, pattern = "last")` | `rtrim(input)` | Legacy rtrim; 12-byte ASCII; no NULLs. Two spaces on the last row of each actual batch, including partial batches. |
| l12-penultimate | `input = rtrimPattern(length = 12, pattern = "penultimate")` | `rtrim(input)` | Legacy rtrim; 12-byte ASCII; no NULLs. Swap the last two whole rows of the last pattern; singleton stays unchanged. |
| l12-all | `input = rtrimPattern(length = 12, pattern = "all")` | `rtrim(input)` | Legacy rtrim; 12-byte ASCII; no NULLs. Two spaces on every row. |
| l13-none | `input = rtrimPattern(length = 13, pattern = "none")` | `rtrim(input)` | Legacy rtrim; 13-byte ASCII; no NULLs. No spaces to trim. |
| l13-first | `input = rtrimPattern(length = 13, pattern = "first")` | `rtrim(input)` | Legacy rtrim; 13-byte ASCII; no NULLs. Two spaces on the first row of each batch. |
| l13-cluster | `input = rtrimPattern(length = 13, pattern = "cluster")` | `rtrim(input)` | Legacy rtrim; 13-byte ASCII; no NULLs. Two spaces on the first min(16, count) rows of each batch. |
| l13-half-even | `input = rtrimPattern(length = 13, pattern = "half-even")` | `rtrim(input)` | Legacy rtrim; 13-byte ASCII; no NULLs. Two spaces on even local row indexes in each batch. |
| l13-last | `input = rtrimPattern(length = 13, pattern = "last")` | `rtrim(input)` | Legacy rtrim; 13-byte ASCII; no NULLs. Two spaces on the last row of each actual batch, including partial batches. |
| l13-penultimate | `input = rtrimPattern(length = 13, pattern = "penultimate")` | `rtrim(input)` | Legacy rtrim; 13-byte ASCII; no NULLs. Swap the last two whole rows of the last pattern; singleton stays unchanged. |
| l13-all | `input = rtrimPattern(length = 13, pattern = "all")` | `rtrim(input)` | Legacy rtrim; 13-byte ASCII; no NULLs. Two spaces on every row. |
| l64-none | `input = rtrimPattern(length = 64, pattern = "none")` | `rtrim(input)` | Legacy rtrim; 64-byte ASCII; no NULLs. No spaces to trim. |
| l64-first | `input = rtrimPattern(length = 64, pattern = "first")` | `rtrim(input)` | Legacy rtrim; 64-byte ASCII; no NULLs. Two spaces on the first row of each batch. |
| l64-cluster | `input = rtrimPattern(length = 64, pattern = "cluster")` | `rtrim(input)` | Legacy rtrim; 64-byte ASCII; no NULLs. Two spaces on the first min(16, count) rows of each batch. |
| l64-half-even | `input = rtrimPattern(length = 64, pattern = "half-even")` | `rtrim(input)` | Legacy rtrim; 64-byte ASCII; no NULLs. Two spaces on even local row indexes in each batch. |
| l64-last | `input = rtrimPattern(length = 64, pattern = "last")` | `rtrim(input)` | Legacy rtrim; 64-byte ASCII; no NULLs. Two spaces on the last row of each actual batch, including partial batches. |
| l64-penultimate | `input = rtrimPattern(length = 64, pattern = "penultimate")` | `rtrim(input)` | Legacy rtrim; 64-byte ASCII; no NULLs. Swap the last two whole rows of the last pattern; singleton stays unchanged. |
| l64-all | `input = rtrimPattern(length = 64, pattern = "all")` | `rtrim(input)` | Legacy rtrim; 64-byte ASCII; no NULLs. Two spaces on every row. |
| l256-none | `input = rtrimPattern(length = 256, pattern = "none")` | `rtrim(input)` | Legacy rtrim; 256-byte ASCII; no NULLs. No spaces to trim. |
| l256-first | `input = rtrimPattern(length = 256, pattern = "first")` | `rtrim(input)` | Legacy rtrim; 256-byte ASCII; no NULLs. Two spaces on the first row of each batch. |
| l256-cluster | `input = rtrimPattern(length = 256, pattern = "cluster")` | `rtrim(input)` | Legacy rtrim; 256-byte ASCII; no NULLs. Two spaces on the first min(16, count) rows of each batch. |
| l256-half-even | `input = rtrimPattern(length = 256, pattern = "half-even")` | `rtrim(input)` | Legacy rtrim; 256-byte ASCII; no NULLs. Two spaces on even local row indexes in each batch. |
| l256-last | `input = rtrimPattern(length = 256, pattern = "last")` | `rtrim(input)` | Legacy rtrim; 256-byte ASCII; no NULLs. Two spaces on the last row of each actual batch, including partial batches. |
| l256-penultimate | `input = rtrimPattern(length = 256, pattern = "penultimate")` | `rtrim(input)` | Legacy rtrim; 256-byte ASCII; no NULLs. Swap the last two whole rows of the last pattern; singleton stays unchanged. |
| l256-all | `input = rtrimPattern(length = 256, pattern = "all")` | `rtrim(input)` | Legacy rtrim; 256-byte ASCII; no NULLs. Two spaces on every row. |
| l64-null50 | `input = rtrimPattern(length = 64, pattern = "half-even", nullPercent = 50)` | `rtrim(input)` | Legacy rtrim; 64-byte ASCII; 50% NULL mask in the source-index 200-row cycle; independent local half-even selection. |
| l64-utf8-half-even | `input = rtrimPattern(length = 64, pattern = "half-even", utf8 = true)` | `rtrim(input)` | Legacy rtrim; 64-byte mixed ASCII and three-byte UTF8; no NULLs; local half-even selection. |
| identity-l10 | `input = rtrimPattern(length = 10, pattern = "none")` | `input` | Legacy rtrim; identity projection of 10-byte no-trim inputs. |
| identity-l256 | `input = rtrimPattern(length = 256, pattern = "none")` | `input` | Legacy rtrim; identity projection of 256-byte no-trim inputs. |
| leading-boundary | `input = rtrimBoundary()` | `rtrim(input)` | Legacy rtrim; preserve two leading spaces; 1% empty strings in a seeded 1000-row cycle. |
| custom | `(input, trimChars) = rtrimCustom()` | `TRIM(TRAILING trimChars FROM input)` | Legacy rtrim; seeded six-row cycle; input NULL at 0 and trimChars NULL at 1; preserve the opposite end. |
