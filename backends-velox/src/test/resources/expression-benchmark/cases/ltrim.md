# ltrim

| case | inputs | expression | description |
| --- | --- | --- | --- |
| standard-string | `input = standard.string(length = 10)` | `ltrim(input)` | Calibration report R:656; projected standard inputs from CalibrationDataGenerator. |
| l10-none | `input = ltrimPattern(length = 10, pattern = "none")` | `ltrim(input)` | Legacy ltrim; 10-byte ASCII; no NULLs. No spaces to trim. |
| l10-first | `input = ltrimPattern(length = 10, pattern = "first")` | `ltrim(input)` | Legacy ltrim; 10-byte ASCII; no NULLs. Two spaces on the first row of each batch. |
| l10-cluster | `input = ltrimPattern(length = 10, pattern = "cluster")` | `ltrim(input)` | Legacy ltrim; 10-byte ASCII; no NULLs. Two spaces on the first min(16, count) rows of each batch. |
| l10-half-even | `input = ltrimPattern(length = 10, pattern = "half-even")` | `ltrim(input)` | Legacy ltrim; 10-byte ASCII; no NULLs. Two spaces on even local row indexes in each batch. |
| l10-last | `input = ltrimPattern(length = 10, pattern = "last")` | `ltrim(input)` | Legacy ltrim; 10-byte ASCII; no NULLs. Two spaces on the last row of each actual batch, including partial batches. |
| l10-penultimate | `input = ltrimPattern(length = 10, pattern = "penultimate")` | `ltrim(input)` | Legacy ltrim; 10-byte ASCII; no NULLs. Swap the last two whole rows of the last pattern; singleton stays unchanged. |
| l10-all | `input = ltrimPattern(length = 10, pattern = "all")` | `ltrim(input)` | Legacy ltrim; 10-byte ASCII; no NULLs. Two spaces on every row. |
| l12-none | `input = ltrimPattern(length = 12, pattern = "none")` | `ltrim(input)` | Legacy ltrim; 12-byte ASCII; no NULLs. No spaces to trim. |
| l12-first | `input = ltrimPattern(length = 12, pattern = "first")` | `ltrim(input)` | Legacy ltrim; 12-byte ASCII; no NULLs. Two spaces on the first row of each batch. |
| l12-cluster | `input = ltrimPattern(length = 12, pattern = "cluster")` | `ltrim(input)` | Legacy ltrim; 12-byte ASCII; no NULLs. Two spaces on the first min(16, count) rows of each batch. |
| l12-half-even | `input = ltrimPattern(length = 12, pattern = "half-even")` | `ltrim(input)` | Legacy ltrim; 12-byte ASCII; no NULLs. Two spaces on even local row indexes in each batch. |
| l12-last | `input = ltrimPattern(length = 12, pattern = "last")` | `ltrim(input)` | Legacy ltrim; 12-byte ASCII; no NULLs. Two spaces on the last row of each actual batch, including partial batches. |
| l12-penultimate | `input = ltrimPattern(length = 12, pattern = "penultimate")` | `ltrim(input)` | Legacy ltrim; 12-byte ASCII; no NULLs. Swap the last two whole rows of the last pattern; singleton stays unchanged. |
| l12-all | `input = ltrimPattern(length = 12, pattern = "all")` | `ltrim(input)` | Legacy ltrim; 12-byte ASCII; no NULLs. Two spaces on every row. |
| l13-none | `input = ltrimPattern(length = 13, pattern = "none")` | `ltrim(input)` | Legacy ltrim; 13-byte ASCII; no NULLs. No spaces to trim. |
| l13-first | `input = ltrimPattern(length = 13, pattern = "first")` | `ltrim(input)` | Legacy ltrim; 13-byte ASCII; no NULLs. Two spaces on the first row of each batch. |
| l13-cluster | `input = ltrimPattern(length = 13, pattern = "cluster")` | `ltrim(input)` | Legacy ltrim; 13-byte ASCII; no NULLs. Two spaces on the first min(16, count) rows of each batch. |
| l13-half-even | `input = ltrimPattern(length = 13, pattern = "half-even")` | `ltrim(input)` | Legacy ltrim; 13-byte ASCII; no NULLs. Two spaces on even local row indexes in each batch. |
| l13-last | `input = ltrimPattern(length = 13, pattern = "last")` | `ltrim(input)` | Legacy ltrim; 13-byte ASCII; no NULLs. Two spaces on the last row of each actual batch, including partial batches. |
| l13-penultimate | `input = ltrimPattern(length = 13, pattern = "penultimate")` | `ltrim(input)` | Legacy ltrim; 13-byte ASCII; no NULLs. Swap the last two whole rows of the last pattern; singleton stays unchanged. |
| l13-all | `input = ltrimPattern(length = 13, pattern = "all")` | `ltrim(input)` | Legacy ltrim; 13-byte ASCII; no NULLs. Two spaces on every row. |
| l64-none | `input = ltrimPattern(length = 64, pattern = "none")` | `ltrim(input)` | Legacy ltrim; 64-byte ASCII; no NULLs. No spaces to trim. |
| l64-first | `input = ltrimPattern(length = 64, pattern = "first")` | `ltrim(input)` | Legacy ltrim; 64-byte ASCII; no NULLs. Two spaces on the first row of each batch. |
| l64-cluster | `input = ltrimPattern(length = 64, pattern = "cluster")` | `ltrim(input)` | Legacy ltrim; 64-byte ASCII; no NULLs. Two spaces on the first min(16, count) rows of each batch. |
| l64-half-even | `input = ltrimPattern(length = 64, pattern = "half-even")` | `ltrim(input)` | Legacy ltrim; 64-byte ASCII; no NULLs. Two spaces on even local row indexes in each batch. |
| l64-last | `input = ltrimPattern(length = 64, pattern = "last")` | `ltrim(input)` | Legacy ltrim; 64-byte ASCII; no NULLs. Two spaces on the last row of each actual batch, including partial batches. |
| l64-penultimate | `input = ltrimPattern(length = 64, pattern = "penultimate")` | `ltrim(input)` | Legacy ltrim; 64-byte ASCII; no NULLs. Swap the last two whole rows of the last pattern; singleton stays unchanged. |
| l64-all | `input = ltrimPattern(length = 64, pattern = "all")` | `ltrim(input)` | Legacy ltrim; 64-byte ASCII; no NULLs. Two spaces on every row. |
| l256-none | `input = ltrimPattern(length = 256, pattern = "none")` | `ltrim(input)` | Legacy ltrim; 256-byte ASCII; no NULLs. No spaces to trim. |
| l256-first | `input = ltrimPattern(length = 256, pattern = "first")` | `ltrim(input)` | Legacy ltrim; 256-byte ASCII; no NULLs. Two spaces on the first row of each batch. |
| l256-cluster | `input = ltrimPattern(length = 256, pattern = "cluster")` | `ltrim(input)` | Legacy ltrim; 256-byte ASCII; no NULLs. Two spaces on the first min(16, count) rows of each batch. |
| l256-half-even | `input = ltrimPattern(length = 256, pattern = "half-even")` | `ltrim(input)` | Legacy ltrim; 256-byte ASCII; no NULLs. Two spaces on even local row indexes in each batch. |
| l256-last | `input = ltrimPattern(length = 256, pattern = "last")` | `ltrim(input)` | Legacy ltrim; 256-byte ASCII; no NULLs. Two spaces on the last row of each actual batch, including partial batches. |
| l256-penultimate | `input = ltrimPattern(length = 256, pattern = "penultimate")` | `ltrim(input)` | Legacy ltrim; 256-byte ASCII; no NULLs. Swap the last two whole rows of the last pattern; singleton stays unchanged. |
| l256-all | `input = ltrimPattern(length = 256, pattern = "all")` | `ltrim(input)` | Legacy ltrim; 256-byte ASCII; no NULLs. Two spaces on every row. |
| l64-null50 | `input = ltrimPattern(length = 64, pattern = "half-even", nullPercent = 50)` | `ltrim(input)` | Legacy ltrim; 64-byte ASCII; 50% NULL mask in the source-index 200-row cycle; independent local half-even selection. |
| l64-utf8-half-even | `input = ltrimPattern(length = 64, pattern = "half-even", utf8 = true)` | `ltrim(input)` | Legacy ltrim; 64-byte mixed ASCII and three-byte UTF8; no NULLs; local half-even selection. |
| identity-l10 | `input = ltrimPattern(length = 10, pattern = "none")` | `input` | Legacy ltrim; identity projection of 10-byte no-trim inputs. |
| identity-l256 | `input = ltrimPattern(length = 256, pattern = "none")` | `input` | Legacy ltrim; identity projection of 256-byte no-trim inputs. |
| trailing-boundary | `input = ltrimBoundary()` | `ltrim(input)` | Legacy ltrim; preserve two trailing spaces; 1% empty strings in a seeded 1000-row cycle. |
| custom | `(input, trimChars) = ltrimCustom()` | `TRIM(LEADING trimChars FROM input)` | Legacy ltrim; seeded six-row cycle; input NULL at 0 and trimChars NULL at 1; preserve the opposite end. |
