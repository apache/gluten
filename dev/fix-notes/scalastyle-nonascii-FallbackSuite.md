# Fix: Scalastyle `nonascii` violations in `backends-velox`

## What broke

CI failed with:

```
error file=...FallbackSuite.scala message=nonascii.message line=408 column=3
Found 1 errors
[ERROR] Failed to execute goal org.scalastyle:scalastyle-maven-plugin:1.0.0:check (default)
        on project backends-velox: You have 1 Scalastyle violation(s).
```

A second run (after the first fix) surfaced a second violation:

```
[ERROR] Failed to execute goal org.scalastyle:scalastyle-maven-plugin:1.0.0:check (default)
        on project backends-velox: You have 1 Scalastyle violation(s).
```

## Root cause

Gluten's Scalastyle configuration enables the `NonASCIICharacters` rule
(`nonascii.message`), which rejects any byte above 0x7F — including multi-byte
UTF-8 sequences in comments — unless the offending line is wrapped in a
`// scalastyle:off nonascii` / `// scalastyle:on` guard.

Two separate commits introduced non-ASCII characters in `backends-velox`:

### Violation 1 — `FallbackSuite.scala` (Unicode em-dashes)

| Line | Character | UTF-8 bytes |
|------|-----------|-------------|
| 412  | `—` (U+2014, em-dash) | `\xe2\x80\x94` |
| 444  | `—` (U+2014, em-dash) | `\xe2\x80\x94` |

Both appeared in newly-added comments.

### Violation 2 — `ColumnarPartialProjectExec.scala` (Chinese full-stop)

| Line | Character | UTF-8 bytes |
|------|-----------|-------------|
| 479  | `。` (U+3002, ideographic full stop) | `\xe3\x80\x82` |

A comment ended with a Chinese full-stop instead of an ASCII period.

## Fixes applied

### Fix 1 — `FallbackSuite.scala`

Replaced both em-dashes with `--` (ASCII double-hyphen):

```scala
// Before
// translated to RE2 \x{XXXX} syntax and run natively — no rlike regex fallback should occur.
// Execute a query with gluten disabled — this mimics what runQueryAndCompare does for

// After
// translated to RE2 \x{XXXX} syntax and run natively -- no rlike regex fallback should occur.
// Execute a query with gluten disabled -- this mimics what runQueryAndCompare does for
```

**File:** `backends-velox/src/test/scala/org/apache/gluten/execution/FallbackSuite.scala`

### Fix 2 — `ColumnarPartialProjectExec.scala`

Replaced the Chinese full-stop with an ASCII period:

```scala
// Before
// expression to find which expression the native backend does not support。

// After
// expression to find which expression the native backend does not support.
```

**File:** `backends-velox/src/main/scala/org/apache/gluten/execution/ColumnarPartialProjectExec.scala`

## Note: `MiscOperatorSuite.scala` is fine

`MiscOperatorSuite.scala` intentionally uses non-ASCII column names (Cyrillic,
Chinese) in test data. Those lines are already wrapped in correct suppression
guards and are not a violation:

```scala
// scalastyle:off nonascii
Seq((1, 2, 3, 4)).toDF("товары", "овары", "国ⅵ", "中文").write.parquet(...)
// scalastyle:on
```

## Unrelated noise in the CI log

The `dev/__pycache__/util.cpython-314.pyc` file visible locally is Python
bytecode cached when a dev script was run. It is gitignored and has no effect
on the Maven/Scalastyle build.

## Prevention

Before committing any Scala file, run:

```bash
python3 -c "
import os, sys

def check(path):
    suppressed = False
    with open(path, 'rb') as f:
        for i, line in enumerate(f, 1):
            text = line.decode('utf-8', errors='replace')
            if 'scalastyle:off' in text and 'nonascii' in text:
                suppressed = True
            if 'scalastyle:on' in text:
                suppressed = False
            if not suppressed and any(b > 127 for b in line):
                print(f'{path}:{i}: {line!r}')

for root, _, files in os.walk(sys.argv[1]):
    for f in files:
        if f.endswith('.scala'):
            check(os.path.join(root, f))
" backends-velox/src
```

Alternatively, configure your editor to highlight non-ASCII characters in
Scala source files, or add a pre-commit hook that runs the snippet above.
