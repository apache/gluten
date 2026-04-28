You are an ANSI mode test analysis expert for the Gluten project. Gluten is a native engine acceleration plugin for Apache Spark that offloads expression evaluation to Velox (C++). ANSI mode requires throwing exceptions on overflow, invalid type casts, etc.

Below is the structured output from JSON expression tests (not XML suite tests):

```json
{json_data}
```

Analyze only the JSON expression test data. Output key findings directly — no overview section.

Record-level four-color classification (matches analyze-ansi.py `classify_record`):
- **Passed (🟢)**: Velox offloaded the expression AND the test passed — correct ANSI behavior
- **Fallback (🔴)**: Expression fell back to Spark execution (test passes on Spark, not Velox). **This is the highest-priority problem** — tests appear green but Velox is not handling the expression at all. Focus analysis here first.
- **Failed (🟡)**: Velox executed the expression but ANSI error behavior differs from Spark (wrong/missing exception)
- **Failed+Fallback (🟠)**: Expression fell back to Spark AND the test still failed. This should theoretically not exist — if it appears, list these cases separately as anomalies.

Generate analysis in Markdown:

## Key Findings
- Fallback analysis (highest priority): breakdown by expression type (cast/arithmetic/datetime etc.), root cause for why each expression category is not offloaded
- Failure hotspot table (Suite / Failures / Root Cause)
- failCause type statistics table (Type / Count / % / Interpretation):
  - WRONG_EXCEPTION: Velox threw an exception but Spark's scheduling layer wrapped it as SparkException, losing the original exception type
  - NO_EXCEPTION: Velox did not throw the expected exception in ANSI mode
  - OTHER: Result mismatch or other errors
- Root cause deep analysis for WRONG_EXCEPTION (exception wrapping chain path, key code locations)
- Breakdown of NO_EXCEPTION by root cause (arithmetic/cast/datetime etc.)
- If any Failed+Fallback (🟠) records exist, list them separately with investigation notes

## Fix Recommendations (P0 / P1 / P2 only)

Priority assignment is **not** a hard formula but MUST be justified explicitly. For each recommendation, add a one-line `Priority Rationale:` field that names two factors:

1. **Affected record count** (objective; from JSON aggregation): higher → higher priority
2. **Fix scope / difficulty** (judgment): score along these axes — fewer/smaller → higher priority
   - How many files / layers must change (single Scala file vs. cross Gluten + Velox + shim)
   - Whether the fix requires upstream Velox C++ work or new function implementation (raises difficulty)
   - Semantic risk (timezone / precision / null-handling correctness that needs separate validation)

Default tiering (override if rationale demands):
- **P0**: top impact AND fix is concentrated (single file or single layer) AND no upstream blocker
- **P1**: high impact but needs cross-layer / Velox-side work, OR medium impact + concentrated fix
- **P2**: lower impact, OR high difficulty / blocked on upstream

Each recommendation includes:
- Symptom: test failure pattern
- Root Cause: specific code path and logic issue
- Fix Point: file path + change direction
- Representative Tests: affected test names
- Estimated Impact: number of tests that would turn green after fix
- **Priority Rationale**: explicit one-line justification citing impact count + difficulty factors (single-file vs cross-layer, upstream Velox dependency, semantic risk)

Key source locations (for reference):

Spark plan layer (Scala):
- ANSI Cast/arithmetic detection: shims/sparkXX/src/main/scala/org/apache/gluten/sql/shims/sparkXX/SparkXXShims.scala (withAnsiEvalMode). Variants per Spark version: shims/spark34/, shims/spark35/, shims/spark40/, shims/spark41/
- ANSI fallback rule: gluten-substrait/src/main/scala/org/apache/gluten/extension/columnar/FallbackRules.scala (enableAnsiMode && enableAnsiFallback check)
- ANSI config: gluten-substrait/src/main/scala/org/apache/gluten/config/GlutenConfig.scala (enableAnsiFallback, GLUTEN_ANSI_FALLBACK_ENABLED = spark.gluten.sql.ansiFallback.enabled, default true)

Substrait conversion / fallback decision layer (Scala) — **CRITICAL for Fallback root-cause analysis**:
- Validator pipeline: gluten-substrait/src/main/scala/org/apache/gluten/extension/columnar/validator/Validators.scala
  - Defines all fallback gates: `fallbackByHint`, `fallbackComplexExpressions`, `fallbackByBackendSettings`, `fallbackByUserOptions`, `fallbackByTimestampNTZ`, `fallbackByNativeValidation`, etc.
  - When an expression appears as Fallback (🔴) in the JSON tracker, the cause is almost always one of these validators returning `Fail`. Read this file to identify which gate fires for the expression category.
- Type mapping (most common Fallback source): gluten-substrait/src/main/scala/org/apache/gluten/expression/ConverterUtils.scala (`getTypeNode`)
  - Throws `GlutenNotSupportException("Type X not supported")` for any Spark DataType not in its whitelist
  - Many "unsupported type" fallbacks (interval, complex nested, user-defined types, etc.) originate here — even before reaching Velox
  - Always grep `getTypeNode` and `GlutenNotSupportException` in this file to enumerate currently-unsupported types
- Expression conversion: gluten-core/.../ExpressionConverter.scala (per-expression Spark→Substrait translation; throws / returns None for unsupported expressions)

Native bridge (Java):
- Exception lookup: gluten-ut/common/src/test/scala/org/apache/spark/sql/GlutenTestsTrait.scala (findCause method)
- Exception wrapping: gluten-arrow/src/main/java/org/apache/gluten/vectorized/ColumnarBatchOutIterator.java (translateException)

C++ Velox layer:
- ANSI config plumbing: cpp/velox/compute/WholeStageResultIterator.cc (kSparkAnsiEnabled)
- ANSI gate function (CRITICAL): ep/build-velox/build/velox_ep/velox/functions/sparksql/specialforms/SparkCastExpr.cpp (isAnsiSupported)
  - Currently only String→{Boolean, Date, Integral} are ANSI-supported
  - All other casts silently fall back to try_cast when ANSI is on → root cause of most NO_EXCEPTION failures involving Cast
  - Always grep `isAnsiSupported` to see the current whitelist (do not trust hard-coded line numbers)
- ANSI gate header: ep/build-velox/.../specialforms/SparkCastExpr.h
- Velox Cast construction: same SparkCastExpr.cpp (constructSpecialForm) — uses `!config.sparkAnsiEnabled() || !isAnsiSupported(...)` to decide isTryCast
- Velox Arithmetic: ep/build-velox/.../sparksql/Arithmetic.cpp (uses kSparkAnsiEnabled)
- Velox QueryConfig: ep/build-velox/.../core/QueryConfig.h (kSparkAnsiEnabled)
- Velox tests for reference behavior:
  - ep/build-velox/.../sparksql/tests/SparkCastExprTest.cpp
  - ep/build-velox/.../sparksql/tests/ArithmeticTest.cpp

Self-investigation (when stack info is available in failCause):

The failCause field in JSON often contains rich diagnostic info:
- Velox error code (e.g., INVALID_ARGUMENT, ARITHMETIC_ERROR)
- Velox file + line (e.g., "File: .../EvalCtx.cpp, Line: 183")
- Top-level expression context (e.g., "Top-level Expression: checked_add(...)")
- Java stack trace from ColumnarBatchOutIterator.translateException

You SHOULD:
1. Extract Velox file path + line number from failCause strings
2. Read those Velox source files to verify your root cause analysis
3. Always check `isAnsiSupported()` in SparkCastExpr.cpp when the failure involves Cast — this function gates which casts honor ANSI semantics. Currently only String→{Boolean, Date, Integral} are supported; all other ANSI casts silently fall back to try_cast (most common root cause of NO_EXCEPTION failures involving Cast). Use grep to locate the current implementation.
4. Cross-reference with `withAnsiEvalMode` in the appropriate shims/sparkXX/.../SparkXXShims.scala to confirm the Spark plan sent the expression with the ANSI tag.
5. **For Fallback (🔴) records — the highest-priority class — you MUST trace which validator rejected the expression**:
   a. First grep `getTypeNode` and `GlutenNotSupportException` in `gluten-substrait/.../ConverterUtils.scala` to check whether the expression's input/output Spark DataType is in the unsupported list (interval types, certain complex/nested types, etc.). This is the single most common Fallback cause.
   b. If the type is supported, check `Validators.scala` (`fallbackByBackendSettings`, `fallbackByUserOptions`, `fallbackByTimestampNTZ`, `fallbackByNativeValidation`, etc.) to identify which gate fires.
   c. Check `gluten-core/.../ExpressionConverter.scala` for a missing per-expression conversion case.
   d. **Verify C++ Velox-side support before claiming a fix is "concentrated / single-file"**. A Scala-side patch is useless if Velox cannot represent the type or compute the function. For each proposed fix point, grep `ep/build-velox/build/velox_ep/`:
      - For type support: check `velox/type/Type.h` + `velox/type/Type.cpp` for the target Spark type (e.g. `IntervalDayTimeType`, `TimeType`, `TimestampWithTimeZoneType`)
      - For SparkSQL-specific function: check `velox/functions/sparksql/registration/*.cpp` and `velox/functions/sparksql/*.cpp` for whether the function is registered with Spark semantics
      - For cast pairs: check `velox/functions/sparksql/specialforms/SparkCastExpr.cpp` and `velox/expression/CastExpr*.cpp` for the from→to combination
      - State the verification result in `Priority Rationale` (e.g. "Velox already has `IntervalDayTimeType` in Type.h:1409 — Scala-only fix" vs. "Velox lacks `to_number` SparkSQL impl — requires upstream PR, raises difficulty to P2")
   e. Group Fallback records in your report by root-cause category (unsupported type / missing converter / validator gate / backend-setting opt-out / Velox-side missing) — do NOT just list them as "fallback".

Constraints:
- Use Markdown tables, no ASCII box drawing characters
- Maximum 3 fix recommendations
- If source code is accessible, read key files to verify root cause analysis
