---
layout: page
title: Updating the Function Support Docs
nav_order: 20
parent: Developer Overview
---
# Updating the Function Support Docs for a New Spark Version

The generator `tools/scripts/gen-function-support-docs.py` runs Gluten's function test suites for the newest
supported Spark version, parses the fallback reasons from the test log, and rewrites
`docs/velox-backend-{scalar,aggregate,window,generator}-function-support.md`. Everything Spark-version-specific
is listed below. Read `docs/velox-backend-support-progress.md` ("Function Support Status") first.

Throughout, `X.Y` is the new Spark version (e.g. `4.2`), `XY` its plain form (`42`), and `OLD` the previous one.

## 1. Prerequisites

1. Gluten must already have the shim and UT module for the version: `shims/sparkXY`, `gluten-ut/sparkXY`, and a
   `spark-X.Y` profile in `pom.xml` (read `<spark.version>` there, e.g. `4.2.0`; build exactly that tag).
2. A machine with the native libs built (`cpp/build/releases/libgluten.so`, `libvelox.so`), JDK 17, Maven, Python 3
   with `pip install findspark tabulate black`.
3. Spark source of that exact tag, built: `./build/mvn -DskipTests -Pyarn -Phive -Phive-thriftserver clean install`
   (~30 min). Spark 3.x `build/mvn` downloads Scala from downloads.lightbend.com, which can 403; pre-extract
   `scala-<ver>.tgz` from GitHub releases into `spark/build/` if so. Spark 4.x does not need that.
4. Gluten built for the version so `package/target/gluten-package-<gluten-version>.jar` exists:
   `./build/mvn clean install -Pbackends-velox -Pspark-X.Y -Pjava-17 -Pscala-2.13 -Pspark-ut -DskipTests -Dmaven.compiler.release=17`
   (Spark 3.x: `-Pspark-3.5` only, Scala 2.12 is the default).
5. In a shared container the checkout may be root-owned: `chown -R <user> /path/to/gluten` and
   `git config --global --add safe.directory /path/to/gluten`, otherwise Maven dies at
   `target/maven-shared-archive-resources`.

## 2. Update the script

All edits are in `tools/scripts/gen-function-support-docs.py`.

1. **FunctionRegistry literal.** Replace the body of `SPARK<OLD>_EXPRESSION_MAPPINGS` with the contents of the
   `val expressions: Map[String, (ExpressionInfo, FunctionBuilder)] = Map(` block from
   `sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/FunctionRegistry.scala` of the new tag
   (the lines strictly between `Map(` and its closing `  )`), and rename the constant to `SPARKXY_...` everywhere.
   Verify the method first by extracting the OLD block from the OLD tag and diffing against the current literal;
   it must be identical.
2. **New registry entry forms.** The parser handles `expression[...]`, `expressionBuilder(...)`, generator variants,
   two-line `expressionBuilder(\n "name", Builder, ...)`, and `<Class>.registryEntry`. For every new
   `<Class>.registryEntry` add the class → function name to `REGISTRY_ENTRY_FUNCTIONS` (look up the name in the
   class's `registryEntry` in Spark source). Then run the offline parser check in section 5 and make sure there are
   no `Could not parse expression` warnings.
3. **ExpressionBuilders.** Spark reports the builder class as the function's class, Gluten's `ExpressionMappings`
   are keyed by the expression classes the builder constructs. `expression_classes()` strips the
   `ExpressionBuilder`/`Builder` suffix; builders whose name does not match the class they build must be added to
   `EXPRESSION_BUILDER_CLASSES`. For every new `object *Builder` in Spark's catalyst sources, read its `build` method
   and check whether suffix-stripping yields a class that exists in `gluten-substrait/.../ExpressionMappings.scala`
   (or the Velox backend's extra `Sig[...]`); if not, add a table entry. Missing this marks the function unsupported
   with no test involved (happened to `hour`/`minute`/`second`, `date_part`, `make_timestamp_*`).
4. **Function groups.** `grep -rhoE 'group = "[a-z_]+"' sql/catalyst/src/main sql/core/src/main | sort -u` in the
   Spark source; add new scalar groups to both `spark_function_missing_groups` and `SCALAR_FUNCTION_GROUPS` with a
   section title. Groups not listed are silently dropped from the docs. `table_funcs` is intentionally excluded.
5. **Maven profiles** in `run_test_suites`: `-Pspark-X.Y`; for Spark 4.x keep `-Pjava-17 -Pscala-2.13
   -Dmaven.compiler.release=17`.
6. **Log path**: `TEST_LOG_FILE` must point at `gluten-ut/sparkXY/target/gen-function-support-docs-tests.log`.
7. **Doc header**: the f-string `... functions in Spark X.Y, Gluten currently ...`.
8. **Extra SQL query tests** (`EXTRA_SQL_QUERY_TESTS`): files that
   `gluten-ut/sparkXY/.../VeloxSQLQueryTestSettings.scala` leaves out of CI but that exercise functions. Recompute
   the gap (section 4) and adjust the list; files that Spark renamed or split (e.g. `group-by.sql` → `mode.sql`)
   are the usual suspects.
9. **Test suites** (`FUNCTION_SUITES`): confirm every suite exists under
   `gluten-ut/sparkXY/src/test/scala/org/apache/spark/sql/`.
9b. **Status tables that need review per Spark version**:
   - `UNSUPPORTED_STATIC_INVOKES`: Spark keeps turning functions into `StaticInvoke` calls (RuntimeReplaceable
     with a StaticInvoke replacement). Gluten logs `Not supported to transform StaticInvoke with object: X,
     function: m` without the function name. The run warns `StaticInvoke not listed in UNSUPPORTED_STATIC_INVOKES`
     for new pairs; find the owning Spark expression in the catalyst sources and add the pair.
   - `REPLACED_FUNCTIONS`: RuntimeReplaceable functions whose replacement is another documented function
     (median/percentile_cont -> percentile, zeroifnull -> coalesce). New RuntimeReplaceable classes:
     `grep -rhozE "case class [A-Za-z0-9]+\([^)]*\)[^{]*RuntimeReplaceable" sql/catalyst/src/main/scala`.
   - Cast aliases (`castAlias("time", TimeType())` in the registry literal) are marked unsupported when the log
     reports `Type <T> not supported` / `data type not supported: <T>`; nothing to maintain, but check the
     `Cast alias ...` warnings in the run output.
   - `Not supported to transform Invoke with function: invoke(<X>Evaluator(` is mapped back to the function via
     the `<X>` class name (parse_url, schema_of_json, xpath_*). Other Invoke targets stay unresolved.
   - `ANALYZER_RESOLVED_FUNCTIONS`: resolved away by the analyzer (`grouping`, `grouping_id`).
10. `docs/velox-backend-support-progress.md`: `spark_version=` and the `shims/sparkXY/spark_home` path.
11. Format with `python3 -m black tools/scripts/gen-function-support-docs.py` (CI runs black via
    `dev/check.py format`).

## 3. Update the Gluten SQL query test suite for the new version

`gluten-ut/sparkXY/src/test/scala/org/apache/spark/sql/GlutenSQLQueryTestSuite.scala` must carry the two
doc-generation hooks that spark40/spark41 have (copy them from spark41 if the new suite was copied from Spark):

- Regular test cases: `SQLConf.ANSI_ENABLED` set from
  `!sys.props.get("gluten.test.sqlQueryTestSuite.ansiEnabled").contains("false")` instead of a hard `true`.
  Spark 4.x golden files assume ANSI on and the main CI sets `SPARK_ANSI_SQL_MODE=false`, so the suite must NOT
  honor that env var; only the doc run flips the property. Without it, Gluten falls back every plan
  ("does not support ansi mode") and every function looks supported.
- `createScalaTestCase` also accepts names from the `gluten.test.sqlQueryTestSuite.extraTests` system property.
  The field must be a `lazy val`: the constructor creates the test cases before later vals are initialized (NPE).
- Scalastyle limits lines to 100 chars; the Maven run fails before any test if violated.

## 4. Check test coverage against the previous version

Before trusting the output, compare which SQL files and how many tests ran for OLD vs new. From the console
output of each run (ScalaTest prints `- <name>` per test; `*** FAILED ***` tests still count as run):

```bash
ran() { grep -E "^- [^ ]+\.sql( \([0-9]|$| \*\*\* FAILED)" "$1" | sed -E 's/^- //; s/ \(.*//; s/ \*\*\*.*//' | sort -u; }
comm -23 <(ran old-run.log) <(ran new-run.log)   # ran on OLD, not on new
```

For each missing file: does it exist in the new Spark's `sql-tests/inputs`? Is it in the new
`VeloxSQLQueryTestSettings` list? Does it carry function coverage (grep the function names)? If yes to the first
and no to the second, add it to `EXTRA_SQL_QUERY_TESTS`. Also compare per-suite counts for the DataFrame suites.
Files `ansi/*.sql` became `nonansi/*.sql` in Spark 4.x; those are not a gap.

## 5. Run and verify

```bash
python3 tools/scripts/gen-function-support-docs.py --spark_home=/path/to/spark-source   # ~20 min of tests
python3 tools/scripts/gen-function-support-docs.py --spark_home=... --skip_test_suite   # reuse the log, ~1 min
```

Offline parser check (no JVM needed; `tabulate` must be importable):

```python
src = open("tools/scripts/gen-function-support-docs.py").read()
head = src[:src.index("def generate_function_list():")].replace("import findspark\n", "")
ns = {}; exec(head, ns); m = ns["create_spark_function_map"](); print(len(m), m.get("when"), m.get("hour"))
```

Then check, in order:

1. Console log: no `*** ABORTED ***`, no Scalastyle errors, no `Could not parse expression`, the extra SQL files
   appear as `- <file>.sql` (failing is fine), and the `Tests: succeeded N, failed M` line looks like a full run
   (~1200 tests on 4.1). A run that finishes in a few minutes did not run the SQL suite.
2. Test log: `grep -c "does not support ansi mode"` should be far below the validation-line count
   (`grep -cE '^ - |^   \|- '`); tens of thousands of ANSI fallbacks with hundreds of validation lines means the
   ANSI property did not take effect.
3. `WARNING:root:Function not found in gluten expressions: ...`: review every name. Builder-registered functions
   that Gluten does map belong in `EXPRESSION_BUILDER_CLASSES`, not in this list.
4. `Number of unknown ... function` lists: internal expressions like `get_timestamp`, `pivotfirst`, `SQLKeywords`
   are normal; real functions there mean a naming mismatch.
5. Any function that flipped to supported while having zero mentions in the test log has no coverage and got the
   default "S". Call these out in the PR; they are not evidence.

Expect golden-file failures (dozens): regular SQL tests compare against ANSI-on goldens, and the extra files are
excluded from CI precisely because they fail. The script ignores results and only reads fallback reasons.

## 5b. Audit the generated statuses

"S" only means no fallback was logged. Cross-check every S row against two independent sources before publishing:

1. **Velox registry**: collect the Spark-prefixed names Velox and Gluten register
   (`grep -rhoE 'prefix \+\s*"[a-z_0-9]+"' velox/velox/functions/sparksql cpp/velox`, plus Presto aggregate /
   window names from `prestosql/aggregates/AggregateNames.h` and `prestosql/window/WindowFunctionsRegistration.cpp`,
   plus `cpp/velox/substrait/SubstraitParser.cc`'s substrait->velox rename map). Map each doc row's class through
   `ExpressionMappings` (`Sig[Class](NAME)`, including `shims/*`) to its Velox name. An S row whose Velox name is
   registered nowhere is wrong unless the class is RuntimeReplaceable, a special form (cast, if, in, coalesce,
   named_struct/row_constructor, concat_ws, generators) or constant-folded (e, pi, version).
2. **Constant folding**: SQL-file tests with literal arguments never reach Gluten, so "no fallback" is not evidence.
   Functions covered only by such tests (encode/decode/parse_url in url-/string-functions.sql) need a DataFrame test
   or a manual decision.

Run the comparison from the previous version's run as well: a function that was unsupported on OLD and becomes S
on new with zero log mentions is almost always a lost-evidence artifact, not new support.

## 6. Summarize for the PR

`tools/scripts/diff-function-support-docs.py` prints, per category, the old/new header counts, existing functions
whose status or restriction changed, new functions grouped by status, and removed functions, comparing the working
tree against `HEAD`. For each flipped existing function explain the cause: Gluten code change (`git log -S`),
script fix, new Spark registration, or coverage change. Use the Spark-version test-log evidence (`Scalar function
X not registered with arguments`, `Could not find a valid substrait mapping name for X`, `Not supported to map
spark function name ... class name: X`, `Function 'X' is not fully supported`).

## Pitfalls seen so far

- `pkill -f gen-function-support` inside `bash -c "..."` kills the invoking shell (pattern matches itself);
  anchor the pattern (`^python3 tools/scripts/...`) or use pgrep first.
- The log4j appender overwrites `target/gen-function-support-docs-tests.log`; the script deletes it before the run
  and errors if the run produced none. Never parse a stale log from a previous configuration.
- `KNOWN_RESTRICTIONS` must be deep-copied in `parse_logs` (it is), and set unions must be assigned (`|=`).
- Lines in the comparison that look like ANSI semantics (overflow, cast, conv) are irrelevant to the doc: the doc
  is ANSI OFF only. ANSI ON offload exists behind `spark.gluten.sql.ansiFallback.enabled=false` but results do not
  yet match Spark; do not add an ANSI ON column without a result-parity signal.
