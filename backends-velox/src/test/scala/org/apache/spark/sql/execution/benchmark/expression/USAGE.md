# Expression benchmark usage

Run scalar expressions and SQL aggregates over prepared inputs using Spark JVM
and Velox execution. This is a microbenchmark of the
expression execution path, not an end-to-end SQL query or a bare function loop.

## Prepare and run

Run from the Gluten checkout in the Linux development environment. Use Java 17
HotSpot and matching native libraries already built in `cpp/build`. The launcher
builds JVM artifacts; it does not build native code.

```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Build/install the JVM reactor, export the test classpath, then list functions.
dev/run-expression-bench.sh --list

# Reuse those artifacts to inspect a function's cases.
dev/run-expression-bench.sh --skip-build --list ltrim

# Measure one registered case.
dev/run-expression-bench.sh --skip-build \
  --cases ltrim/l13-half-even \
  --rows 4000000 --batch-size 10240 \
  --seed 20260912 --key-cardinality 4000000 \
  --warmup-seconds 10 --measurement-seconds 50
```

`--skip-build` must be the first argument. It skips both Maven compilation and
classpath export. After changing Java/Scala or case resources, run without it
before relying on the outputs. After native changes, rebuild native code first
and then rebuild/package the JVM artifacts so they contain the new libraries.

The launcher uses the `java-17,spark-4.1,scala-2.13,backends-velox,hadoop-3.3,delta`
profiles. It retains the backend `classes` and `test-classes` directories and
exports dependencies through the `gluten-arrow,backends-velox` reactor.
Normal Maven formatting and style checks remain enabled.

Selectors are either a comma-separated function list or `--cases`, not both:

```bash
dev/run-expression-bench.sh --skip-build ltrim,rtrim \
  --warmup-seconds 10 --measurement-seconds 50

dev/run-expression-bench.sh --skip-build \
  --cases 'ltrim/l13-*,rtrim/l13-*' \
  --warmup-seconds 10 --measurement-seconds 50
```

Quote patterns so the shell does not expand them. `*` and `?` match within a
function/case segment, not across `/`. Unknown or unmatched selectors fail.
`--list` accepts an optional function list but cannot be combined with run
options or `--config`.

## Options and configuration

| CLI option | JSON field | Default |
| --- | --- | --- |
| Positional function list | `functions` | No default |
| `--cases` | `cases` | No default |
| `--rows` | `rows` | Markdown rows, otherwise 4000000 |
| `--batch-size` | `batchSize` | 10240 |
| `--seed` | `seed` | 20260912 |
| `--key-cardinality` | `keyCardinality` | Input row count |
| `--input-partitions` / `--maps` | `inputPartitions` | MD maps, otherwise 3 |
| `--aggregate-mode` | `aggregateMode` | MD mode, otherwise partial-final |
| `--reducers` | `reducers` | MD reducers, otherwise 1; execution currently uses 1 |
| `--shuffle-dir` | `shuffleDir` | JVM temporary directory |
| `--warmup-seconds` | `warmupSeconds` | 10 |
| `--measurement-seconds` | `measurementSeconds` | 60 |
| `--async-profiler` | `asyncProfiler` | Disabled |
| `--profile-event` | `profileEvent` | `cpu` when profiling is enabled |
| `--profile-output` | `profileOutput` | `target/expression-benchmark-profiles` |

Explicit warmup and measurement durations must be supplied together and be
positive integer seconds. Rows and batch size are positive integers.
The input seed is a signed 64-bit integer; key cardinality is positive and
controls generator inputs, not a guarantee about the output's distinct count.
The runner performs at least two measured iterations per engine.

For example, save this as `target/trim-run.json`:

```json
{
  "cases": ["ltrim/l13-half-even"],
  "rows": 4000000,
  "batchSize": 10240,
  "seed": 20260912,
  "keyCardinality": 4000000,
  "warmupSeconds": 10,
  "measurementSeconds": 50
}
```

```bash
dev/run-expression-bench.sh --skip-build --config target/trim-run.json
```

JSON selectors are nonempty arrays, not comma-separated strings. CLI options
override JSON values; a CLI selector replaces the JSON selector. Each source
must be valid on its own: overrides do not hide invalid JSON values.
Unknown fields, repeated CLI options, duplicate JSON keys and malformed types
are errors.

Relative CLI paths resolve from the directory where the launcher was invoked.
Profiler paths supplied by JSON resolve from the configuration file's directory.

## Add a catalog case

Resources live in `backends-velox/src/test/resources/expression-benchmark/cases`.
A scalar function file contains one table with four required columns:

```markdown
# ltrim

| case | inputs | expression | description |
| --- | --- | --- | --- |
| ascii-half-32 | `input = ltrimPattern(length=32, pattern="half-even", nullPercent=0, utf8=false)` | `ltrim(input)` | 32-byte ASCII inputs; even batch-local rows need trimming. |
```

The file `ltrim.md` and local ID `ascii-half-32` form the selector
`ltrim/ascii-half-32`. For a new function file, add its filename to `index.txt`.
Use one unique lowercase `.md` filename per line, without blank or comment
lines. Each file needs at least one case.

The exact column names are `case`, `inputs`, `expression`, `description`.
IDs use lowercase letters, digits, `_` or `-`. Input and SQL cells each contain
one complete single-line code span. Descriptions are plain, nonempty text;
HTML, links and mixed markup are not accepted there. Escape a literal table
pipe as `\|`, including within a code span.

Inputs use named generator arguments, with bindings separated by semicolons:

```text
a = standard.long(); b = hashedLong(column=1)
(value, chars) = rtrimCustom()
```

Argument values are integers, booleans or double-quoted strings. Column names
must be unique, including case-insensitive duplicates.

Use the standalone `none` marker for an expression without input columns:

```markdown
| case | inputs | expression | description |
| --- | --- | --- | --- |
| seeded | `none` | `rand(7)` | Explicit seed without input columns. |
```

`none` produces an empty schema, empty JVM rows and zero-column native batches.
`--rows` and `--batch-size` still determine the logical row count and batch
boundaries. No placeholder input column is generated. Blank input cells remain
invalid, and `none` cannot be combined with generator bindings.

Common generator choices:

| Generator | Arguments and constraints |
| --- | --- |
| `standard.long/int/double/date/timestamp/binary/boundedInt/intArray/stringArray/struct/timestampString` | Basic prepared inputs; see `Data.scala` for exact distributions. |
| `standard.string`, `standard.stringArray` | Optional nonnegative `length`, default 10. |
| `standard.fractionalDouble`, `standard.positiveDouble` | No arguments; key plus 0.125, with alternating signs for fractionalDouble. |
| `standard.text` | No arguments; mixed-case words, a changing numeric key and UTF-8 text. |
| `standard.base64String`, `standard.hexString`, `standard.jsonArrayString`, `standard.urlString` | No arguments; valid encoded strings prepared before evaluation. JSON arrays cycle through empty and three-element arrays containing NULL. |
| `standard.dateString` | No arguments; ISO dates cycling through 2020-2029, including leap days. |
| `standard.intMap`, `standard.mapEntries` | No arguments; two unique string keys with bounded integer values and a periodically NULL second value, as a map or an array of key/value structs. |
| `rtrimPattern`, `ltrimPattern`, `trimPattern` | Required `length >= 2` in bytes and `pattern`; optional `nullPercent` from 0 to 100 and `utf8`, default false. |
| `rtrimBoundary`, `ltrimBoundary` | No arguments; deterministic boundary/empty-string distribution. |
| `rtrimCustom`, `ltrimCustom` | No arguments; bind both value and trim-character columns. |
| `hashedLong` | Required nonnegative `column` to distinguish changing columns. |
| `hashedString`, `hashedBinary` | Required `column` and positive `length`; optional `prefix` smaller than `length`, and `nullPercent` from 0 to 100. |
| `nanPayloadDouble` | No arguments; Double NaNs with distinct payload bits by row index. |
| `nullableBoundedInt` | Optional `nullPercent` from 0 to 100; bounded integer values. |

Trim patterns are `none`, `first`, `cluster`, `half-even`, `last`,
`penultimate`, and `all`. Their positions are batch-local; for example,
`cluster` means the first 16 rows of each batch.

New distributions belong in `Data.scala`, outside timing. Reuse existing
generators where possible. Update catalog registration/count expectations when
adding files or cases. A listed case is not a promise of native support:
unsupported native preparation is reported as `SKIPPED`, not as a timing result.

Expressions may be deterministic or nondeterministic. Aggregate execution is detected
from the optimized logical plan, even without a grouping declaration; grouping specifies
keys rather than the execution type. Generators and subqueries are outside
the supported benchmark execution model. Plans rejected by fallback validation are
reported as SKIPPED; unsupported executor operations and other errors fail.
No fallback is counted as native performance.

Common preparation fixes current-time/catalog constants and resolves random seeds.
Engines use independent execution state, initialized for each iteration and retained
across its batches. The input seed does not set SQL random seeds. Nondeterministic values
are not expected to match across engines.

## Collect CPU or allocation profiles

Use an async-profiler installation whose `lib/libasyncProfiler.so` is compatible
with the async-profiler Java API on the benchmark classpath.

```bash
dev/run-expression-bench.sh --skip-build \
  --cases ltrim/l13-half-even \
  --rows 4000000 --batch-size 10240 \
  --warmup-seconds 10 --measurement-seconds 50 \
  --async-profiler "$ASYNC_PROFILER_HOME" \
  --profile-event cpu \
  --profile-output target/expression-profiles
```

The actual output root is printed once on stderr. Profiles are saved as
function/case/vanilla/profile.collapsed and function/case/native/profile.collapsed.

CPU profiles use a 1 ms interval, DWARF native stacks and thread labels.
Allocation mode uses `--profile-event alloc` with a 512 KB allocation interval;
it is not a complete measurement of native malloc traffic.
Profile event/output options require `--async-profiler`.

Profiles cover the measured engine phase, including gaps between iterations;
warmup is excluded. Profile weights are not the same as benchmark wall time.
Keep profiled timings separate from unprofiled performance comparisons.

## Measurement and correctness scope

SQL is optimized once into a shared logical plan. Spark and Gluten then perform
standard physical planning independently. ExpressionEvaluator owns the case inputs;
JvmExecutor pulls local Spark RDD iterators, while NativeExecutor drives ExprSet
projections and GroupingSet aggregation without running a Velox Driver pipeline.
Aggregate preparation retains an unstarted Task for OperatorCtx.

Planning and validation precede execution. Input conversion and compilation are
lazy, triggered by first use, normally during warmup; without warmup their cost
can enter measurement. Both engines' input batches are prepared together and reused;
raw rows are not retained. Each iteration creates fresh execution state, while Spark
caches generated code. Input batches, native handles and shuffle directories belong
to the case; iteration resources are released on completion or failure.

JVM timing includes execution and blackhole consumption. Native timing includes
execution, output vectors, JNI/handle work and closing output batches, without an
extra per-row result scan. Tests collect owned rows separately. Native output is
consumed one batch at a time; early close releases execution state and shuffle readers.

The launcher supplies the required compiler-blackhole flags, uses `-Xmx4g`
without fixed `-Xms`, and does not pin CPUs. Normal benchmark SQL settings are
UTC, ANSI disabled, case-insensitive analysis, `CODEGEN_ONLY` and
`alwaysInlineCommonExpr=false`.

Results contain Best/Avg/Stdev time, throughput and time per row, with JVM
(`vanilla`) before Native. Different engines have different internal layouts and
output-materialization costs. These are not bare-function instruction timings
or ready-made marginal cost coefficients.

`ExpressionEvaluatorSuite` contains two nested suites, `scalar` and `aggregate`.
Case names are registered without starting Spark. Native support is validated in
beforeAll using the group's existing Spark fixture; unsupported cases receive
ScalaTest Ignore tags before test execution. Selecting a function suite directly
uses the same group fixture and closes it after the selected tests finish.
Run the parent suite to exercise both groups. Multi-case files have nested function
suites; CLI selectors always use function/case. Tests own result collection and
comparison; the benchmark CLI only executes and times. BenchmarkSuite covers
codegen/timing, and AggregateShuffleBenchmarkSuite covers aggregate lifecycle.

```bash
./dev/run-scala-test.sh \
  -Pjava-17,spark-4.1,scala-2.13,backends-velox,delta \
  -pl backends-velox \
  -s org.apache.spark.sql.execution.benchmark.expression.ExpressionEvaluatorSuite
```

These are small smoke tests, not timed benchmarks. Result comparison reuses Spark
checkResult, with an additional finite Float/Double tolerance check:
`abs(a - b) <= max(absTol, relTol * max(abs(a), abs(b)))`:

| Output type | Absolute tolerance | Relative tolerance |
| --- | --- | --- |
| Float | 1e-6 | 1e-5 |
| Double | 1e-12 | 1e-12 |

Comparison uses InternalRow and the output schema. Deterministic grouping keys
allow rows to be matched across engines; nondeterministic output columns skip value
comparison. Known HLL estimate and NaN-payload cases use ignoreCompare: both engines
execute, but result comparison is skipped entirely. HLL repeated-long still compares.
These smoke tests do not replace comprehensive Gluten expression tests.

For a code-change comparison, retain exact source commits, runtime/library
identities, inputs, seed and batch size for each arm. Build/packaging can overwrite
mutable artifacts: do not assume an existing classpath or native library matches
the current worktree. The runner does not schedule interleaved A/B experiments or
prove repeatability from one run.

## SQL aggregate benchmarks

Aggregate resources are registered in `expression-benchmark/aggregate/index.txt`.
Their Markdown table has five required columns: `case`, `inputs`, `expression`,
`description`, `grouping`. A plain `-` in grouping means global aggregation;
otherwise supply one SQL expression in a code span, such as `pmod(id, 256)`.
Optional columns are mode (complete or partial-final), maps, reducers and
key-cardinality. Use a plain - for the default. Scalar tables also accept
key-cardinality. Explicit CLI values override JSON, which overrides Markdown;
otherwise defaults apply. Rows also supports a Markdown case default; batch size remains a CLI/JSON option.
Maps, reducers and key-cardinality must be positive integers. Reducers is parsed
and retained, but execution currently uses one reducer regardless of its value.

Use `sum`, `count`, `count-star`, `min`,
`max`, or `avg`, with `global` and `grouped` cases:

```bash
JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 dev/run-expression-bench.sh \
  --cases sum/global,sum/grouped,avg/grouped \
  --rows 100000 --key-cardinality 100 --batch-size 4096 --warmup-seconds 2 --measurement-seconds 2 \
  --shuffle-dir /dev/shm/gluten-benchmark
```

`--input-partitions N` (JSON: `inputPartitions`) selects the number of input
partitions, default 3, and must be a positive integer. Rows are split into
contiguous slices whose sizes differ by at most one. Empty partitions are kept
when N exceeds the row count. Partial aggregations execute sequentially; this
parameter does not control parallel threads. The output partition count remains 1.

`--shuffle-dir` (JSON: `shuffleDir`) selects the storage directory. An omitted
option uses the normal temporary directory. A supplied path is created
if absent; an unusable path fails rather than silently switching storage. No
mount is performed. Use an existing tmpfs mount such as `/dev/shm` to avoid
physical disk I/O. Each microbenchmark case owns a temporary child directory;
the supplied parent is preserved. The default temporary directory may itself be tmpfs. CLI
relative paths resolve against the caller directory, JSON paths against its file.

Each case supports one aggregate expression and at most one grouping key.
Mode partial-final runs Partial → local file shuffle → Final. Mode complete runs
one Complete aggregation over all input batches without shuffle; maps does not
split its aggregation state. DISTINCT, aggregate FILTER and more than one Exchange
are unsupported. ANSI and shuffle compression are disabled.

Both engines calculate partition hashes for hash shuffle. JVM serializes rows;
native uses the columnar shuffle writer and configured batch resizing. There are
no Spark jobs or network calls. Encoding, file I/O, decoding and result consumption
are timed. Files are truncated between iterations and deleted when the case closes,
including on failure. Native spilling is disabled; JVM spilling fails the run.

## TODO

- Redesign standard.hllSketch(). It currently returns the same sketch for {1, 2, 3}
  on every row. Add varied sketches with partially overlapping and disjoint members
  so hll_union_agg benchmarks cover more than repeated identical inputs.
- Align native Partial aggregation with production HashAggregation's adaptive
  policy: abandon grouping when reduction is poor and adjust the Partial memory
  budget after flushing. The current GroupingSet driver uses a fixed budget;
  high-cardinality or memory-limited results do not include these production policies.
- Support multiple reducers. Requested reducers are recorded, but execution uses
  exactly one reducer today.

## Per-case row counts

A scalar or aggregate Markdown table may include an optional rows column.
Values must be positive 32-bit integers; a missing column or dash leaves the
case default unset. Effective precedence is CLI --rows, JSON rows, Markdown
rows, then 4,000,000. Explicit overrides are never reduced automatically.
The runner prints the effective rows and batch size before each case.
The array_join/long-elements case defaults to 2,000,000 rows. The launcher
uses a 16 GB JVM heap; native allocations need additional host memory.
Correctness suites retain their own small row counts.

## Test scope

This is a benchmark framework, not a function-validation framework. Tests should
focus on benchmark planning, execution, timing, and resource ownership. Do not
add function-specific partial-flush regression tests here; those belong in the
aggregation implementation's tests. Catalog result checks may still be used to
validate benchmark inputs and execution.

For aggregate cases, key-cardinality applies only to input bindings referenced
by the grouping expression. Other bindings use the full row range: with
16 grouping keys, k cycles through 0..15 while standard.long() values v grow
with the row ID. Global aggregates have no grouping bindings. Scalar generator
behavior is unchanged. An expression used as both grouping input and value
shares that input; use separate bindings when independent values are needed.

The collect_set/global and collect_set/grouped cases execute both engines but
skip result comparison because their output arrays have unspecified order.
No sorting is added to their timed SQL or to the generic comparison framework.

## Window benchmarks

Window cases use the existing expression cell, for example
`sum(id) OVER (PARTITION BY pmod(id, 7) ORDER BY id ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING)`.
Select `--cases 'window/*'` to run the catalog examples. One logical window
specification is supported, with at most one Exchange and one actual reducer.
Input maps use the existing maps option; global windows also gather their inputs
through local shuffle. Multiple window specifications are not supported yet.

Spark executes its standard Sort and Window plans through local RDD iterators.
Native executes Velox OrderBy and Window operators through batch iterators. An
unstarted Driver only supplies the operator memory-reclaimer context; no Driver
pipeline or Spark job runs. Sort and window state is recreated each iteration.
Sorting, frame evaluation, shuffle and output consumption are timed; spilling is
disabled, and JVM spilling fails the run.

Window tests compare output multisets (including duplicate counts), not output
batch boundaries or row order. The initial cases have orderable result types.
Use deterministic expressions and unique ordering keys for row_number, lag/lead
and ROWS frames; rank/dense_rank and RANGE cases deliberately exercise peers.
Empty input, repeated execution, early close and consumer failures are checked
in the existing lifecycle suite. Sorting by result value does not establish
which input row produced a result; small known-answer checks supplement comparison.
