---
layout: page
title: Velox Backend CI
nav_order: 6
parent: Developer Overview
---
# Velox Backend CI

GitHub Actions (GHA) workflows are defined under `.github/workflows/`.

PR-triggered workflows group runs by repository, PR number and workflow, with
`cancel-in-progress: true`. A replacement run from a push, force-push or rerun cancels
the workflow's older pending and running checks for that PR. Other PRs and other
workflows keep running independently. Iceberg has its own PR-triggered workflow run.

## Docker Build
A weekly job defined in `docker_image.yml` builds the Docker images used for CI verification. The Dockerfiles (under `dev/docker/`) and their corresponding images are listed below:

file | images | comments
-- | -- | --
Dockerfile.centos7-gcc13-static-build | apache/gluten:vcpkg-centos-7-gcc13 | centos 7, static link, jdk8
Dockerfile.centos8-gcc13-static-build | apache/gluten:vcpkg-centos-8-gcc13 | centos 8, static link, jdk8
Dockerfile.centos8-dynamic-build | apache/gluten:centos-8-jdk8 | centos 8, dynamic link, jdk8
Dockerfile.centos8-dynamic-build | apache/gluten:centos-8-jdk11 | centos 8, dynamic link, jdk11
Dockerfile.centos8-dynamic-build | apache/gluten:centos-8-jdk17 | centos 8, dynamic link, jdk17
cudf/Dockerfile.centos-9-jdk17-cuda13.1-cudf | apache/gluten:centos-9-jdk17-cuda13.1-cudf | centos 9, dynamic link, jdk17

The Docker images can be found at [https://hub.docker.com/r/apache/gluten/tags](https://hub.docker.com/r/apache/gluten/tags).

## Vcpkg Caching
The Gluten main branch is pulled during the static build in Docker, and vcpkg caches binary data for all dependencies defined under `dev/vcpkg`.
This binary data is cached into `/var/cache/vcpkg`, and CI jobs can reuse it in later builds. Setting `VCPKG_BINARY_SOURCES=clear` in the
environment disables reuse of the vcpkg cache.

## Arrow Libs Pre-installation
Arrow libs are pre-installed in the Docker image, since they don't change often and don't need to be rebuilt on every run.

## .M2 Cache
Dependency libraries are pre-installed into `/root/.m2` via `mvn dependency:go-offline`. Spark is set to 3.5 by default.

## Ccache
Since the Docker image is rebuilt weekly, the ccache is mostly outdated, so it is removed from the image.

## Updating the Docker Image
The GitHub secrets `DOCKERHUB_USER` and `DOCKERHUB_TOKEN` are used to push Docker images to [Docker Hub](https://hub.docker.com/r/apache/gluten/tags).
Note that GitHub secrets are not accessible in PRs from forked repos.

## Delta Spark UT
`delta_spark_ut.yml` runs delta-io/delta's own `spark` test suite against a Gluten Velox bundle, so Gluten is
validated against a real Delta release.
A number of those tests fail today.
Not because Gluten declines to offload a plan -- that should fall back to vanilla Spark and the test should
still pass.
Some are real gaps (fallback not happening where it should, metrics that differ from vanilla, native-side
bugs), and some are expected: a test that asserts on the query plan sees a different plan once the scan or
operators are offloaded, which is by design rather than a defect.
So the job does not gate on "any failure": it compares each run against a committed baseline of known failures
in `.github/workflows/util/delta-spark-ut/known-failures.txt` and fails on a **new** failure, or on a baseline
test that starts **passing** (which means the baseline needs updating).
It also fails outright if a run produced no usable results -- missing or truncated JUnit reports, or fewer
shards than expected -- rather than passing on partial data.

It runs per PR only when Delta-relevant paths change (`gluten-delta/**`, `backends-velox/src-delta*/**`, or
the pipeline's own files), nightly at 05:00 UTC for full coverage, and on demand via `workflow_dispatch` --
use the manual run to check a Velox/core change against Delta before merging.

To refresh the baseline after fixing something, run the workflow with `update_baseline=true`, download the
`delta-spark-ut-known-failures` artifact, and use the `known-failures.txt` it contains to replace
`.github/workflows/util/delta-spark-ut/known-failures.txt` in the repo.
See [.github/workflows/util/delta-spark-ut/README.md](https://github.com/apache/gluten/blob/main/.github/workflows/util/delta-spark-ut/README.md)
for the gate, the flaky-test quarantine and baseline bootstrapping.
Open follow-ups are tracked in [#12743](https://github.com/apache/gluten/issues/12743).

## PR workflow layout and execution budget

The budget is **80 minutes for normal Velox PR verification with warm caches**.
Independent workflows start from the same PR event, so their elapsed times overlap;
end-to-end verification takes the longest workflow, not the sum of their times.
Scheduled maintenance, releases, Delta-specific verification and cold native builds
are outside this budget. GitHub runner queueing is outside job execution timeouts.
The time estimates below are projections from existing hosted reports, not results
of a hosted run of this local revision. Timeouts fail unfinished checks; they do not
make partial test coverage pass.

The before column is `origin/main` at `680ba5c39` (including the new Spark 4.2 unit tests).
Counts expand matrices and called workflows into actual runner jobs, omit skipped
jobs, and assume a broad Java change or a C++ change. A shim-only PR runs fewer jobs.

| Workflow | Before: Java / C++ PR jobs | After: Java / C++ PR jobs | Peak concurrency before → after | Warm elapsed estimate after |
| --- | ---: | ---: | ---: | --- |
| `velox_backend_x86.yml` | 41 / 47 | 28 / 28 | 55 → 19 | 55–70 min |
| `velox_backend_x86_integration.yml` (new independent workflow) | Part of x86 | 18 / 24 | Part of x86 → 17 | 25–40 min Java; 65–80 min C++ |
| `velox_backend_spark42.yml` (new independent workflow) | 5 within x86 | 8 / 8 | Part of x86 → 5 | 55–70 min, projected from 4.1 timings |
| `velox_backend_enhanced.yml` | 7 / 7 | 11 / 11 | 5 → 6 | 55–70 min |
| `velox_backend_arm.yml` | 3 / 4 | 3 / 4 | 2 → 2 | 20–40 min |
| `iceberg_spark_ut.yml` (new independent workflow) | None | 9 / 9 | None → 5 | 60–75 min |

Peak concurrency is a conservative bound including the full matrices used outside
PRs. In particular, x86 previously expanded to **57 full-run jobs**: detection, one
native build, 28 JVM jobs, 23 TPC jobs and four auxiliary jobs. Its full-run jobs now
split into 28 JVM-workflow jobs, 40 integration-workflow jobs and 8 Spark 4.2 jobs.
The extra build jobs replace compilation previously repeated inside each consumer; a higher job
count does not by itself mean more runner-minutes.

The normal warm-cache PR target is **60–75 minutes overall**, with an **80-minute
active execution budget**. C++ PRs may approach that limit because the random-kill
TPC-DS workload contains a particularly slow query. Historical warm examples before
this change took [82 minutes for x86](https://github.com/apache/gluten/actions/runs/37917650510),
[102 minutes for enhanced](https://github.com/apache/gluten/actions/runs/37917650544),
and [113 minutes for a C++ x86 run](https://github.com/apache/gluten/actions/runs/37917116480).

| Critical dependency chain | Configured execution budget |
| --- | --- |
| x86 JVM / Spark 4.2 | Detection 1 + native 6 + compile 10 + tests 60 = **77 min** |
| Enhanced | Detection 1 + native 8 + compile 10 + tests 60 = **79 min** |
| Integration with shared builds | Detection 1 + native 6 + compile 8 + tests 65 = **80 min** |
| Integration auxiliary / Uniffle | Detection 1 + native 6 + job 72 = **79 min** |
| ARM TPC / C++ | Detection 1 + native 10 + TPC 69, or detection 1 + C++ 79 = **80 min** |
| Iceberg | Prepare 1 + native 6 + JVM 7 + tests 60 + gate 2 = **76 min** |

### Where the jobs went

Previously, each of the 23 Spark 3.4–4.1 JVM test jobs built its whole reactor with
`clean test`. There were five Spark 3.4 jobs, five Spark 3.5 jobs, three Spark 3.5
Scala 2.13 jobs, five Spark 4.0 jobs including Hive, and five Spark 4.1 jobs.
Main then added five Spark 4.2 jobs, each running both `clean install -DskipTests`
and `clean test`, adding ten reactor builds. They now run in an independent workflow
with one shared compilation and five class shards; all three standard groups and
both slow tags are retained. The existing Spark 4.2 packaging check stays in integration.
The enhanced workflow had five test jobs but six builds, because its slow Spark 3.5
job compiled separately for extended and Hive selections.

Now, `velox_jvm_tests.yml` is a reusable implementation, not another independent
pipeline. Each configuration has one producer and parallel test consumers:

| Configuration | Builds | Test shards |
| --- | ---: | ---: |
| Spark 3.4 | 1 | 3 |
| Spark 3.5, Scala 2.12 | 1 | 4 |
| Spark 3.5, Scala 2.13 | 1 | 2 |
| Spark 4.0 | 1 | 4 |
| Spark 4.0 Hive, retaining its separate profiles | 1 | 1 |
| Spark 4.1 standard | 1 | 3 |
| Spark 4.1 slow, retaining its separate profiles | 1 | 2 |
| Spark 4.2 standard and slow, in its independent workflow | 1 | 5 |
| Enhanced Spark 3.5 standard | 1 | 1 |
| Enhanced Spark 3.5 slow | 1 | 2 |
| Enhanced Spark 4.0 | 1 | 3 |

`jvm-test-build.py` partitions the classes found in the current compiled reactor.
`jvm-test-timings.json` supplies scheduling weights only: missing or stale timing
entries cannot exclude a new test. Both Surefire and ScalaTest retain their normal
discovery rules, with an additional class-shard filter. Standard suite prefixes and
slow tags are retained, and each JUnit case runs once per configuration. Inner
classes stay with their enclosing class. The three original standard groups,
extended SQL tests and slow Hive tests run in separate JVM invocations, restoring
clean fixtures between them. Combining these groups can corrupt eagerly created
Spark/Hive sessions even when tags exclude a suite's test methods.
Until Spark 4.2 has its own timing history, its scheduling weights combine the
measured Spark 4.1 standard and slow costs. New classes are still discovered and run.

The producer runs normal `test-compile`, including style checks. The archive records
the commit, configuration, architecture and build arguments. Consumers verify these
before restoring clean target directories and running Maven `test` with
`ci-reuse-test-build`. That profile disables the already completed compilation and
style checks; both test runners still run. Each selection's reports are preserved
before restoration removes temporary data and mutated fixtures. Failure of one
selection does not suppress the remaining selection.

The independent integration workflow receives the existing Ubuntu and CentOS TPC,
OOM, random-kill, Uniffle, Celeborn, C++/UDF, GPU, fast-build and Spark 4.2 jobs from
x86. PR coverage remains Ubuntu's four Spark/JDK pairs, CentOS's two pairs, both
Celeborn writers, and Uniffle; C++ changes also run the existing stress and native
checks. Nightly/manual runs retain the wider JDK and Celeborn matrices.

Four plain TPC build producers and one Celeborn producer replace repeated builds
across matching PR consumers. Uniffle retains its distinct build. Producers compile
production code with `fast-build` and `-DskipTests`, which also packages the test
jars required by downstream reactor modules without executing root tests; the
`gluten-it` build still runs its own tests. `tpc-build.py` transfers the complete
runtime JAR directory and validates the commit, Spark, JDK and shuffle configuration.
Consumers continue executing the existing queries in their original OS/JDK environments.

Random-kill still runs all queries once across three jobs. Query 72 has its own job;
the other two partition the remaining queries. In the historical C++ sample, query
72 alone took about 59 minutes and data generation took about 30 minutes. Reusing
that input data is essential to the warm-cache budget. An expired or invalidated
fixture cache causes regeneration, not skipped tests, and falls outside that estimate.

### Caches and artifacts

| Cache / artifact | Before | After and purpose |
| --- | --- | --- |
| Docker vcpkg, Arrow and Maven dependencies | Preinstalled | Retained; avoid rebuilding/downloading third-party dependencies |
| Native ccache | Apache Stash with Actions cache fallback | Retained per architecture/build mode; recompiles changed native inputs |
| Current native libraries | Shared inside x86; enhanced and ARM build separately | Shared inside each independent workflow; normal, enhanced and ARM configurations remain separate |
| JVM test snapshots | Each test job compiled again | One current-run artifact per configuration, retained one day; compiled classes and clean fixtures, not cached test outcomes |
| TPC runtime | Rebuilt in every TPC consumer | One current-run artifact per Spark/JDK/shuffle configuration, retained one day |
| TPC-DS SF30 input data | Regenerated by OOM and each random-kill job | Actions cache keyed by architecture and generator/build inputs; vanilla Spark data plus completion marker; existing main cache job warms it on a miss |
| Iceberg build | No upstream pipeline on main | One current-head native/JVM build shared by all five shards; retained one day |
| Iceberg baseline results | No upstream pipeline on main | Small verified result artifact from the latest merged tree, keyed by tree, harness and pinned images; normal artifact retention |

There is no base build, base test job or scheduled duplicate Iceberg run. A missing
or incompatible baseline fails comparison while preserving complete head reports.
The detailed provenance checks and bootstrap behavior are described below.

### Concurrency and total usage

The [Apache policy](https://infra.apache.org/github-actions-policy.html) limits a
workflow to 20 simultaneous jobs across all matrices and reusable calls.
`workflow-concurrency.py` expands the complete job dependency graph, resolves literal
reusable-workflow matrix inputs, applies matrix caps and considers overlapping
branches. License CI runs this audit and its tests, including the 80-minute PR
execution-budget regression test. Docker's caps reduce its peak from 22 to 14;
its 31 total jobs are unchanged.

```bash
python3 .github/workflows/util/workflow-concurrency.py
python3 -m unittest discover -s gluten-ut/src-ci/test/workflows
```

Runner-minutes are the sum of job durations. Workflow splitting and concurrency caps
alone do not save usage. Savings come from replacing the copied Iceberg suites,
reducing ordinary/enhanced JVM builds from 39 to 11, sharing TPC builds,
and reusing stress-test input data. Iceberg-only
harness edits also avoid unrelated ordinary/enhanced/ARM builds and tests.

The existing timing samples account for about **92 runner-minutes of removed legacy
Iceberg tests** and **86 minutes of repeated JVM compilation/style checks**. The new
head suite projects about **167 test minutes**, plus controls, builds, startup and
artifact transfer. These samples predate the Spark 4.2 unit-test addition; its new
independent native build also belongs in the combined usage measurement. Shared TPC
compilation and warm SF30 data provide further savings;
in the C++ example, repeated SF30 generation alone cost roughly 120 minutes across
four jobs, before cache-transfer and occasional main-cache warming costs.

These are component measurements and projections, not a measured final net total.
In particular, the earlier near-neutral estimate for seven long JVM jobs does not
apply to the parallel layout. **Lower aggregate usage remains a hosted validation
requirement**, especially for Java-only PRs that do not run the SF30 stress jobs.
Compare x86, integration, Spark 4.2, enhanced, ARM and Iceberg together against
equivalent main runs, including build/artifact overhead, failed runs, reruns and amortized cache
warming. Do not claim a confirmed decrease from job counts or timeouts alone.

### Other existing workflows

Main has 22 workflow files. The revised layout has 25 independently triggered
workflows, plus three reusable implementations (`velox_native_build.yml`,
`velox_jvm_tests.yml`, `velox_backend_x86_tests.yml`). The three new independent
workflows are integration, Spark 4.2 and Iceberg. Reusable calls contribute to their
caller's job count and concurrency, not separate runs.

| Other existing workflow file | Expanded jobs before → after | Purpose / elapsed information |
| --- | ---: | --- |
| `docker_image.yml` | 31 → 31 | Weekly/main image production; peak 22 → 14, elapsed not measured for new caps |
| `velox_backend_cache.yml` | 7 → 7 | Main dependency/native caches; historical median about 19 min; SF30 warming adds work only on a fixture miss |
| `velox_nightly.yml` | 15 → 15 | Scheduled release checks; unchanged, elapsed not estimated |
| `velox_weekly.yml` | 7 → 7 | Extended configuration checks; unchanged by this optimization, elapsed not estimated |
| `delta_spark_ut.yml` | 12 → 12 | Delta-specific tests, separate from the agreed normal Velox budget |
| `velox_backend_ansi.yml` | 7 full / 2 analysis → same | Manual ANSI validation/analysis; mutually exclusive paths |
| `build_bundle_package.yml` | 2 → 2 | Packaging then publication; unchanged, elapsed not estimated |
| `build_release.yml` | 1 → 1 | Release build; unchanged, elapsed not estimated |
| `flink.yml` | 1 → 1 | Separate Flink verification; historical median about 65 min |
| `code_format.yml` | 1 → 1 | C++ format checks; historical median about 4.5 min |
| `scala_code_format.yml` | 1 → 1 | Java/Scala format checks; historical median about 3.3 min |
| `ch_code_style.yml` | 2 → 2 | ClickHouse formatting; observed about 1 min |
| `check_license.yml` | 1 → 1 | License plus concurrency/budget audit; historical median about 2 min |
| `clickhouse_be_trigger.yml` | 1 → 1 | External ClickHouse CI trigger; historical median about 2.6 min for the trigger only |
| `pr_bot.yml` | 2 → 2 | PR metadata checks; historical median about 2.6 min |
| `stale.yml` | 1 → 1 | Stale-PR housekeeping; historical median under 1 min |
| `take.yml` | 1 → 1 | Issue assignment; elapsed not measured |
| `test_report.yml` | 1 → 1 | Report publication; elapsed not measured |
| `nightly_sync.yml` | 1 → 1 | Documentation synchronization; historical median about 6.6 min |

Historical medians here use completed successful runs from October 5–9, 2026 and
include runner queueing. They are context for unchanged workflows, not guarantees
or measurements of this revision.

## Iceberg Spark UT

The `Iceberg Spark UT (Gluten)` workflow in `iceberg_spark_ut.yml` runs upstream Iceberg Spark
query and write tests with Gluten enabled, for Spark 3.5.
It is triggered directly on PR changes that can affect its Spark 3.5 runtime, and on demand
with `workflow_dispatch`. It owns its head native/JVM builds, test jobs, gate and reports.
It is not called by x86 and does not add jobs to the x86 workflow.
Surefire discovers the tests directly from the test JARs selected by `iceberg.version`;
upgrading Iceberg automatically updates the discovered tests. A JUnit discovery filter
inspects each test method and its helpers for Spark SQL data queries or Dataset/writer
execution calls. It follows inherited methods, overrides, super calls, lambdas, Spark
actions and procedure implementations. Catalog-only DDL, parser checks, serialization and standalone Java reader/writer
tests are excluded before execution. Selection uses code, not a maintained list of upstream
test names, and does not depend on whether Gluten supports a feature yet.

The test runtime uses unshaded Iceberg libraries and resolves matching Avro, Parquet,
ORC, DataSketches and Mockito dependencies.
It extracts test resources from the published JARs so tests that require filesystem paths
can access their fixtures. Test classes continue to load from the published JARs.
Iceberg's own disabled tests and assumptions still apply. A JUnit/Spark listener records
the Iceberg scan and write nodes in executed SQL plans for each test invocation, including
parameterized tests. Upstream test sources and assertions remain unchanged.
ANSI mode is disabled by default in this job;
tests that explicitly enable ANSI mode still exercise that configuration.

The uploaded `execution-coverage.json`, `all-tests.csv` and `execution-coverage.tsv` distinguish:

| Result | Meaning |
| --- | --- |
| `PASSED_NATIVE` | Passed the unchanged upstream assertions. Every observed native Iceberg scan/write has positive executor metrics from successful tasks during the test body, with no Iceberg fallback or uncertain native execution. |
| `PASSED_FALLBACK` | Passed, with at least one Iceberg scan/write using Spark fallback, including mixed native/fallback execution. |
| `FAILED` | Failed an upstream test, regardless of native/fallback execution. |
| `NO_ICEBERG_EXECUTION` | Passed without an observed Iceberg scan/write, such as a scalar query or expected analysis exception. This is not native coverage. |
| `PASSED_UNVERIFIED` | Passed, but native execution could not be proved for every observed Iceberg operation, or an expected-error check prevents attributing native work to the operation being checked. This is not a native pass. |
| `SKIPPED` | Disabled or aborted by an upstream assumption. |
| `COVERAGE_ERROR` | Execution evidence is missing or could not be collected; the reporting step fails. |

Each record includes the actual parameter fields, Spark test settings, node types and native
executor metrics. The observer inspects final adaptive plans, command plans and subqueries.
It matches native scan/write metric IDs against positive accumulator updates from successful
tasks during the test method. A planned native node or an already populated cache cannot
provide native proof by itself. Reads that materialize a cache during the body can provide
proof. A cached Spark fallback scan also needs fresh task metrics; otherwise it is unverified.
A JUnit extension brackets the body, excluding all annotated setup and cleanup.
Spark's listener queue is drained at these boundaries; tests run sequentially within each fork.
Setup helpers called inside the method remain part of the body. Classification covers Iceberg
I/O, not every Spark operator or JVM catalog/DDL operation, and relies on the upstream assertions
for correctness. Expected-exception checks cannot borrow native evidence from another operation.

Upstream parameter providers and fixtures use `ThreadLocalRandom`, including for AQE settings.
The harness seeds it independently before each parameter template and annotated setup method;
JUnit's random method-order seed is also fixed. The JDK 17 profile opens `java.lang` so this
test-only initialization can reset the thread seed. Failure to initialize it fails observation.
The gate checks the captured parameters and Spark settings as well as outcomes. It normalizes
ephemeral REST fixture ports and the random v4 UUID literal in the upstream UUID default-value
case; the original values remain in the exports. A different type, invocation index,
vectorization or AQE setting is not comparable. The UUID control checks the same typed case,
not identical randomly generated bytes.

`FAILED` records an upstream assertion or error; it does not establish that Gluten returned
incorrect results. Some upstream assertions depend on Spark plan text, metric names,
exception classes or writer layout. CI reruns failing methods with `-Dspark.plugins=` in a
separate reports directory. `control.py` requires a passing vanilla result with matching
parameters and settings before assigning `failure_origin=GLUTEN`. A missing control, a skipped
control, a configuration mismatch or a failure reproduced without Gluten fails the job.
Timeouts, resource exhaustion and broken-runtime errors remain `UNVERIFIED_RUNTIME_FAILURE`
even when the control passes: a different timing or resource load does not establish their
cause. These fail the gate and remain in the failure CSV; the harness does not retry them
into passes or silently quarantine tests.
Attribution to Gluten establishes an observed difference; the assertion still needs inspection
to distinguish result correctness from compatibility expectations. Failure phase and messages
are retained for diagnosis, including failures in setup or cleanup.

CI builds and tests only the proposed code, with one native build and one Iceberg JVM build
shared by its five shards.
The shards download the compiled backend classes,
native libraries, observer, upstream fixtures and freshly installed sibling Gluten artifacts.
The archive records the production commit and restoration rejects a different checkout.
Unrelated Gluten artifacts from the container's Maven cache are not included. The archive is
produced in the same pinned JDK 17 container as the test jobs.

Every complete head result with valid execution evidence is saved as a small baseline artifact,
keyed by its Git tree, harness fingerprint and pinned native/JVM container image digests.
Once the PR merges, subsequent PRs reuse that saved result directly; no post-merge copy or test
run is required. The gate checks the artifact's repository and workflow, the final merged PR
head, and that the tested tree equals the actual merged base tree. Comparing trees permits a
squash/rebase commit to have a different SHA without accepting different code. A PR tested
before intervening changes landed cannot provide a baseline for a different merged tree.
Manual target-branch results can also supply a baseline; those runs compare with
the previous commit. `baseline-identity.json` identifies the tested revision, compared base
revision and original workflow run. Baseline artifacts use the repository's normal retention
period and require only read access to Actions; no privileged promotion workflow is needed.

Baseline lookup happens after all head tests. The initial introduction is detected from
the target Git tree: it must contain neither the Iceberg workflow nor its harness.
Only this case validates and saves the complete head evidence without requiring a comparison.
The summary and JSON explicitly identify this bootstrap; they do not claim zero regressions.
After the harness exists on main, a missing, expired, incompatible or unverified
baseline fails the comparison without running base or suppressing head results. The complete
head CSVs and raw logs remain downloadable for manual review. The JSON says that comparison
was unavailable, and the changes-only CSV has a header with no rows because no differences
could be established. Such a run can still publish a validated head result: if maintainers
review and merge it, that result becomes eligible as the next baseline. An unmerged PR,
incomplete test run or invalid execution evidence cannot supply a baseline.
A manual target-branch run can refresh an expired or missing result.

Each run has five shards, with one Spark fork per runner. With this odd shard count,
the stable CRC32 partition assigns every method of a class to the same runner. All
parameterized invocations remain together, and class setup executes once rather than
being repeated on four runners. The union of the shards must equal complete unsharded
discovery, with no duplicate methods; the harness checks this against both published
Spark test JARs. No test-name list needs updating when Iceberg adds tests.

Each shard has a 60-minute timeout, including setup and vanilla controls. Preparation,
native build, JVM build and aggregation have limits of 1, 6, 7 and 2 minutes respectively.
These limits bound failures; they are not a forecast of runner usage. Runner queue time
is outside job timeouts. Timeouts fail the check and cannot turn incomplete coverage into
a pass.

The gate requires all five head shard artifacts, validates their identities and
test assignments, rejects duplicate invocations, and regenerates complete CSVs before comparing
base with head. Raw shard reports remain available for debugging. The combined download is
`iceberg-upstream-spark3.5-reports`, containing `iceberg-base/`, `iceberg-head/`,
`iceberg-comparison.json` and the changes-only `iceberg-comparison.csv`.
Intermediate shard archives expire after one day; the combined download keeps the head raw
reports and both sets of CSVs for the repository's normal artifact retention period. The base
identity links to its source run for the original raw reports.

Each run summary links to its downloadable artifact. It contains `native.csv`, `fallback.csv`
and `failed.csv`, plus `unverified.csv`, `skipped.csv`, the complete report and raw JUnit logs.
CSV fields preserve commas, quotes and multiline failure messages. Failed CSV rows with
unverified attribution or broken observation are explicitly marked; they are not certified
Gluten failures.

The `iceberg-upstream-gate (3.5)` job compares every test invocation:

| Base result | Allowed current results |
| --- | --- |
| `FAILED` | Still failed, or passing. Fixes are allowed. |
| `PASSED_FALLBACK` | Fallback pass or native pass. |
| `PASSED_NATIVE` | Native pass only. |

Removing or newly skipping an executed test fails the gate. Losing native/fallback
execution evidence also fails. New tests must pass or be disabled upstream; new failures
cannot become expected failures automatically. Empty reports, duplicate results, missing
artifacts, observer errors, mismatched test configurations and failed build/test infrastructure
checks fail the gate. Old report schemas cannot be compared with the stricter observer.
The combined artifact includes JSON validation details and a CSV containing only changed
outcomes, node evidence, failure type/phase or attribution. Each changed row includes before/after
parameters, native metrics and failure messages. Ordinary timing/counter variation is not a
change. Diagnostic differences are still exported when available after a failed validation,
but they never make an invalid run pass. Reusing the verified merged PR result means
merged improvements automatically become requirements for subsequent PRs, without
maintaining baseline files. Repository branch protection must require the gate check
to enforce this at merge time. This Spark 3.5 workflow replaces the copied upstream tests
and the handwritten Velox Iceberg query, writer and TPC-H suites. The ordinary Spark 3.4,
4.0 and 4.1 jobs do not run Iceberg integration tests. Gluten serialization unit tests remain;
the shared Iceberg test base and fixtures are still used by the Bolt and ClickHouse backends.

To reproduce locally after building the native backend, use JDK 17 and run:

```bash
ICEBERG_PROFILES=-Pbackends-velox,iceberg,iceberg-upstream-test,hadoop-3.4,java-17,spark-3.5,scala-2.12
./build/mvn clean install -pl backends-velox -am $ICEBERG_PROFILES -Pfast-build -DskipTests
./build/mvn surefire:test@iceberg-upstream -pl backends-velox $ICEBERG_PROFILES
python3 .github/workflows/util/iceberg-upstream/summarize.py backends-velox/target/iceberg-upstream-reports
```

Use `-Dtest=org.apache.iceberg.spark.sql.TestSelect` to select one
class. Local runs fail on test failures by default. The upstream profile uses a
separate test class directory so local test classes cannot shadow the published ones.
Start with a clean reports directory when changing test selections, to avoid mixing runs.
Run the observer's regression checks with the same Surefire command and
`'-Dtest=org.apache.gluten.integration.*Test'`.
Use `-Diceberg.upstream.reports=/path/to/reports` to keep separate runs' results apart.
The default is one fork at a time: upstream concurrency tests have short deadlines and
competing Spark contexts can produce infrastructure failures. `-Diceberg.upstream.forks=N`
overrides this for machines with enough resources.
To reproduce one CI shard, add `-Diceberg.upstream.shards=3 -Diceberg.upstream.shard=N`
to both the Gluten and vanilla Surefire commands, where `N` is between 0 and 2.
Without these options, local runs discover the complete suite.

Before comparing runs containing failures, verify them against vanilla Spark:

```bash
reports=/path/to/reports
scripts=.github/workflows/util/iceberg-upstream
python3 "$scripts/control.py" "$reports" --select "$reports/vanilla-selection.txt"
selection=$(cat "$reports/vanilla-selection.txt")
if [ -n "$selection" ]; then
  ./build/mvn surefire:test@iceberg-upstream -pl backends-velox $ICEBERG_PROFILES \
    "-Dtest=$selection" -Dspark.plugins= -Dmaven.test.failure.ignore=true \
    -Diceberg.upstream.reports="$reports/vanilla"
fi
python3 "$scripts/control.py" "$reports" --vanilla "$reports/vanilla"
```

After collecting a base run and a current run, enforce the same transition rules locally:

```bash
python3 .github/workflows/util/iceberg-upstream/compare.py \
  /path/to/base/execution-coverage.json /path/to/current/execution-coverage.json \
  --output /path/to/comparison.json
```

This also writes `/path/to/comparison.csv`, with headers and no data rows if nothing changed.
