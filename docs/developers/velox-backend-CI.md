---
layout: page
title: Velox Backend CI
nav_order: 6
parent: Developer Overview
---
# Velox Backend CI

GitHub Actions (GHA) workflows are defined under `.github/workflows/`.

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

## Iceberg Spark UT

The standalone `Iceberg Spark UT (Gluten)` workflow in `iceberg_spark_ut.yml` runs upstream Iceberg Spark
query and write tests with Gluten enabled, for Spark 3.5.
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

CI runs both the PR base commit and the proposed code, using
each revision's own native libraries. The base checkout receives only the current test
harness; its production sources, dependencies and build configuration remain at the base
revision. This also bootstraps comparison when the base branch predates the harness.
Scheduled/manual runs compare with the previous commit. This workflow owns its native builds
and runs independently of `velox_backend_x86.yml`.

Each revision runs 12 shards, with one Spark fork per runner. A stable CRC32 partitions tests
by concrete class and method name into three class groups with four method partitions each.
This splits the longest suites while repeating a class's setup on at most four runners.
All parameterized invocations of a method stay together. The union of the shards must equal
the complete unsharded discovery, with no duplicate methods; the harness checks this against
both published Spark test JARs.
There is no test-name list to update when Iceberg adds tests.

The workflow budgets 40 minutes along its execution path: 1 minute for preparation,
8 for the parallel native builds, 29 for the parallel JVM builds/tests/vanilla controls,
and 2 for report aggregation and comparison. Runner queue time is outside job timeouts.
Timeouts fail the check; they cannot turn an incomplete run into passing coverage.

The gate requires all 12 shard artifacts for each revision, validates their identities and
test assignments, rejects duplicate invocations, and regenerates complete CSVs before comparing
base with head. Raw shard reports remain available for debugging. The combined download is
`iceberg-upstream-spark3.5-reports`, containing `iceberg-base/`, `iceberg-head/`,
`iceberg-comparison.json` and the changes-only `iceberg-comparison.csv`.
Intermediate shard archives expire after one day; the combined download keeps the raw reports
and CSVs for the repository's normal artifact retention period.

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
but they never make an invalid run pass. A fresh base run means
merged improvements automatically become requirements for subsequent PRs, without
maintaining baseline files. Repository branch protection must require the gate check
to enforce this at merge time. Existing Gluten Iceberg tests remain in their jobs.

To reproduce locally after building the native backend, use JDK 17 and run:

```bash
ICEBERG_PROFILES=-Pbackends-velox,iceberg,iceberg-upstream-test,hadoop-3.4,java-17,spark-3.5,scala-2.12
./build/mvn clean install -pl backends-velox -am $ICEBERG_PROFILES -Pfast-build -DskipTests
./build/mvn surefire:test@iceberg-upstream -pl backends-velox $ICEBERG_PROFILES
python3 .github/workflows/util/iceberg-upstream/summarize.py backends-velox/target/iceberg-upstream-reports
```

Use `-Dtest=org.apache.iceberg.spark.sql.TestSelect` to select one
class. Local runs fail on test failures by default. The upstream profile uses a
separate test class directory so copied Iceberg classes cannot shadow the published
ones, even after building with `-Piceberg-test`.
Start with a clean reports directory when changing test selections, to avoid mixing runs.
Run the observer's regression checks with the same Surefire command and
`'-Dtest=org.apache.gluten.integration.*Test'`.
Use `-Diceberg.upstream.reports=/path/to/reports` to keep separate runs' results apart.
The default is one fork at a time: upstream concurrency tests have short deadlines and
competing Spark contexts can produce infrastructure failures. `-Diceberg.upstream.forks=N`
overrides this for machines with enough resources.
To reproduce one CI shard, add `-Diceberg.upstream.shards=12 -Diceberg.upstream.shard=N`
to both the Gluten and vanilla Surefire commands, where `N` is between 0 and 11.
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
