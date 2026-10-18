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
Dockerfile.almalinux8-gcc13-static-build | apache/gluten:vcpkg-almalinux-8-gcc13 | almalinux 8, static link, jdk8
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

`iceberg_spark_ut.yml` runs upstream Iceberg query/write tests on Spark 3.5. PR runs are
limited to Iceberg sources and the harness, native/Velox code, shared production code in
the Velox backend, core, Substrait and Arrow modules, Spark 3.5/common shims, and their
Maven/native-build inputs.

`iceberg_upstream_revision.yml` builds each revision's native and JVM artifacts once and
shares them across five class shards, with one Spark fork per runner.

### Results and gate

The observer records Iceberg scans/writes during each test body, excluding annotated setup
and cleanup. Native proof requires positive executor metrics from successful tasks, including
final adaptive plans and subqueries.

| Result | Meaning |
| --- | --- |
| `PASSED_NATIVE` | Passed with proven native execution for every observed Iceberg operation. |
| `PASSED_FALLBACK` | Passed with Spark fallback, possibly alongside native execution. |
| `FAILED` | An upstream assertion or error failed. |
| `NO_ICEBERG_EXECUTION` | Passed without observed Iceberg I/O. |
| `PASSED_UNVERIFIED` | Passed, but native execution could not be proved for every operation. |
| `SKIPPED` | Disabled upstream or aborted by an assumption. |
| `COVERAGE_ERROR` | Missing or invalid execution evidence; reporting fails. |

Failing methods run again without Gluten unless whitelisted. `control.py` attributes a failure
to Gluten only when vanilla Spark passes with matching parameters and settings. Missing,
skipped, failing or mismatched controls fail the gate. Timeouts, resource exhaustion and
broken-runtime failures remain unverified even if the control passes.

Parameter/setup randomness is seeded.

`iceberg-upstream-gate (3.5)` applies these rules to each invocation:

- Native passes must remain native; newly failed invocations fail the gate.
- Other changes are informational, including missing/skipped non-native tests, base/head
  parameter differences, and lost native node coverage without a native-status regression.
- Missing artifacts, duplicate results, incompatible report schemas, invalid execution evidence,
  unverified failure attribution and failed infrastructure checks fail the gate.

Tests listed by `class,method` in [flaky-tests.csv](../../.github/workflows/util/iceberg-upstream/flaky-tests.csv) still run and appear in reports, but do not affect the gate.

`iceberg-upstream-spark3.5-reports` contains base/head coverage JSON and CSVs, head raw logs,
comparison validation details and a changes-only CSV. Separate CSVs show native, fallback,
failed, unverified and skipped outcomes.

### Local reproduction

After building the native backend, use JDK 17:

```bash
ICEBERG_PROFILES=-Pbackends-velox,iceberg,iceberg-upstream-test,hadoop-3.4,java-17,spark-3.5,scala-2.12
./build/mvn clean install -pl backends-velox -am $ICEBERG_PROFILES -Pfast-build -DskipTests
./build/mvn surefire:test@iceberg-upstream -pl backends-velox $ICEBERG_PROFILES
python3 .github/workflows/util/iceberg-upstream/summarize.py backends-velox/target/iceberg-upstream-reports
```

Use `-Dtest=org.apache.iceberg.spark.sql.TestSelect` for one class, or
`'-Dtest=org.apache.gluten.integration.*Test'` for observer regressions. Local failures fail
Maven by default. Use clean, separate directories via `-Diceberg.upstream.reports=/path` when
changing selections. To reproduce a CI shard, add `-Diceberg.upstream.shards=5` and
`-Diceberg.upstream.shard=N` (0–4) to both Gluten and vanilla commands. Without these options,
local discovery covers the full suite. The default is one fork; override
`-Diceberg.upstream.forks=N` only with enough resources for concurrent Spark contexts.

Before comparing reports containing failures, run vanilla controls for each report directory:

```bash
reports="$PWD/backends-velox/target/iceberg-upstream-reports"
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

Compare base and current reports with the same gate rules:

```bash
python3 .github/workflows/util/iceberg-upstream/compare.py \
  /path/to/base/execution-coverage.json /path/to/current/execution-coverage.json \
  --output /path/to/comparison.json
```

This also writes `comparison.csv`, with headers only when nothing changed.
