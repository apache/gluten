# Iceberg Spark UT (Gluten)

Support files for [`iceberg_spark_ut.yml`](../../workflows/iceberg_spark_ut.yml),
which runs **Apache Iceberg's own** `spark` and `spark-extensions` unit tests
against Gluten/Velox built from this repository, and gates the result against a
committed known-failures baseline.

## What it runs, and how Gluten gets in

Iceberg publishes every Spark test class in its `-tests.jar` artifacts, and
Gluten's `-Piceberg` profile already declares those jars as `test-jar`
dependencies. So, unlike the Delta pipeline, this one clones nothing and needs
no Gluten bundle jar — the whole suite comes out of the Maven dependency graph:

* **Discovery** — `-DdependenciesToScan=org.apache.iceberg:iceberg-spark-<v>_<s>,org.apache.iceberg:iceberg-spark-extensions-<v>_<s>`
  makes surefire scan those jars for test classes, so we run the *whole* upstream
  suite (~195 test classes per Spark version).
* **Gluten** — no upstream source is patched. No Iceberg test sets
  `spark.plugins`, and Spark's `SparkConf` loads every `spark.*` JVM system
  property, so passing the Gluten conf through surefire's `argLine` enables
  Gluten in every `SparkSession` the tests build — including the ~25 classes
  that build their own session instead of extending `TestBase`. The conf set
  mirrors `org.apache.gluten.TestConfUtil`.
* **`-Piceberg-test` is mandatory.** Iceberg's `TestBaseWithCatalog` — the base
  of nearly every Spark test class — references `RESTCatalogServer` /
  `RESTServerExtension` from the `iceberg-open-api` test fixtures, which only
  that profile declares. Without it virtually every suite aborts with
  `NoClassDefFoundError`.

Targets are `Spark 3.4 / Scala 2.12`, `Spark 3.5 / Scala 2.12` and
`Spark 4.0 / Scala 2.13`: those are the Iceberg Spark modules that exist for the
pinned Iceberg release (its `v4.0` module is Scala-2.13 only) and they match
Gluten's `-Pspark-3.4` / `-Pspark-3.5` / `-Pspark-4.0` profiles. The Iceberg
version itself is never hardcoded here — it is read from the selected Spark
profile's `iceberg.version` property at run time.

This pipeline **replaced** the vendored Iceberg tests that used to live in
`backends-velox/src-iceberg-spark34/` — ~35 hand-picked upstream test classes
copied into the repo (plus patched copies of Iceberg's test base classes to
inject the Gluten conf), which ran inside `velox_backend_x86.yml`'s Spark-3.4
`spark-test-spark34` jobs. Running the published test jars instead covers ~195
classes per target on three Spark versions, with nothing to keep in sync when
Iceberg is bumped. What remains in that directory is Gluten's OWN
`TestTPCHStoragePartitionedJoins` (it has no upstream equivalent: it runs TPC-H
queries over Iceberg tables with Gluten toggled on and off) and the two base
classes it extends. Those still run in the Spark-3.4 `spark-test-spark34` jobs;
this pipeline compiles but never selects them, and their names
(`SparkTestBase` / `SparkTestBaseWithCatalog`) no longer exist in Iceberg, so
they cannot shadow anything in the test jars.

## Files

| File | Purpose |
| --- | --- |
| `run-iceberg-tests.sh` | Runs one shard end to end: build, select classes, run surefire, gate. Also the supported local repro. |
| `shard-test-classes.py` | Enumerates the test classes in Iceberg's published test jars and prints one shard's worth as a surefire `-Dtest` value. |
| `known-failures-spark-3.4.txt`, `-3.5.txt`, `-4.0.txt` | The enforced baselines, one per Spark target. |
| `flaky-tests.txt` | Tests quarantined as non-deterministic (neither a regression nor a fix). Shared by all targets. |
| `flaky-error-patterns.txt` | Same, but quarantined by error signature — for a bug that lands on a different test each run. |

The gate itself is
[`../delta-spark-ut/compare-test-results.py`](../delta-spark-ut/compare-test-results.py),
shared with the Delta pipeline: it is engine agnostic (JUnit XML in, `suite#test`
lists out). Both pipelines' `paths:` filters include it, so a change to it runs
both suites.

## Bootstrapping / refreshing a baseline

All three baselines start empty, and the gate degrades to `seed` mode (never
red) while a baseline has no entries. To seed or refresh one:

1. Run the workflow from the Actions tab (`workflow_dispatch`) with
   `update_baseline = true`, and set `spark_versions` to the single target you
   are seeding.
2. Download the `iceberg-spark-ut-known-failures-<spark>` artifact produced by
   the `iceberg-spark-aggregate` job and commit it over
   `known-failures-spark-<spark>.txt`.

Refresh under the **same** `ICEBERG_NUM_SHARDS` × `ICEBERG_FORK_COUNT` the gate
runs with: failures that depend on memory pressure differ between configurations
and would otherwise show up as regressions.

**Sanity-check the first seed run.** A batch of failures is the expected shape —
Gluten does not yet offload every Iceberg code path, and tests that assert on
the query plan see a different plan once the scan is offloaded. If a seed run
comes back with almost *no* failures, suspect that Gluten was not actually
enabled (i.e. the conf never reached the forked JVM) and that the run measured
vanilla Spark; check the shard log for the `-Dspark.plugins=...` in surefire's
forked command line before trusting the baseline.

Once a baseline is populated, every run enforces it:

* a failure **not** in the baseline is a **regression** → red;
* a baseline entry that now **passes** is → red as well (`fail_on_fixed`), so
  the baseline stays honest — remove the entry in the PR that fixes the test;
* an entry no longer present in any shard is reported as **stale** by the
  aggregate job.

## Running a shard locally

```bash
# Native libs must already be built into cpp/build/releases (see the
# ci-velox-buildstatic-* scripts or dev/buildbundle-veloxbe.sh).
SPARK_VERSION=3.5 SCALA_VERSION=2.12 \
SHARD_ID=0 NUM_SHARDS=6 FORK_COUNT=2 \
GITHUB_WORKSPACE="$PWD" \
  bash .github/workflows/util/iceberg-spark-ut/run-iceberg-tests.sh
```

Set `NUM_SHARDS=1 SHARD_ID=0` to run every class in one go, and
`UPDATE_BASELINE=true` to record failures instead of enforcing them. To iterate
on a single class, skip the script and run surefire directly with
`-Dtest=TestSelect` (the script's log prints the exact command it used).

## Triggers

* **Pull requests** touching this pipeline, `gluten-iceberg/**`,
  `backends-velox/src-iceberg*/**` or the Velox revision (`paths:` in the
  workflow).
* **`/iceberg-test`** as a PR comment — forces a run for a PR that the `paths:`
  filter skipped.
* **Nightly at 07:00 UTC** — full coverage for general Velox/core changes,
  which are touched on most PRs and deliberately not in the `paths:` filter.
* **`workflow_dispatch`** — choose the target(s), fork count, and the baseline
  seed / fail-on-fixed behaviour.

## Tuning

`ICEBERG_NUM_SHARDS` (workflow `env`) must equal the length of the test job's
`shard` matrix list; `shard-test-classes.py` spreads classes round-robin over
the sorted class list, so the split stays balanced as that number changes.
`ICEBERG_FORK_COUNT` is surefire's `forkCount`; each fork needs ~4G (2G heap +
2G Velox off-heap), so keep `forkCount × 4G` inside the runner's ~16G. Forks are
not reused (`-DreuseForks=false`): every test class gets a fresh JVM, which keeps
Velox native allocations from accumulating and makes `CLASS_TIMEOUT` a per-class
hang guard.
