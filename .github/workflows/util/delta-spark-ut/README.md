<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Delta Spark UT (Gluten) — managing expected failures

Running delta-io/delta's `spark` ScalaTest suite against the Gluten Velox
bundle produces **many expected failures**: Gluten does not yet offload every
Delta code path, and falls back or behaves differently in places. If CI simply
went red on any failure, the signal would be useless and we could never tell a
*new* breakage from the hundreds of already-known ones.

To make this manageable we keep a **baseline of known failures** and gate each
run against it. The build is green when the only failing tests are ones already
recorded in the baseline; it goes red the moment a **previously-passing test
starts failing** (a regression).

## Files

| File | Purpose |
|---|---|
| `known-failures.txt` | Committed baseline: the tests currently expected to fail. One `<suite>#<test>` per line. |
| `flaky-tests.txt` | Quarantine list by test name: tests whose pass/fail is non-deterministic. Ignored by the gate whether they pass or fail. `<suite-glob>#<test>` per line. |
| `flaky-error-patterns.txt` | Quarantine list by error signature: regex patterns matched against a failure's text, for bugs that surface on a different test each run (e.g. the native DV bitmap row-index error). |
| `compare-test-results.py` | Parses the JUnit XML from `sbt spark/test` and gates / seeds / aggregates against the baseline. Standard-library only. |
| `run-delta-tests.sh` | The shard step's body: runs `sbt spark/test` (tuned JVM/heap flags) under a hang watchdog, prints memory forensics, then gates the results against the baseline via `compare-test-results.py`. |
| `java-test-args.sh` | Shared JVM flags (`--add-opens` + Netty property) needed to run the suite on JDK 17 with the Gluten bundle. Sourced by `run-delta-tests.sh` and by local runs. |
| `setup-delta.sh` | Clones Delta, drops in the Gluten bundle, and patches `DeltaSQLCommandTest`. |

## How the gate works

Each test shard:

1. Runs `sbt spark/test` with ScalaTest's JUnit XML reporter enabled
   (`-u target/test-reports`), so every suite writes per-test results. (Delta
   itself only configures the console reporter, so the workflow injects this.)
2. Runs `compare-test-results.py --mode enforce`, which classifies every test:
   - **regression** — failed, but not in the baseline → **fails the shard**.
   - **expected** — failed and in the baseline → ignored.
   - **now-passing** — in the baseline but passed this run → fails the shard
     (so the baseline is kept honest), unless `fail_on_fixed=false`.
   - **quarantined** — matches an entry in `flaky-tests.txt` → always ignored,
     whether it passed or failed (see [Flaky tests](#flaky-tests) below).

A final `aggregate` job merges every shard's results into a single, sorted,
ready-to-commit `known-failures.txt` artifact and reports **stale** baseline
entries (tests no longer present in any shard, e.g. after a Delta version bump).

Because Delta shards **by suite**, every suite (and therefore every test) runs
in exactly one shard, so per-shard enforcement sees complete suites and never
double-counts.

## When it runs

To keep GitHub Actions usage in check, the suite does **not** run on every PR:

- **Per PR** — `velox_backend_x86.yml` runs the Delta suite only when the PR
  touches a **high-signal Delta path**: the Delta integration code
  (`backends-velox/src-delta*`), the `gluten-delta` module, or this pipeline's
  own files (`delta_spark_ut.yml`, `util/delta-spark-ut/**`,
  `velox_backend_x86.yml`). Changes to general Velox/core/native code can also
  affect Delta offload, but they're touched on most PRs, so per-PR they skip the
  suite — the nightly run and the opt-in label are the safety nets. Add the
  **`run-delta-ci`** label to force the suite on any PR (the label is read from
  the triggering event, so apply it before/with a push).
- **Nightly** — `delta_spark_ut.yml` runs the **full** suite against the latest
  default branch on a `schedule` (05:00 UTC), so rarer regressions are still
  caught daily. The nightly run enforces the baseline **and** fails on
  now-passing tests (`fail_on_fixed=true`), so baseline drift surfaces as a red
  nightly — the signal to refresh `known-failures.txt`.
- **Manually** — **Actions → Delta Spark UT (Gluten) → Run workflow**
  (`workflow_dispatch`), e.g. to refresh the baseline (see below).

## Bootstrapping the baseline (first time)

While `known-failures.txt` has no entries the gate auto-runs in **seed mode**
(it never fails — it only records failures). To create the initial baseline:

1. Trigger **Actions → Delta Spark UT (Gluten) → Run workflow** with
   `update_baseline = true`.
2. When it finishes, download the **`delta-spark-ut-known-failures`** artifact.
3. Replace `known-failures.txt` with the file from that artifact and commit it.

From the next run onward the gate enforces the baseline.

## Day-to-day: fixing tests incrementally

- **You fixed Gluten and some Delta tests now pass.** CI will flag them as
  *now-passing*. Delete those lines from `known-failures.txt` in your PR. That
  is the whole point — the baseline only ever shrinks as coverage improves.
- **You intentionally added a new expected failure** (e.g. a Delta path Gluten
  can't offload yet). Add the exact `Suite#test` line(s) the gate prints under
  *Regressions* to `known-failures.txt`, ideally with a comment explaining why.
- **A genuine regression.** Fix it; do **not** add it to the baseline.

The error log prints copy-pasteable `Suite#test` lines for both regressions and
now-passing tests, and each run's job summary shows the full breakdown.

## Regenerating / refreshing the whole baseline

After a Delta version bump or a large Gluten change, regenerate from scratch the
same way as bootstrapping: run the workflow with `update_baseline=true`, download
the `delta-spark-ut-known-failures` artifact, and commit it. The aggregate job
also lists **stale** entries you can prune.

The aggregate job passes `--expected-shards` (the shard count), so if a shard
dies before writing its gate lists (or its artifact fails to download) the
aggregate **fails** instead of emitting a baseline that silently omits that
shard's failures — which would otherwise shrink `known-failures.txt` and red the
next run. Re-run the workflow if this happens.

## Flaky tests

Some tests are genuinely non-deterministic (e.g. the Delta MERGE-with-deletion-vector
suites that intermittently hit a native row-index bug). Such a test would otherwise
red the gate as a **regression** when it flakes to a failure, or as **now-passing**
when it flakes to a pass — noise either way.

List these in **`flaky-tests.txt`** to **quarantine** them: the gate ignores a
quarantined test whether it passes or fails, and never writes it into the
regenerated baseline. Format is one `<suite-glob>#<test>` per line, `#`-comments
and blank lines allowed:

```
# suite portion is an fnmatch glob; test portion is matched exactly.
*DVs*Suite#matched only merge - enabled - with update and delete - isPartitioned: true
```

- The **suite** portion is an `fnmatch` glob, so `*DVs*Suite` covers every
  generated deletion-vector merge variant in one line. Use the narrowest glob
  that still covers the root-cause family.
- The **test** portion is matched **exactly** (test names are freeform and may
  contain glob metacharacters), so a same-named test in a non-matching suite is
  still gated normally.

### Quarantine by error signature

Some bugs surface on a **different test each run** — for example the native Delta
DV bitmap row-index error (the aggregator gets a garbage row index during a MERGE
that writes deletion vectors and aborts, e.g. `Delta RoaringBitmapArray row index
... exceeds max representable value` or `Delta bitmap row index cannot be
negative: ...`) lands on a different `*DVs*Suite` MERGE test every time. Chasing
those by name is whack-a-mole, so quarantine them by **root cause** in
**`flaky-error-patterns.txt`** instead: each line is a regex matched against a
failed test's `<failure>`/`<error>` text. Any failure that matches is treated as
flaky regardless of which test it hit (and is dropped from the shard's failures
list so it can't leak into the baseline):

```
# regex matched against the failure message + stack (enforce mode).
# one explicit pattern per known error, deliberately specific.
Delta RoaringBitmapArray row index \d+ exceeds max representable value
Delta bitmap row index cannot be negative: -?\d+
```

This is more precise than a name glob: a *different* real failure in the same
suite is still caught, because only failures carrying the signature are ignored.

Quarantining (either kind) is an **interim** measure — it hides a real bug from
CI. Each entry should reference the tracking issue, and be removed once the
underlying bug is fixed so the test is enforced again.

## Caveats

- **Known failures still execute** (and fail) — they are gated *after* the run,
  not skipped — so they still consume CI time. This keeps us decoupled from
  Delta's sources; skipping them at runtime would require patching Delta.

## Running the comparison locally

```bash
# after an sbt spark/test run that wrote delta/**/target/test-reports/*.xml
python3 .github/workflows/util/delta-spark-ut/compare-test-results.py \
  --mode enforce \
  --reports-dir delta \
  --known-failures .github/workflows/util/delta-spark-ut/known-failures.txt \
  --flaky-tests .github/workflows/util/delta-spark-ut/flaky-tests.txt \
  --failures-out /tmp/failures.txt --ran-out /tmp/ran.txt
```

## Running the suite locally

`sbt spark/test` needs extra JDK-17 JVM flags to run the Delta suite against the
Gluten bundle (`--add-opens` + the Netty reflection property). CI and local runs
share one definition in `java-test-args.sh` — `source` it before invoking sbt so
the flags reach the sbt launcher and the forked test JVM:

```bash
# from the Delta clone prepared by setup-delta.sh (which has the Gluten bundle):
source <gluten>/.github/workflows/util/delta-spark-ut/java-test-args.sh
./build/sbt "++ 2.13.16" spark/test        # or a single suite via testOnly
```

`run-delta-tests.sh` sources the same file, so CI and local runs use identical
flags.
