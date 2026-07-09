#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#
# Runs the Delta `spark` module tests for one shard under the Gluten bundle:
# arms a hang watchdog (thread-dumps + kills a wedged fork), invokes sbt
# spark/test with the tuned JVM/heap flags, prints cgroup memory forensics, and
# then gates the results against the baseline (compare-test-results.py). Extracted
# from delta_spark_ut.yml so the workflow step stays readable.
#
# Driven by environment (set by the workflow step / job):
#   SHARD_ID          - this shard's id (matrix.shard)
#   SPARK_VERSION     - Delta -DsparkVersion value
#   UPDATE_BASELINE   - 'true' -> gate seed mode; else enforce
#   FAIL_ON_FIXED     - passed through to the gate
#   DELTA_SCALA_VERSION, NUM_SHARDS, TEST_PARALLELISM_COUNT, DELTA_TESTING
#                     - test env (see the workflow step's `env:` block)
#   GITHUB_WORKSPACE  - repo root (holds the Delta clone + util scripts)
#
# JAVA_TOOL_OPTIONS is set by sourcing java-test-args.sh (below), not the caller.

set -euo pipefail
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk
export PATH=$JAVA_HOME/bin:$PATH
# Gluten/JDK17 test JVM flags (--add-opens + Netty property), shared with local
# dev runs. Sets JAVA_TOOL_OPTIONS so it reaches the sbt launcher + forked JVMs.
# shellcheck source=./java-test-args.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/java-test-args.sh"
cd "$GITHUB_WORKSPACE/delta"
chmod +x build/sbt
# Only run the unified `spark` sbt project, NOT `sparkGroup/test` --
# `sparkGroup` aggregates many other projects (sparkV2, contribs,
# sharing, connect*, ...) that are out of scope for this pipeline.
#
# JVM heap layout -- two memory consumers on the ~16G runner:
# * sbt launcher JVM: -J-Xmx4G for the test compile, then forced to
#   return idle memory during the (long) test phase via G1 periodic GC
#   (G1PeriodicGCInterval=10s; G1PeriodicGCSystemLoadThreshold=0 so the
#   busy fork doesn't suppress it; -XX:-G1PeriodicGCInvokesConcurrent
#   forces each periodic GC to a full STW collection) that uncommits to a
#   tight free ratio (Min/MaxHeapFreeRatio 5/15, JEP 346) above a low
#   -Xms512m floor.
#   Without this the idle launcher holds ~5.3G for the whole run; with
#   it, it drops back to ~1-2G. These flags touch no Gluten/Spark runtime
#   config, so they cannot affect the measured pass/fail signal.
# * Forked test JVM: -Xmx2G via the `set ... Test / javaOptions` command
#   below. Delta caps its fork at -Xmx1024m in build.sbt; `++=` appends
#   so our -Xmx2G comes last and wins. Gluten offloads data to Velox
#   off-heap (capped at 2g via spark.memory.offHeap.size in the patched
#   DeltaSQLCommandTest), so the fork's heap need is modest. A larger
#   fork heap pushed the cgroup peak past the ~16G OOM threshold and the
#   kernel OOM-killed the fork mid-shard (no hs_err), wedging sbt -- 2G
#   keeps headroom. Keep heap-dump-on-OOM so a real >2G heap OOM is
#   analyzable.
# `-u target/test-reports` enables ScalaTest's JUnit XML reporter so
# every suite writes per-test results. Delta itself only configures
# the console reporter (-oDF), so without this we'd have no machine-
# readable results to gate on. The path is relative to the forked
# test JVM's working dir (Test / baseDirectory = spark/), i.e.
# delta/spark/target/test-reports/TEST-*.xml.
#
# We deliberately do NOT let an sbt non-zero exit (which fires on the
# MANY expected Delta-on-Gluten failures) fail this step directly.
# Instead the known-failures gate below decides pass/fail: the build
# is green when the only failures are ones already recorded in the
# baseline, and red on a genuine regression.
set +e
# --- hang watchdog ---------------------------------------------------
# Shard 2 (and occasionally others) hangs indefinitely after a suite's
# last test with no further output. ScalaTest's failAfter only wraps
# individual test BODIES, so a wedge in suite teardown/afterAll -- or in
# a non-interruptible native Velox/JNI call that ignores
# Thread.interrupt() -- has no timeout and stalls until the 350-min job
# limit with zero diagnostics. This watchdog dumps the forked test JVM's
# threads (to the job log, and to a file for the artifact) once the test
# output has been silent for too long, so the deadlock is diagnosable.
SBT_LOG="/tmp/sbt-spark-test-shard-${SHARD_ID}.log"
: > "$SBT_LOG"
rm -f /tmp/sbt-done
(
  # CRITICAL: the step shell runs with `bash -eo pipefail`, which the
  # subshell inherits. Without `set +e` here, ANY non-zero command --
  # e.g. fork detection finding no match, or `kill`/`jps` returning
  # non-zero -- silently kills this watchdog. That errexit kill (plus a
  # /proc detection miss) once made the watchdog capture ZERO dumps. A
  # diagnostic must never abort on a failed probe.
  set +e +o pipefail
  JSTACK="${JAVA_HOME}/bin/jstack"
  JPS="${JAVA_HOME}/bin/jps"
  silent_limit=900   # 15 min with no new test output => treat as hung
  dumps=0
  fork_pids() {
    # The sbt test fork's main class is sbt.ForkMain. Prefer jps (reads
    # the main class from hsperfdata, robust to sbt's @argfile launch);
    # fall back to scanning /proc cmdline + @argfile.
    "$JPS" -l 2>/dev/null | awk '/sbt\.ForkMain/ {print $1}'
    local p cl arg
    for p in /proc/[0-9]*; do
      [ "$(cat "$p/comm" 2>/dev/null)" = "java" ] || continue
      cl="$(tr '\0' ' ' < "$p/cmdline" 2>/dev/null)"
      case "$cl" in *sbt.ForkMain*) echo "${p##*/}"; continue ;; esac
      arg="$(printf '%s' "$cl" | tr ' ' '\n' | sed -n 's/^@//p' | head -1)"
      [ -n "$arg" ] && [ -f "$arg" ] && grep -qa 'sbt\.ForkMain' "$arg" 2>/dev/null \
        && echo "${p##*/}"
    done
  }
  all_java_pids() {
    "$JPS" -q 2>/dev/null
    local p
    for p in /proc/[0-9]*; do
      [ "$(cat "$p/comm" 2>/dev/null)" = "java" ] && echo "${p##*/}"
    done
  }
  echo "HANG WATCHDOG armed: dumps the test JVM after ${silent_limit}s of output silence"
  hb=0
  while [ ! -f /tmp/sbt-done ]; do
    sleep 60
    [ -f "$SBT_LOG" ] || continue
    now=$(date +%s)
    mtime=$(stat -c %Y "$SBT_LOG" 2>/dev/null || echo "$now")
    silent=$(( now - mtime ))
    # Per-minute memory profile: heap tuning proved the ~16G OOM peak is
    # NATIVE-driven, so log which JVM (sbt launcher vs fork) actually grows
    # toward it -- the last lines before a hang reveal the real hog to cut.
    # Read /proc directly (no `ps` dependency in the minimal container).
    memnow=$(awk '{printf "%.2fG",$1/1073741824}' /sys/fs/cgroup/memory.current 2>/dev/null)
    jvmrss=""
    for mp in $(all_java_pids 2>/dev/null | sort -un); do
      r=$(awk '/^VmRSS:/{print $2}' "/proc/$mp/status" 2>/dev/null)
      [ -n "$r" ] && jvmrss="$jvmrss $(( r / 1024 ))M(p$mp)"
    done
    echo "MEM cgroup=${memnow} JVMs=[${jvmrss# }]"
    hb=$(( hb + 1 ))
    # Heartbeat every ~5 min so we can SEE the watchdog is alive (and how
    # long the test has been silent) without waiting for a hang.
    [ $(( hb % 5 )) -eq 0 ] && echo "HANG WATCHDOG: alive; last test output ${silent}s ago"
    if [ "$silent" -ge "$silent_limit" ] && [ "$dumps" -lt 3 ]; then
      dumps=$(( dumps + 1 ))
      pids="$(fork_pids | sort -un)"
      # Safety net: if the fork JVM cannot be pinpointed, dump EVERY JVM.
      [ -n "$pids" ] || pids="$(all_java_pids | sort -un)"
      echo "::group::HANG WATCHDOG: test output silent ${silent}s -- thread dump #${dumps} (pids:$(printf ' %s' $pids))"
      [ -n "$pids" ] || echo "HANG WATCHDOG: no java process found to dump"
      for pid in $pids; do
        # SIGQUIT makes the JVM print a full thread dump to its OWN stderr,
        # which sbt relays into the test log via the SAME stream as test
        # output -- so it lands in the job log even when a separately
        # spawned jstack child's output would be buffered/lost. Also write
        # jstack to a file for the per-shard artifact.
        echo "----- SIGQUIT + jstack pid ${pid} -----"
        kill -QUIT "$pid" 2>/dev/null || echo "HANG WATCHDOG: kill -QUIT failed for pid ${pid}"
        timeout 120 "$JSTACK" -l "$pid" > "/tmp/threaddump-shard-${SHARD_ID}-${dumps}-${pid}.txt" 2>&1 \
          || echo "HANG WATCHDOG: jstack failed/timed out for pid ${pid}"
      done
      echo "::endgroup::"
      # The dump is now captured (job log via SIGQUIT + artifact via
      # jstack file). A hung JVM otherwise stalls the whole shard until
      # the 350-min job timeout AND keeps the job log frozen so the dump
      # never becomes reachable. So KILL the wedged JVM(s): the suite
      # fails fast (acceptable -- errors are expected; only an
      # unrecoverable hang blocks CI), the job proceeds/ends, and the log
      # + artifacts flush. Give SIGQUIT a moment to print first.
      sleep 20
      echo "HANG WATCHDOG: killing wedged JVM(s) to unblock the shard: $(printf '%s ' $pids)"
      for pid in $pids; do kill -KILL "$pid" 2>/dev/null; done
    fi
  done
) &
WATCHDOG_PID=$!

./build/sbt \
  -DsparkVersion=${SPARK_VERSION} \
  -v \
  -J-XX:+UseG1GC -J-Xms512m -J-Xmx4G \
  -J-XX:G1PeriodicGCInterval=10000 \
  -J-XX:G1PeriodicGCSystemLoadThreshold=0 \
  -J-XX:-G1PeriodicGCInvokesConcurrent \
  -J-XX:MinHeapFreeRatio=5 -J-XX:MaxHeapFreeRatio=15 \
  "++ ${DELTA_SCALA_VERSION}" \
  'set spark / Test / javaOptions ++= Seq("-Xmx2G", "-XX:+HeapDumpOnOutOfMemoryError", "-XX:HeapDumpPath=/tmp/")' \
  'set spark / Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-u", "target/test-reports")' \
  "spark/test" 2>&1 | tee "$SBT_LOG"
SBT_EXIT=${PIPESTATUS[0]}
touch /tmp/sbt-done
kill "$WATCHDOG_PID" 2>/dev/null || true
set -e
echo "sbt spark/test exited with ${SBT_EXIT}"

# Memory forensics: a sudden forked-JVM death with no hs_err and no heap
# dump is almost always a kernel/cgroup OOM-kill (Velox off-heap + JVM
# heap exceeding the ~16G runner). Surface the cgroup peak + oom_kill
# count so we can confirm/measure it (cgroup v2 paths; best-effort).
( echo "=== cgroup memory forensics (exit ${SBT_EXIT}) ==="
  for f in /sys/fs/cgroup/memory.peak /sys/fs/cgroup/memory.max \
           /sys/fs/cgroup/memory.current /sys/fs/cgroup/memory.events; do
    [ -r "$f" ] && { echo "--- $f ---"; cat "$f"; }
  done ) || true

# A compile/launch failure leaves no reports at all. In that case the
# gate would see zero failures and pass spuriously, so fail loudly.
REPORT_COUNT=$(find . -path '*/target/test-reports/*.xml' 2>/dev/null | wc -l || true)
echo "Found ${REPORT_COUNT} JUnit XML report file(s)."
if [ "${REPORT_COUNT}" -eq 0 ]; then
  echo "::error::sbt produced no test reports (exit ${SBT_EXIT}) -- likely a compile or launch failure, not test failures."
  exit 1
fi

# Classify this shard's results against the baseline: seed mode when
# UPDATE_BASELINE=true (record failures, never fail) so the baseline can be
# (re)generated; otherwise enforce against it. Writes this shard's gate-out/*.txt
# for the aggregate job.
UTIL_DIR="$GITHUB_WORKSPACE/.github/workflows/util/delta-spark-ut"
GATE_MODE=enforce
if [ "${UPDATE_BASELINE}" = "true" ]; then
  GATE_MODE=seed
fi
mkdir -p "$GITHUB_WORKSPACE/gate-out"
python3 "$UTIL_DIR/compare-test-results.py" \
  --mode "$GATE_MODE" \
  --reports-dir "$GITHUB_WORKSPACE/delta" \
  --known-failures "$UTIL_DIR/known-failures.txt" \
  --flaky-tests "$UTIL_DIR/flaky-tests.txt" \
  --failures-out "$GITHUB_WORKSPACE/gate-out/failures-shard-${SHARD_ID}.txt" \
  --ran-out "$GITHUB_WORKSPACE/gate-out/ran-shard-${SHARD_ID}.txt" \
  --fail-on-fixed "${FAIL_ON_FIXED}"
