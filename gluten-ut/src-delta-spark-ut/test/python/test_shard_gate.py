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

import os
from pathlib import Path
import subprocess
import tempfile
import unittest

UTIL = Path(".github/workflows/util/delta-spark-ut")
SCRIPTS = Path(__file__).resolve().parents[4] / UTIL


class ShardGateTest(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)
        self.shard = self.root.name
        for name in ("sbt-spark-test-shard-{}.log", "sbt-done-shard-{}"):
            path = Path("/tmp") / name.format(self.shard)
            self.addCleanup(path.unlink, missing_ok=True)

        util = self.root / UTIL
        util.mkdir(parents=True)
        (util / "compare-test-results.py").symlink_to(SCRIPTS / "compare-test-results.py")
        (util / "known-failures.txt").write_text("FinishedSuite#known assertion\n")

        build = self.root / "delta/build"
        build.mkdir(parents=True)
        (build / "sbt").write_text(
            """#!/usr/bin/env bash
set -eu
mkdir -p spark/target/test-reports
cat > spark/target/test-reports/TEST-FinishedSuite.xml <<'XML'
<testsuite name="FinishedSuite"><testcase classname="FinishedSuite" name="known assertion">
<failure message="assertion failed"/></testcase></testsuite>
XML
echo "$SBT_MESSAGE"
printf 'oom 10\\noom_kill %s\\n' "$AFTER" > "$CGROUP_DIR/memory.events"
exit "$SBT_EXIT"
"""
        )
        self.failures = self.root / "gate-out" / f"failures-shard-{self.shard}.txt"

    def run_shard(self, message="", before=3, after=3, sbt_exit=1, seed=False):
        if before is not None:
            (self.root / "memory.events").write_text(f"oom 9\noom_kill {before}\n")
        env = dict(
            PATH=os.environ["PATH"],
            GITHUB_WORKSPACE=str(self.root),
            SHARD_ID=self.shard,
            SPARK_VERSION="4.1",
            DELTA_SCALA_VERSION="2.13",
            FAIL_ON_FIXED="true",
            UPDATE_BASELINE=str(seed).lower(),
            CGROUP_DIR=str(self.root),
            SBT_MESSAGE=message,
            SBT_EXIT=str(sbt_exit),
            AFTER=str(after),
        )
        log = self.root / "run.log"
        with log.open("w") as out:
            result = subprocess.run(
                ["bash", str(SCRIPTS / "run-delta-tests.sh")],
                env=env,
                stdout=out,
                stderr=subprocess.STDOUT,
                timeout=30,
            )
        return result.returncode, log.read_text()

    def test_abnormal_fork_exit_with_partial_reports(self):
        failure = "sbt.ForkMain 45161 failed with exit code 137"
        code, log = self.run_shard(message=f"[error] {failure}", before=None)
        self.assertEqual(1, code, log)
        self.assertIn(f"::error::Shard {self.shard}: {failure}", log)
        self.assertFalse(self.failures.exists())

    def test_new_oom_kill_without_fork_diagnostic_blocks_seeding(self):
        code, log = self.run_shard(after=4, sbt_exit=0, seed=True)
        self.assertEqual(1, code, log)
        self.assertIn("cgroup oom_kill increased from 3 to 4", log)
        self.assertFalse(self.failures.exists())

    def test_baselined_failure_with_preexisting_oom_kills(self):
        code, log = self.run_shard(message="[error] Tests unsuccessful")
        self.assertEqual(0, code, log)
        self.assertEqual("FinishedSuite#known assertion\n", self.failures.read_text())


if __name__ == "__main__":
    unittest.main()
