#!/usr/bin/env python3
#
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

import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).with_name("compare-test-results.py")


class AggregatePrunedBaselineTest(unittest.TestCase):
    def test_pruned_baseline_removes_only_confirmed_passing_tests(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            inputs = root / "inputs"
            inputs.mkdir()

            baseline = root / "known-failures.txt"
            baseline.write_text(
                "# Keep this header verbatim.\n"
                "suite.Fixed#now passes\n"
                "suite.Expected#still fails\n"
                "suite.Skipped#not executed\n"
                "suite.Stale#not seen\n"
                "suite.Flaky#sometimes passes\n",
                encoding="utf-8",
            )
            flaky = root / "flaky-tests.txt"
            flaky.write_text(
                "suite.Flaky#sometimes passes\n",
                encoding="utf-8",
            )
            (inputs / "failures-shard-0.txt").write_text(
                "suite.Expected#still fails\n" "suite.Regression#new failure\n",
                encoding="utf-8",
            )
            (inputs / "ran-shard-0.txt").write_text(
                "suite.Fixed#now passes\n"
                "suite.Expected#still fails\n"
                "suite.Flaky#sometimes passes\n"
                "suite.Regression#new failure\n",
                encoding="utf-8",
            )
            (inputs / "skipped-shard-0.txt").write_text(
                "suite.Skipped#not executed\n",
                encoding="utf-8",
            )

            refreshed = root / "refreshed.txt"
            pruned = root / "pruned.txt"
            result = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    "--mode",
                    "aggregate",
                    "--inputs-dir",
                    str(inputs),
                    "--expected-shards",
                    "1",
                    "--known-failures",
                    str(baseline),
                    "--flaky-tests",
                    str(flaky),
                    "--baseline-out",
                    str(refreshed),
                    "--pruned-baseline-out",
                    str(pruned),
                ],
                capture_output=True,
                text=True,
                check=False,
            )

            self.assertEqual(0, result.returncode, result.stderr)
            self.assertEqual(
                "# Keep this header verbatim.\n"
                "suite.Expected#still fails\n"
                "suite.Skipped#not executed\n"
                "suite.Stale#not seen\n"
                "suite.Flaky#sometimes passes\n",
                pruned.read_text(encoding="utf-8"),
            )
            refreshed_text = refreshed.read_text(encoding="utf-8")
            self.assertIn("suite.Regression#new failure\n", refreshed_text)
            self.assertNotIn(
                "suite.Regression#new failure\n",
                pruned.read_text(encoding="utf-8"),
            )
            self.assertNotIn("suite.Stale#not seen\n", refreshed_text)


if __name__ == "__main__":
    unittest.main()
