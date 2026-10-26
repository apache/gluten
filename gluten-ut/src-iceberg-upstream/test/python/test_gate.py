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

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

SCRIPT = (
    Path(__file__).resolve().parents[4]
    / ".github/workflows/util/iceberg-upstream/gate.py"
)
sys.path.insert(0, str(SCRIPT.parent))
from gate import unavailable_reasons


class GateTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.status = {
            "PREPARE_RESULT": "success",
            "UPSTREAM_RESULT": "success",
            "HEAD_OUTCOME": "success",
            "RUN_BASE": "false",
            "BASELINE_OUTCOME": "success",
            "BASE_RESULT": "skipped",
            "BASE_OUTCOME": "skipped",
            "INITIAL_BASELINE": "false",
        }
        for name in ("iceberg-base", "iceberg-head"):
            (self.root / name).mkdir()
            (self.root / name / "execution-coverage.json").write_text("{}")

    def test_cached_and_fresh_baselines_allow_comparison(self):
        self.assertEqual([], unavailable_reasons(self.root, self.status))
        self.status.update(
            RUN_BASE="true",
            BASELINE_OUTCOME="failure",
            BASE_RESULT="success",
            BASE_OUTCOME="success",
        )
        self.assertEqual([], unavailable_reasons(self.root, self.status))

    def test_failed_jobs_or_missing_inputs_cannot_claim_a_comparison(self):
        for field in (
            "PREPARE_RESULT",
            "UPSTREAM_RESULT",
            "HEAD_OUTCOME",
            "BASELINE_OUTCOME",
        ):
            for result in ("failure", "skipped", "cancelled", ""):
                with self.subTest(field=field, result=result):
                    self.assertTrue(
                        unavailable_reasons(
                            self.root, dict(self.status, **{field: result})
                        )
                    )
        for name in ("iceberg-base", "iceberg-head"):
            path = self.root / name / "execution-coverage.json"
            path.unlink()
            with self.subTest(name=name):
                self.assertTrue(unavailable_reasons(self.root, self.status))
            path.write_text("{}")

    def test_failed_base_does_not_use_partial_reports_or_successful_lookup(self):
        self.status.update(
            RUN_BASE="true", BASE_RESULT="failure", BASE_OUTCOME="skipped"
        )
        errors = unavailable_reasons(self.root, self.status)
        self.assertIn("Base build/tests: failure", errors)
        self.assertIn("A complete, validated baseline is unavailable", errors)

    def test_bootstrap_still_requires_a_complete_successful_head(self):
        (self.root / "iceberg-base/execution-coverage.json").unlink()
        self.status.update(INITIAL_BASELINE="true", BASELINE_OUTCOME="failure")
        self.assertEqual([], unavailable_reasons(self.root, self.status))
        self.status["UPSTREAM_RESULT"] = "failure"
        self.assertTrue(unavailable_reasons(self.root, self.status))

    def test_failed_build_exports_explicit_diagnostics_and_preserves_base(self):
        (self.root / "iceberg-head/execution-coverage.json").unlink()
        (self.root / "iceberg-diagnostics").mkdir()
        log = self.root / "iceberg-diagnostics/native-build.log"
        log.write_text("dependency download failed")
        identity = self.root / "iceberg-base/baseline-identity.json"
        identity.write_text('{"kind": "base"}')
        self.status.update(UPSTREAM_RESULT="failure", HEAD_OUTCOME="skipped")
        summary = self.root / "summary.md"
        result = subprocess.run(
            [sys.executable, str(SCRIPT)],
            cwd=self.root,
            env=dict(os.environ, **self.status, GITHUB_STEP_SUMMARY=str(summary)),
            text=True,
            capture_output=True,
        )
        self.assertEqual(1, result.returncode, result.stderr)
        self.assertIn("comparison could not run", result.stdout)
        self.assertIn("Upstream head build/tests: failure", summary.read_text())
        self.assertNotIn("Traceback", result.stderr)
        diagnostic = json.loads((self.root / "iceberg-comparison.json").read_text())
        self.assertFalse(diagnostic["comparison_performed"])
        self.assertTrue(diagnostic["validation_errors"])
        self.assertTrue((self.root / "iceberg-comparison.csv").is_file())
        self.assertEqual('{"kind": "base"}', identity.read_text())
        self.assertEqual("dependency download failed", log.read_text())


if __name__ == "__main__":
    unittest.main()
