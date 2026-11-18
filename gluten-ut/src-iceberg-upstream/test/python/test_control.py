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

import importlib.util
from pathlib import Path
import sys
import unittest

SCRIPT = (
    Path(__file__).resolve().parents[4]
    / ".github/workflows/util/iceberg-upstream/control.py"
)
sys.path.insert(0, str(SCRIPT.parent))
SPEC = importlib.util.spec_from_file_location("control", SCRIPT)
control = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(control)


def row(status, name="read()[1]"):
    return {
        "class": "org.apache.iceberg.TestScan",
        "name": name,
        "status": status,
        "parameters": {"vectorized": "false"},
    }


class ControlTest(unittest.TestCase):
    def test_selection_deduplicates_failed_methods_and_ignores_passes(self):
        self.assertEqual(
            "org.apache.iceberg.TestScan#read",
            control.selection(
                [
                    row("FAILED"),
                    row("FAILED", "read()[2]"),
                    row("PASSED_NATIVE", "write"),
                ]
            ),
        )
        self.assertEqual("", control.selection([row("PASSED_NATIVE")]))

    def test_only_matching_passing_vanilla_control_attributes_gluten(self):
        cases = [
            (row("PASSED_FALLBACK"), "GLUTEN"),
            (row("FAILED"), "VANILLA_ALSO_FAILS"),
            (row("SKIPPED"), "VANILLA_ALSO_FAILS"),
            (None, "CONTROL_MISSING"),
        ]
        for vanilla, expected in cases:
            result = control.attribute([row("FAILED")], [vanilla] if vanilla else [])
            self.assertEqual(expected, result[0]["failure_origin"])

    def test_parameter_mismatch_or_native_execution_is_not_a_valid_control(self):
        mismatch = row("PASSED_FALLBACK")
        mismatch["parameters"]["vectorized"] = "true"
        result = control.attribute([row("FAILED")], [mismatch])
        self.assertEqual("PARAMETER_MISMATCH", result[0]["failure_origin"])
        native = row("PASSED_NATIVE")
        native["native"] = ["IcebergScanTransformer"]
        self.assertNotEqual(
            "GLUTEN", control.attribute([row("FAILED")], [native])[0]["failure_origin"]
        )

    def test_duplicate_controls_are_rejected(self):
        with self.assertRaises(ValueError):
            control.attribute([row("FAILED")], [row("PASSED_FALLBACK")] * 2)

    def test_passing_control_does_not_certify_runtime_or_timing_failures(self):
        for error in (
            "ConditionTimeoutException: not fulfilled within 5 seconds",
            "java.lang.OutOfMemoryError",
            "java.lang.UnsatisfiedLinkError",
        ):
            failure = dict(row("FAILED"), failure=error)
            result = control.attribute([failure], [row("PASSED_FALLBACK")])
            self.assertEqual("UNVERIFIED_RUNTIME_FAILURE", result[0]["failure_origin"])


if __name__ == "__main__":
    unittest.main()
