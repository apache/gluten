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
import itertools
from pathlib import Path
import unittest

SCRIPT = (
    Path(__file__).resolve().parents[4]
    / ".github/workflows/util/workflow-concurrency.py"
)
SPEC = importlib.util.spec_from_file_location("concurrency", SCRIPT)
concurrency = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(concurrency)


class WorkflowConcurrencyTest(unittest.TestCase):
    def test_pr_workflows_budget_all_dependencies_and_matrix_waves(self):
        root = SCRIPT.parents[3]
        for workflow in (
            "velox_backend_x86",
            "velox_backend_x86_integration",
            "velox_backend_enhanced",
            "velox_backend_arm",
            "iceberg_spark_ut",
        ):
            with self.subTest(workflow=workflow):
                weights, edges = concurrency.expand(
                    ".github/workflows/" + workflow + ".yml",
                    lambda path: (root / path).read_text(),
                    job_weight=concurrency.pr_timeout,
                )
                self.assertLessEqual(concurrency.critical_path(weights, edges), 80)
        self.assertEqual(
            120,
            concurrency.pr_timeout(
                {
                    "timeout-minutes": 60,
                    "strategy": {"max-parallel": 2, "matrix": {"shard": [0, 1, 2]}},
                }
            ),
        )
        with self.assertRaisesRegex(ValueError, "Unresolved"):
            concurrency.pr_timeout(
                {
                    "timeout-minutes": 60,
                    "strategy": {
                        "matrix": {"shard": "${{ needs.discovery.outputs.shards }}"}
                    },
                }
            )

    def test_called_matrix_uses_explicit_shards_and_defaults(self):
        files = {
            ".github/workflows/parent.yml": """
jobs:
  first:
    uses: ./.github/workflows/child.yml
    with:
      shards: '[0, 1, 2]'
  second:
    uses: ./.github/workflows/child.yml
""",
            ".github/workflows/child.yml": """
on:
  workflow_call:
    inputs:
      shards:
        default: '[0]'
jobs:
  build: {}
  test:
    needs: build
    strategy:
      max-parallel: 4
      matrix:
        shard: '${{ fromJSON(inputs.shards) }}'
""",
        }
        self.assertEqual(
            4,
            concurrency.peak(
                *concurrency.expand(".github/workflows/parent.yml", files.__getitem__)
            ),
        )

    def test_slow_independent_matrix_overlaps_later_stages(self):
        weights = {"build": 1, "slow": 8, "test": 12, "gate": 1}
        self.assertEqual(
            20, concurrency.peak(weights, {("build", "test"), ("test", "gate")})
        )
        weights["test"] = 13
        self.assertEqual(
            21, concurrency.peak(weights, {("build", "test"), ("test", "gate")})
        )

    def test_nested_calls_count_as_part_of_the_caller(self):
        files = {
            ".github/workflows/parent.yml": """
jobs:
  slow:
    strategy:
      matrix:
        shard: [1, 2, 3, 4, 5, 6, 7, 8, 9]
  child:
    uses: ./.github/workflows/child.yml
  gate:
    needs: child
""",
            ".github/workflows/child.yml": """
jobs:
  build: {}
  test:
    needs: build
    strategy:
      max-parallel: 12
      matrix:
        shard: '${{ inputs.shards }}'
""",
        }
        self.assertEqual(
            21,
            concurrency.peak(
                *concurrency.expand(".github/workflows/parent.yml", files.__getitem__)
            ),
        )

    def test_static_cartesian_products_excludes_and_dynamic_caps(self):
        self.assertEqual(
            5,
            concurrency.matrix_width(
                {
                    "matrix": {
                        "jdk": [8, 17],
                        "spark": [3, 4, 5],
                        "exclude": [{"jdk": 8, "spark": 5}],
                    }
                }
            ),
        )
        self.assertEqual(
            2,
            concurrency.matrix_width(
                {"matrix": {"shard": "${{ inputs.shards }}"}, "max-parallel": 2}
            ),
        )
        with self.assertRaisesRegex(ValueError, "Dynamic"):
            concurrency.matrix_width({"matrix": {"shard": "${{ inputs.shards }}"}})

    def test_weighted_bound_agrees_with_exhaustive_small_dags(self):
        # Enumerate all DAGs on four ordered nodes, and all possible simultaneous
        # job sets. This catches overlap that a per-stage count would miss.
        weights = {0: 3, 1: 1, 2: 4, 3: 2}
        possible = list(itertools.combinations(weights, 2))
        for bits in itertools.product((False, True), repeat=len(possible)):
            edges = {edge for edge, present in zip(possible, bits) if present}
            closure = set(edges)
            for middle in weights:
                closure |= {
                    (a, b)
                    for a in weights
                    for b in weights
                    if (a, middle) in closure and (middle, b) in closure
                }
            expected = 0
            for mask in itertools.product((False, True), repeat=len(weights)):
                subset = {n for n, present in zip(weights, mask) if present}
                if not any(a in subset and b in subset for a, b in closure):
                    expected = max(expected, sum(weights[n] for n in subset))
            self.assertEqual(expected, concurrency.peak(weights, edges))


if __name__ == "__main__":
    unittest.main()
