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

import csv
import json
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(
    0,
    str(
        Path(__file__).resolve().parents[4] / ".github/workflows/util/iceberg-upstream"
    ),
)
from merge import merge, shard_for
from summarize import REPORT_VERSION, write_reports


class MergeTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)
        self.rows = []
        for index in range(2):
            name = next(
                f"query{i}"
                for i in range(100)
                if shard_for("org.apache.iceberg.TestScan", f"query{i}", 2) == index
            )
            rows = [
                {
                    "class": "org.apache.iceberg.TestScan",
                    "name": f"{name}()[{invocation}]",
                    "status": "PASSED_FALLBACK",
                    "fallback": ["BatchScanExec"],
                    "parameters": {"format": "parquet"},
                }
                for invocation in range(1, 3)
            ]
            self.rows.extend(rows)
            shard = self.directory / f"shard{index}"
            shard.mkdir()
            (shard / "shard.json").write_text(json.dumps({"index": index, "count": 2}))
            (shard / "execution-coverage.json").write_text(
                json.dumps({"version": REPORT_VERSION, "tests": rows})
            )

    def test_merges_every_invocation_and_exports_complete_csvs(self):
        results = merge(self.directory, 2)
        self.assertCountEqual(self.rows, results)
        self.assertEqual(0, write_reports(self.directory, results))
        # The aggregate must not be read as another input shard on a later invocation.
        self.assertEqual(results, merge(self.directory, 2))
        with (self.directory / "fallback.csv").open() as source:
            self.assertEqual(4, len(list(csv.DictReader(source))))

    def test_missing_shard_cannot_produce_a_partial_success(self):
        (self.directory / "shard1/execution-coverage.json").unlink()
        with self.assertRaisesRegex(ValueError, "Expected 2 shard reports"):
            merge(self.directory, 2)

    def test_duplicate_or_mismatched_shard_metadata_is_rejected(self):
        for metadata in ({"index": 0, "count": 2}, {"index": 1, "count": 4}):
            with self.subTest(metadata=metadata):
                (self.directory / "shard1/shard.json").write_text(json.dumps(metadata))
                with self.assertRaisesRegex(ValueError, "Invalid or duplicate shard"):
                    merge(self.directory, 2)

    def test_wrong_shard_and_duplicate_results_are_rejected(self):
        path = self.directory / "shard1/execution-coverage.json"
        report = json.loads(path.read_text())
        report["tests"] = [
            dict(self.rows[0], name=self.rows[0]["name"].replace("[1]", "[3]"))
        ]
        path.write_text(json.dumps(report))
        with self.assertRaisesRegex(ValueError, "wrong shard"):
            merge(self.directory, 2)
        report["tests"] = [self.rows[0]]
        path.write_text(json.dumps(report))
        with self.assertRaisesRegex(ValueError, "Duplicate test"):
            merge(self.directory, 2)

    def test_empty_and_incompatible_reports_are_rejected(self):
        path = self.directory / "shard1/execution-coverage.json"
        for report in (
            {"version": REPORT_VERSION, "tests": []},
            {"version": 1, "tests": self.rows},
        ):
            with self.subTest(report=report):
                path.write_text(json.dumps(report))
                with self.assertRaisesRegex(ValueError, "Invalid or empty"):
                    merge(self.directory, 2)

    def test_assignment_matches_java_and_limits_repeated_fixtures(self):
        # These same reference values are checked by IcebergQueryTestFilterTest.
        self.assertEqual(
            0, shard_for("org.apache.iceberg.spark.sql.TestSelect", "testSelect", 1)
        )
        self.assertEqual(
            5, shard_for("org.apache.iceberg.spark.sql.TestSelect", "testSelect", 12)
        )
        assignments = {
            shard_for("org.apache.iceberg.spark.sql.TestSelect", f"test{i}", 12)
            for i in range(100)
        }
        self.assertEqual(4, len(assignments))
        with self.assertRaisesRegex(ValueError, "positive"):
            merge(self.directory, 0)


if __name__ == "__main__":
    unittest.main()
