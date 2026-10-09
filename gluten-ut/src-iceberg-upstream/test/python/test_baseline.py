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

import copy
import importlib.util
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch
from urllib.request import Request
import zipfile

SCRIPT = (
    Path(__file__).resolve().parents[4]
    / ".github/workflows/util/iceberg-upstream/baseline.py"
)
sys.path.insert(0, str(SCRIPT.parent))
SPEC = importlib.util.spec_from_file_location("baseline", SCRIPT)
baseline = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(baseline)

BASE, TESTED, HEAD, TREE = (character * 40 for character in "bcte")
KEY = baseline.artifact_key(TREE, "harness", "native-digest", "test-digest")
REPORT = {
    "version": 2,
    "tests": [{"class": "TestIceberg", "name": "read", "status": "PASSED_FALLBACK"}],
}


def archive(identity, report=REPORT):
    data = io.BytesIO()
    with zipfile.ZipFile(data, "w") as output:
        output.writestr("baseline-identity.json", json.dumps(identity))
        output.writestr("execution-coverage.json", json.dumps(report))
    return data.getvalue()


class Source:
    repository = "apache/gluten"

    def __init__(self):
        self.identity = {
            "key": KEY,
            "revision": TESTED,
            "tree": TREE,
            "repository": self.repository,
            "run_id": 123,
            "run_attempt": 1,
            "pull_request": 99,
        }
        self.artifact = {
            "id": 456,
            "name": KEY,
            "expired": False,
            "workflow_run": {"id": 123},
        }
        self.run = {
            "repository": {"full_name": self.repository},
            "head_repository": {"full_name": "contributor/gluten"},
            "head_sha": HEAD,
            "path": ".github/workflows/iceberg_spark_ut.yml",
            "status": "completed",
            # A missing comparison may fail the run while the head result is complete.
            "conclusion": "failure",
            "event": "pull_request",
        }
        self.pr = {
            "merged": True,
            "base": {"ref": "main", "repo": {"full_name": self.repository}},
            "head": {"sha": HEAD, "repo": {"full_name": "contributor/gluten"}},
            "merge_commit_sha": BASE,
        }
        self.commits = {
            BASE: {"tree": {"sha": TREE}},
            TESTED: {
                "tree": {"sha": TREE},
                "parents": [{"sha": "a" * 40}, {"sha": HEAD}],
            },
        }
        self.artifacts = [self.artifact]
        self.report = copy.deepcopy(REPORT)

    def tree(self, revision):
        return self.commits[revision]

    def request(self, path, binary=False):
        if path.startswith("actions/artifacts?"):
            return {"artifacts": self.artifacts}
        if path == "actions/artifacts/456/zip" and binary:
            return archive(self.identity, self.report)
        if path == "actions/runs/123/attempts/1":
            return self.run
        if path == "pulls/99":
            return self.pr
        raise AssertionError("Unexpected API request: " + path)

    def verify(self):
        baseline.verify_source(self, self.artifact, self.identity, KEY, TREE, "main")


class BaselineTest(unittest.TestCase):
    def test_every_comparison_input_invalidates_the_saved_result(self):
        values = [TREE, "harness-hash", "native@sha256:one", "jdk@sha256:two"]
        key = baseline.artifact_key(*values)
        for index in range(len(values)):
            changed = values.copy()
            changed[index] += "-changed"
            self.assertNotEqual(key, baseline.artifact_key(*changed))

    def test_harness_edits_invalidate_without_generated_python_cache_noise(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            harness = root / "backends-velox/src-iceberg-upstream/Observer.java"
            harness.parent.mkdir(parents=True)
            harness.write_text("original observer")
            original = baseline.harness_hash(root)
            generated = harness.parent / "__pycache__/observer.pyc"
            generated.parent.mkdir()
            generated.write_bytes(b"generated locally")
            self.assertEqual(original, baseline.harness_hash(root))
            harness.write_text("new native execution proof")
            self.assertNotEqual(original, baseline.harness_hash(root))

    def test_complete_head_is_recorded_without_a_baseline_but_invalid_evidence_is_not(
        self,
    ):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "execution-coverage.json").write_text(json.dumps(REPORT))
            event = root / "event.json"
            event.write_text(json.dumps({"pull_request": {"number": 99}}))
            env = {
                "GITHUB_EVENT_PATH": str(event),
                "GITHUB_REPOSITORY": "apache/gluten",
                "GITHUB_RUN_ID": "123",
                "GITHUB_RUN_ATTEMPT": "1",
            }
            with patch.dict(os.environ, env), patch.object(
                baseline, "git_tree", return_value=TREE
            ):
                baseline.record(root, KEY, TESTED)
                identity = json.loads((root / "baseline-identity.json").read_text())
                self.assertEqual(TESTED, identity["revision"])
                self.assertEqual(99, identity["pull_request"])
                invalid = copy.deepcopy(REPORT)
                invalid["tests"][0]["status"] = "PASSED_NATIVE"
                (root / "execution-coverage.json").write_text(json.dumps(invalid))
                with self.assertRaisesRegex(ValueError, "Unproven native"):
                    baseline.record(root, KEY, TESTED)

    def test_squash_merge_reuses_the_tested_tree_despite_a_different_commit_sha(self):
        source = Source()
        self.assertNotEqual(BASE, TESTED)
        source.verify()
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            baseline.restore(root, KEY, BASE, "main", source)
            self.assertTrue((root / "fallback.csv").exists())
            identity = json.loads((root / "baseline-identity.json").read_text())
            self.assertEqual(BASE, identity["compared_revision"])
            self.assertEqual(TESTED, identity["revision"])

    def test_a_changed_merge_tree_or_final_pr_head_cannot_be_promoted(self):
        source = Source()
        source.commits[BASE]["tree"]["sha"] = "d" * 40
        with self.assertRaisesRegex(ValueError, "merged different code"):
            source.verify()
        source = Source()
        source.pr["head"]["sha"] = "d" * 40
        with self.assertRaisesRegex(ValueError, "final head"):
            source.verify()
        source = Source()
        source.commits[TESTED]["parents"][1]["sha"] = "d" * 40
        with self.assertRaisesRegex(ValueError, "final head"):
            source.verify()

    def test_unmerged_or_wrong_target_pr_is_not_a_baseline(self):
        for change in ("unmerged", "branch", "repository"):
            source = Source()
            if change == "unmerged":
                source.pr["merged"] = False
            elif change == "branch":
                source.pr["base"]["ref"] = "other"
            else:
                source.pr["base"]["repo"]["full_name"] = "other/repo"
            with self.subTest(change=change), self.assertRaisesRegex(
                ValueError, "final head"
            ):
                source.verify()

    def test_identity_and_workflow_provenance_are_verified(self):
        for field, value in (
            ("key", "wrong-runtime"),
            ("repository", "other/repo"),
            ("run_id", 999),
            ("run_attempt", "1"),
            ("tree", "different"),
        ):
            source = Source()
            source.identity[field] = value
            with self.subTest(field=field), self.assertRaisesRegex(
                ValueError, "identity"
            ):
                source.verify()
        for field, value in (
            ("path", ".github/workflows/other.yml"),
            ("status", "in_progress"),
            ("conclusion", "cancelled"),
        ):
            source = Source()
            source.run[field] = value
            with self.subTest(field=field), self.assertRaisesRegex(
                ValueError, "completed Iceberg"
            ):
                source.verify()

    def test_manual_main_result_can_seed_baseline_without_a_passing_comparison(self):
        source = Source()
        source.run.update(
            event="workflow_dispatch",
            head_sha=TESTED,
            head_branch="main",
            head_repository={"full_name": source.repository},
        )
        source.verify()
        source.run["head_branch"] = "unmerged-branch"
        with self.assertRaisesRegex(ValueError, "target-branch run"):
            source.verify()

    def test_missing_expired_and_unverified_reports_cannot_supply_a_baseline(self):
        for condition in ("missing", "expired", "unverified", "unmerged"):
            source = Source()
            if condition == "missing":
                source.artifacts = []
            elif condition == "expired":
                source.artifact["expired"] = True
            elif condition == "unverified":
                source.report["tests"][0]["status"] = "FAILED"
            else:
                source.pr["merged"] = False
            with self.subTest(
                condition=condition
            ), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                with self.assertRaisesRegex(
                    ValueError, "No compatible, verified baseline"
                ):
                    baseline.restore(root, KEY, BASE, "main", source)
                self.assertFalse((root / "execution-coverage.json").exists())

    def test_downloaded_archive_cannot_extract_files_or_supply_extra_payloads(self):
        data = io.BytesIO(archive(Source().identity))
        with zipfile.ZipFile(data, "a") as output:
            output.writestr("../compare.py", "untrusted code")
        with self.assertRaisesRegex(ValueError, "Unexpected baseline artifact"):
            baseline.read_artifact(data.getvalue())

    def test_download_redirect_does_not_forward_github_token_to_artifact_storage(self):
        request = Request(
            "https://api.github.com/artifact",
            headers={"Authorization": "Bearer secret"},
        )
        redirected = baseline.ArtifactRedirect().redirect_request(
            request, None, 302, "Found", {}, "https://storage.example/artifact.zip"
        )
        self.assertIsNone(redirected.get_header("Authorization"))


if __name__ == "__main__":
    unittest.main()
