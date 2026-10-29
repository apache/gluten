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
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch
from urllib.request import Request
import zipfile
import xml.etree.ElementTree as ET

SCRIPT = (
    Path(__file__).resolve().parents[4]
    / ".github/workflows/util/iceberg-upstream/baseline.py"
)
sys.path.insert(0, str(SCRIPT.parent))
SPEC = importlib.util.spec_from_file_location("baseline", SCRIPT)
baseline = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(baseline)

BASE, TESTED, HEAD, TREE = (character * 40 for character in "bcte")
INPUTS = "f" * 64
KEY = baseline.artifact_key(INPUTS, "harness", "native-digest", "test-digest")
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
            "kind": "head",
            "revision": TESTED,
            "tree": TREE,
            "inputs": INPUTS,
            "harness": "harness",
            "harness_revision": TESTED,
            "native_image": "native-digest",
            "test_image": "test-digest",
            "shard_count": 5,
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
                "parents": [{"sha": BASE}, {"sha": HEAD}],
            },
        }
        self.fingerprints = {TREE: INPUTS}
        self.job_results = []
        for kind, prefix in (
            ("head", "iceberg-upstream-test"),
            ("base", "iceberg-base-test"),
        ):
            for name in ["native / build", "build"] + [
                f"iceberg-upstream-test (3.5, {kind}, {shard})" for shard in range(5)
            ]:
                self.job_results.append(
                    {
                        "name": f"{prefix} / {name}",
                        "status": "completed",
                        "conclusion": "success",
                    }
                )
        self.artifacts = [self.artifact]
        self.report = copy.deepcopy(REPORT)

    def tree(self, revision):
        return self.commits[revision]

    def inputs(self, tree):
        return self.fingerprints[tree]

    def jobs(self, run_id, attempt):
        assert (run_id, attempt) == (123, 1)
        return self.job_results

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
        baseline.verify_source(self, self.artifact, self.identity, KEY, INPUTS, "main")


class BaselineTest(unittest.TestCase):
    def setUp(self):
        inputs = patch.object(baseline, "git_inputs", return_value=INPUTS)
        self.inputs = inputs.start()
        self.addCleanup(inputs.stop)

    def test_changed_harness_runs_on_base_without_copying_production_changes(self):
        namespace = "{http://maven.apache.org/POM/4.0.0}"
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            base, head = root / "base", root / "head"
            for checkout, version in ((base, "base"), (head, "head")):
                (checkout / "backends-velox").mkdir(parents=True)
                (checkout / "backends-velox/pom.xml").write_text(
                    '<project xmlns="http://maven.apache.org/POM/4.0.0">'
                    f"<version>{version}</version><profiles>"
                    f"<profile><id>iceberg</id><properties><production>{version}"
                    "</production></properties></profile>"
                    "<profile><id>iceberg-upstream-test</id><properties>"
                    f"<observer>{version}</observer></properties></profile>"
                    "</profiles></project>"
                )
                (checkout / "production.java").write_text(version)
                listener = (
                    checkout / "backends-velox/src-iceberg-upstream/Observer.java"
                )
                listener.parent.mkdir(parents=True)
                listener.write_text(version)
            stale = base / "backends-velox/src-iceberg-upstream/RemovedObserver.java"
            stale.touch()
            key = baseline.artifact_key(
                INPUTS, baseline.harness_hash(head), "native", "jdk"
            )
            source = Source()
            # Even a valid saved base result cannot be reused with a changed harness.
            with self.assertRaisesRegex(ValueError, "No compatible, verified baseline"):
                baseline.restore(root / "reports", key, BASE, "main", source)
            baseline.overlay_harness(base, head)
            self.assertEqual("base", (base / "production.java").read_text())
            pom = ET.parse(base / "backends-velox/pom.xml")
            self.assertEqual("base", pom.findtext(namespace + "version"))
            profiles = {
                item.findtext(namespace + "id"): item
                for item in pom.findall(namespace + "profiles/" + namespace + "profile")
            }
            self.assertEqual(
                "base",
                profiles["iceberg"].findtext(
                    namespace + "properties/" + namespace + "production"
                ),
            )
            self.assertEqual(
                "head",
                profiles["iceberg-upstream-test"].findtext(
                    namespace + "properties/" + namespace + "observer"
                ),
            )
            self.assertEqual(
                "head",
                (
                    base / "backends-velox/src-iceberg-upstream/Observer.java"
                ).read_text(),
            )
            self.assertFalse(stale.exists())
            # Both the build runner and shard runners apply the same overlay.
            first_pom = (base / "backends-velox/pom.xml").read_bytes()
            baseline.overlay_harness(base, head)
            self.assertEqual(first_pom, (base / "backends-velox/pom.xml").read_bytes())
            with self.assertRaisesRegex(ValueError, "separate checkout"):
                baseline.overlay_harness(base, base)

    def test_initial_baseline_requires_a_target_tree_without_any_harness(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            git = ["git", "-C", str(root)]
            subprocess.run(git + ["init", "-q"], check=True)
            commit = git + [
                "-c",
                "user.name=CI test",
                "-c",
                "user.email=ci@example.invalid",
                "-c",
                "commit.gpgsign=false",
                "commit",
                "--allow-empty",
                "-qm",
                "fixture",
            ]
            subprocess.run(commit, check=True)
            original = Path.cwd()
            try:
                os.chdir(root)
                self.assertTrue(baseline.initial_baseline("HEAD"))
                with self.assertRaises(subprocess.CalledProcessError):
                    baseline.initial_baseline("nonexistent-revision")
                # An existing harness still disallows bootstrap if its workflow
                # was renamed or temporarily removed from main.
                harness = root / "backends-velox/src-iceberg-upstream/Observer.java"
                harness.parent.mkdir(parents=True)
                harness.touch()
                subprocess.run(git + ["add", "."], check=True)
                subprocess.run(commit, check=True)
                self.assertFalse(baseline.initial_baseline("HEAD"))
                self.assertTrue(baseline.initial_baseline("HEAD^"))
                harness.unlink()
                workflow = root / ".github/workflows/iceberg_spark_ut.yml"
                workflow.parent.mkdir(parents=True)
                workflow.touch()
                subprocess.run(git + ["add", "."], check=True)
                subprocess.run(commit, check=True)
                self.assertFalse(baseline.initial_baseline("HEAD"))
            finally:
                os.chdir(original)

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
                "GITHUB_SHA": TESTED,
                "ICEBERG_SHARD_COUNT": "5",
            }
            with patch.dict(os.environ, env), patch.object(
                baseline, "git_tree", return_value=TREE
            ), patch.object(baseline, "harness_hash", return_value="harness"):
                baseline.record(
                    root, KEY, TESTED, "head", "native-digest", "test-digest"
                )
                identity = json.loads((root / "baseline-identity.json").read_text())
                self.assertEqual(TESTED, identity["revision"])
                self.assertEqual(99, identity["pull_request"])
                self.assertEqual("head", identity["kind"])
                self.assertEqual(INPUTS, identity["inputs"])
                baseline.record(root, KEY, BASE, "base", "native-digest", "test-digest")
                identity = json.loads((root / "baseline-identity.json").read_text())
                self.assertEqual("base", identity["kind"])
                self.assertEqual(BASE, identity["revision"])
                self.assertEqual(TESTED, identity["harness_revision"])
                with self.assertRaisesRegex(ValueError, "tested inputs and runtimes"):
                    baseline.record(root, KEY, BASE, "base", "old-image", "test-digest")
                invalid = copy.deepcopy(REPORT)
                invalid["tests"][0]["status"] = "PASSED_NATIVE"
                (root / "execution-coverage.json").write_text(json.dumps(invalid))
                with self.assertRaisesRegex(ValueError, "Unproven native"):
                    baseline.record(
                        root, KEY, TESTED, "head", "native-digest", "test-digest"
                    )

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
        with self.assertRaisesRegex(ValueError, "target branch"):
            source.verify()

    def test_successful_base_survives_a_failed_unmerged_and_updated_pr_head(self):
        source = Source()
        source.identity.update(kind="base", revision=BASE)
        source.pr.update(merged=False, merge_commit_sha=None)
        # The source PR can have new commits since the saved run. The run's
        # original merge parents, not the latest head, identify its tested base.
        source.pr["head"]["sha"] = "d" * 40
        for job in source.job_results:
            if job["name"].startswith("iceberg-upstream-test / "):
                job["conclusion"] = "failure"
        source.verify()
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            baseline.restore(root, KEY, BASE, "main", source)
            saved = json.loads((root / "baseline-identity.json").read_text())
            self.assertEqual("base", saved["kind"])
            self.assertTrue((root / "fallback.csv").is_file())

    def test_every_base_build_and_shard_must_succeed_in_the_recorded_attempt(self):
        for job_index in range(7, 14):
            for failure in (
                "failure",
                "cancelled",
                "skipped",
                "missing",
                "duplicate",
                "in_progress",
            ):
                source = Source()
                source.identity.update(kind="base", revision=BASE)
                if failure == "missing":
                    del source.job_results[job_index]
                elif failure == "duplicate":
                    source.job_results.append(source.job_results[job_index].copy())
                elif failure == "in_progress":
                    source.job_results[job_index]["status"] = failure
                else:
                    source.job_results[job_index]["conclusion"] = failure
                with self.subTest(
                    job=job_index, failure=failure
                ), self.assertRaisesRegex(ValueError, "successful job"):
                    source.verify()

    def test_base_revision_and_runtime_identities_are_not_accepted_on_trust(self):
        for field in ("harness", "native_image", "test_image"):
            source = Source()
            source.identity.update(kind="base", revision=BASE)
            source.identity[field] = "incompatible"
            with self.subTest(field=field), self.assertRaisesRegex(
                ValueError, "harness or image"
            ):
                source.verify()
        source = Source()
        source.identity.update(kind="base", revision=TESTED)
        with self.assertRaisesRegex(ValueError, "base revision"):
            source.verify()
        source = Source()
        source.identity.update(kind="base", revision=BASE)
        source.fingerprints[TREE] = "changed-production-inputs"
        with self.assertRaisesRegex(ValueError, "tested inputs"):
            source.verify()
        source = Source()
        source.identity.update(kind="base", revision=BASE, shard_count=1)
        with self.assertRaisesRegex(ValueError, "shard count"):
            source.verify()

    def test_base_result_from_a_target_branch_run_tests_its_first_parent(self):
        source = Source()
        source.identity.update(kind="base", revision=BASE)
        source.run.update(
            event="workflow_dispatch",
            head_sha=TESTED,
            head_branch="main",
            head_repository={"full_name": source.repository},
        )
        source.verify()
        source.identity["revision"] = TESTED
        with self.assertRaisesRegex(ValueError, "run's revision"):
            source.verify()

    def test_older_report_is_reusable_after_unrelated_commits(self):
        source = Source()
        current = "d" * 40
        source.commits[current] = {"tree": {"sha": "e" * 40}}
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            baseline.restore(root, KEY, current, "main", source)
            saved = json.loads((root / "baseline-identity.json").read_text())
            self.assertEqual(current, saved["compared_revision"])
            self.assertEqual(TESTED, saved["revision"])
            self.inputs.assert_called_with(current)
        self.inputs.return_value = "changed-inputs"
        with tempfile.TemporaryDirectory() as temporary, self.assertRaisesRegex(
            ValueError, "base inputs"
        ):
            baseline.restore(Path(temporary), KEY, current, "main", source)

    def test_truncated_remote_tree_cannot_establish_input_compatibility(self):
        api = baseline.GitHub("apache/gluten")
        with patch.object(api, "request", return_value={"truncated": True, "tree": []}):
            with self.assertRaisesRegex(ValueError, "truncated"):
                api.inputs(TREE)

    def test_job_lookup_paginates_only_the_recorded_attempt(self):
        api = baseline.GitHub("apache/gluten")
        with patch.object(
            api,
            "request",
            side_effect=[
                {"jobs": [{"name": "first-page"}] * 100},
                {"jobs": [{"name": "last-page"}]},
            ],
        ) as request:
            self.assertEqual(101, len(api.jobs(123, 2)))
            self.assertEqual(
                [call.args[0] for call in request.call_args_list],
                [
                    f"actions/runs/123/attempts/2/jobs?per_page=100&page={page}"
                    for page in (1, 2)
                ],
            )

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
                ValueError, "final head|target branch"
            ):
                source.verify()

    def test_identity_and_workflow_provenance_are_verified(self):
        for field, value in (
            ("key", "wrong-runtime"),
            ("repository", "other/repo"),
            ("run_id", 999),
            ("run_attempt", "1"),
            ("tree", "different"),
            ("inputs", "different"),
            ("kind", "unknown"),
        ):
            source = Source()
            source.identity[field] = value
            with self.subTest(field=field), self.assertRaisesRegex(
                ValueError, "identity|tested inputs"
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

    def test_result_from_the_x86_backend_workflow_is_a_baseline(self):
        source = Source()
        source.run["path"] = ".github/workflows/velox_backend_x86.yml"
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


class InputFingerprintTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.git = ["git", "-C", str(self.root)]
        subprocess.run(self.git + ["init", "-q"], check=True)
        self.original = Path.cwd()
        os.chdir(self.root)
        self.addCleanup(os.chdir, self.original)

    def tree(self):
        subprocess.run(self.git + ["add", "."], check=True)
        return subprocess.check_output(self.git + ["write-tree"], text=True).strip()

    def write(self, name, data):
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(data)

    def test_only_known_non_runtime_changes_preserve_the_fingerprint(self):
        self.write("pom.xml", "production dependency versions")
        first = self.tree()
        fingerprint = baseline.git_inputs(first)
        for name in (
            "docs/developers/Guide.md",
            "README.md",
            "AGENTS.md",
            ".github/ISSUE_TEMPLATE/bug.yml",
            ".devcontainer/devcontainer.json",
            ".github/workflows/unrelated.yml",
        ):
            self.write(name, "unrelated change")
            with self.subTest(name=name):
                self.assertNotEqual(first, self.tree())
                self.assertEqual(fingerprint, baseline.git_inputs(self.tree()))
        for name in (
            "pom.xml",
            "backends-velox/pom.xml",
            "gluten-core/src/main/Runtime.scala",
            "gluten-ui/src/main/Listener.scala",
            "gluten-iceberg/src/main/Scan.java",
            "gluten-substrait/src/main/plan.proto",
            "gluten-arrow/pom.xml",
            "backends-velox/src/test/resources/schema.md",
            "shims/spark35/pom.xml",
            "cpp/velox/Scan.cc",
            "ep/build-velox/src/velox.patch",
            ".gitmodules",
            "dev/vcpkg/vcpkg.json",
            "dev/builddeps-veloxbe.sh",
            "build/mvn",
            ".mvn/maven.config",
            "new-module/src/main/Dependency.java",
            ".github/workflows/util/build.sh",
            ".github/workflows/velox_native_build.yml",
            "backends-velox/src-iceberg-upstream/test/java/Observer.java",
        ):
            self.write(name, "runtime change")
            changed = baseline.git_inputs(self.tree())
            with self.subTest(name=name):
                self.assertNotEqual(fingerprint, changed)
            fingerprint = changed

    def test_renames_deletions_modes_symlinks_and_submodules_are_inputs(self):
        self.write("build/mvn", "build script")
        previous = baseline.git_inputs(self.tree())
        (self.root / "build/mvn").chmod(0o755)
        changed = baseline.git_inputs(self.tree())
        self.assertNotEqual(previous, changed)
        previous = changed
        (self.root / "build/mvn").rename(self.root / "build/renamed")
        changed = baseline.git_inputs(self.tree())
        self.assertNotEqual(previous, changed)
        previous = changed
        (self.root / "build/renamed").unlink()
        self.assertNotEqual(previous, baseline.git_inputs(self.tree()))
        for kind, mode in (("blob", "120000"), ("commit", "160000")):
            entry = {"path": "dependency", "mode": mode, "type": kind, "sha": "a" * 40}
            previous = baseline.input_fingerprint([entry])
            entry["sha"] = "b" * 40
            self.assertNotEqual(previous, baseline.input_fingerprint([entry]))

    def test_local_and_api_trees_use_the_same_fingerprint(self):
        self.write("build/mvn", "script")
        (self.root / "linked-input").symlink_to("build/mvn")
        self.write("docs/ignored.md", "doc")
        tree = self.tree()
        entries = []
        for record in subprocess.check_output(
            self.git + ["ls-tree", "-rz", tree]
        ).split(b"\0"):
            if record:
                metadata, path = record.decode().split("\t", 1)
                mode, kind, sha = metadata.split()
                entries.append(dict(path=path, mode=mode, type=kind, sha=sha))
        # GitHub's recursive response also contains intermediate trees.
        entries.append(dict(path="build", mode="040000", type="tree", sha="c" * 40))
        api = baseline.GitHub("apache/gluten")
        with patch.object(
            api, "request", return_value={"truncated": False, "tree": entries}
        ):
            self.assertEqual(baseline.git_inputs(tree), api.inputs(tree))


if __name__ == "__main__":
    unittest.main()
