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
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

SCRIPT = Path(__file__).resolve().parents[4] / "dev/ci-velox-key.py"
SPEC = importlib.util.spec_from_file_location("velox_key", SCRIPT)
key = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(key)
BASE = "apache/gluten@sha256:" + "a" * 64
REVISION = "b" * 40


class VeloxKeyTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        subprocess.run(["git", "init", "-q", str(self.root)], check=True)
        for name in key.INPUTS:
            if name in ("ep/build-velox/src", "dev/vcpkg"):
                name += "/input.txt"
            self.write(name, "original\n")
        self.write(
            "ep/build-velox/src/get-velox.sh",
            "VELOX_REPO=https://github.com/IBM/velox.git\n"
            "VELOX_BRANCH=release\nVELOX_ENHANCED_BRANCH=enhanced\n",
        )
        self.write(
            key.CI_SCRIPT,
            "./dev/builddeps-veloxbe.sh --enable_vcpkg=ON --build_tests=OFF \\\n"
            "  --enable_s3=ON\n",
        )
        subprocess.run(["git", "-C", str(self.root), "add", "."], check=True)

    def write(self, name, text):
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")

    def info(self, base=BASE, revision=REVISION):
        return key.metadata(self.root, base, revision)

    def test_identity_is_repeatable_and_independent_of_checkout_path(self):
        first = self.info()
        with tempfile.TemporaryDirectory() as directory:
            subprocess.run(
                [
                    "git",
                    "-C",
                    str(self.root),
                    "checkout-index",
                    "--all",
                    "--prefix=" + Path(directory).as_posix() + "/",
                ],
                check=True,
            )
            subprocess.run(["git", "init", "-q", directory], check=True)
            subprocess.run(["git", "-C", directory, "add", "."], check=True)
            self.assertEqual(first, key.metadata(Path(directory), BASE, REVISION))

    def test_each_build_input_invalidates_the_key(self):
        before = self.info()["key"]
        for name in self.info()["inputs"]:
            path = self.root / name
            original = path.read_bytes()
            with self.subTest(name=name):
                path.write_bytes(original + b"\n# changed\n")
                self.assertNotEqual(before, self.info()["key"])
            path.write_bytes(original)

    def test_new_tracked_vcpkg_port_invalidates_the_key(self):
        before = self.info()["key"]
        self.write("dev/vcpkg/ports/new/portfile.cmake", "port changed\n")
        subprocess.run(["git", "-C", str(self.root), "add", "."], check=True)
        self.assertNotEqual(before, self.info()["key"])

    def test_generated_build_outputs_are_not_hashed(self):
        before = self.info()
        self.write("dev/vcpkg/vcpkg_installed/x64-linux-avx/lib/lib.a", "binary\n")
        self.write("ep/build-velox/build/velox_ep/_build/release/lib.a", "binary\n")
        self.assertEqual(before, self.info())

    def test_base_digest_and_resolved_revision_invalidate_the_key(self):
        before = self.info()["key"]
        self.assertNotEqual(before, self.info(base=BASE[:-64] + "c" * 64)["key"])
        self.assertNotEqual(before, self.info(revision="d" * 40)["key"])

    def test_flags_are_read_from_the_ci_script(self):
        flags = self.info()["flags"]
        self.assertEqual(
            flags, ["--enable_vcpkg=ON", "--build_tests=OFF", "--enable_s3=ON"]
        )
        command = key.native_command(Path("/checkout"), self.info(), "build_gluten_cpp")
        self.assertEqual(command[2:5], flags)
        self.assertEqual(command[-1], "build_gluten_cpp")
        self.assertIn("--velox_branch=" + REVISION, command)

    def test_missing_or_deleted_inputs_are_refused(self):
        (self.root / "dev/vcpkg/input.txt").unlink()
        with self.assertRaises(OSError):
            self.info()

    def test_mutable_base_tags_and_invalid_commits_are_refused(self):
        with self.assertRaisesRegex(ValueError, "digest"):
            self.info(base="apache/gluten:latest")
        with self.assertRaisesRegex(ValueError, "SHA-1"):
            self.info(revision="short")

    def test_shell_expansions_in_flags_or_refs_are_not_executed(self):
        self.write(
            key.CI_SCRIPT, "./dev/builddeps-veloxbe.sh --enable_vcpkg=ON $(bad)\n"
        )
        with self.assertRaisesRegex(ValueError, "literal"):
            self.info()
        self.write(
            "ep/build-velox/src/get-velox.sh", "VELOX_REPO=$(bad)\nVELOX_BRANCH=main\n"
        )
        with self.assertRaisesRegex(ValueError, "literal"):
            key.velox_reference(self.root)

    def test_remote_ref_resolution_uses_the_peeled_tag_commit(self):
        with patch.object(
            subprocess,
            "check_output",
            return_value=(
                "a" * 40
                + "\trefs/tags/release\n"
                + REVISION
                + "\trefs/tags/release^{}\n"
            ).encode(),
        ):
            self.assertEqual(key.resolve_revision("repo", "release"), REVISION)

    def test_missing_and_ambiguous_remote_refs_are_refused(self):
        for response in (
            b"",
            (
                REVISION + "\trefs/heads/release\n" + "c" * 40 + "\trefs/tags/release\n"
            ).encode(),
        ):
            with self.subTest(response=response), patch.object(
                subprocess, "check_output", return_value=response
            ):
                with self.assertRaisesRegex(ValueError, "one commit"):
                    key.resolve_revision("repo", "release")

    def test_full_commit_ref_does_not_need_remote_lookup(self):
        with patch.object(subprocess, "check_output") as remote:
            self.assertEqual(key.resolve_revision("repo", REVISION), REVISION)
            remote.assert_not_called()

    def test_real_git_annotated_tag_resolves_to_its_commit(self):
        subprocess.run(
            [
                "git",
                "-C",
                str(self.root),
                "-c",
                "user.name=Test",
                "-c",
                "user.email=test@example.invalid",
                "commit",
                "-qm",
                "fixture",
            ],
            check=True,
        )
        subprocess.run(
            [
                "git",
                "-C",
                str(self.root),
                "-c",
                "user.name=Test",
                "-c",
                "user.email=test@example.invalid",
                "tag",
                "-a",
                "release",
                "-m",
                "fixture",
            ],
            check=True,
        )
        revision = key.git(self.root, "rev-parse", "HEAD").strip()
        self.assertEqual(
            key.resolve_revision(self.root.as_posix(), "release"), revision
        )

    def test_cli_emits_json_and_github_outputs_for_the_same_identity(self):
        command = [
            sys.executable,
            str(SCRIPT),
            "key",
            "--repository-root",
            str(self.root),
            "--base-image",
            BASE,
            "--velox-commit",
            REVISION,
        ]
        info = json.loads(subprocess.check_output(command))
        outputs = dict(
            line.split("=", 1)
            for line in subprocess.check_output(
                command + ["--github-output"], text=True
            ).splitlines()
        )
        self.assertEqual(
            outputs,
            {name: info[name] for name in ("key", "base_image", "velox_commit")},
        )

    def labels(self, velox_key, sequence):
        return {key.KEY_LABEL: velox_key, key.SEQUENCE_LABEL: str(sequence)}

    def test_a_published_key_reuses_its_slot(self):
        published = [self.labels("a", 3), self.labels("b", 4), None]
        self.assertEqual(
            key.choose_slot("b", published), {"found": True, "slot": 1, "sequence": 4}
        )

    def test_a_new_key_fills_an_empty_slot_first(self):
        published = [self.labels("a", 0), None, self.labels("b", 1)]
        self.assertEqual(
            key.choose_slot("c", published), {"found": False, "slot": 1, "sequence": 2}
        )

    def test_a_new_key_replaces_the_least_recently_published_slot(self):
        published = [self.labels("a", 9), self.labels("b", 7), self.labels("c", 8)]
        self.assertEqual(
            key.choose_slot("d", published), {"found": False, "slot": 1, "sequence": 10}
        )

    def test_unlabelled_slots_are_replaced_first(self):
        published = [self.labels("a", 2), {}, {key.SEQUENCE_LABEL: "bad"}]
        self.assertEqual(key.choose_slot("d", published)["slot"], 1)

    def inspect(self, returncode=0, stdout="", stderr=""):
        return patch.object(
            subprocess,
            "run",
            return_value=subprocess.CompletedProcess([], returncode, stdout, stderr),
        )

    def test_published_labels_read_single_and_multi_platform_images(self):
        labels = self.labels("a", 1)
        single = json.dumps({"config": {"Labels": labels}})
        multi = json.dumps({"linux/amd64": {"config": {"Labels": labels}}})
        for output in (single, multi):
            with self.subTest(output=output), self.inspect(stdout=output):
                self.assertEqual(key.published_labels(0), labels)

    def test_missing_slots_are_empty_and_other_inspect_errors_fail(self):
        with self.inspect(1, stderr="ERROR: docker.io/apache/gluten:x: not found"):
            self.assertIsNone(key.published_labels(0))
        with self.inspect(1, stderr="ERROR: unauthorized"):
            with self.assertRaisesRegex(ValueError, "unauthorized"):
                key.published_labels(0)

    def test_a_mismatched_build_key_is_refused_before_running_any_build(self):
        info = self.info()
        with patch.object(subprocess, "run") as run:
            with self.assertRaisesRegex(ValueError, "cache key"):
                key.build(self.root, info, "wrong-key")
            run.assert_not_called()


if __name__ == "__main__":
    unittest.main()
