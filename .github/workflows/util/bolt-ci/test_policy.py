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
import unittest

from policy import (
    EXPECTED_JOBS,
    QueueWindow,
    affected,
    command_sha,
    gate_result,
    never_assigned,
    parse_identity,
    parse_title,
    required_steps,
    run_matches,
)

SHA = "a" * 40
CONTROL_SHA = "b" * 40
IDENTITY = dict(
    v=1,
    pr=2,
    sha=SHA,
    repo="someone/gluten",
    request="100/1",
    control=CONTROL_SHA,
    run_id=200,
)


def job(name, status="completed", conclusion="success", runner_id=10):
    return dict(
        id=44 if name == "native" else 55,
        name=name,
        status=status,
        conclusion=conclusion,
        runner_id=runner_id,
        labels=["self-hosted", "Linux", "X64", "bolt-medium"],
        steps=(
            [
                dict(name=step, status="completed", conclusion="success")
                for step in required_steps(name)
            ]
            if status == "completed"
            else []
        ),
    )


def queued_jobs():
    return [job("Validate Bolt CI request"), job("native", "queued", None, 0)]


def run(status="queued"):
    return dict(
        id=200,
        status=status,
        conclusion=None,
        run_attempt=1,
        path=".github/workflows/bolt_backend_x86.yml",
        event="workflow_dispatch",
        head_branch="main",
        head_sha=CONTROL_SHA,
        repository={"full_name": "owner/gluten"},
        display_title="Bolt CI PR 2 check 300",
    )


class PolicyTest(unittest.TestCase):
    def test_paths_cover_renames_and_incomplete_responses(self):
        self.assertFalse(affected([{"filename": "README.md"}], 1))
        self.assertTrue(
            affected([{"filename": "README.md", "previous_filename": "dev/test.sh"}], 1)
        )
        self.assertTrue(affected([], 3))
        for path in (
            "gluten-ut/a",
            "cpp/a",
            ".github/workflows/bolt_ci_listener.yml",
            ".github/actions/restore-bolt-native/action.yml",
        ):
            self.assertTrue(affected([{"filename": path}], 1))

    def test_exact_commands_and_sha(self):
        self.assertEqual(command_sha("/bolt ci"), "")
        self.assertEqual(command_sha("/bolt ci " + SHA), SHA)
        for text in (
            "/bolt ci abc",
            "/bolt ci $(id)",
            "/bolt ci\nmore",
            "Run other CI",
        ):
            self.assertIsNone(command_sha(text))

    def test_identity_rejects_bad_metadata(self):
        import json

        self.assertEqual(parse_identity(json.dumps(IDENTITY)), IDENTITY)
        for replacement in ({"pr": 0}, {"sha": "bad"}, {"repo": "../repo"}, {"v": 2}):
            self.assertIsNone(parse_identity(json.dumps(IDENTITY | replacement)))
        self.assertIsNone(parse_identity("not-json"))
        self.assertEqual(parse_title("Bolt CI PR 2 check 300"), (2, 300))
        self.assertIsNone(parse_title("Bolt CI PR 2 check 300 suffix"))

    def test_run_must_match_controller_and_recorded_id(self):
        # GitHub rewrites details_url for checks created by GITHUB_TOKEN.
        check = dict(
            id=300, head_sha=SHA, details_url="https://github.com/owner/gluten/runs/300"
        )
        self.assertTrue(run_matches(run(), check, IDENTITY, "owner/gluten", "main"))
        for replacement in (
            {"head_branch": "pr-branch"},
            {"head_sha": SHA},
            {"run_attempt": 2},
            {"id": 201},
            {"event": "pull_request"},
            {"path": "other.yml"},
            {"display_title": "Bolt CI PR 3 check 300"},
        ):
            self.assertFalse(
                run_matches(
                    run() | replacement, check, IDENTITY, "owner/gluten", "main"
                )
            )
        self.assertFalse(
            run_matches(
                run(), check, IDENTITY | {"run_id": 201}, "owner/gluten", "main"
            )
        )

    def test_gate_requires_every_expected_job(self):
        jobs = [job(name) for name in EXPECTED_JOBS]
        finished = run("completed") | {"conclusion": "success"}
        self.assertEqual(gate_result(finished, jobs), "success")
        self.assertEqual(gate_result(finished, jobs[:-1]), "failure")
        self.assertEqual(gate_result(finished, jobs + [jobs[0]]), "failure")
        for conclusion in ("failure", "skipped", "cancelled", "timed_out", "neutral"):
            changed = copy.deepcopy(jobs)
            changed[-1]["conclusion"] = conclusion
            self.assertEqual(gate_result(finished, changed), "failure")
        self.assertEqual(
            gate_result(finished | {"conclusion": "cancelled"}, jobs), "failure"
        )
        self.assertEqual(gate_result(run("in_progress"), jobs), "in_progress")

    def test_successful_job_with_skipped_or_missing_test_step_fails(self):
        jobs = [job(name) for name in EXPECTED_JOBS]
        finished = run("completed") | {"conclusion": "success"}
        jobs[-1]["steps"][-1]["conclusion"] = "skipped"
        self.assertEqual(gate_result(finished, jobs), "failure")
        jobs[-1]["steps"] = []
        self.assertEqual(gate_result(finished, jobs), "failure")

    def test_thirty_minutes_need_continuous_samples(self):
        window = QueueWindow()
        for now in range(0, 1800, 60):
            self.assertFalse(window.observe(run(), queued_jobs(), now))
        self.assertTrue(window.observe(run(), queued_jobs(), 1800))
        self.assertEqual(
            window.evidence(), dict(first=0, last=1800, samples=31, job_id=44)
        )

    def test_sampling_gap_or_api_failure_resets_clock(self):
        window = QueueWindow()
        window.observe(run(), queued_jobs(), 0)
        window.observe(run(), queued_jobs(), 120)
        self.assertEqual(window.first, 120)
        window.reset()
        self.assertFalse(window.observe(run(), queued_jobs(), 2000))

    def test_concurrency_and_approval_wait_do_not_count(self):
        for status in ("pending", "waiting", "requested", "completed"):
            window = QueueWindow()
            self.assertFalse(window.observe(run(status), queued_jobs(), 0))
            self.assertFalse(window.observe(run(status), queued_jobs(), 2000))
        jobs = queued_jobs()
        jobs[0]["conclusion"] = "failure"
        self.assertFalse(QueueWindow().observe(run(), jobs, 0))

    def test_assignment_or_step_execution_disqualifies_waiver(self):
        for change in (
            {"runner_id": 1},
            {"runner_name": "worker"},
            {"steps": [{"status": "in_progress", "conclusion": None}]},
            {"steps": [{"status": "completed", "conclusion": "success"}]},
            {
                "steps": [
                    {
                        "status": "completed",
                        "conclusion": "cancelled",
                        "started_at": "2026-11-06T00:00:00Z",
                    }
                ]
            },
        ):
            window = QueueWindow()
            jobs = queued_jobs()
            jobs[1].update(change)
            self.assertFalse(window.observe(run(), jobs, 0))
            self.assertTrue(window.assigned)
            self.assertFalse(window.observe(run(), queued_jobs(), 2000))
            self.assertFalse(never_assigned(jobs[1]))


if __name__ == "__main__":
    unittest.main()
