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
import json
import unittest

from control import Controller, GitHubError
from policy import EXPECTED_JOBS, GATE
from test_policy import CONTROL_SHA, IDENTITY, SHA, job, queued_jobs, run


class API:
    def __init__(self):
        self.check = dict(
            id=300,
            name=GATE,
            head_sha=SHA,
            status="in_progress",
            conclusion=None,
            app={"slug": "github-actions"},
            external_id=json.dumps(IDENTITY),
            details_url="https://github.com/owner/gluten/runs/300",
            output={},
        )
        self.pr = dict(
            number=2,
            state="open",
            head={"sha": SHA, "repo": {"full_name": "someone/gluten"}},
            changed_files=1,
        )
        self.run = run("completed") | {"conclusion": "success"}
        self.jobs = [job(name) for name in EXPECTED_JOBS]
        self.permission = "read"
        self.files = [{"filename": "backends-bolt/a"}]
        self.comments = []
        self.writes = []
        self.assign_on_cancel = False
        self.cancelled_before_post = False

    def get(self, path):
        if path.endswith("/pulls/2"):
            return copy.deepcopy(self.pr)
        if path.endswith("/check-runs/300"):
            return copy.deepcopy(self.check)
        if path.endswith("/actions/runs/200"):
            return copy.deepcopy(self.run)
        if path.endswith("/permission"):
            return {"permission": self.permission}
        if "/git/ref/heads/" in path:
            return {"object": {"sha": CONTROL_SHA}}
        raise AssertionError(path)

    def list(self, path, field=None):
        if "check-runs?" in path:
            return [copy.deepcopy(self.check)]
        if path.endswith("/jobs"):
            return copy.deepcopy(self.jobs)
        if path.endswith("/files"):
            return self.files
        if path.endswith("/comments"):
            return copy.deepcopy(self.comments)
        raise AssertionError(path)

    def patch(self, path, data):
        if any(
            key in data and data[key] is None for key in ("conclusion", "completed_at")
        ):
            raise GitHubError("PATCH", 422)
        self.writes.append((path, data))
        if path.endswith("/check-runs/300"):
            self.check.update(copy.deepcopy(data))
            if data.get("status") == "in_progress":
                self.check.update(conclusion=None, completed_at=None)
            return copy.deepcopy(self.check)
        if "/issues/comments/" in path:
            self.comments[0].update(data)
            return self.comments[0]
        raise AssertionError(path)

    def post(self, path, data=None):
        self.writes.append((path, data))
        if path.endswith("/cancel"):
            self.run.update(status="completed", conclusion="cancelled")
            self.jobs = queued_jobs()
            self.jobs[1].update(status="completed", conclusion="cancelled")
            if self.assign_on_cancel:
                self.jobs[1]["runner_id"] = 9
            if self.cancelled_before_post:
                raise GitHubError("POST", 409)
            return None
        if path.endswith("/comments"):
            self.comments = [
                dict(id=400, user={"login": "github-actions[bot]"}, **data)
            ]
            return self.comments[0]
        if path.endswith("/dispatches"):
            return {"workflow_run_id": 200}
        if path.endswith("/check-runs"):
            self.check.update(copy.deepcopy(data))
            return copy.deepcopy(self.check)
        raise AssertionError(path)


def controller(api):
    return Controller(
        api,
        "owner/gluten",
        "main",
        CONTROL_SHA,
        "101/1",
        now=lambda: 1800,
        sleep=lambda seconds: None,
        allow_queue_exemption=True,
    )


class ControlTest(unittest.TestCase):
    def test_duplicate_event_recovers_observer_without_another_worker(self):
        api = API()
        event = {"number": 2, "pull_request": api.pr, "sender": {"login": "visitor"}}
        result = controller(api).listen(event, "pull_request_target")
        dispatches = [
            (path, data) for path, data in api.writes if path.endswith("/dispatches")
        ]
        self.assertEqual(len(dispatches), 1)
        self.assertTrue(dispatches[0][0].endswith("/bolt_ci_commenter.yml/dispatches"))
        self.assertEqual(result["run_id"], 200)

    def test_initial_comment_failure_keeps_observer_and_queue_monitor(self):
        api = API()
        api.permission = "write"
        control = controller(api)

        def failed_comment(*args):
            raise GitHubError("POST", 503)

        control.comment = failed_comment
        event = {
            "inputs": {"pr": "2", "head_sha": SHA},
            "sender": {"login": "maintainer"},
        }
        result = control.listen(event, "workflow_dispatch")
        self.assertEqual(result["run_id"], 200)
        self.assertTrue(
            any(
                path.endswith("/bolt_ci_commenter.yml/dispatches")
                for path, data in api.writes
            )
        )

    def test_listener_dispatches_an_independent_commenter(self):
        api = API()
        api.permission = "write"
        event = {
            "inputs": {"pr": "2", "head_sha": SHA},
            "sender": {"login": "maintainer"},
        }
        controller(api).listen(event, "workflow_dispatch")
        reporters = [
            data
            for path, data in api.writes
            if path.endswith("/bolt_ci_commenter.yml/dispatches")
        ]
        self.assertEqual(reporters, [{"ref": "main", "inputs": {"run_id": "200"}}])

    def test_observer_waits_for_terminal_run_without_writing_checks(self):
        api = API()
        api.run.update(status="in_progress", conclusion=None)
        control = controller(api)
        control.sleep = lambda seconds: api.run.update(
            status="completed", conclusion="failure"
        )
        result = control.observe(200)
        self.assertEqual(result, {"pr": 2, "pending": "false"})
        self.assertEqual(api.writes, [])

    def test_observer_hands_off_long_runs_and_rejects_stale_requests(self):
        api = API()
        api.run.update(status="in_progress", conclusion=None)
        elapsed = [0]
        control = controller(api)
        control.now = lambda: elapsed[0]
        control.sleep = lambda seconds: elapsed.__setitem__(0, elapsed[0] + 18000)
        self.assertEqual(control.observe(200), {"pr": 2, "pending": "true"})
        self.assertEqual(api.writes, [])
        api.pr["head"]["sha"] = "c" * 40
        self.assertIsNone(control.observe(200))

    def test_complete_matrix_updates_gate_and_one_comment(self):
        api = API()
        control = controller(api)
        control.report(200)
        self.assertEqual(api.check["conclusion"], "success")
        self.assertEqual(len(api.comments), 1)
        control.report(200)
        self.assertEqual(len(api.comments), 1)
        self.assertIn(SHA, api.comments[0]["body"])

    def test_failures_and_missing_jobs_block(self):
        for jobs in ([job("native", conclusion="failure")], [job("native")]):
            api = API()
            api.jobs = jobs
            controller(api).report(200)
            self.assertEqual(api.check["conclusion"], "failure")

    def test_stale_sha_and_replaced_request_cannot_write(self):
        for change in ("sha", "run_id", "app"):
            api = API()
            if change == "sha":
                api.pr["head"]["sha"] = "c" * 40
            if change == "run_id":
                api.check["external_id"] = json.dumps(IDENTITY | {"run_id": 201})
            if change == "app":
                api.check["app"]["slug"] = "other"
            controller(api).report(200)
            self.assertEqual(api.writes, [])

    def test_ui_rerun_clears_old_success_and_reports_invalid_attempt(self):
        api = API()
        api.check.update(status="completed", conclusion="success")
        api.run.update(status="in_progress", conclusion=None, run_attempt=2)
        controller(api).report(200)
        self.assertEqual(api.check["status"], "in_progress")
        self.assertIsNone(api.check["conclusion"])
        api.run.update(status="completed", conclusion="failure")
        controller(api).report(200)
        self.assertEqual(api.check["conclusion"], "failure")
        self.assertIn("/bolt ci", api.comments[0]["body"])

    def test_unprivileged_comment_cannot_dispatch(self):
        api = API()
        event = {
            "issue": {"number": 2, "pull_request": {}},
            "comment": {"body": "/bolt ci " + SHA},
            "sender": {"login": "visitor"},
        }
        self.assertIsNone(controller(api).listen(event, "issue_comment"))
        self.assertEqual(api.writes, [])

    def test_new_fork_sha_waits_for_explicit_authorization(self):
        api = API()
        api.check["external_id"] = ""
        event = {"number": 2, "pull_request": api.pr, "sender": {"login": "visitor"}}
        result = controller(api).listen(event, "pull_request_target")
        self.assertIsNone(result)
        self.assertEqual(api.check["status"], "in_progress")
        self.assertFalse(any(path.endswith("/dispatches") for path, _ in api.writes))
        self.assertIn("/bolt ci " + SHA, api.comments[0]["body"])

    def test_writer_authorizes_exact_sha_and_resets_previous_success(self):
        api = API()
        api.permission = "write"
        api.check.update(status="completed", conclusion="success")
        event = {
            "issue": {"number": 2, "pull_request": {}},
            "comment": {"body": "/bolt ci " + SHA},
            "sender": {"login": "maintainer"},
        }
        result = controller(api).listen(event, "issue_comment")
        self.assertEqual(result["run_id"], 200)
        self.assertEqual(api.check["status"], "in_progress")
        self.assertIsNone(api.check["conclusion"])
        dispatch = next(
            data for path, data in api.writes if path.endswith("/dispatches")
        )
        self.assertEqual(dispatch["ref"], "main")
        self.assertEqual(dispatch["inputs"]["head_sha"], SHA)

    def test_old_authorization_does_not_authorize_new_fork_sha(self):
        api = API()
        api.permission = "admin"
        event = {
            "issue": {"number": 2, "pull_request": {}},
            "comment": {"body": "/bolt ci " + "c" * 40},
            "sender": {"login": "maintainer"},
        }
        self.assertIsNone(controller(api).listen(event, "issue_comment"))
        self.assertEqual(api.writes, [])

    def test_queue_waiver_requires_accepted_cancel_and_terminal_evidence(self):
        api = API()
        api.run = run()
        api.jobs = queued_jobs()
        evidence = dict(first=0, last=1800, samples=31, job_id=44)
        controller(api).expire(200, evidence)
        self.assertEqual(api.check["conclusion"], "neutral")
        controller(api).report(200)
        self.assertEqual(api.check["conclusion"], "neutral")

    def test_confirmed_queue_waiver_is_enabled_by_default(self):
        api = API()
        api.run = run()
        api.jobs = queued_jobs()
        control = Controller(
            api,
            "owner/gluten",
            "main",
            CONTROL_SHA,
            "101/1",
            now=lambda: 1800,
            sleep=lambda seconds: None,
        )
        control.expire(200, dict(first=0, last=1800, samples=31, job_id=44))
        self.assertEqual(api.check["conclusion"], "neutral")

    def test_assignment_during_cancellation_prevents_waiver(self):
        api = API()
        api.run = run()
        api.jobs = queued_jobs()
        api.assign_on_cancel = True
        controller(api).expire(200, dict(first=0, last=1800, samples=31, job_id=44))
        self.assertEqual(api.check["conclusion"], "failure")

    def test_human_cancel_after_verified_queue_window_qualifies(self):
        for timing in ("before_expire", "during_cancel"):
            api = API()
            api.run = run()
            api.jobs = queued_jobs()
            if timing == "before_expire":
                api.run.update(status="completed", conclusion="cancelled")
                api.jobs[1].update(status="completed", conclusion="cancelled")
            else:
                api.cancelled_before_post = True
            controller(api).expire(200, dict(first=0, last=1800, samples=31, job_id=44))
            self.assertEqual(api.check["conclusion"], "neutral")

    def test_manual_cancel_or_incomplete_evidence_never_grants_waiver(self):
        for change in ("cancelled", "gap", "young", "samples"):
            api = API()
            api.run = run()
            api.jobs = queued_jobs()
            evidence = dict(first=0, last=1800, samples=31, job_id=44)
            if change == "cancelled":
                api.run.update(status="completed", conclusion="cancelled")
            if change == "gap":
                evidence["last"] = 1600
            if change == "young":
                evidence["first"] = 1
            if change == "samples":
                evidence["samples"] = 30
            controller(api).expire(200, evidence)
            self.assertNotEqual(api.check["conclusion"], "neutral")


if __name__ == "__main__":
    unittest.main()
