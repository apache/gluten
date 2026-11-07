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

"""Trusted GitHub-side control for the Bolt worker workflow."""

import argparse
import json
import os
import sys
import time
from urllib.error import HTTPError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

from policy import (
    EXPECTED_JOBS,
    GATE,
    MARKER,
    SHA_RE,
    WORKFLOW,
    QueueWindow,
    affected,
    cancelled_without_execution,
    command_sha,
    eligible,
    gate_result,
    parse_identity,
    parse_title,
    run_matches,
)

QUEUE_TITLE = "Runner queue timeout (30 minutes)"
# Verified queue eligibility determines exemption, including simultaneous cancels.
ALLOW_QUEUE_EXEMPTION = True


class GitHubError(RuntimeError):
    def __init__(self, method, status):
        super().__init__(f"GitHub {method} failed with HTTP {status}")
        self.status = status


class GitHub:
    def __init__(self, token):
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2026-03-10",
        }

    def request(self, method, path, data=None):
        if not path.startswith("/repos/"):
            raise ValueError("Unexpected GitHub API path")
        request = Request(
            "https://api.github.com" + path,
            method=method,
            headers=self.headers,
            data=json.dumps(data).encode() if data is not None else None,
        )
        try:
            with urlopen(request, timeout=30) as response:
                content = response.read()
                return json.loads(content) if content else None
        except HTTPError as error:
            # Response payloads and request headers may contain private data.
            raise GitHubError(method, error.code) from None

    def get(self, path):
        return self.request("GET", path)

    def post(self, path, data=None):
        return self.request("POST", path, data)

    def patch(self, path, data):
        return self.request("PATCH", path, data)

    def list(self, path, field=None):
        result = []
        separator = "&" if "?" in path else "?"
        for page in range(1, 101):
            data = self.get(f"{path}{separator}per_page=100&page={page}")
            items = data[field] if field else data
            result.extend(items)
            if len(items) < 100:
                return result
        raise RuntimeError("GitHub pagination exceeded the supported limit")


class Controller:
    def __init__(
        self,
        api,
        repository,
        default_branch,
        control_sha,
        request,
        now=time.time,
        sleep=time.sleep,
        allow_queue_exemption=ALLOW_QUEUE_EXEMPTION,
    ):
        self.api = api
        self.repository = repository
        self.base = f"/repos/{repository}"
        self.default_branch = default_branch
        self.control_sha = control_sha
        self.request = request
        self.now = now
        self.sleep = sleep
        self.allow_queue_exemption = allow_queue_exemption

    def matching_checks(self, pr):
        query = urlencode({"check_name": GATE, "filter": "all"})
        checks = self.api.list(
            f"{self.base}/commits/{pr['head']['sha']}/check-runs?{query}", "check_runs"
        )
        matches = []
        for check in checks:
            identity = parse_identity(check.get("external_id"))
            if (
                check.get("app", {}).get("slug") == "github-actions"
                and identity
                and identity["pr"] == pr["number"]
                and identity["sha"] == pr["head"]["sha"]
            ):
                matches.append(check)
        return matches

    def latest_check(self, pr):
        return max(
            self.matching_checks(pr), key=lambda check: check["id"], default=None
        )

    def comment(self, pr, body):
        body = MARKER + "\n" + body
        comments = self.api.list(f"{self.base}/issues/{pr['number']}/comments")
        own = [
            comment
            for comment in comments
            if comment.get("user", {}).get("login") == "github-actions[bot]"
            and comment.get("body", "").startswith(MARKER)
        ]
        if own:
            comment = min(own, key=lambda item: item["id"])
            if comment["body"] != body:
                self.api.patch(
                    f"{self.base}/issues/comments/{comment['id']}", {"body": body}
                )
        else:
            self.api.post(f"{self.base}/issues/{pr['number']}/comments", {"body": body})

    def begin(self, pr, authorized):
        identity = dict(
            v=1,
            pr=pr["number"],
            sha=pr["head"]["sha"],
            repo=pr["head"]["repo"]["full_name"],
            request=self.request,
            control=self.control_sha,
            authorized=authorized,
        )
        data = dict(
            name=GATE,
            status="in_progress",
            external_id=json.dumps(identity, separators=(",", ":")),
            details_url=f"https://github.com/{self.repository}/pull/{pr['number']}",
            output={
                "title": "Awaiting Bolt CI",
                "summary": f"Commit `{identity['sha']}` is awaiting verification.",
            },
        )
        check = self.api.post(
            f"{self.base}/check-runs", data | {"head_sha": identity["sha"]}
        )
        # Completed checks cannot be reopened. Retire every older required context
        # only after the fresh blocking check exists, including partial retry recovery.
        for previous in self.matching_checks(pr):
            if previous["id"] < check["id"]:
                self.api.patch(
                    f"{self.base}/check-runs/{previous['id']}",
                    {"name": GATE + " (superseded)"},
                )
        return check

    def finish(self, pr, check, conclusion, title, summary):
        self.api.patch(
            f"{self.base}/check-runs/{check['id']}",
            {
                "status": "completed",
                "conclusion": conclusion,
                "output": {"title": title, "summary": summary},
            },
        )
        self.comment(pr, summary)

    def listen(self, event, event_name):
        explicit_sha = None
        if event_name == "issue_comment":
            if "pull_request" not in event.get("issue", {}):
                return None
            explicit_sha = command_sha(event["comment"]["body"])
            if explicit_sha is None:
                return None
            number = event["issue"]["number"]
        elif event_name == "workflow_dispatch":
            number = int(event["inputs"]["pr"])
            explicit_sha = event["inputs"].get("head_sha", "")
        else:
            number = event["number"]
        pr = self.api.get(f"{self.base}/pulls/{number}")
        if pr["state"] != "open" or not pr["head"].get("repo"):
            return None
        sha = pr["head"]["sha"]
        if (
            event_name == "pull_request_target"
            and event["pull_request"]["head"]["sha"] != sha
        ):
            return None
        if explicit_sha is not None:
            actor = quote(event["sender"]["login"], safe="")
            permission = self.api.get(f"{self.base}/collaborators/{actor}/permission")[
                "permission"
            ]
            if permission not in ("admin", "maintain", "write") or (
                explicit_sha and explicit_sha != sha
            ):
                return None
        previous = self.latest_check(pr)
        # Duplicate PR events and a resumed listener must not create another build.
        if previous:
            identity = parse_identity(previous["external_id"])
            already_dispatched = bool(identity.get("run_id"))
            if already_dispatched and (
                explicit_sha is None
                or identity["request"].split("/")[0] == self.request.split("/")[0]
            ):
                self.start_commenter(identity["run_id"])
                return dict(
                    pr=number, check_id=previous["id"], run_id=identity["run_id"]
                )
        same_repo = pr["head"]["repo"]["full_name"].lower() == self.repository.lower()
        old_identity = parse_identity(previous["external_id"]) if previous else {}
        previously_authorized = old_identity.get("repo") == pr["head"]["repo"][
            "full_name"
        ] and (
            bool(old_identity.get("run_id")) or old_identity.get("authorized") is True
        )
        authorized = same_repo or explicit_sha == sha or previously_authorized
        files = self.api.list(f"{self.base}/pulls/{number}/files")
        check = self.begin(pr, authorized)
        if not affected(files, pr["changed_files"]):
            summary = f"Bolt CI · `{sha}`\n\nThe changed paths do not require Bolt CI."
            self.finish(pr, check, "success", "No Bolt changes", summary)
            return None
        if not authorized:
            self.comment(
                pr,
                f"Bolt CI · `{sha}`\n\nAwaiting maintainer authorization for this commit. "
                f"After review, a maintainer can comment `/bolt ci {sha}`.",
            )
            return None
        branch = quote(self.default_branch, safe="")
        if (
            self.api.get(f"{self.base}/git/ref/heads/{branch}")["object"]["sha"]
            != self.control_sha
        ):
            raise RuntimeError(
                "The default branch changed. Run the listener again from its current version."
            )
        result = self.api.post(
            f"{self.base}/actions/workflows/{WORKFLOW.rsplit('/', 1)[-1]}/dispatches",
            {
                "ref": self.default_branch,
                "inputs": {
                    "pr": str(number),
                    "head_repo": pr["head"]["repo"]["full_name"],
                    "head_sha": sha,
                    "check_id": str(check["id"]),
                },
            },
        )
        if not result or not isinstance(result.get("workflow_run_id"), int):
            raise RuntimeError("Dispatch did not return a verifiable workflow run ID")
        run_id = result["workflow_run_id"]
        url = f"https://github.com/{self.repository}/actions/runs/{run_id}"
        identity = parse_identity(check["external_id"]) | {"run_id": run_id}
        # GITHUB_TOKEN checks cannot retain a custom details_url; use metadata.
        self.api.patch(
            f"{self.base}/check-runs/{check['id']}",
            {
                "external_id": json.dumps(identity, separators=(",", ":")),
                "output": {
                    "title": "Awaiting Bolt CI",
                    "summary": f"[Build and tests]({url})",
                },
            },
        )
        self.start_commenter(run_id)
        try:
            self.comment(
                pr,
                f"Bolt CI · `{sha}`\n\n[Build and tests]({url}) are waiting to run.\n\n"
                "Maintainers can request a fresh run with `/bolt ci`.",
            )
        except (OSError, GitHubError):
            print(
                "Initial comment unavailable; the independent commenter will publish the result."
            )
        return dict(pr=number, check_id=check["id"], run_id=run_id)

    def start_commenter(self, run_id):
        # GITHUB_TOKEN dispatches do not reliably emit a subsequent workflow_run.
        self.api.post(
            f"{self.base}/actions/workflows/bolt_ci_commenter.yml/dispatches",
            {
                "ref": self.default_branch,
                "inputs": {"run_id": str(run_id)},
            },
        )

    def observe(self, run_id):
        # Hand off after five hours to stay below the hosted runner job limit.
        deadline = self.now() + 18000
        while True:
            try:
                run = self.api.get(f"{self.base}/actions/runs/{run_id}")
                binding = self.binding(run, allow_rerun=True)
                if not binding:
                    return None
                if run["status"] == "completed":
                    return {"pr": binding[0]["number"], "pending": "false"}
                if self.now() >= deadline:
                    return {"pr": binding[0]["number"], "pending": "true"}
            except (OSError, RuntimeError, KeyError, ValueError):
                print("Result temporarily unavailable; preserving the blocking check.")
                if self.now() >= deadline:
                    return {"pending": "true"}
            self.sleep(60)

    def resume_report(self, run_id):
        run = self.api.get(f"{self.base}/actions/runs/{run_id}")
        if self.binding(run, allow_rerun=True):
            self.start_commenter(run_id)

    def binding(self, run, allow_rerun=False):
        title = parse_title(run.get("display_title", ""))
        if not title:
            return None
        number, check_id = title
        check = self.api.get(f"{self.base}/check-runs/{check_id}")
        identity = parse_identity(check.get("external_id"))
        if (
            check.get("app", {}).get("slug") != "github-actions"
            or check.get("name") != GATE
            or not identity
        ):
            return None
        if not run_matches(
            run, check, identity, self.repository, self.default_branch, allow_rerun
        ):
            return None
        pr = self.api.get(f"{self.base}/pulls/{number}")
        if (
            pr["state"] != "open"
            or pr["head"]["sha"] != identity["sha"]
            or not pr["head"].get("repo")
            or pr["head"]["repo"]["full_name"] != identity["repo"]
        ):
            return None
        latest = self.latest_check(pr)
        if (
            not latest
            or latest["id"] != check_id
            or latest["external_id"] != check["external_id"]
        ):
            return None
        return pr, check

    def jobs(self, run_id):
        return self.api.list(
            f"{self.base}/actions/runs/{run_id}/attempts/1/jobs", "jobs"
        )

    def report(self, run_id):
        run = self.api.get(f"{self.base}/actions/runs/{run_id}")
        binding = self.binding(run, allow_rerun=True)
        if not binding:
            return
        pr, check = binding
        if run["run_attempt"] != 1:
            summary = (
                f"Bolt CI · `{pr['head']['sha']}`\n\nThis attempt was rerun through Actions. "
                "A maintainer must comment `/bolt ci` to register and verify a fresh request."
            )
            self.finish(
                pr, check, "failure", "A fresh Bolt CI request is required", summary
            )
            return
        if (
            check.get("conclusion") == "neutral"
            and check.get("output", {}).get("title") == QUEUE_TITLE
        ):
            return
        jobs = self.jobs(run_id)
        conclusion = gate_result(run, jobs)
        if conclusion == "in_progress":
            return
        title = "Bolt CI passed" if conclusion == "success" else "Bolt CI failed"
        url = f"https://github.com/{self.repository}/actions/runs/{run_id}"
        summary = f"Bolt CI · `{pr['head']['sha']}`\n\n[{title}]({url})\n\n"
        native = next((job for job in jobs if job["name"] == "native"), {})
        steps = {step["name"]: step for step in native.get("steps", [])}
        for name in (
            "Compile native libraries, examples, benchmarks and test executables",
            "Run native UT",
        ):
            step = steps.get(name, {})
            summary += f"{name} — {step.get('conclusion') or step.get('status') or 'not executed'}\n\n"
        summary += "| Job | Result |\n| --- | --- |\n"
        by_name = {job["name"]: job for job in jobs}
        for name in EXPECTED_JOBS:
            job = by_name.get(name)
            result = (job.get("conclusion") or job["status"]) if job else "not executed"
            link = (
                f"[{name}]({job['html_url']})" if job and job.get("html_url") else name
            )
            summary += f"| {link} | {result} |\n"
        summary += "\nMaintainers can request a fresh run with `/bolt ci`."
        self.finish(pr, check, conclusion, title, summary)

    def watch(self, run_id):
        window = QueueWindow()
        deadline = self.now() + 3900
        while self.now() < deadline:
            try:
                run = self.api.get(f"{self.base}/actions/runs/{run_id}")
                if not self.binding(run) or run["status"] == "completed":
                    return None
                if window.observe(run, self.jobs(run_id), self.now()):
                    return window.evidence()
                if window.assigned:
                    return None
            except (OSError, RuntimeError, KeyError, ValueError):
                window.reset()
                print(
                    "Queue evidence interrupted. Restarting the observation interval."
                )
            self.sleep(60)
        return None

    def expire(self, run_id, evidence):
        # This operation runs under the same per-PR concurrency lock as listen/report.
        if (
            not evidence
            or evidence["last"] - evidence["first"] < 1800
            or not 0 <= self.now() - evidence["last"] <= 90
            or evidence["samples"] < 31
        ):
            return
        run = self.api.get(f"{self.base}/actions/runs/{run_id}")
        binding = self.binding(run)
        jobs = self.jobs(run_id)
        if not binding or not (
            eligible(run, jobs)
            or cancelled_without_execution(run, jobs, evidence["job_id"])
        ):
            return
        pr, check = binding
        url = f"https://github.com/{self.repository}/actions/runs/{run_id}"
        native = next(job for job in jobs if job["name"] == "native")
        if native["id"] != evidence["job_id"]:
            return
        if run["status"] != "completed":
            try:
                self.api.post(f"{self.base}/actions/runs/{run_id}/cancel")
            except GitHubError as error:
                if error.status != 409:
                    return
            except RuntimeError:
                return
        for _ in range(24):
            run = self.api.get(f"{self.base}/actions/runs/{run_id}")
            jobs = self.jobs(run_id)
            if run["status"] == "completed":
                break
            self.sleep(5)
        waived = cancelled_without_execution(
            run, jobs, evidence["job_id"]
        ) and self.binding(run)
        if waived and self.allow_queue_exemption:
            summary = (
                f"Bolt CI · `{pr['head']['sha']}`\n\nRunner queue timeout exemption. "
                "The native job remained eligible and unassigned for at least 30 minutes. "
                "The run was cancelled, and the monitor verified that execution never started. "
                f"Tests were not executed. [Queue evidence]({url})."
            )
            self.finish(pr, check, "neutral", QUEUE_TITLE, summary)
        elif waived:
            self.finish(
                pr,
                check,
                "failure",
                "Queue timeout requires confirmation",
                f"Bolt CI · `{pr['head']['sha']}`\n\nThe native job waited at least 30 minutes "
                "without starting, and was cancelled. Automatic queue exemption is disabled "
                "by the repository's queue exemption policy.",
            )
        else:
            self.finish(
                pr,
                check,
                "failure",
                "Queue exemption could not be verified",
                f"Bolt CI · `{pr['head']['sha']}`\n\nCancellation or runner assignment "
                f"prevented a verified exemption. [Run]({url}).",
            )

    def validate(self, event, run_id):
        inputs = event.get("inputs", {})
        check_id = inputs.get("check_id", "")
        if check_id:
            for _ in range(12):
                run = self.api.get(f"{self.base}/actions/runs/{run_id}")
                binding = self.binding(run)
                if binding:
                    pr, check = binding
                    if (
                        str(check["id"]) != check_id
                        or str(pr["number"]) != inputs.get("pr")
                        or pr["head"]["sha"] != inputs.get("head_sha")
                        or pr["head"]["repo"]["full_name"] != inputs.get("head_repo")
                        or check["status"] != "in_progress"
                    ):
                        raise RuntimeError(
                            "The worker request is stale or does not match its gate"
                        )
                    return dict(
                        head_sha=pr["head"]["sha"],
                        head_repo=pr["head"]["repo"]["full_name"],
                    )
                self.sleep(5)
            raise RuntimeError(
                "The worker is not associated with an active authorized gate"
            )
        if any(inputs.get(key) for key in ("pr", "head_repo", "head_sha")):
            raise RuntimeError(
                "PR builds must be dispatched through the Bolt CI listener"
            )
        # The standalone manual build retains the selected workflow branch.
        sha = os.environ["GITHUB_SHA"]
        if not SHA_RE.fullmatch(sha):
            raise RuntimeError("Invalid manual build SHA")
        return dict(head_repo=self.repository, head_sha=sha)


def output(values):
    if not values:
        return
    with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as stream:
        for key, value in values.items():
            text = (
                json.dumps(value, separators=(",", ":"))
                if isinstance(value, dict)
                else str(value)
            )
            if "\n" in text or "\r" in text:
                raise ValueError("Invalid workflow output")
            stream.write(f"{key}={text}\n")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "operation",
        choices=(
            "listen",
            "report",
            "route",
            "watch",
            "expire",
            "validate",
            "resume-report",
        ),
    )
    args = parser.parse_args()
    with open(os.environ["GITHUB_EVENT_PATH"], encoding="utf-8") as stream:
        event = json.load(stream)
    controller = Controller(
        GitHub(os.environ["GH_TOKEN"]),
        os.environ["GITHUB_REPOSITORY"],
        event["repository"]["default_branch"],
        os.environ["CONTROL_SHA"],
        f"{os.environ['GITHUB_RUN_ID']}/{os.environ['GITHUB_RUN_ATTEMPT']}",
    )
    if args.operation == "listen":
        output(controller.listen(event, os.environ["GITHUB_EVENT_NAME"]))
    elif args.operation == "validate":
        output(controller.validate(event, int(os.environ["GITHUB_RUN_ID"])))
    elif args.operation == "route":
        if os.environ["GITHUB_EVENT_NAME"] == "workflow_dispatch":
            output(controller.observe(int(event["inputs"]["run_id"])))
        else:
            run = controller.api.get(
                f"{controller.base}/actions/runs/{event['workflow_run']['id']}"
            )
            binding = controller.binding(run, allow_rerun=True)
            if binding:
                output({"pr": binding[0]["number"]})
    elif args.operation == "report":
        run_id = (
            int(event["inputs"]["run_id"])
            if os.environ["GITHUB_EVENT_NAME"] == "workflow_dispatch"
            else event["workflow_run"]["id"]
        )
        controller.report(run_id)
    elif args.operation == "resume-report":
        controller.resume_report(int(event["inputs"]["run_id"]))
    elif args.operation == "watch":
        evidence = controller.watch(int(os.environ["BUILD_RUN_ID"]))
        if evidence:
            output({"evidence": evidence})
    else:
        controller.expire(
            int(os.environ["BUILD_RUN_ID"]), json.loads(os.environ["QUEUE_EVIDENCE"])
        )


if __name__ == "__main__":
    main()
