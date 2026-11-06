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

"""Decisions shared by the trusted Bolt CI workflows."""

import json
import re
from collections import Counter
from fnmatch import fnmatchcase

GATE = "Bolt CI / gate"
WORKFLOW = ".github/workflows/bolt_backend_x86.yml"
MARKER = "<!-- bolt-ci-report -->"
PACKAGES = (
    "spark34", "spark35", "spark35-scala213", "spark35-java8", "spark34-java8",
    "spark35-java11", "spark40", "spark40-fast-build", "spark41", "spark40-java21",
    "spark40-java25", "spark41-java21", "spark41-java25", "spark42",
)
SPARKS = ("3.4", "3.5", "4.0", "4.1", "4.2")
STAGES = ("spark-1", "spark-2", "spark-3", "extended", "slow-hive")
EXPECTED_JOBS = (
    "Validate Bolt CI request", "native",
    *(f"Compile and package {name}" for name in PACKAGES),
    *(f"Spark {spark} tests ({stage})" for spark in SPARKS for stage in STAGES),
    "Spark 3.5 tests (jvm)", "Spark 3.5 tests (tpc)",
)
PATHS = (
    ".github/workflows/bolt*", ".github/workflows/util/bolt-ci/*",
    ".github/actions/restore-bolt-native/*", "pom.xml", "backends-bolt/*",
    "gluten-*/*", "package/*", "shims/*", "cpp/*", "dev/*", "build/mvn",
    "tools/gluten-it/*", ".github/workflows/util/install-spark-resources.sh",
)
SHA_RE = re.compile(r"[0-9a-f]{40}")
REPO_RE = re.compile(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+")


def affected(files, total):
    if len(files) != total or len(files) >= 3000:
        return True
    return any(fnmatchcase(file.get(key, ""), pattern)
               for file in files for key in ("filename", "previous_filename")
               for pattern in PATHS)


def command_sha(body):
    match = re.fullmatch(r"/bolt ci(?: ([0-9a-f]{40}))?", body.strip())
    return (match.group(1) or "") if match else None


def parse_identity(value):
    try:
        identity = json.loads(value)
        valid = (
            identity["v"] == 1 and type(identity["pr"]) is int and identity["pr"] > 0
            and SHA_RE.fullmatch(identity["sha"])
            and SHA_RE.fullmatch(identity["control"])
            and REPO_RE.fullmatch(identity["repo"])
            and all(part not in (".", "..") for part in identity["repo"].split("/"))
            and re.fullmatch(r"[1-9][0-9]*/[1-9][0-9]*", identity["request"])
        )
        return identity if valid else None
    except (ValueError, TypeError, KeyError):
        return None


def parse_title(title):
    match = re.fullmatch(r"Bolt CI PR ([1-9][0-9]*) check ([1-9][0-9]*)", title)
    return tuple(map(int, match.groups())) if match else None


def run_matches(run, check, identity, repository, default_branch, allow_rerun=False):
    return (
        run.get("event") == "workflow_dispatch"
        and run.get("path") == WORKFLOW
        and run.get("head_branch") == default_branch
        and run.get("head_sha") == identity["control"]
        and run.get("repository", {}).get("full_name", "").lower() == repository.lower()
        and (run.get("run_attempt") == 1 or (allow_rerun and run.get("run_attempt", 0) > 1))
        and parse_title(run.get("display_title", "")) == (identity["pr"], check["id"])
        and check.get("head_sha") == identity["sha"]
        and check.get("details_url") == f"https://github.com/{repository}/actions/runs/{run['id']}"
    )


def required_steps(name):
    if name == "native":
        return ("Compile native libraries, examples, benchmarks and test executables", "Run native UT")
    if name.startswith("Compile and package "):
        return ("Compile main and test sources and package the Gluten JAR",
                "Verify native libraries are embedded in the packaged JAR")
    if name.startswith("Spark "):
        stage = name.rsplit("(", 1)[-1].rstrip(")")
        test_step = {
            "jvm": "Run backend JVM, native loader and Delta suites",
            "tpc": "Compare TPC-H and TPC-DS against vanilla Spark",
            "extended": "Run Extended SQL and Slow Hive tests",
            "slow-hive": "Run Extended SQL and Slow Hive tests",
        }.get(stage, "Run the reference Spark SQL unit test groups")
        return ("Compile the Spark test modules", "Provision Gluten Spark test environment", test_step)
    return ()


def gate_result(run, jobs):
    if run["status"] != "completed":
        return "in_progress"
    if run.get("conclusion") != "success":
        return "failure"
    if Counter(job["name"] for job in jobs) != Counter(EXPECTED_JOBS):
        return "failure"
    for job in jobs:
        if job["status"] != "completed" or job["conclusion"] != "success":
            return "failure"
        steps = {step["name"]: step for step in job.get("steps", [])}
        if any(steps.get(name, {}).get("conclusion") != "success" for name in required_steps(job["name"])):
            return "failure"
    return "success"


def never_assigned(job):
    return (
        not job.get("runner_id") and not job.get("runner_name")
        and all(not step.get("started_at") and (
            step.get("status") in ("queued", "pending")
            or step.get("conclusion") in ("skipped", "cancelled"))
            for step in job.get("steps", []))
    )


def eligible(run, jobs):
    native = [job for job in jobs if job["name"] == "native"]
    validation = [job for job in jobs if job["name"] == "Validate Bolt CI request"]
    return (
        run["status"] in ("queued", "in_progress") and run["run_attempt"] == 1
        and len(validation) == 1 and validation[0].get("conclusion") == "success"
        and len(native) == 1 and native[0]["status"] == "queued"
        and never_assigned(native[0])
        and {"self-hosted", "linux", "x64", "bolt-medium"}.issubset(
            {label.lower() for label in native[0].get("labels", [])})
    )


class QueueWindow:
    def __init__(self):
        self.assigned = False
        self.reset()

    def reset(self):
        self.first = None
        self.last = None
        self.samples = 0
        self.job_id = None

    def observe(self, run, jobs, now):
        native = next((job for job in jobs if job["name"] == "native"), None)
        if native and not never_assigned(native):
            self.assigned = True
        if self.assigned or not eligible(run, jobs):
            self.reset()
            return False
        if self.last is None or now - self.last > 90 or now < self.last or self.job_id != native["id"]:
            self.reset()
            self.first = now
            self.job_id = native["id"]
        self.last = now
        self.samples += 1
        return now - self.first >= 1800 and self.samples >= 31

    def evidence(self):
        return dict(first=self.first, last=self.last, samples=self.samples, job_id=self.job_id)
