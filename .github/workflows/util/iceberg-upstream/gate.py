#!/usr/bin/env python3
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

"""Explain infrastructure failures before attempting an Iceberg comparison."""

import json
import os
from pathlib import Path

from compare import write_changes


def unavailable_reasons(directory, status):
    errors = []
    for name, label in (
        ("PREPARE_RESULT", "Baseline preparation"),
        ("UPSTREAM_RESULT", "Upstream head build/tests"),
    ):
        if status.get(name) != "success":
            errors.append(f"{label}: {status.get(name) or 'unavailable'}")
    if (
        status.get("HEAD_OUTCOME") != "success"
        or not (directory / "iceberg-head/execution-coverage.json").is_file()
    ):
        errors.append("Complete head shard reports are unavailable")
    if status.get("RUN_BASE") == "true":
        if status.get("BASE_RESULT") != "success":
            errors.append(
                f"Base build/tests: {status.get('BASE_RESULT') or 'unavailable'}"
            )
        baseline_ready = status.get("BASE_OUTCOME") == "success"
    else:
        baseline_ready = status.get("BASELINE_OUTCOME") == "success"
    if status.get("INITIAL_BASELINE") != "true" and (
        not baseline_ready
        or not (directory / "iceberg-base/execution-coverage.json").is_file()
    ):
        errors.append("A complete, validated baseline is unavailable")
    return errors


def main():
    errors = unavailable_reasons(Path.cwd(), os.environ)
    if not errors:
        return 0
    text = (
        "Iceberg comparison could not run. The gate remains failed; available "
        "build diagnostics and test reports are retained.\n\n"
        + "\n".join("- " + error for error in errors)
        + "\n"
    )
    print(text)
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as output:
            output.write(text)
    Path("iceberg-comparison.json").write_text(
        json.dumps(
            {"comparison_performed": False, "validation_errors": errors, "changes": []},
            indent=2,
        )
        + "\n"
    )
    write_changes(Path("iceberg-comparison.csv"), [])
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
