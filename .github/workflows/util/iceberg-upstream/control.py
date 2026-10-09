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

"""Select vanilla controls and attribute failures only after matching passing controls."""

import argparse
import collections
import json
import re
from pathlib import Path
import xml.etree.ElementTree as ET

from compare import PASSING, parameters
from summarize import classify, write_reports

RUNTIME_FAILURE = re.compile(
    r"TimeoutException|OutOfMemoryError|UnsatisfiedLinkError|NoClassDefFoundError|NoSuchMethodError"
    r"|No space left on device|Connection refused|Connection reset"
)


def selection(results):
    methods = collections.defaultdict(set)
    for row in results:
        if row["status"] == "FAILED":
            methods[row["class"]].add(row["name"].split("(")[0])
    return ",".join(
        name + "#" + "+".join(sorted(tests)) for name, tests in sorted(methods.items())
    )


def attribute(results, controls):
    index = {(row["class"], row["name"]): row for row in controls}
    if len(index) != len(controls):
        raise ValueError("Duplicate vanilla controls")
    for row in results:
        if row["status"] != "FAILED":
            continue
        control = index.get((row["class"], row["name"]))
        if control is None:
            row["failure_origin"] = "CONTROL_MISSING"
        elif parameters(row) != parameters(control):
            row["failure_origin"] = "PARAMETER_MISMATCH"
        elif control["status"] in PASSING and not control.get("native"):
            # A faster/smaller control run cannot establish the cause of a timeout,
            # resource exhaustion or broken runtime. Preserve the failure and fail closed.
            error = row.get("failure_type", "") + "\n" + row.get("failure", "")
            row["failure_origin"] = (
                "UNVERIFIED_RUNTIME_FAILURE"
                if RUNTIME_FAILURE.search(error)
                else "GLUTEN"
            )
        else:
            row["failure_origin"] = "VANILLA_ALSO_FAILS"
        row["vanilla_status"] = control["status"] if control else "MISSING"
        row["vanilla_failure"] = control.get("failure", "") if control else ""
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("reports", type=Path)
    parser.add_argument("--select", type=Path)
    parser.add_argument("--vanilla", type=Path)
    args = parser.parse_args()
    results = json.loads((args.reports / "execution-coverage.json").read_text())[
        "tests"
    ]
    if args.select is not None:
        args.select.write_text(selection(results))
        return 0
    if args.vanilla is None:
        parser.error("Specify --select or --vanilla")
    controls = []
    if any(row["status"] == "FAILED" for row in results):
        for path in args.vanilla.glob("TEST-*.xml"):
            props = {
                p.get("name"): p.get("value")
                for p in ET.parse(path).getroot().findall("properties/property")
            }
            if props.get("spark.plugins") != "":
                raise ValueError(
                    f"Vanilla control did not disable Spark plugins: {path}"
                )
        controls = classify(args.vanilla)
        write_reports(args.vanilla, controls, label="Vanilla Spark controls")
    results = attribute(results, controls)
    broken = [
        row
        for row in results
        if row["status"] == "FAILED" and row.get("failure_origin") != "GLUTEN"
    ]
    write_reports(args.reports, results, label="Gluten coverage after vanilla controls")
    if broken:
        print(
            f"{len(broken)} failures could not be attributed reliably; see failed.csv"
        )
    return int(
        bool(broken) or any(row["status"] == "COVERAGE_ERROR" for row in results)
    )


if __name__ == "__main__":
    raise SystemExit(main())
