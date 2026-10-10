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

"""Require every shard exactly once before exporting complete Iceberg coverage."""

import argparse
import json
from pathlib import Path
import zlib

from summarize import REPORT_VERSION, write_reports


def shard_for(class_name, method_name, count):
    if count < 1:
        raise ValueError("Shard count must be positive")
    # Keep identical to IcebergQueryTestFilter.shardFor.
    parts = min(4, count & -count)
    return (
        zlib.crc32(class_name.encode()) % (count // parts) * parts
        + zlib.crc32(method_name.encode()) % parts
    )


def merge(directory, count):
    if count < 1:
        raise ValueError("Shard count must be positive")
    reports = sorted(directory.glob("*/execution-coverage.json"))
    if len(reports) != count:
        raise ValueError(f"Expected {count} shard reports, found {len(reports)}")
    shards, tests, results = set(), set(), []
    for path in reports:
        metadata = json.loads((path.parent / "shard.json").read_text())
        index = metadata.get("index")
        if (
            type(index) is not int
            or not 0 <= index < count
            or metadata.get("count") != count
            or index in shards
        ):
            raise ValueError(f"Invalid or duplicate shard metadata: {path.parent}")
        shards.add(index)
        report = json.loads(path.read_text())
        if report.get("version") != REPORT_VERSION or not report.get("tests"):
            raise ValueError(f"Invalid or empty shard report: {path}")
        for row in report["tests"]:
            key = (row["class"], row["name"])
            if key in tests:
                raise ValueError(f"Duplicate test across shards: {key}")
            if shard_for(key[0], key[1].split("(")[0], count) != index:
                raise ValueError(f"Test assigned to the wrong shard: {key}")
            tests.add(key)
            results.append(row)
    return sorted(results, key=lambda row: (row["class"], row["name"]))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--shards", type=int, required=True)
    args = parser.parse_args()
    return write_reports(
        args.directory,
        merge(args.directory, args.shards),
        label=f"Complete Iceberg coverage ({args.shards} shards)",
    )


if __name__ == "__main__":
    raise SystemExit(main())
