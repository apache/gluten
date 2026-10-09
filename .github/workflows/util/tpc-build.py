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

"""Transfer the current commit's integration runtime without rebuilding it per test job."""

import argparse
import json
from pathlib import Path
import shutil
import subprocess
import zipfile

LIBRARY = Path("tools/gluten-it/package/target/lib")


def identity(checkout, spark, java, shuffle):
    revision = subprocess.check_output(
        [
            "git",
            "-c",
            "safe.directory=" + str(checkout),
            "-C",
            str(checkout),
            "rev-parse",
            "HEAD",
        ],
        universal_newlines=True,
    ).strip()
    return dict(revision=revision, spark=spark, java=java, shuffle=shuffle)


def pack(checkout, archive, expected):
    files = sorted((checkout / LIBRARY).glob("*.jar"))
    if not any(path.name.startswith("gluten-package-") for path in files):
        raise ValueError("Missing current Gluten bundle")
    if not any(path.name.startswith("gluten-it-common-") for path in files):
        raise ValueError("Missing integration test runtime")
    with zipfile.ZipFile(archive, "w", zipfile.ZIP_STORED) as output:
        output.writestr("manifest.json", json.dumps(expected))
        for path in files:
            output.write(path, path.name)


def restore(checkout, archive, expected):
    with zipfile.ZipFile(archive) as source:
        if json.loads(source.read("manifest.json")) != expected:
            raise ValueError("Integration build revision/configuration mismatch")
        files = [item for item in source.infolist() if item.filename != "manifest.json"]
        if not files or any(
            Path(item.filename).name != item.filename
            or not item.filename.endswith(".jar")
            for item in files
        ):
            raise ValueError("Unexpected integration archive entry")
        destination = checkout / LIBRARY
        if destination.exists():
            shutil.rmtree(destination)
        destination.mkdir(parents=True)
        for item in files:
            with source.open(item) as content, (destination / item.filename).open(
                "wb"
            ) as target:
                shutil.copyfileobj(content, target)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("pack", "restore"))
    parser.add_argument("--archive", type=Path, required=True)
    parser.add_argument("--spark", required=True)
    parser.add_argument("--java", required=True)
    parser.add_argument("--shuffle", default="plain")
    args = parser.parse_args()
    checkout = Path.cwd().resolve()
    operation = pack if args.mode == "pack" else restore
    operation(
        checkout, args.archive, identity(checkout, args.spark, args.java, args.shuffle)
    )
