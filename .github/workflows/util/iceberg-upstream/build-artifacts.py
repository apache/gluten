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

"""Share one Spark 3.5 build per revision with all of its Iceberg test shards."""

import argparse
from pathlib import Path
import re
import shutil
import subprocess
import zipfile

CLASSES = (
    "backends-velox/target/scala-2.12/classes",
    "backends-velox/target/iceberg-upstream-test-classes-3.5_2.12",
)


def revision(checkout):
    return subprocess.check_output(
        ["git", "-C", str(checkout), "rev-parse", "HEAD"], text=True
    ).strip()


def pack(checkout, repository, archive, install_log):
    files = {}
    for directory in CLASSES:
        entries = [path for path in (checkout / directory).rglob("*") if path.is_file()]
        if not entries:
            raise ValueError(f"Missing compiled classes: {directory}")
        for path in entries:
            files["checkout/" + path.relative_to(checkout).as_posix()] = path

    # The image's Maven cache can contain other Gluten revisions and profiles. Ship only
    # artifacts installed by this build, plus their local SNAPSHOT metadata. Surefire uses
    # the backend's classes above, so its large bundled JAR and source JARs are unnecessary.
    installed = set()
    for line in install_log.read_text().splitlines():
        match = re.fullmatch(r"\[INFO\] Installing .+ to (.+)", line)
        if not match:
            continue
        path = Path(match[1])
        relative = path.relative_to(repository)
        if relative.parts[:3] != ("org", "apache", "gluten"):
            continue
        if relative.parts[3] == "backends-velox" or path.name.endswith("-sources.jar"):
            continue
        installed.add(path)
        for metadata in ("maven-metadata-local.xml", "_remote.repositories"):
            if (path.parent / metadata).is_file():
                installed.add(path.parent / metadata)
    if not installed:
        raise ValueError("No installed Gluten dependencies in the build log")
    for path in installed:
        files["repository/" + path.relative_to(repository).as_posix()] = path

    with zipfile.ZipFile(archive, "w", zipfile.ZIP_DEFLATED, compresslevel=1) as output:
        output.writestr("revision", revision(checkout))
        for name, path in sorted(files.items()):
            output.write(path, name)


def restore(checkout, repository, archive):
    with zipfile.ZipFile(archive) as source:
        if source.read("revision").decode() != revision(checkout):
            raise ValueError("Build artifact does not match the checkout revision")
        roots = {"checkout": checkout, "repository": repository}
        for name in source.namelist():
            if name == "revision":
                continue
            prefix, relative = name.split("/", 1)
            root = roots[prefix].resolve()
            destination = (root / relative).resolve()
            destination.relative_to(root)
            destination.parent.mkdir(parents=True, exist_ok=True)
            with source.open(name) as content, destination.open("wb") as output:
                shutil.copyfileobj(content, output)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("pack", "restore"))
    parser.add_argument("checkout", type=Path)
    parser.add_argument("repository", type=Path)
    parser.add_argument("archive", type=Path)
    parser.add_argument("--install-log", type=Path)
    args = parser.parse_args()
    if args.mode == "pack":
        if args.install_log is None:
            parser.error("pack requires --install-log")
        pack(args.checkout, args.repository, args.archive, args.install_log)
    else:
        restore(args.checkout, args.repository, args.archive)
