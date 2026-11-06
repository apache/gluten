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

"""Identify and build the amd64 static Velox image; no third-party modules required."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shlex
import shutil
import subprocess
import sys
import time

VARIANT = "static-almalinux-8-gcc13-amd64"
CI_SCRIPT = "dev/ci-velox-buildstatic-centos-8.sh"
INPUTS = (
    "ep/build-velox/src",
    "dev/vcpkg",
    "dev/builddeps-veloxbe.sh",
    "dev/build-helper-functions.sh",
    CI_SCRIPT,
    "dev/ci-velox-key.py",
    "dev/docker/Dockerfile.almalinux8-gcc13-static-build",
    "dev/docker/install-ci-tools.sh",
    "dev/docker/Dockerfile.velox-static-almalinux8-gcc13",
)
BUILD_ENV = {"CPU_TARGET": "avx", "NUM_THREADS": "2", "VELOX_BUILD_SHARED": "OFF"}
# CI can't delete Docker Hub tags, so images rotate through a fixed set of slots.
IMAGE_REPOSITORY = "apache/gluten"
SLOT_COUNT = 8
KEY_LABEL = "org.apache.gluten.velox-key"
SEQUENCE_LABEL = "org.apache.gluten.velox-sequence"


def git(root, *args):
    return subprocess.check_output(
        ["git", "-c", f"safe.directory={root}", "-C", str(root), *args]
    ).decode("utf-8")


def build_flags(root):
    script = (root / CI_SCRIPT).read_text(encoding="utf-8")
    command = re.search(
        r"^\./dev/builddeps-veloxbe\.sh (.+)$",
        script.replace("\\\n", " "),
        re.MULTILINE,
    )
    if command is None:
        raise ValueError("Cannot find the static CI build command")
    flags = shlex.split(command.group(1))
    if "--enable_vcpkg=ON" not in flags or not all(
        re.fullmatch(r"--[a-z0-9_]+=[A-Za-z0-9_.-]+", flag) for flag in flags
    ):
        raise ValueError(
            "Static CI build flags must be literal --option=value arguments"
        )
    return flags


def velox_reference(root):
    source = (root / "ep/build-velox/src/get-velox.sh").read_text(encoding="utf-8")
    values = {}
    for name in ("VELOX_REPO", "VELOX_BRANCH"):
        match = re.search(r"^" + name + r"=(.+)$", source, re.MULTILINE)
        words = shlex.split(match.group(1)) if match else []
        if len(words) != 1 or re.search(r"[$`\s]", words[0]):
            raise ValueError(f"{name} must be a literal repository or ref")
        values[name] = words[0]
    return values["VELOX_REPO"], values["VELOX_BRANCH"]


def resolve_revision(repository, ref):
    if re.fullmatch(r"[0-9a-f]{40}", ref):
        return ref
    refs = [ref, f"refs/heads/{ref}", f"refs/tags/{ref}", f"refs/tags/{ref}^{{}}"]
    output = subprocess.check_output(
        ["git", "ls-remote", repository, *refs], timeout=60
    ).decode()
    matches = dict(line.split()[::-1] for line in output.splitlines())
    # Annotated tags identify the peeled commit, not the tag object.
    for name in list(matches):
        if name.endswith("^{}"):
            matches[name[:-3]] = matches.pop(name)
    commits = set(matches.values())
    if len(commits) != 1:
        raise ValueError(f"Velox ref {ref!r} did not resolve to one commit")
    revision = commits.pop()
    if not re.fullmatch(r"[0-9a-f]{40}", revision):
        raise ValueError("Invalid resolved Velox commit")
    return revision


def input_files(root):
    records = git(root, "ls-files", "--stage", "-z", "--", *INPUTS).split("\0")
    files = {}
    for record in filter(None, records):
        entry, name = record.split("\t", 1)
        mode, _, stage = entry.split()
        if stage != "0" or mode not in ("100644", "100755"):
            raise ValueError(f"Unmerged or unsupported input: {name}")
        content = (root / name).read_bytes()
        files[name] = {"mode": mode, "sha256": hashlib.sha256(content).hexdigest()}
    for name in INPUTS:
        if not any(path == name or path.startswith(name + "/") for path in files):
            raise ValueError(f"No tracked image inputs at {name}")
    return dict(sorted(files.items()))


def metadata(root, base_image, revision=None):
    if not re.fullmatch(r"apache/gluten@sha256:[0-9a-f]{64}", base_image):
        raise ValueError("Pin the apache/gluten base image by digest")
    repository, branch = velox_reference(root)
    revision = revision or resolve_revision(repository, branch)
    if not re.fullmatch(r"[0-9a-f]{40}", revision):
        raise ValueError("Velox commit must be a full lowercase SHA-1")
    result = {
        "version": 1,
        "variant": VARIANT,
        "base_image": base_image,
        "velox_repo": repository,
        "velox_branch": branch,
        "velox_commit": revision,
        "flags": build_flags(root),
        "environment": BUILD_ENV,
        "inputs": input_files(root),
    }
    result["key"] = hashlib.sha256(
        json.dumps(result, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return result


def native_command(root, info, *steps):
    return [
        "bash",
        str(root / "dev/builddeps-veloxbe.sh"),
        *info["flags"],
        "--velox_home=/opt/velox",
        "--velox_repo=" + info["velox_repo"],
        "--velox_branch=" + info["velox_commit"],
        *steps,
    ]


def build(root, info, expected_key):
    if info["key"] != expected_key:
        raise ValueError("Image inputs do not match the requested cache key")
    if platform.system() != "Linux" or platform.machine() != "x86_64":
        raise ValueError("This image variant supports Linux amd64 only")
    # Preserve these absolute paths: Velox's CMake exports refer to vcpkg here.
    destination = Path("/opt/gluten")
    for name in info["inputs"]:
        target = destination / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(root / name, target)
    velox = Path("/opt/velox")
    subprocess.run(["git", "init", str(velox)], check=True)
    subprocess.run(
        [
            "git",
            "-C",
            str(velox),
            "fetch",
            "--depth=1",
            info["velox_repo"],
            info["velox_commit"],
        ],
        check=True,
    )
    subprocess.run(
        ["git", "-C", str(velox), "checkout", "--detach", "FETCH_HEAD"], check=True
    )
    env = dict(os.environ, **BUILD_ENV, CCACHE_DISABLE="1", PROMPT_ALWAYS_RESPOND="y")
    subprocess.run(
        native_command(destination, info, "get_velox", "build_velox"),
        cwd=destination,
        env=env,
        check=True,
    )
    if git(velox, "rev-parse", "HEAD").strip() != info["velox_commit"]:
        raise ValueError("Built a different Velox revision than the key identifies")
    (velox / ".velox-key").write_text(info["key"] + "\n")
    (velox / ".velox-build.json").write_text(json.dumps(info, indent=2) + "\n")


def slot_image(slot):
    return f"{IMAGE_REPOSITORY}:velox-static-slot-{slot}"


def published_labels(slot):
    """Return the labels of a published slot, or None if the slot is empty."""
    result = subprocess.run(
        [
            "docker",
            "buildx",
            "imagetools",
            "inspect",
            slot_image(slot),
            "--format",
            "{{json .Image}}",
        ],
        capture_output=True,
        text=True,
        timeout=120,
    )
    if result.returncode != 0:
        if "not found" in result.stderr.lower():
            return None
        raise ValueError(f"Cannot inspect {slot_image(slot)}: {result.stderr.strip()}")
    image = json.loads(result.stdout) or {}
    # A manifest list is reported per platform; this image variant is amd64 only.
    if "config" not in image:
        image = image.get("linux/amd64") or {}
    return (image.get("config") or {}).get("Labels") or {}


def sequence(labels):
    try:
        return int((labels or {}).get(SEQUENCE_LABEL, -1))
    except ValueError:
        return -1


def choose_slot(key, published):
    """Find the slot holding key, or else the empty or least recently published one."""
    for slot, labels in enumerate(published):
        if labels and labels.get(KEY_LABEL) == key:
            return {"found": True, "slot": slot, "sequence": sequence(labels)}
    sequences = [sequence(labels) for labels in published]
    slot = min(range(len(published)), key=lambda index: sequences[index])
    return {"found": False, "slot": slot, "sequence": max(sequences) + 1}


def smoke(root):
    info = json.loads(Path("/opt/velox/.velox-build.json").read_text())
    if metadata(root, info["base_image"], info["velox_commit"])["key"] != info["key"]:
        raise ValueError("Smoke checkout does not match the image's build inputs")
    subprocess.run(
        ["git", "config", "--global", "--add", "safe.directory", str(root)],
        check=True,
    )
    start = time.monotonic()
    subprocess.run(
        native_command(root, info, "build_gluten_cpp"),
        cwd=root,
        env=dict(os.environ, **BUILD_ENV, PROMPT_ALWAYS_RESPOND="y"),
        check=True,
    )
    for name in ("libvelox.so", "libgluten.so"):
        if not (root / "cpp/build/releases" / name).is_file():
            raise ValueError(f"Missing Gluten-only build output: {name}")
    print(f"Gluten-only build: {time.monotonic() - start:.1f} seconds", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("key", "build", "smoke", "slot"))
    parser.add_argument(
        "--repository-root", type=Path, default=Path(__file__).resolve().parents[1]
    )
    parser.add_argument("--base-image")
    parser.add_argument("--velox-commit")
    parser.add_argument("--expected-key")
    parser.add_argument("--github-output", action="store_true")
    args = parser.parse_args()
    root = args.repository_root.resolve()
    if args.mode == "smoke":
        smoke(root)
        return
    if args.mode == "slot":
        if not re.fullmatch(r"[0-9a-f]{64}", args.expected_key or ""):
            raise ValueError("Pass the Velox key as --expected-key")
        published = [published_labels(slot) for slot in range(SLOT_COUNT)]
        result = choose_slot(args.expected_key, published)
        result["image"] = slot_image(result["slot"])
        for name, value in result.items():
            value = str(value).lower() if isinstance(value, bool) else value
            print(f"{name}={value}" if args.github_output else f"{name}: {value}")
        return
    info = metadata(root, args.base_image or "", args.velox_commit)
    if args.mode == "build":
        build(root, info, args.expected_key)
    elif args.github_output:
        for name in ("key", "base_image", "velox_commit"):
            print(f"{name}={info[name]}")
    else:
        print(json.dumps(info, indent=2))


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, subprocess.CalledProcessError) as error:
        print(str(error), file=sys.stderr)
        sys.exit(1)
