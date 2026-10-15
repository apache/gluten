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

"""Reuse saved Iceberg results only after verifying that their tested tree was merged."""

import argparse
from datetime import datetime, timezone
import hashlib
import io
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
from urllib.parse import urlencode, urlparse
from urllib.request import HTTPRedirectHandler, Request, build_opener, urlopen
import zipfile
import xml.etree.ElementTree as ET

from compare import index
from summarize import write_reports

NATIVE_IMAGE = "apache/gluten:vcpkg-almalinux-8-gcc13"
TEST_IMAGE = "apache/gluten:centos-9-jdk17"
HARNESS_PATHS = (
    ".github/workflows/iceberg_spark_ut.yml",
    ".github/workflows/iceberg_upstream_revision.yml",
    ".github/workflows/velox_native_build.yml",
    ".github/workflows/util/iceberg-upstream",
    "backends-velox/pom.xml",
    "backends-velox/src-iceberg-upstream",
    "gluten-ut/src-iceberg-upstream",
)


def overlay_harness(checkout, harness):
    """Run a production checkout with the comparison's observer and test profile."""
    if checkout.resolve() == harness.resolve():
        raise ValueError("The harness must be a separate checkout")
    namespace = "http://maven.apache.org/POM/4.0.0"
    tag = lambda name: "{" + namespace + "}" + name
    pom = "backends-velox/pom.xml"
    parser = ET.XMLParser(target=ET.TreeBuilder(insert_comments=True))
    target = ET.parse(checkout / pom, parser=parser)
    source = ET.parse(harness / pom)
    profile = next(
        item
        for item in source.findall(tag("profiles") + "/" + tag("profile"))
        if item.findtext(tag("id")) == "iceberg-upstream-test"
    )
    profiles = target.find(tag("profiles"))
    for item in list(profiles):
        if item.findtext(tag("id")) == "iceberg-upstream-test":
            profiles.remove(item)
    profiles.append(profile)
    # Do not copy the head POM: all production dependencies, plugins and other
    # profiles must continue to describe the base revision being tested.
    ET.register_namespace("", namespace)
    target.write(checkout / pom, encoding="utf-8", xml_declaration=True)
    for name in HARNESS_PATHS:
        if name == pom:
            continue
        src, dst = harness / name, checkout / name
        if dst.is_dir():
            shutil.rmtree(dst)
        elif dst.exists():
            dst.unlink()
        if src.is_dir():
            shutil.copytree(src, dst, ignore=shutil.ignore_patterns("__pycache__"))
        elif src.is_file():
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, dst)


def pin_image(image):
    repository, tag = image.split(":", 1)
    request = (
        "https://auth.docker.io/token?service=registry.docker.io&scope=repository:"
        + repository
        + ":pull"
    )
    with urlopen(request, timeout=20) as response:
        token = json.load(response)["token"]
    request = Request(
        "https://registry-1.docker.io/v2/" + repository + "/manifests/" + tag,
        headers={
            "Authorization": "Bearer " + token,
            "Accept": "application/vnd.oci.image.index.v1+json,application/vnd.docker.distribution.manifest.list.v2+json,application/vnd.docker.distribution.manifest.v2+json",
        },
    )
    with urlopen(request, timeout=20) as response:
        digest = response.headers.get("Docker-Content-Digest", "")
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", digest):
        raise ValueError("Missing container image digest for " + image)
    return image + "@" + digest


def harness_hash(checkout):
    digest = hashlib.sha256()
    for name in HARNESS_PATHS:
        root = checkout / name
        paths = sorted(root.rglob("*")) if root.is_dir() else [root]
        for path in paths:
            if path.is_file() and "__pycache__" not in path.parts:
                digest.update(path.relative_to(checkout).as_posix().encode() + b"\0")
                digest.update(path.read_bytes())
    return digest.hexdigest()


def artifact_key(tree, harness, native_image, test_image):
    runtime = hashlib.sha256(
        (harness + "\0" + native_image + "\0" + test_image).encode()
    ).hexdigest()
    return "iceberg-baseline-v2-" + tree + "-" + runtime


def git_tree(revision):
    return subprocess.check_output(
        ["git", "rev-parse", "--verify", revision + "^{tree}"], text=True
    ).strip()


def initial_baseline(revision):
    """Only introduction of the harness can precede any merged baseline result."""
    tree = git_tree(revision)
    existing = subprocess.check_output(
        [
            "git",
            "ls-tree",
            "-r",
            "--name-only",
            tree,
            "--",
            ".github/workflows/iceberg_spark_ut.yml",
            ".github/workflows/iceberg_upstream_revision.yml",
            ".github/workflows/util/iceberg-upstream",
            "backends-velox/src-iceberg-upstream",
        ],
        text=True,
    )
    return not existing.strip()


def record(directory, key, revision):
    # Called only after all shards succeeded and their complete union was merged.
    index(json.loads((directory / "execution-coverage.json").read_text()))
    tree = git_tree(revision)
    if not key.startswith("iceberg-baseline-v2-" + tree + "-"):
        raise ValueError("Baseline key does not identify the tested tree")
    event = json.loads(Path(os.environ["GITHUB_EVENT_PATH"]).read_text())
    identity = {
        "key": key,
        "revision": revision,
        "tree": tree,
        "repository": os.environ["GITHUB_REPOSITORY"],
        "run_id": int(os.environ["GITHUB_RUN_ID"]),
        "run_attempt": int(os.environ["GITHUB_RUN_ATTEMPT"]),
        "pull_request": event.get("pull_request", {}).get("number"),
        "recorded_at": datetime.now(timezone.utc).isoformat(),
    }
    (directory / "baseline-identity.json").write_text(json.dumps(identity, indent=2))


class ArtifactRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        result = super().redirect_request(req, fp, code, msg, headers, newurl)
        if urlparse(req.full_url).netloc != urlparse(newurl).netloc:
            result.remove_header("Authorization")
        return result


class GitHub:
    def __init__(self, repository):
        self.repository = repository
        self.opener = build_opener(ArtifactRedirect())

    def request(self, path, binary=False):
        request = Request(
            os.environ.get("GITHUB_API_URL", "https://api.github.com")
            + "/repos/"
            + self.repository
            + "/"
            + path,
            headers={
                "Authorization": "Bearer " + os.environ["GH_TOKEN"],
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
            },
        )
        with self.opener.open(request, timeout=15) as response:
            data = response.read(128 * 1024 * 1024 + 1)
        if len(data) > 128 * 1024 * 1024:
            raise ValueError("Baseline response exceeds 128 MiB")
        return data if binary else json.loads(data)

    def tree(self, revision):
        if not re.fullmatch(r"[0-9a-f]{40}", revision):
            raise ValueError("Invalid baseline commit")
        return self.request("git/commits/" + revision)


def read_artifact(data):
    # Read two JSON documents, never extract or execute downloaded files.
    with zipfile.ZipFile(io.BytesIO(data)) as archive:
        names = {"baseline-identity.json", "execution-coverage.json"}
        if set(archive.namelist()) != names or len(archive.infolist()) != 2:
            raise ValueError("Unexpected baseline artifact contents")
        if sum(item.file_size for item in archive.infolist()) > 512 * 1024 * 1024:
            raise ValueError("Baseline expands beyond 512 MiB")
        return tuple(
            json.loads(archive.read(name))
            for name in ("baseline-identity.json", "execution-coverage.json")
        )


def verify_source(api, artifact, identity, key, tree, branch):
    run_id = artifact["workflow_run"]["id"]
    attempt = identity.get("run_attempt")
    if (
        identity.get("key") != key
        or identity.get("tree") != tree
        or identity.get("repository") != api.repository
        or identity.get("run_id") != run_id
        or type(attempt) is not int
        or attempt < 1
    ):
        raise ValueError("Baseline identity does not match this comparison")
    run = api.request(f"actions/runs/{run_id}/attempts/{attempt}")
    if (
        run["repository"]["full_name"] != api.repository
        or run["path"] != ".github/workflows/iceberg_spark_ut.yml"
        or run["status"] != "completed"
        or run["conclusion"] not in ("success", "failure")
    ):
        raise ValueError("Baseline is not from a completed Iceberg workflow")
    tested = api.tree(identity["revision"])
    if tested["tree"]["sha"] != tree:
        raise ValueError("Baseline tested tree differs from the base tree")
    if run["event"] == "pull_request":
        number = identity.get("pull_request")
        if type(number) is not int or number < 1:
            raise ValueError("Baseline has no source pull request")
        pr = api.request(f"pulls/{number}")
        if (
            not pr["merged"]
            or pr["base"]["repo"]["full_name"] != api.repository
            or pr["base"]["ref"] != branch
            or pr["head"]["sha"] != run["head_sha"]
            or (pr["head"].get("repo") or {}).get("full_name")
            != (run.get("head_repository") or {}).get("full_name")
            or len(tested["parents"]) != 2
            or tested["parents"][1]["sha"] != run["head_sha"]
        ):
            raise ValueError("Baseline was not tested at the merged PR's final head")
        if api.tree(pr["merge_commit_sha"])["tree"]["sha"] != tree:
            raise ValueError("PR merged different code than the saved test run")
    elif (
        run["event"] not in ("schedule", "workflow_dispatch")
        or run["head_branch"] != branch
        or run["head_repository"]["full_name"] != api.repository
        or run["head_sha"] != identity["revision"]
    ):
        raise ValueError("Baseline is not a merged PR or a target-branch run")


def restore(directory, key, revision, branch, api):
    tree = api.tree(revision)["tree"]["sha"]
    if not key.startswith("iceberg-baseline-v2-" + tree + "-"):
        raise ValueError("Baseline key does not identify the base tree")
    errors = []
    page = 1
    while True:
        query = urlencode({"name": key, "per_page": 100, "page": page})
        artifacts = api.request("actions/artifacts?" + query)["artifacts"]
        for artifact in sorted(artifacts, key=lambda item: item["id"], reverse=True):
            if artifact["expired"] or artifact["name"] != key:
                continue
            try:
                identity, report = read_artifact(
                    api.request(f"actions/artifacts/{artifact['id']}/zip", binary=True)
                )
                verify_source(api, artifact, identity, key, tree, branch)
                index(report)
            except (ValueError, KeyError, OSError, zipfile.BadZipFile) as error:
                errors.append(f"Artifact {artifact['id']}: {error}")
                continue
            directory.mkdir(parents=True, exist_ok=True)
            identity["compared_revision"] = revision
            identity["source_run"] = "{}/{}/actions/runs/{}".format(
                os.environ.get("GITHUB_SERVER_URL", "https://github.com"),
                api.repository,
                identity["run_id"],
            )
            (directory / "baseline-identity.json").write_text(
                json.dumps(identity, indent=2)
            )
            write_reports(directory, report["tests"], label="Saved Iceberg baseline")
            print("Reusing verified baseline from " + identity["source_run"])
            return
        if len(artifacts) < 100:
            break
        page += 1
    raise ValueError(
        "No compatible, verified baseline for "
        + revision
        + (
            ": " + "; ".join(errors)
            if errors
            else "; the saved report may be missing or expired"
        )
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("prepare", "record", "restore", "overlay"))
    parser.add_argument("--revision")
    parser.add_argument("--directory", type=Path)
    parser.add_argument("--harness", type=Path)
    parser.add_argument("--key")
    parser.add_argument("--branch")
    args = parser.parse_args()
    if args.mode == "overlay":
        if args.directory is None or args.harness is None:
            parser.error("overlay requires --directory and --harness")
        overlay_harness(args.directory, args.harness)
        raise SystemExit(0)
    if args.revision is None:
        parser.error("prepare/record/restore require --revision")
    if args.mode == "prepare":
        checkout = Path.cwd()
        harness = harness_hash(checkout)
        native, test = pin_image(NATIVE_IMAGE), pin_image(TEST_IMAGE)
        print("base_sha=" + args.revision)
        print("initial_baseline=" + str(initial_baseline(args.revision)).lower())
        print("native_image=" + native)
        print("test_image=" + test)
        print(
            "baseline_key="
            + artifact_key(git_tree(args.revision), harness, native, test)
        )
        print("head_key=" + artifact_key(git_tree("HEAD"), harness, native, test))
    else:
        if args.directory is None or args.key is None:
            parser.error("record/restore require --directory and --key")
        if args.mode == "record":
            record(args.directory, args.key, args.revision)
        else:
            if not args.branch:
                parser.error("restore requires --branch")
            try:
                restore(
                    args.directory,
                    args.key,
                    args.revision,
                    args.branch,
                    GitHub(os.environ["GITHUB_REPOSITORY"]),
                )
            except (ValueError, KeyError, OSError) as error:
                args.directory.mkdir(parents=True, exist_ok=True)
                (args.directory / "baseline-unavailable.txt").write_text(
                    str(error) + "\n"
                )
                raise SystemExit(str(error))
