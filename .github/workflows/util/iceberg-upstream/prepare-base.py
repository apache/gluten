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

"""Overlay only the upstream-test harness on a checkout of the PR's base commit."""

import copy
from pathlib import Path
import shutil
import sys
import xml.etree.ElementTree as ET

NS = "http://maven.apache.org/POM/4.0.0"
ET.register_namespace("", NS)
ET.register_namespace("xsi", "http://www.w3.org/2001/XMLSchema-instance")


def child(parent, tag):
    found = parent.find(f"{{{NS}}}{tag}")
    return found if found is not None else ET.SubElement(parent, f"{{{NS}}}{tag}")


def identified(parent, tag, key, value):
    found = parent.find(f"{{{NS}}}{tag}[{{{NS}}}{key}='{value}']")
    if found is None:
        found = ET.SubElement(parent, f"{{{NS}}}{tag}")
        ET.SubElement(found, f"{{{NS}}}{key}").text = value
    return found


def prepare(harness, base):
    if harness.resolve() == base.resolve():
        raise ValueError("The harness and base checkout must be different directories")
    source = ET.parse(harness / "backends-velox/pom.xml")
    destination = ET.parse(base / "backends-velox/pom.xml")
    source_profiles = source.getroot().find(f"{{{NS}}}profiles")
    profiles = child(destination.getroot(), "profiles")
    profile = source_profiles.find(
        f"{{{NS}}}profile[{{{NS}}}id='iceberg-upstream-test']"
    )
    if profile is None:
        raise ValueError("Missing upstream-test harness profile")
    old = profiles.find(f"{{{NS}}}profile[{{{NS}}}id='iceberg-upstream-test']")
    if old is not None:
        profiles.remove(old)
    profiles.append(copy.deepcopy(profile))

    for base_profile in profiles.findall(f"{{{NS}}}profile"):
        if base_profile.findtext(f"{{{NS}}}id") == "iceberg-upstream-test":
            continue
        for executions in base_profile.findall(
            f"{{{NS}}}build/{{{NS}}}plugins/{{{NS}}}plugin"
            f"[{{{NS}}}artifactId='maven-surefire-plugin']/{{{NS}}}executions"
        ):
            for old in executions.findall(
                f"{{{NS}}}execution[{{{NS}}}id='iceberg-upstream']"
            ):
                executions.remove(old)

    # Spark-specific test runtime overrides are part of the harness. Preserve every other
    # base dependency, build option and execution, including production Spark profiles.
    for profile in source_profiles.findall(f"{{{NS}}}profile"):
        identifier = profile.findtext(f"{{{NS}}}id", "")
        if identifier == "iceberg-upstream-test":
            continue
        path = (
            f"{{{NS}}}build/{{{NS}}}plugins/{{{NS}}}plugin"
            f"[{{{NS}}}artifactId='maven-surefire-plugin']/{{{NS}}}executions/"
            f"{{{NS}}}execution[{{{NS}}}id='iceberg-upstream']"
        )
        execution = profile.find(path)
        if execution is None:
            continue
        target = identified(profiles, "profile", "id", identifier)
        plugins = child(child(target, "build"), "plugins")
        plugin = identified(plugins, "plugin", "artifactId", "maven-surefire-plugin")
        executions = child(plugin, "executions")
        old = executions.find(f"{{{NS}}}execution[{{{NS}}}id='iceberg-upstream']")
        if old is not None:
            executions.remove(old)
        executions.append(copy.deepcopy(execution))
    destination.write(
        base / "backends-velox/pom.xml", encoding="utf-8", xml_declaration=True
    )
    for directory in (
        "backends-velox/src-iceberg-upstream",
        "gluten-ut/src-iceberg-upstream",
    ):
        if (base / directory).exists():
            shutil.rmtree(base / directory)
        shutil.copytree(
            harness / directory,
            base / directory,
            ignore=shutil.ignore_patterns("__pycache__"),
        )


if __name__ == "__main__":
    prepare(Path(sys.argv[1]), Path(sys.argv[2]))
