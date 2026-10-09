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

"""Share compiled Maven reactors without changing test profiles or the test lifecycle."""

import argparse
import json
import os
from pathlib import Path
import platform
import re
import shutil
import subprocess
import sys
import xml.etree.ElementTree as ET
import zipfile

NS = {"m": "http://maven.apache.org/POM/4.0.0"}
CONFIGURATIONS = {
    "spark34": "spark-3.4,java-17,backends-velox,delta,hudi,paimon,spark-ut",
    "spark35": "spark-3.5,java-17,backends-velox,iceberg,delta,hudi,paimon,spark-ut",
    "spark35-scala213": "spark-3.5,scala-2.13,java-17,backends-velox,iceberg,delta,spark-ut",
    "spark40": "spark-4.0,scala-2.13,java-17,backends-velox,delta,paimon,spark-ut",
    "spark40-hive": "spark-4.0,scala-2.13,java-17,backends-velox,delta,spark-ut",
    "spark41": "spark-4.1,scala-2.13,java-17,backends-velox,spark-ut,delta",
    "spark41-slow": "spark-4.1,scala-2.13,java-17,backends-velox,spark-ut",
    "enhanced-spark35-slow": "spark-3.5,java-17,backends-velox,iceberg,delta,hudi,paimon,spark-ut",
    "enhanced-spark35": "spark-3.5,java-17,backends-velox,iceberg,delta,hudi",
    "enhanced-spark40": "spark-4.0,scala-2.13,java-17,backends-velox,delta,spark-ut",
}
TEST_PROPERTIES = {
    "argLine",
    "test",
    "suites",
    "wildcardSuites",
    "tagsToInclude",
    "tagsToExclude",
    "maven.test.failure.ignore",
    "suffixes",
    "surefire.excludesFile",
}

# The union of the former three ScalaTest groups. Keep the same suite prefixes;
# combining them lets Surefire and shared fixtures run once per configuration.
SUITES = (
    "org.apache.spark.sql.streaming,org.apache.spark.GlutenSortShuffleSuite,org.apache.gluten,"
    "org.apache.spark.sql.execution,org.apache.spark.sql.catalyst,org.apache.spark.sql.errors,"
    "org.apache.spark.sql.extension,org.apache.spark.sql.GlutenSQL,org.apache.spark.sql.Gluten,"
    "org.apache.spark.sql.connector,org.apache.spark.sql.sources,org.apache.spark.sql.hive,"
    "org.apache.spark.sql.gluten,org.apache.spark.sql.shim"
)


def selections(configuration):
    excluded = [
        "org.apache.spark.tags.ExtendedSQLTest",
        "org.apache.spark.tags.SlowHiveTest",
        "org.apache.gluten.tags.UDFTest",
        "org.apache.gluten.tags.CudfTest",
        "org.apache.gluten.tags.SkipTest",
    ]
    if not configuration.startswith("enhanced-"):
        excluded.append("org.apache.gluten.tags.EnhancedFeaturesTest")
    result = []
    if configuration not in ("spark40-hive", "spark41-slow", "enhanced-spark35-slow"):
        result.append(
            (
                "standard",
                ["-DtagsToExclude=" + ",".join(excluded), "-DwildcardSuites=" + SUITES],
            )
        )
        if configuration == "enhanced-spark35":
            # This existing job discovers the complete reactor, without a suite-prefix filter.
            result[-1][1].pop()
    tags = []
    if configuration in (
        "spark34",
        "spark35",
        "spark40",
        "spark41-slow",
        "enhanced-spark35-slow",
    ):
        tags.append("org.apache.spark.tags.ExtendedSQLTest")
    if configuration in (
        "spark34",
        "spark35",
        "spark40-hive",
        "spark41-slow",
        "enhanced-spark35-slow",
    ):
        tags.append("org.apache.spark.tags.SlowHiveTest")
    if tags:
        result.append(("slow", ["-DtagsToInclude=" + ",".join(tags)]))
    return result


def partition(classes, count, timings):
    """Assign every discovered class, including new classes without timing history."""
    if count < 1:
        raise ValueError("Shard count must be positive")
    groups, loads = [[] for _ in range(count)], [0.0] * count
    # Keep inner classes with their enclosing class. Timings only affect balance;
    # they never decide which tests exist or whether to execute a class.
    for name in sorted(set(classes), key=lambda n: (-timings.get(n, 1.0), n)):
        index = min(range(count), key=lambda i: (loads[i], i))
        groups[index].append(name)
        loads[index] += timings.get(name, 1.0)
    return [sorted(group) for group in groups]


def shard_filters(archive, configuration, shard, shards, reports):
    if not 0 <= shard < shards:
        raise ValueError("Invalid JVM shard")
    with zipfile.ZipFile(archive) as source:
        manifest = json.loads(source.read("manifest.json"))
    classes = manifest.get("classes")
    if not classes:
        raise ValueError("Shared build contains no discovery class names")
    hints = Path(__file__).with_name("jvm-test-timings.json")
    timings = json.loads(hints.read_text()).get(configuration, {})
    groups = partition(classes, shards, timings)
    selected = groups[shard]
    if not selected:
        raise ValueError("Empty JVM shard")
    reports.mkdir(parents=True, exist_ok=True)
    (reports / "shard.json").write_text(
        json.dumps({"index": shard, "count": shards, "classes": selected}, indent=2)
    )
    excludes = reports / "junit-excludes.txt"
    excluded = sorted(set(classes) - set(selected))
    excludes.write_text(
        "**/*$*\n" + "".join(name.replace(".", "/") + ".class\n" for name in excluded)
    )
    # ScalaTest performs its usual suite discovery and applies this additional
    # regex. Helpers/abstract classes remain available on the full classpath.
    pattern = "(?:" + "|".join(re.escape(name) for name in selected) + r")(?:\$.*)?"
    return ["-Dsuffixes=" + pattern, "-Dsurefire.excludesFile=" + str(excludes)]


def run_selections(
    checkout, archive, configuration, arguments, reports, shard=0, shards=1
):
    failed = False
    assignment = (
        shard_filters(archive, configuration, shard, shards, reports)
        if shards > 1
        else []
    )
    for index, (name, filters) in enumerate(selections(configuration)):
        restore(checkout, archive, configuration, arguments)
        # Surefire is independent of ScalaTest tags. Run each JUnit test once,
        # rather than repeating it for standard/extended/Hive selections.
        junit = ["-Dtest=__no_repeated_junit_tests__"] if index else []
        status = subprocess.call(
            [
                str(checkout / "build/mvn"),
                "-ntp",
                "test",
                "-Pci-reuse-test-build",
                *arguments,
                *filters,
                *assignment,
                *junit,
            ],
            env=os.environ,
        )
        # Save each selection before the next restoration clears target/. Keep
        # the relative module paths so equal report names cannot overwrite.
        for pattern in (
            "**/surefire-reports/TEST-*.xml",
            "**/target/*.log",
            "**/hs_err_*.log",
            "**/core.*",
        ):
            for path in checkout.glob(pattern):
                if path.is_file() and reports not in path.parents:
                    destination = reports / name / path.relative_to(checkout)
                    destination.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(path, destination)
        failed |= status != 0
        if status < 0:
            break
    return int(failed)


def signature(arguments):
    profiles, properties = set(), []
    for argument in arguments:
        if argument.startswith("-P"):
            profiles.update(argument[2:].split(","))
        elif argument.startswith("-D"):
            if argument[2:].split("=", 1)[0] not in TEST_PROPERTIES:
                properties.append(argument)
        else:
            raise ValueError("Unexpected Maven build argument: " + argument)
    return {"profiles": sorted(profiles), "properties": sorted(properties)}


def identity(checkout, configuration):
    return {
        "revision": subprocess.check_output(
            # Container checkouts can belong to the runner's UID. Trust this path
            # for this command without changing global Git configuration.
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
        ).strip(),
        "configuration": configuration,
        "architecture": platform.machine(),
    }


def pack(checkout, archive, effective_pom, configuration, arguments):
    root = ET.parse(effective_pom).getroot()
    projects = (
        root.findall("m:project", NS) if root.tag.endswith("projects") else [root]
    )
    targets, class_roots = [], []
    for project in projects:
        target = Path(project.findtext("m:build/m:directory", namespaces=NS))
        relative = target.relative_to(checkout)
        if relative.name != "target":
            raise ValueError("Unexpected build directory: " + str(relative))
        targets.append(relative.as_posix())
        for field in ("outputDirectory", "testOutputDirectory"):
            value = project.findtext("m:build/m:" + field, namespaces=NS)
            if value:
                directory = Path(value)
                directory.relative_to(checkout)
                class_roots.append(directory)
    if not any(
        path.is_file()
        for target in targets
        for path in (checkout / target).rglob("*.class")
    ):
        raise ValueError("No compiled classes in the reactor")
    classes = sorted(
        {
            path.relative_to(directory).as_posix()[:-6].replace("/", ".").split("$")[0]
            for directory in class_roots
            for path in directory.rglob("*.class")
            if path.name != "module-info.class"
        }
    )
    manifest = dict(
        identity(checkout, configuration),
        build=signature(arguments),
        targets=targets,
        classes=classes,
    )
    # Artifact upload handles compression once; local restores remain cheap.
    with zipfile.ZipFile(archive, "w", zipfile.ZIP_STORED) as output:
        output.writestr("manifest.json", json.dumps(manifest))
        for target in targets:
            for path in sorted((checkout / target).rglob("*")):
                if path.is_file() and not any(
                    part in {"analysis", "surefire-reports", "surefire"}
                    for part in path.parts
                ):
                    output.write(path, path.relative_to(checkout).as_posix())


def restore(checkout, archive, configuration, arguments):
    with zipfile.ZipFile(archive) as source:
        manifest = json.loads(source.read("manifest.json"))
        expected = identity(checkout, configuration)
        if any(manifest.get(key) != value for key, value in expected.items()):
            raise ValueError(
                "Shared build revision/configuration/architecture mismatch"
            )
        if manifest["build"] != signature(arguments):
            raise ValueError(
                "Test profiles or compiler properties differ from the shared build"
            )
        targets = []
        for name in manifest["targets"]:
            target = (checkout / name).resolve()
            target.relative_to(checkout)
            if target.name != "target":
                raise ValueError("Unexpected build directory: " + name)
            targets.append(target)
        entries = []
        for entry in source.infolist():
            if entry.filename == "manifest.json":
                continue
            relative = Path(entry.filename)
            if relative.is_absolute() or ".." in relative.parts:
                raise ValueError("Archive entry outside the build directories")
            # A previous test may have replaced a fixture with a symlink. The target
            # directories are removed before extraction, so do not follow old fixtures.
            destination = checkout / relative
            if not any(target in destination.parents for target in targets):
                raise ValueError("Archive entry outside the build directories")
            entries.append((entry, destination))
        # Match clean-test isolation: retain no reports, generated test data or mutated
        # fixtures from a previous suite, but restore the verified compiled classes.
        for target in targets:
            if target.exists():
                shutil.rmtree(target)
        for entry, destination in entries:
            destination.parent.mkdir(parents=True, exist_ok=True)
            with source.open(entry) as content, destination.open("wb") as output:
                shutil.copyfileobj(content, output)
            destination.chmod((entry.external_attr >> 16) & 0o777)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("arguments", "pack", "test", "run"))
    parser.add_argument("--archive", type=Path)
    parser.add_argument("--configuration", required=True, choices=CONFIGURATIONS)
    parser.add_argument("--effective-pom", type=Path)
    parser.add_argument("--reports", type=Path)
    parser.add_argument("--shard", type=int, default=0)
    parser.add_argument("--shards", type=int, default=1)
    args, arguments = parser.parse_known_args()
    arguments = arguments[1:] if arguments[:1] == ["--"] else arguments
    if args.mode == "arguments":
        print("-P" + CONFIGURATIONS[args.configuration])
        sys.exit(0)
    if args.archive is None:
        parser.error("pack/test require --archive")
    checkout = Path.cwd().resolve()
    if args.mode == "pack":
        if args.effective_pom is None:
            parser.error("pack requires --effective-pom")
        pack(checkout, args.archive, args.effective_pom, args.configuration, arguments)
    elif args.mode == "run":
        if args.reports is None:
            parser.error("run requires --reports")
        sys.exit(
            run_selections(
                checkout,
                args.archive,
                args.configuration,
                arguments,
                args.reports.resolve(),
                args.shard,
                args.shards,
            )
        )
    else:
        restore(checkout, args.archive, args.configuration, arguments)
        sys.exit(
            subprocess.call(
                [
                    str(checkout / "build/mvn"),
                    "-ntp",
                    "test",
                    "-Pci-reuse-test-build",
                    *arguments,
                ],
                env=os.environ,
            )
        )
