#!/usr/bin/env python3
#
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

"""Split Apache Iceberg's published Spark test classes into CI shards.

The Iceberg Spark UT pipeline does not clone apache/iceberg: it runs the test
classes straight out of the `-tests.jar` artifacts Iceberg publishes for its
`spark` and `spark-extensions` modules, which Gluten's `-Piceberg` profile
already declares as `test-jar` dependencies (surefire is pointed at them with
`-DdependenciesToScan`). This script enumerates those classes from the jars in
the local Maven repository and prints ONE shard's worth as a surefire `-Dtest`
value.

Selection: entries named `Test*.class` (Iceberg's convention for a test class)
that are not inner classes. Abstract bases match that pattern too (e.g.
`TestBase`); they are harmless -- the JUnit Platform discovers no tests in them
-- so they are kept rather than decoded out of the class files.

Output form: FULLY QUALIFIED class names. Surefire accepts them in `-Dtest` (and
applies the filter to `-DdependenciesToScan` classes, verified on the 3.5.6
plugin this repo pins), so one pattern selects exactly one class. That matters
because Iceberg has a couple of same-named classes in different packages (e.g.
`TestTimestampWithoutZone` exists in both the spark and spark-extensions
modules): a simple-name pattern would run every one of them, so two shards could
each run the whole family and report the same tests twice.

Assignment is round-robin over the alphabetically sorted list (index %
num_shards) rather than contiguous blocks: Iceberg's slowest suites cluster by
name (TestCopyOnWrite*, TestMergeOnRead*, ...), and interleaving spreads those
families across shards instead of stacking them into one.
"""

import argparse
import os
import sys
import zipfile


def eprint(*args):
    print(*args, file=sys.stderr)


def jar_path(m2_repo, artifact, version):
    """Path of an artifact's published test jar inside the local m2 repository."""
    return os.path.join(
        m2_repo,
        "org",
        "apache",
        "iceberg",
        artifact,
        version,
        "{}-{}-tests.jar".format(artifact, version),
    )


def test_classes(jar):
    """Fully qualified names of the `Test*` classes in one test jar."""
    found = []
    with zipfile.ZipFile(jar) as zf:
        for entry in zf.namelist():
            if not entry.endswith(".class"):
                continue
            simple = entry.rsplit("/", 1)[-1][: -len(".class")]
            # `$` filters inner/anonymous classes, whose tests are discovered
            # through their outer class.
            if "$" in simple or not simple.startswith("Test"):
                continue
            found.append(entry[: -len(".class")].replace("/", "."))
    return found


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spark-version", required=True, help="e.g. 3.5")
    parser.add_argument("--scala-version", required=True, help="e.g. 2.12")
    parser.add_argument("--iceberg-version", required=True, help="e.g. 1.10.0")
    parser.add_argument(
        "--m2-repo",
        default=os.path.join(os.path.expanduser("~"), ".m2", "repository"),
        help="Local Maven repository holding the Iceberg test jars.",
    )
    parser.add_argument("--num-shards", type=int, required=True)
    parser.add_argument("--shard-id", type=int, required=True)
    parser.add_argument(
        "--out",
        help="Write this shard's class names here (one per line) -- the same "
        "list that is printed comma-separated on stdout. The caller uses the "
        "line count to sanity-check how many suites actually reported.",
    )
    args = parser.parse_args(argv)

    if args.num_shards < 1:
        parser.error("--num-shards must be >= 1")
    if not 0 <= args.shard_id < args.num_shards:
        parser.error("--shard-id must be in [0, --num-shards)")

    suffix = "{}_{}".format(args.spark_version, args.scala_version)
    artifacts = [
        "iceberg-spark-{}".format(suffix),
        "iceberg-spark-extensions-{}".format(suffix),
    ]

    all_classes = []
    for artifact in artifacts:
        jar = jar_path(args.m2_repo, artifact, args.iceberg_version)
        if not os.path.exists(jar):
            # A missing jar means the dependency was not resolved (wrong
            # version, or the build step did not run), which would silently
            # shrink the suite to whatever the other jar holds. Fail loudly.
            eprint("ERROR: Iceberg test jar not found: {}".format(jar))
            return 1
        names = test_classes(jar)
        if not names:
            eprint("ERROR: no Test* classes found in {}".format(jar))
            return 1
        eprint("{}: {} test classes".format(os.path.basename(jar), len(names)))
        all_classes.extend(names)

    # Sort for a stable assignment: the same (num_shards, shard_id) must always
    # select the same classes, whatever order the jars were read in.
    all_classes = sorted(set(all_classes))
    shard_classes = [
        fqcn
        for index, fqcn in enumerate(all_classes)
        if index % args.num_shards == args.shard_id
    ]

    eprint(
        "shard {}/{}: {} of {} test classes".format(
            args.shard_id, args.num_shards, len(shard_classes), len(all_classes)
        )
    )

    if args.out:
        with open(args.out, "w", encoding="utf-8") as fh:
            for fqcn in shard_classes:
                fh.write(fqcn + "\n")

    # The surefire `-Dtest` value.
    print(",".join(shard_classes))
    return 0


if __name__ == "__main__":
    sys.exit(main())
