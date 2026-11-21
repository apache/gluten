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

"""Replays recorded ScalaTest forks across parallel JVMs.

The Spark UT jobs run `mvn test` with -Dscalatest.jvm=record-fork.sh, so every
module's ScalaTest fork is saved (working directory, environment and arguments,
which already include the module's classpath, tags, -DwildcardSuites and
argLine) instead of being run serially inside Maven. This script discovers the
suites each fork would run, splits them into chunks and runs the chunks in
--jobs concurrent JVMs. Chunks of one module reuse its recorded command with the
suite selection replaced by -s, so the result matches the serial Maven run.

Many suites depend on JVM-wide state left by the suites before them, e.g. Gluten's
once-per-JVM static initialization or a leftover default SparkSession. So a chunk
is a contiguous slice of the order the serial run uses, and the span of suites
matching a --sticky-pattern, plus the suite before it, is never split. Suites
matching a --colocate-pattern write the same fixed paths, so they share a chunk.

Every JVM keeps the module basedir as working directory, which some suites
resolve paths against, and gets its own java.io.tmpdir, derby.system.home and
gluten.test.dir so concurrent suites do not share scratch space. Chunk logs go
to <module>/target/parallel-ut-<chunk>.log; JUnit XML reports go where Maven
would have written them.
"""

import argparse
import os
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor

RUNNER = "org.scalatest.tools.Runner"
DISCOVER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "Discover.java")
# Runner options that take a value. Reporter options (-f, -h, -u, -C) may carry
# glued configuration letters, e.g. -fW <file>.
VALUE_OPTS = set("-R -l -n -s -w -m -z -t -q -j -b -M -A -F -T -y -i".split())
REPORTER_OPTS = ("-f", "-h", "-u", "-C")
FLAG_OPTS = ("-o", "-e", "-P")
SELECTION_OPTS = {"-s", "-w", "-m"}
# Selections the replay cannot split; such forks run unchanged in one JVM.
UNSPLITTABLE_OPTS = {"-z", "-t", "-i", "-j", "-b", "-A"}
TESTS_LINE = re.compile(
    r"Tests: succeeded (\d+), failed (\d+), canceled (\d+), "
    r"ignored (\d+), pending (\d+)"
)
FAILURE_LINE = re.compile(r"\*\*\* (FAILED|ABORTED|RUN ABORTED) \*\*\*")

print_lock = threading.Lock()


def log(message):
    with print_lock:
        print(message, flush=True)


class Fork(object):
    """One recorded scalatest-maven-plugin fork, i.e. one Maven module."""

    def __init__(self, path):
        with open(os.path.join(path, "cwd")) as f:
            self.cwd = f.read().strip()
        with open(os.path.join(path, "env"), "rb") as f:
            entries = [e.decode() for e in f.read().split(b"\0") if e]
        self.env = dict(e.split("=", 1) for e in entries if "=" in e)
        with open(os.path.join(path, "args"), "rb") as f:
            args = [a.decode() for a in f.read().split(b"\0")[:-1]]
        split = args.index(RUNNER)
        self.jvm_args = args[:split]
        self.options = parse_options(args[split + 1 :])
        self.module = os.path.relpath(self.cwd, os.getcwd())

    def values(self, name):
        return [value for opt, value in self.options if opt == name]

    def splittable(self):
        return not any(opt in UNSPLITTABLE_OPTS for opt, _ in self.options)

    def java(self):
        java_home = self.env.get("JAVA_HOME")
        return os.path.join(java_home, "bin", "java") if java_home else "java"


def parse_options(args):
    options = []
    i = 0
    while i < len(args):
        opt = args[i]
        if opt in VALUE_OPTS or opt.startswith(REPORTER_OPTS):
            if i + 1 >= len(args):
                raise ValueError("Missing value for ScalaTest option " + opt)
            options.append((opt, args[i + 1]))
            i += 2
        elif opt.startswith(FLAG_OPTS):
            options.append((opt, None))
            i += 1
        else:
            raise ValueError("Unsupported ScalaTest option " + opt)
    return options


def runpath(fork):
    entries = []
    for value in fork.values("-R"):
        entries += [
            e.replace("\\ ", " ") for e in re.split(r"(?<!\\) +", value.strip()) if e
        ]
    return entries


def discover(fork):
    """Returns [(suite, weight)] for the suites the recorded fork would run, in the
    order the serial run uses."""
    paths = runpath(fork)
    out = subprocess.run(
        [fork.java(), DISCOVER] + paths,
        cwd=fork.cwd,
        env=fork.env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
    )
    if out.returncode != 0:
        raise RuntimeError(
            "Suite discovery failed for {}:\n{}".format(fork.module, out.stderr)
        )
    explicit = fork.values("-s")
    wildcards = fork.values("-w")
    members = fork.values("-m")
    include = set(t for v in fork.values("-n") for t in v.split())
    exclude = set(t for v in fork.values("-l") for t in v.split())
    discovered = []
    for line in out.stdout.splitlines():
        name, _, annotations = line.partition("\t")
        discovered.append((name, set(annotations.split(",")) if annotations else set()))
    # ScalaTest's Runner runs -s suites first, then one DiscoverySuite per -w and per
    # -m in argument order, each in discovery order (DiscoverySuite.wildcardList /
    # narrowList). A suite matched twice runs once here.
    if explicit or wildcards or members:
        ordered = list(explicit)
        for w in wildcards:
            ordered += [name for name, _ in discovered if name.startswith(w)]
        for m in members:
            ordered += [
                name
                for name, _ in discovered
                if name.startswith(m + ".") and "." not in name[len(m) + 1 :]
            ]
    else:
        ordered = [name for name, _ in discovered]
    tags_of = dict(discovered)
    suites, seen = [], set()
    for name in ordered:
        if name in seen:
            continue
        seen.add(name)
        weight = class_size(paths, name)
        # Class-level tags decide most suites up front. Those that run nothing still
        # start, so keep a small weight rather than dropping them.
        tags = tags_of.get(name, set())
        if (include and not tags & include) or tags & exclude:
            weight = max(1, weight // 50)
        suites.append((name, weight))
    return suites


def class_size(paths, suite):
    """Bytecode size of the suite and its nested classes, a proxy for its runtime."""
    relative = suite.replace(".", "/")
    size = 0
    for path in paths:
        directory = os.path.join(path, os.path.dirname(relative))
        if not os.path.isdir(directory):
            continue
        base = os.path.basename(relative)
        for f in os.listdir(directory):
            if f == base + ".class" or (
                f.startswith(base + "$") and f.endswith(".class")
            ):
                size += os.path.getsize(os.path.join(directory, f))
    return max(size, 1)


class Chunk(object):
    def __init__(self, fork, suites):
        self.fork = fork
        self.suites = [s for s, _ in suites]
        self.weight = sum(w for _, w in suites)
        self.id = None
        self.returncode = None
        self.seconds = 0
        self.log = None


def make_chunks(fork, suites, target, sticky_patterns, colocate_patterns):
    """Cuts the serially ordered suites into contiguous chunks of about `target`
    weight.

    For each sticky pattern, the span from the suite before its first match to its
    last match stays in one chunk, so those suites see the same predecessors as in
    the serial run. Suites matching one colocate pattern share fixed paths but not
    state: they move next to the first of them, so they run one after another.
    """
    if not fork.splittable():
        return [Chunk(fork, suites)]
    for pattern in colocate_patterns:
        members = [s for s in suites if pattern.search(s[0])]
        if len(members) > 1:
            first = suites.index(members[0])
            rest = [s for s in suites if s not in members]
            suites = rest[:first] + members + rest[first:]
    # no_cut[i]: suite i must stay in the same chunk as suite i - 1.
    no_cut = [False] * len(suites)
    for pattern in sticky_patterns + colocate_patterns:
        hits = [i for i, s in enumerate(suites) if pattern.search(s[0])]
        if hits:
            for i in range(hits[0], hits[-1] + 1):
                no_cut[i] = True
    chunks, current = [], []
    for i, suite in enumerate(suites):
        if current and not no_cut[i] and sum(w for _, w in current) >= target:
            chunks.append(Chunk(fork, current))
            current = []
        current.append(suite)
    if current:
        chunks.append(Chunk(fork, current))
    return chunks


def command(chunk, work, heap, jvm_opts):
    fork = chunk.fork
    jvm_args = list(fork.jvm_args)
    if heap and not any(a.startswith("-Xmx") for a in jvm_args):
        jvm_args.append("-Xmx" + heap)
    jvm_args += jvm_opts.split()
    jvm_args += [
        "-Djava.io.tmpdir=" + os.path.join(work, "tmp"),
        "-Dderby.system.home=" + os.path.join(work, "derby"),
        "-Dgluten.test.dir=" + os.path.join(work, "gluten"),
    ]
    runner_args = []
    for opt, value in fork.options:
        if fork.splittable() and opt in SELECTION_OPTS:
            continue
        if opt.startswith("-f") and value:
            # File reporters would overwrite each other; give every chunk its own file.
            root, ext = os.path.splitext(value)
            value = "{}-{}{}".format(root, chunk.id, ext)
        runner_args.append(opt)
        if value is not None:
            runner_args.append(value)
    if fork.splittable():
        for suite in chunk.suites:
            runner_args += ["-s", suite]
    return [fork.java()] + jvm_args + [RUNNER] + runner_args


def run_chunk(chunk, args, total):
    fork = chunk.fork
    work = tempfile.mkdtemp(prefix="chunk-{}-".format(chunk.id), dir=args.work_dir)
    for sub in ("tmp", "derby", "gluten"):
        os.makedirs(os.path.join(work, sub))
    target = os.path.join(fork.cwd, "target")
    os.makedirs(target, exist_ok=True)
    chunk.log = os.path.join(target, "parallel-ut-{}.log".format(chunk.id))
    cmd = command(chunk, work, args.heap, args.jvm_opts)
    start = time.time()
    with open(chunk.log, "w") as out:
        out.write("# " + " ".join(cmd) + "\n")
        out.flush()
        try:
            chunk.returncode = subprocess.run(
                cmd,
                cwd=fork.cwd,
                env=fork.env,
                stdout=out,
                stderr=subprocess.STDOUT,
                timeout=args.timeout or None,
            ).returncode
        except subprocess.TimeoutExpired:
            out.write(
                "\n*** RUN ABORTED *** chunk timed out after {}s\n".format(args.timeout)
            )
            chunk.returncode = -1
    chunk.seconds = time.time() - start
    with open(chunk.log, errors="replace") as f:
        text = f.read()
    chunk.passed = (
        chunk.returncode == 0
        and "Run completed in" in text
        and "*** RUN ABORTED ***" not in text
    )
    # Keep a failed chunk's scratch directory: some suites write the actual output
    # they compared (e.g. plan stability's *.actual.*.txt) to java.io.tmpdir.
    if chunk.passed:
        shutil.rmtree(work, ignore_errors=True)
    else:
        text += "\nScratch directory kept for inspection: {}\n".format(work)
    with print_lock:
        print(
            "::group::[{}] chunk {}/{} {} ({} suites, {:.0f}s)".format(
                "PASS" if chunk.passed else "FAIL",
                chunk.id,
                total,
                fork.module,
                len(chunk.suites),
                chunk.seconds,
            )
        )
        print(text)
        print("::endgroup::", flush=True)
    chunk.counts = [
        sum(int(m.group(i)) for m in TESTS_LINE.finditer(text)) for i in range(1, 6)
    ]
    chunk.failures = [
        line.strip() for line in text.splitlines() if FAILURE_LINE.search(line)
    ]


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--forks-dir",
        default=os.environ.get("GLUTEN_UT_FORK_DIR"),
        help="Directory record-fork.sh wrote to (default: $GLUTEN_UT_FORK_DIR)",
    )
    parser.add_argument("--jobs", type=int, default=3, help="Concurrent test JVMs")
    parser.add_argument(
        "--chunks-per-job",
        type=int,
        default=6,
        help="Chunks per JVM slot; more balances better, fewer starts fewer JVMs",
    )
    parser.add_argument(
        "--heap",
        default="3g",
        help="-Xmx for each test JVM unless the fork already sets one",
    )
    parser.add_argument(
        "--jvm-opts",
        # Fewer GC threads per JVM, since the JVMs share few cores. Do not add
        # -XX:TieredStopAtLevel=1: C1-only code made suites several times slower and
        # its larger frames overflowed the stack in Spark's variant tests.
        default="-XX:ParallelGCThreads=2 -XX:ConcGCThreads=1",
        help="Extra options for each test JVM",
    )
    parser.add_argument(
        "--sticky-pattern",
        action="append",
        help="Regex of suites that must run after the same suites as in the serial "
        "run (repeatable). Default: Hive suites, which share the TestHive session, "
        "its added jars and a warehouse; and VeloxTransitionSuite, which initializes "
        "Gluten from a mock executor context and breaks a later real driver start "
        "unless a Gluten session already ran in the JVM",
    )
    parser.add_argument(
        "--colocate-pattern",
        action="append",
        help="Regex of suites that write the same fixed paths and so must not run "
        "concurrently (repeatable). Default: the Velox TPC-H Iceberg suites, which "
        "share test-classes/tpch-data-iceberg-velox",
    )
    parser.add_argument(
        "--timeout", type=int, default=0, help="Per-chunk timeout in seconds"
    )
    parser.add_argument(
        "--work-dir", default=None, help="Parent of per-JVM scratch directories"
    )
    args = parser.parse_args()
    if not args.forks_dir or not os.path.isdir(args.forks_dir):
        sys.exit(
            "No recorded forks in {!r}; was Maven run with -Dscalatest.jvm=record-fork.sh?".format(
                args.forks_dir
            )
        )
    args.work_dir = args.work_dir or tempfile.mkdtemp(prefix="gluten-parallel-ut-")
    os.makedirs(args.work_dir, exist_ok=True)

    forks = sorted(
        (Fork(os.path.join(args.forks_dir, d)) for d in os.listdir(args.forks_dir)),
        key=lambda fork: fork.cwd,
    )
    start = time.time()
    with ThreadPoolExecutor(max_workers=args.jobs) as pool:
        discovered = list(pool.map(discover, forks))
    total_weight = sum(w for suites in discovered for _, w in suites)
    target = max(1, total_weight // max(1, args.jobs * args.chunks_per_job))
    sticky_patterns = [
        re.compile(p)
        for p in args.sticky_pattern
        or [r"^org\.apache\.spark\.sql\.hive\.", r"\.VeloxTransitionSuite$"]
    ]
    colocate_patterns = [
        re.compile(p)
        for p in args.colocate_pattern
        or [r"\.Velox(PartitionedTable)?TPCHIcebergSuite$"]
    ]
    chunks = []
    for fork, suites in zip(forks, discovered):
        log("{}: {} suites".format(fork.module, len(suites)))
        if suites:
            chunks += make_chunks(
                fork, suites, target, sticky_patterns, colocate_patterns
            )
    # Heaviest first, so the long chunks do not end up as the tail.
    chunks.sort(key=lambda chunk: -chunk.weight)
    for i, chunk in enumerate(chunks, 1):
        chunk.id = i
    log(
        "Discovered {} suites in {} modules in {:.0f}s; running {} chunks in {} JVMs".format(
            sum(len(c.suites) for c in chunks),
            len(forks),
            time.time() - start,
            len(chunks),
            args.jobs,
        )
    )
    with ThreadPoolExecutor(max_workers=args.jobs) as pool:
        for future in [
            pool.submit(run_chunk, chunk, args, len(chunks)) for chunk in chunks
        ]:
            future.result()

    failed = [c for c in chunks if not c.passed]
    counts = [sum(c.counts[i] for c in chunks) for i in range(5)]
    print("\nParallel ScalaTest summary ({:.0f}s):".format(time.time() - start))
    for chunk in sorted(chunks, key=lambda c: c.id):
        print(
            "  [{}] chunk {:3d} {:6.0f}s {:4d} suites  {}  {}".format(
                "PASS" if chunk.passed else "FAIL",
                chunk.id,
                chunk.seconds,
                len(chunk.suites),
                chunk.fork.module,
                os.path.relpath(chunk.log),
            )
        )
    print(
        "Tests: succeeded {}, failed {}, canceled {}, ignored {}, pending {}".format(
            *counts
        )
    )
    if failed:
        print("\n{} of {} chunks failed:".format(len(failed), len(chunks)))
        for chunk in failed:
            print(
                "  chunk {} ({}, exit code {}), log: {}".format(
                    chunk.id,
                    chunk.fork.module,
                    chunk.returncode,
                    os.path.relpath(chunk.log),
                )
            )
            for line in chunk.failures or ["(no failed test reported; see the log)"]:
                print("    " + line)
        sys.exit(1)
    print("All chunks passed.")


if __name__ == "__main__":
    main()
